package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
)

type backendClient interface {
	CheckLocalBackupReadiness(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) (map[string]uint64, map[string]uint64, error)
	AcquireLocalBackupQuiesce(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) error
	ReleaseLocalBackupQuiesce(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) error
	AcquireLocalRaftBackupFreeze(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) (backend.BackupRaftFreeze, error)
	ReleaseLocalRaftBackupFreeze(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) error
	CreateLocalBackupArchive(ctx context.Context, addr string, in backend.CreateLocalBackupArchiveInput) (backend.CreateLocalBackupArchiveResult, error)
}

type ClusterBackupNode struct {
	PodName              string
	NodeID               string
	Ordinal              int
	RaftNodeID           uint64
	BackendAdvertiseAddr string
}

type TriggerClusterBackupInput struct {
	Reason        string
	OutputDir     string
	ArchiveFormat backupcore.ArchiveFormat
	ClusterID     string
	Nodes         []ClusterBackupNode
}

type ClusterBackupRunStatus struct {
	BackupSetID  string
	ClusterID    string
	Reason       string
	Phase        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  time.Time
	Expected     []ClusterBackupNode
	Barriers     map[string]uint64
	Nodes        []clusterbackup.NodeArtifact
	ManifestURI  string
	FailurePhase string
	Error        string
}

func (m *Module) EnableClusterBackupNetworking(localRaftNodeID uint64, raftNodeAddrs []string, backendAuthToken string) {
	m.clusterLocalRaftNode = consensus.NodeID(localRaftNodeID)
	m.clusterNodeAddrs = append([]string(nil), raftNodeAddrs...)
	m.clusterBackendClient = backend.Client{AuthToken: backendAuthToken}
}

func (m *Module) TriggerClusterBackup(ctx context.Context, input TriggerClusterBackupInput) (ClusterBackupRunStatus, error) {
	createdAt := time.Now().UTC()
	clusterID := firstNonEmptyString(input.ClusterID, m.localIdentity.ClusterID)
	backupSetID := newClusterBackupSetID(createdAt, clusterID)
	nodes := normalizeClusterBackupNodes(input.Nodes)
	format := input.ArchiveFormat
	if format == "" {
		format = m.Policy().ArchiveFormat
	}
	if !backupcore.IsSupportedArchiveFormat(format) {
		return ClusterBackupRunStatus{}, fmt.Errorf("unsupported archive_format %q", format)
	}
	input.ArchiveFormat = format
	if strings.TrimSpace(input.OutputDir) == "" || !filepath.IsAbs(input.OutputDir) {
		return ClusterBackupRunStatus{}, fmt.Errorf("output_dir must be an absolute path")
	}
	if err := validateLocalClusterBackupDestination(m.dataDir, input.OutputDir); err != nil {
		return ClusterBackupRunStatus{}, err
	}
	if len(nodes) == 0 {
		return ClusterBackupRunStatus{}, fmt.Errorf("expected nodes are required")
	}
	expected := make([]clusterBackupExpectedNode, 0, len(nodes))
	for _, node := range nodes {
		expected = append(expected, clusterBackupExpectedNode{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: node.RaftNodeID})
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: backupSetID, ClusterID: clusterID, Reason: input.Reason, CreatedAt: createdAt, Expected: expected}); err != nil {
		return ClusterBackupRunStatus{}, err
	}
	fail := func(phase clusterBackupPhase, err error) (ClusterBackupRunStatus, error) {
		_ = m.commitRaft(context.Background(), recordTypeClusterBackupFail, clusterBackupFailureRecord{BackupSetID: backupSetID, Phase: phase, Message: err.Error(), UpdatedAt: time.Now().UTC()})
		st, _ := m.ClusterBackupStatus(backupSetID)
		return st, err
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhasePrechecking, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhasePrechecking, err)
	}
	readiness, groups, err := m.collectCoordinatorReadiness(ctx, input, backupSetID, clusterID, createdAt, nodes)
	if err != nil {
		return fail(clusterBackupPhasePrechecking, err)
	}
	precheck := m.EvaluateClusterBackupPreconditions(buildCoordinatorPrecheck(backupSetID, clusterID, nodes, input.OutputDir, groups, readiness))
	if !precheck.OK {
		return fail(clusterBackupPhasePrechecking, precheck.Error())
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseQuiescing, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseQuiescing, err)
	}
	acquired := []ClusterBackupNode{}
	for _, node := range nodes {
		if err := retryClusterBackupPeer(ctx, func() error {
			return m.clusterPeerClient().AcquireLocalBackupQuiesce(ctx, node.BackendAdvertiseAddr, backendInput(input, backupSetID, clusterID, createdAt, node, nil))
		}); err != nil {
			m.releaseClusterQuiesce(context.Background(), input, backupSetID, clusterID, createdAt, acquired)
			return fail(clusterBackupPhaseQuiescing, fmt.Errorf("acquire quiesce on %s: %w", node.PodName, err))
		}
		acquired = append(acquired, node)
	}
	defer m.releaseClusterQuiesce(context.Background(), input, backupSetID, clusterID, createdAt, acquired)
	_, barrierGroups, err := m.collectCoordinatorReadiness(ctx, input, backupSetID, clusterID, createdAt, nodes)
	if err != nil {
		return fail(clusterBackupPhaseBarrierWait, err)
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseBarrierWait, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseBarrierWait, err)
	}
	barriers := clusterBackupBarriersFromPrecheckGroups(barrierGroups)
	if err := m.commitRaft(ctx, recordTypeClusterBackupBarrier, clusterBackupBarrierRecord{BackupSetID: backupSetID, Barriers: barriers, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseBarrierWait, err)
	}
	barrierList := make([]backend.BackupRaftBarrier, 0, len(barriers))
	for groupID, index := range barriers {
		barrierList = append(barrierList, backend.BackupRaftBarrier{GroupID: groupID, Index: index})
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseArchiving, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseArchiving, err)
	}
	frozen := []clusterBackupFrozenNode{}
	for _, node := range nodes {
		freezeIn := backendInput(input, backupSetID, clusterID, createdAt, node, barrierList)
		freezeIn.TTL = 10 * time.Minute
		var freeze backend.BackupRaftFreeze
		if err := retryClusterBackupPeer(ctx, func() error {
			var err error
			freeze, err = m.clusterPeerClient().AcquireLocalRaftBackupFreeze(ctx, node.BackendAdvertiseAddr, freezeIn)
			return err
		}); err != nil {
			m.releaseClusterRaftFreeze(context.Background(), input, backupSetID, clusterID, createdAt, frozen)
			return fail(clusterBackupPhaseArchiving, fmt.Errorf("acquire raft freeze on %s: %w", node.PodName, err))
		}
		frozen = append(frozen, clusterBackupFrozenNode{node: node, freeze: freeze})
	}
	results := make([]backend.CreateLocalBackupArchiveResult, 0, len(nodes))
	for _, frozenNode := range frozen {
		node := frozenNode.node
		archiveIn := backendInput(input, backupSetID, clusterID, createdAt, node, barrierList)
		archiveIn.FreezeLeaseID = frozenNode.freeze.LeaseID
		var result backend.CreateLocalBackupArchiveResult
		if err := retryClusterBackupPeer(ctx, func() error {
			var err error
			result, err = m.clusterPeerClient().CreateLocalBackupArchive(ctx, node.BackendAdvertiseAddr, archiveIn)
			return err
		}); err != nil {
			m.releaseClusterRaftFreeze(context.Background(), input, backupSetID, clusterID, createdAt, frozen)
			return fail(clusterBackupPhaseArchiving, fmt.Errorf("archive %s: %w", node.PodName, err))
		}
		results = append(results, result)
	}
	if err := m.releaseClusterRaftFreeze(context.Background(), input, backupSetID, clusterID, createdAt, frozen); err != nil {
		return fail(clusterBackupPhaseArchiving, err)
	}
	releasedAt := time.Now().UTC()
	for _, result := range results {
		if result.RaftFreeze.ReleasedAt.IsZero() {
			result.RaftFreeze.ReleasedAt = releasedAt
		}
		artifact := clusterbackup.NodeArtifact{PodName: result.PodName, NodeID: result.NodeID, Ordinal: result.Ordinal, RaftNodeID: result.RaftNodeID, ArchiveName: result.ArchiveName, ArchiveURI: result.ArchiveURI, ManifestName: result.ManifestName, ManifestURI: result.ManifestURI, SizeBytes: result.SizeBytes, ChecksumSHA256: result.ChecksumSHA256, AppliedIndexes: result.AppliedIndexes, RaftFreeze: clusterbackup.RaftFreezeEvidence{LeaseID: result.RaftFreeze.LeaseID, AcquiredAt: result.RaftFreeze.AcquiredAt, ReleasedAt: result.RaftFreeze.ReleasedAt, ExpiresAt: result.RaftFreeze.ExpiresAt, Groups: clusterFreezeGroupsFromBackend(result.RaftFreeze.Groups)}}
		if err := m.commitRaft(ctx, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: backupSetID, Node: artifact, UpdatedAt: time.Now().UTC()}); err != nil {
			return fail(clusterBackupPhaseArchiving, err)
		}
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseValidating, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseValidating, err)
	}
	manifest, err := m.buildClusterBackupManifest(backupSetID, clusterID, input.Reason, createdAt, format)
	if err != nil {
		return fail(clusterBackupPhaseValidating, err)
	}
	manifest.ManifestURI = fileURI(filepath.Join(input.OutputDir, "backup-set.json"))
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseCommittingManifest, UpdatedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseCommittingManifest, err)
	}
	if err := writeClusterBackupSetManifest(input.OutputDir, manifest); err != nil {
		return fail(clusterBackupPhaseCommittingManifest, err)
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupComplete, clusterBackupCompleteRecord{BackupSetID: backupSetID, Manifest: manifest, CompletedAt: time.Now().UTC()}); err != nil {
		return fail(clusterBackupPhaseCommittingManifest, err)
	}
	return m.ClusterBackupStatus(backupSetID)
}

func (m *Module) clusterPeerClient() backendClient {
	if m.clusterBackendClient != nil {
		return m.clusterBackendClient
	}
	return backend.Client{}
}

func retryClusterBackupPeer(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("%w (last peer error: %v)", err, last)
			}
			return err
		}
		if err := fn(); err != nil {
			last = err
		} else {
			return nil
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (m *Module) releaseClusterQuiesce(ctx context.Context, input TriggerClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []ClusterBackupNode) {
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		if err := m.clusterPeerClient().ReleaseLocalBackupQuiesce(ctx, node.BackendAdvertiseAddr, backendInput(input, backupSetID, clusterID, ts, node, nil)); err != nil && m.logger != nil {
			m.logger.Warn("failed to release cluster backup peer quiesce", "backup_set_id", backupSetID, "pod", node.PodName, "error", err)
		}
	}
}

type clusterBackupFrozenNode struct {
	node   ClusterBackupNode
	freeze backend.BackupRaftFreeze
}

func (m *Module) releaseClusterRaftFreeze(ctx context.Context, input TriggerClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []clusterBackupFrozenNode) error {
	var firstErr error
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i].node
		in := backendInput(input, backupSetID, clusterID, ts, node, nil)
		in.FreezeLeaseID = nodes[i].freeze.LeaseID
		if err := m.clusterPeerClient().ReleaseLocalRaftBackupFreeze(ctx, node.BackendAdvertiseAddr, in); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("release raft freeze on %s: %w", node.PodName, err)
			}
			if m.logger != nil {
				m.logger.Warn("failed to release cluster backup raft freeze", "backup_set_id", backupSetID, "pod", node.PodName, "error", err)
			}
		}
	}
	return firstErr
}

func clusterFreezeGroupsFromBackend(input map[string]backend.BackupRaftFreezeGroup) map[string]clusterbackup.RaftFreezeGroup {
	out := make(map[string]clusterbackup.RaftFreezeGroup, len(input))
	for key, group := range input {
		out[key] = clusterbackup.RaftFreezeGroup{GroupID: group.GroupID, BarrierIndex: group.BarrierIndex, AppliedIndex: group.AppliedIndex, CommitIndex: group.CommitIndex, Term: group.Term, LastIndex: group.LastIndex, SnapshotIndex: group.SnapshotIndex, Leader: group.Leader}
	}
	return out
}

func backendInput(input TriggerClusterBackupInput, backupSetID, clusterID string, ts time.Time, node ClusterBackupNode, barriers []backend.BackupRaftBarrier) backend.CreateLocalBackupArchiveInput {
	return backend.CreateLocalBackupArchiveInput{ClusterID: clusterID, RequesterNodeID: uint64(0), BackupSetID: backupSetID, Reason: input.Reason, PodName: node.PodName, NodeID: node.NodeID, RaftNodeID: node.RaftNodeID, Ordinal: node.Ordinal, OutputDir: input.OutputDir, ArchiveFormat: string(input.ArchiveFormat), UTCTimestamp: ts, Barriers: barriers}
}

func (m *Module) buildClusterBackupManifest(backupSetID, clusterID, reason string, createdAt time.Time, format backupcore.ArchiveFormat) (clusterbackup.Manifest, error) {
	_, runs := m.clusterBackupSnapshot()
	run, ok := runs[backupSetID]
	if !ok {
		return clusterbackup.Manifest{}, fmt.Errorf("cluster backup %s not found", backupSetID)
	}
	nodes := make([]clusterbackup.NodeArtifact, 0, len(run.NodeResults))
	for _, artifact := range run.NodeResults {
		nodes = append(nodes, artifact)
	}
	manifest := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: backupSetID, CreatedAt: createdAt, CompletedAt: time.Now().UTC(), ClusterID: clusterID, Complete: true, State: clusterbackup.StateSucceeded, Reason: reason, ExpectedNodes: len(run.Expected), DataDir: m.dataDir, ArchiveFormat: format, RaftBarriers: cloneStringUint64Map(run.Barriers), Nodes: nodes}
	if err := clusterbackup.Validate(manifest, clusterbackup.ValidationModeRestore); err != nil {
		return clusterbackup.Manifest{}, err
	}
	return manifest, nil
}

func writeClusterBackupSetManifest(outputDir string, manifest clusterbackup.Manifest) error {
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return err
	}
	raw, err := manifest.MarshalDeterministic()
	if err != nil {
		return err
	}
	path := filepath.Join(outputDir, "backup-set.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *Module) ClusterBackupStatus(backupSetID string) (ClusterBackupRunStatus, error) {
	active, runs := m.clusterBackupSnapshot()
	_ = active
	if strings.TrimSpace(backupSetID) == "" {
		backupSetID = active
	}
	run, ok := runs[backupSetID]
	if !ok {
		return ClusterBackupRunStatus{}, fmt.Errorf("cluster backup %s not found", backupSetID)
	}
	return clusterBackupStatusFromRun(run), nil
}

func (m *Module) ListClusterBackups() []ClusterBackupRunStatus {
	_, runs := m.clusterBackupSnapshot()
	out := make([]ClusterBackupRunStatus, 0, len(runs))
	for _, run := range runs {
		out = append(out, clusterBackupStatusFromRun(run))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

func clusterBackupStatusFromRun(run clusterBackupRun) ClusterBackupRunStatus {
	expected := make([]ClusterBackupNode, 0, len(run.Expected))
	for _, node := range run.Expected {
		expected = append(expected, ClusterBackupNode{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: node.RaftNodeID})
	}
	nodes := make([]clusterbackup.NodeArtifact, 0, len(run.NodeResults))
	for _, node := range run.NodeResults {
		nodes = append(nodes, node)
	}
	var completed time.Time
	manifestURI := ""
	if run.Manifest != nil {
		completed = run.Manifest.CompletedAt
		manifestURI = run.Manifest.ManifestURI
	}
	failurePhase, msg := "", ""
	if run.Failure != nil {
		failurePhase, msg = string(run.Failure.Phase), run.Failure.Message
	}
	return ClusterBackupRunStatus{BackupSetID: run.BackupSetID, ClusterID: run.ClusterID, Reason: run.Reason, Phase: string(run.Phase), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, CompletedAt: completed, Expected: expected, Barriers: cloneStringUint64Map(run.Barriers), Nodes: nodes, ManifestURI: manifestURI, FailurePhase: failurePhase, Error: msg}
}

func ValidateClusterBackupSet(ctx context.Context, path string) (clusterbackup.Manifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return clusterbackup.Manifest{}, fmt.Errorf("backup_set_path is required")
	}
	manifestPath := path
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		manifestPath = filepath.Join(path, "backup-set.json")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	manifest, err := clusterbackup.Parse(raw)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	if err := clusterbackup.ValidateArchiveFiles(ctx, manifest); err != nil {
		return clusterbackup.Manifest{}, err
	}
	return manifest, nil
}

func normalizeClusterBackupNodes(in []ClusterBackupNode) []ClusterBackupNode {
	out := make([]ClusterBackupNode, 0, len(in))
	for _, node := range in {
		node.PodName = strings.TrimSpace(node.PodName)
		node.NodeID = strings.TrimSpace(node.NodeID)
		node.BackendAdvertiseAddr = strings.TrimSpace(node.BackendAdvertiseAddr)
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
	return out
}

func newClusterBackupSetID(ts time.Time, clusterID string) string {
	cleanCluster := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_").Replace(firstNonEmptyString(clusterID, "cluster"))
	return "backup-set-" + ts.UTC().Format("20060102T150405Z") + "-" + cleanCluster
}

type coordinatorReadiness struct {
	applied map[string]uint64
	commits map[string]uint64
}

func (m *Module) collectCoordinatorReadiness(ctx context.Context, input TriggerClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []ClusterBackupNode) (map[string]coordinatorReadiness, []ClusterBackupPrecheckRaftGroup, error) {
	readiness := make(map[string]coordinatorReadiness, len(nodes))
	commitByGroup := map[string]uint64{}
	quorumLeaderByGroup := map[string]uint64{}
	appliedByGroup := map[string]map[uint64]uint64{}
	seenGroups := map[string]struct{}{}
	for _, node := range nodes {
		var applied map[string]uint64
		var commits map[string]uint64
		if err := retryClusterBackupPeer(ctx, func() error {
			var err error
			applied, commits, err = m.clusterPeerClient().CheckLocalBackupReadiness(ctx, node.BackendAdvertiseAddr, backendInput(input, backupSetID, clusterID, ts, node, nil))
			return err
		}); err != nil {
			return nil, nil, fmt.Errorf("precheck %s: %w", node.PodName, err)
		}
		if len(applied) == 0 {
			return nil, nil, fmt.Errorf("precheck %s returned no raft apply evidence", node.PodName)
		}
		readiness[node.PodName] = coordinatorReadiness{applied: applied, commits: commits}
		for groupID, commit := range commits {
			if commit > commitByGroup[groupID] {
				commitByGroup[groupID] = commit
				quorumLeaderByGroup[groupID] = node.RaftNodeID
			}
		}
		for groupID, appliedIndex := range applied {
			seenGroups[groupID] = struct{}{}
			if appliedByGroup[groupID] == nil {
				appliedByGroup[groupID] = map[uint64]uint64{}
			}
			appliedByGroup[groupID][node.RaftNodeID] = appliedIndex
		}
	}
	for groupID := range seenGroups {
		if commitByGroup[groupID] == 0 {
			return nil, nil, fmt.Errorf("raft group %s has no verified quorum read", groupID)
		}
	}
	byGroup := map[string]ClusterBackupPrecheckRaftGroup{}
	for _, group := range m.raftGroupsStatusForPrecheck() {
		byGroup[string(group.GroupID)] = group
	}
	groups := make([]ClusterBackupPrecheckRaftGroup, 0, len(commitByGroup))
	for groupID, commit := range commitByGroup {
		group := byGroup[groupID]
		group.GroupID = consensus.GroupID(groupID)
		group.Leader = consensus.NodeID(quorumLeaderByGroup[groupID])
		group.HasQuorum = true
		group.CommitIndex = commit
		group.AppliedByNode = appliedByGroup[groupID]
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	return readiness, groups, nil
}

func buildCoordinatorPrecheck(backupSetID, clusterID string, nodes []ClusterBackupNode, outputDir string, groups []ClusterBackupPrecheckRaftGroup, readiness map[string]coordinatorReadiness) ClusterBackupPrecheckInput {
	preNodes := make([]ClusterBackupPrecheckNode, 0, len(nodes))
	dests := make([]ClusterBackupDestinationCheck, 0, len(nodes))
	for _, node := range nodes {
		_, ok := readiness[node.PodName]
		preNodes = append(preNodes, ClusterBackupPrecheckNode{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: node.RaftNodeID, BackendAdvertiseAddr: node.BackendAdvertiseAddr, ClusterID: clusterID, Reachable: ok && node.BackendAdvertiseAddr != "", Ready: ok, Admitted: ok, CaughtUp: ok})
		dests = append(dests, ClusterBackupDestinationCheck{PodName: node.PodName, Path: outputDir, Writable: ok, OutsideDataDir: ok})
	}
	return ClusterBackupPrecheckInput{BackupSetID: backupSetID, ClusterID: clusterID, ExpectedNodeCount: len(nodes), Nodes: preNodes, RaftGroups: groups, Destinations: dests}
}

func clusterBackupBarriersFromPrecheckGroups(groups []ClusterBackupPrecheckRaftGroup) map[string]uint64 {
	out := make(map[string]uint64, len(groups))
	for _, group := range groups {
		out[string(group.GroupID)] = group.CommitIndex
	}
	return out
}

func (m *Module) raftGroupsStatusForPrecheck() []ClusterBackupPrecheckRaftGroup {
	if m.raftGroups == nil {
		return nil
	}
	out := []ClusterBackupPrecheckRaftGroup{}
	for _, st := range m.raftGroups.Status() {
		out = append(out, ClusterBackupPrecheckRaftGroup{GroupID: st.GroupID, Leader: st.Leader, HasQuorum: st.Leader != 0, CommitIndex: st.CommitIndex, AppliedByNode: map[uint64]uint64{uint64(st.NodeID): st.AppliedIndex}})
	}
	return out
}
