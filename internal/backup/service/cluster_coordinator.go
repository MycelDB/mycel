package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"

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

type StartClusterBackupInput struct {
	Reason             string
	OutputDir          string
	ArchiveFormat      backupcore.ArchiveFormat
	ClusterID          string
	ClusterName        string
	Namespace          string
	StatefulSet        string
	Image              string
	ImageDigest        string
	Nodes              []ClusterBackupNode
	IdempotencyKey     string
	ConvergenceTimeout time.Duration
	RepositoryID       string
	RepositoryPrefix   string
	CaptureProvider    string
	QuiesceReadProfile string
	Validation         backupcore.ValidationOptions
}

type ClusterBackupRunStatus struct {
	BackupSetID      string
	ClusterID        string
	Reason           string
	Phase            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      time.Time
	Expected         []ClusterBackupNode
	Barriers         map[string]uint64
	Nodes            []clusterbackup.NodeArtifact
	ManifestURI      string
	RepositoryID     string
	RepositoryPrefix string
	CaptureProvider  string
	FailurePhase     string
	Error            string
	Blockers         []ClusterBackupBlocker
	Cancel           bool
}

func (m *Module) EnableClusterBackupNetworking(localRaftNodeID uint64, raftNodeAddrs []string, backendAuthToken string) {
	m.clusterLocalRaftNode = consensus.NodeID(localRaftNodeID)
	m.clusterNodeAddrs = append([]string(nil), raftNodeAddrs...)
	m.clusterBackendClient = backend.Client{AuthToken: backendAuthToken}
}

func (m *Module) StartClusterBackup(ctx context.Context, input StartClusterBackupInput) (ClusterBackupRunStatus, error) {
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.IdempotencyKey != "" {
		if existing, ok := m.findClusterBackupByIdempotencyKey(input.IdempotencyKey); ok {
			return existing, nil
		}
	}
	createdAt := time.Now().UTC()
	clusterID := firstNonEmptyString(input.ClusterID, m.localIdentity.ClusterID)
	backupSetID := newClusterBackupSetID(createdAt, clusterID)
	nodes := normalizeClusterBackupNodes(input.Nodes)
	policy := m.Policy()
	format := input.ArchiveFormat
	if format == "" {
		format = policy.ArchiveFormat
	}
	if !backupcore.IsSupportedArchiveFormat(format) {
		return ClusterBackupRunStatus{}, fmt.Errorf("unsupported archive_format %q", format)
	}
	input.ArchiveFormat = format
	repositoryID := strings.TrimSpace(input.RepositoryID)
	if repositoryID == "" {
		repositoryID = strings.TrimSpace(policy.DefaultRepositoryID)
	}
	input.RepositoryID = repositoryID
	captureProvider := strings.TrimSpace(input.CaptureProvider)
	if captureProvider == "" || captureProvider == backupcore.DefaultProductionCaptureProvider {
		captureProvider = "filesystem_immutable_copy"
	}
	captureProvider = strings.ToLower(captureProvider)
	if captureProvider != "filesystem_immutable_copy" {
		return ClusterBackupRunStatus{}, fmt.Errorf("capture_provider %q is not implemented; use filesystem_immutable_copy", captureProvider)
	}
	input.CaptureProvider = captureProvider
	if strings.TrimSpace(input.QuiesceReadProfile) == "" {
		input.QuiesceReadProfile = policy.QuiesceReadProfile
	}
	if strings.TrimSpace(input.QuiesceReadProfile) == "" {
		input.QuiesceReadProfile = backupcore.DefaultProductionQuiesceReadProfile
	}
	if strings.TrimSpace(input.OutputDir) == "" {
		if repositoryID == "" {
			return ClusterBackupRunStatus{}, fmt.Errorf("output_dir must be an absolute path")
		}
		input.OutputDir = filepath.Join(policy.BackupDir, ".cluster-scratch", backupSetID)
	}
	if !filepath.IsAbs(input.OutputDir) {
		return ClusterBackupRunStatus{}, fmt.Errorf("output_dir must be an absolute path")
	}
	if err := validateLocalClusterBackupDestination(m.dataDir, input.OutputDir); err != nil {
		return ClusterBackupRunStatus{}, err
	}
	repositoryPrefix := ""
	if repositoryID != "" {
		repo, err := m.GetBackupRepository(repositoryID)
		if err != nil {
			return ClusterBackupRunStatus{}, err
		}
		if repo.Validation.State == ValidationStateInvalid || repo.Validation.State == ValidationStateUnsupported {
			return ClusterBackupRunStatus{}, fmt.Errorf("backup repository %s validation state is %s", repositoryID, repo.Validation.State)
		}
		repositoryPrefix = repositoryBackupSetPrefix(repo, backupSetID)
		input.RepositoryPrefix = repositoryPrefix
	}
	if len(nodes) == 0 {
		return ClusterBackupRunStatus{}, fmt.Errorf("expected nodes are required")
	}
	expected := make([]clusterBackupExpectedNode, 0, len(nodes))
	for _, node := range nodes {
		expected = append(expected, clusterBackupExpectedNode{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: node.RaftNodeID})
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: backupSetID, ClusterID: clusterID, Reason: input.Reason, IdempotencyKey: input.IdempotencyKey, RepositoryID: repositoryID, RepositoryPrefix: repositoryPrefix, CaptureProvider: input.CaptureProvider, CreatedAt: createdAt, Expected: expected}); err != nil {
		return ClusterBackupRunStatus{}, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	if m.clusterBackupCancels == nil {
		m.clusterBackupCancels = map[string]context.CancelFunc{}
	}
	m.clusterBackupCancels[backupSetID] = cancel
	m.mu.Unlock()
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.clusterBackupCancels, backupSetID)
			m.mu.Unlock()
		}()
		m.runClusterBackupOperation(runCtx, input, backupSetID, clusterID, createdAt, nodes)
	}()
	return m.ClusterBackupStatus(backupSetID)
}

func (m *Module) CancelClusterBackup(ctx context.Context, backupSetID, reason string) (ClusterBackupRunStatus, error) {
	backupSetID = strings.TrimSpace(backupSetID)
	if backupSetID == "" {
		active, _ := m.clusterBackupSnapshot()
		backupSetID = active
	}
	if backupSetID == "" {
		return ClusterBackupRunStatus{}, fmt.Errorf("backup_set_id is required")
	}
	m.mu.Lock()
	cancel := m.clusterBackupCancels[backupSetID]
	m.mu.Unlock()
	msg := firstNonEmptyString(reason, "cluster backup canceled")
	if cancel != nil {
		cancel()
		if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseCanceling, UpdatedAt: time.Now().UTC()}); err != nil {
			st, statusErr := m.ClusterBackupStatus(backupSetID)
			if statusErr == nil && (st.Phase == string(clusterBackupPhaseSucceeded) || st.Phase == string(clusterBackupPhaseFailed) || st.Phase == string(clusterBackupPhaseCanceled)) {
				return st, nil
			}
			return ClusterBackupRunStatus{}, err
		}
		return m.ClusterBackupStatus(backupSetID)
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupAbort, clusterBackupFailureRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseCanceling, Message: msg, UpdatedAt: time.Now().UTC()}); err != nil {
		st, statusErr := m.ClusterBackupStatus(backupSetID)
		if statusErr == nil && (st.Phase == string(clusterBackupPhaseSucceeded) || st.Phase == string(clusterBackupPhaseFailed) || st.Phase == string(clusterBackupPhaseCanceled)) {
			return st, nil
		}
		return ClusterBackupRunStatus{}, err
	}
	return m.ClusterBackupStatus(backupSetID)
}

func (m *Module) runClusterBackupOperation(ctx context.Context, input StartClusterBackupInput, backupSetID, clusterID string, createdAt time.Time, nodes []ClusterBackupNode) {
	failRecord := recordTypeClusterBackupFail
	repositoryCompletePublished := false
	fail := func(phase clusterBackupPhase, err error) {
		if ctx.Err() != nil {
			failRecord = recordTypeClusterBackupAbort
			err = firstNonNilError(err, ctx.Err())
			phase = clusterBackupPhaseCanceling
		}
		if strings.TrimSpace(input.RepositoryID) != "" && !repositoryCompletePublished {
			deleted, cleanupErr := m.cleanupIncompleteRepositoryBackupSet(context.Background(), input, backupSetID)
			if cleanupErr != nil {
				if m.logger != nil {
					m.logger.Warn("failed to clean incomplete repository backup set", "backup_set_id", backupSetID, "repository_id", input.RepositoryID, "prefix", input.RepositoryPrefix, "error", cleanupErr)
				}
				err = fmt.Errorf("%w; incomplete repository prefix cleanup failed: %v", err, cleanupErr)
			} else if deleted > 0 && m.logger != nil {
				m.logger.Info("cleaned incomplete repository backup set", "backup_set_id", backupSetID, "repository_id", input.RepositoryID, "prefix", input.RepositoryPrefix, "deleted_objects", deleted)
			}
		}
		_ = m.commitRaft(context.Background(), failRecord, clusterBackupFailureRecord{BackupSetID: backupSetID, Phase: phase, Message: err.Error(), UpdatedAt: time.Now().UTC()})
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhasePrechecking, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhasePrechecking, err)
		return
	}
	convergenceTimeout := input.ConvergenceTimeout
	if convergenceTimeout <= 0 {
		convergenceTimeout = 5 * time.Minute
	}
	deadline := time.Now().Add(convergenceTimeout)
	var readiness map[string]coordinatorReadiness
	var groups []ClusterBackupPrecheckRaftGroup
	for {
		var err error
		readiness, groups, err = m.collectCoordinatorReadiness(ctx, input, backupSetID, clusterID, createdAt, nodes)
		if err != nil {
			fail(clusterBackupPhasePrechecking, err)
			return
		}
		precheck := m.EvaluateClusterBackupPreconditions(buildCoordinatorPrecheck(backupSetID, clusterID, nodes, input.OutputDir, groups, readiness))
		if precheck.OK {
			_ = m.commitRaft(context.Background(), recordTypeClusterBackupBlockers, clusterBackupBlockersRecord{BackupSetID: backupSetID, UpdatedAt: time.Now().UTC()})
			break
		}
		blockers := clusterBackupBlockersFromPrecheck(groups, nodes)
		if len(blockers) == 0 {
			blockers = []ClusterBackupBlocker{{Reason: "precheck_failed", Detail: strings.Join(precheck.Failures, "; ")}}
		}
		_ = m.commitRaft(context.Background(), recordTypeClusterBackupBlockers, clusterBackupBlockersRecord{BackupSetID: backupSetID, Blockers: blockers, UpdatedAt: time.Now().UTC()})
		if time.Now().After(deadline) {
			fail(clusterBackupPhasePrechecking, precheck.Error())
			return
		}
		select {
		case <-ctx.Done():
			fail(clusterBackupPhasePrechecking, ctx.Err())
			return
		case <-time.After(2 * time.Second):
		}
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseReady, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseReady, err)
		return
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseQuiescing, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseQuiescing, err)
		return
	}
	acquired := []ClusterBackupNode{}
	for _, node := range nodes {
		if err := retryClusterBackupPeer(ctx, func() error {
			return m.clusterPeerClient().AcquireLocalBackupQuiesce(ctx, node.BackendAdvertiseAddr, backendInput(input, backupSetID, clusterID, createdAt, node, nil))
		}); err != nil {
			m.releaseClusterQuiesce(context.Background(), input, backupSetID, clusterID, createdAt, acquired)
			fail(clusterBackupPhaseQuiescing, fmt.Errorf("acquire quiesce on %s: %w", node.PodName, err))
			return
		}
		acquired = append(acquired, node)
	}
	defer m.releaseClusterQuiesce(context.Background(), input, backupSetID, clusterID, createdAt, acquired)
	_, barrierGroups, err := m.collectCoordinatorReadiness(ctx, input, backupSetID, clusterID, createdAt, nodes)
	if err != nil {
		fail(clusterBackupPhaseBarrierWait, err)
		return
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseBarrierWait, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseBarrierWait, err)
		return
	}
	barriers := clusterBackupBarriersFromPrecheckGroups(barrierGroups)
	if err := m.commitRaft(ctx, recordTypeClusterBackupBarrier, clusterBackupBarrierRecord{BackupSetID: backupSetID, Barriers: barriers, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseBarrierWait, err)
		return
	}
	barrierList := make([]backend.BackupRaftBarrier, 0, len(barriers))
	for groupID, index := range barriers {
		barrierList = append(barrierList, backend.BackupRaftBarrier{GroupID: groupID, Index: index})
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseArchiving, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseArchiving, err)
		return
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
			fail(clusterBackupPhaseArchiving, fmt.Errorf("acquire raft freeze on %s: %w", node.PodName, err))
			return
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
			fail(clusterBackupPhaseArchiving, fmt.Errorf("archive %s: %w", node.PodName, err))
			return
		}
		results = append(results, result)
	}
	if err := m.releaseClusterRaftFreeze(context.Background(), input, backupSetID, clusterID, createdAt, frozen); err != nil {
		fail(clusterBackupPhaseArchiving, err)
		return
	}
	releasedAt := time.Now().UTC()
	for _, result := range results {
		if result.RaftFreeze.ReleasedAt.IsZero() {
			result.RaftFreeze.ReleasedAt = releasedAt
		}
		artifact := clusterbackup.NodeArtifact{PodName: result.PodName, NodeID: result.NodeID, Ordinal: result.Ordinal, RaftNodeID: result.RaftNodeID, ArchiveName: result.ArchiveName, ArchiveURI: result.ArchiveURI, ManifestName: result.ManifestName, ManifestURI: result.ManifestURI, InventoryName: result.InventoryName, InventoryURI: result.InventoryURI, InventoryChecksumSHA256: result.InventoryChecksumSHA256, SizeBytes: result.SizeBytes, ChecksumSHA256: result.ChecksumSHA256, AppliedIndexes: result.AppliedIndexes, RaftFreeze: clusterbackup.RaftFreezeEvidence{LeaseID: result.RaftFreeze.LeaseID, AcquiredAt: result.RaftFreeze.AcquiredAt, ReleasedAt: result.RaftFreeze.ReleasedAt, ExpiresAt: result.RaftFreeze.ExpiresAt, Groups: clusterFreezeGroupsFromBackend(result.RaftFreeze.Groups)}}
		if err := m.commitRaft(ctx, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: backupSetID, Node: artifact, UpdatedAt: time.Now().UTC()}); err != nil {
			fail(clusterBackupPhaseArchiving, err)
			return
		}
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseValidating, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseValidating, err)
		return
	}
	manifest, err := m.buildClusterBackupManifest(backupSetID, clusterID, input.Reason, createdAt, input.ArchiveFormat)
	if err != nil {
		fail(clusterBackupPhaseValidating, err)
		return
	}
	completedAt := time.Now().UTC()
	if strings.TrimSpace(input.RepositoryID) != "" {
		if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseUploading, UpdatedAt: time.Now().UTC()}); err != nil {
			fail(clusterBackupPhaseUploading, err)
			return
		}
		repo, err := m.GetBackupRepository(input.RepositoryID)
		if err != nil {
			fail(clusterBackupPhaseUploading, err)
			return
		}
		manifest, err = m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, clusterID, createdAt, completedAt)
		if err != nil {
			fail(clusterBackupPhaseUploading, err)
			return
		}
		repositoryCompletePublished = true
	} else {
		manifest.ManifestURI = fileURI(filepath.Join(input.OutputDir, clusterbackup.BackupSetManifestName))
		if err := writeClusterBackupSetManifest(input.OutputDir, manifest); err != nil {
			fail(clusterBackupPhaseCommittingManifest, err)
			return
		}
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: backupSetID, Phase: clusterBackupPhaseCommittingManifest, UpdatedAt: time.Now().UTC()}); err != nil {
		fail(clusterBackupPhaseCommittingManifest, err)
		return
	}
	if err := m.commitRaft(ctx, recordTypeClusterBackupComplete, clusterBackupCompleteRecord{BackupSetID: backupSetID, Manifest: manifest, CompletedAt: completedAt}); err != nil {
		fail(clusterBackupPhaseCommittingManifest, err)
		return
	}
	if repositoryCompletePublished {
		m.applyRepositoryRetentionAfterBackup(backupSetID, input.RepositoryID)
	}
}

func (m *Module) applyRepositoryRetentionAfterBackup(backupSetID, repositoryID string) {
	retentionCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	plan, err := m.ApplyRepositoryRetention(retentionCtx, RepositoryRetentionPlanOptions{RepositoryID: repositoryID})
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("repository backup retention failed", "backup_set_id", backupSetID, "repository_id", repositoryID, "error", err)
		}
		return
	}
	if len(plan.Warnings) > 0 && m.logger != nil {
		m.logger.Warn("repository backup retention completed with warnings", "backup_set_id", backupSetID, "repository_id", repositoryID, "warnings", strings.Join(plan.Warnings, "; "), "deleted_objects", plan.DeletedObjects)
	} else if plan.DeletedObjects > 0 && m.logger != nil {
		m.logger.Info("repository backup retention applied", "backup_set_id", backupSetID, "repository_id", repositoryID, "deleted_objects", plan.DeletedObjects)
	}
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

func (m *Module) releaseClusterQuiesce(ctx context.Context, input StartClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []ClusterBackupNode) {
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

func (m *Module) releaseClusterRaftFreeze(ctx context.Context, input StartClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []clusterBackupFrozenNode) error {
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

func backendInput(input StartClusterBackupInput, backupSetID, clusterID string, ts time.Time, node ClusterBackupNode, barriers []backend.BackupRaftBarrier) backend.CreateLocalBackupArchiveInput {
	return backend.CreateLocalBackupArchiveInput{ClusterID: clusterID, RequesterNodeID: uint64(0), BackupSetID: backupSetID, Reason: input.Reason, PodName: node.PodName, NodeID: node.NodeID, RaftNodeID: node.RaftNodeID, Ordinal: node.Ordinal, OutputDir: input.OutputDir, ArchiveFormat: string(input.ArchiveFormat), UTCTimestamp: ts, Barriers: barriers, RepositoryID: input.RepositoryID, RepositoryPrefix: repositoryBackupSetPrefixFromInput(input, backupSetID), CaptureProvider: input.CaptureProvider, ReadAllowlistProfile: input.QuiesceReadProfile}
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

func (m *Module) publishRepositoryClusterBackupSet(ctx context.Context, input StartClusterBackupInput, repo BackupRepository, backupSetID, clusterID string, createdAt, completedAt time.Time) (summary clusterbackup.Manifest, retErr error) {
	store, err := m.repositoryObjectStore(ctx, repo)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	if strings.TrimSpace(input.RepositoryPrefix) == "" {
		input.RepositoryPrefix = repositoryBackupSetPrefix(repo, backupSetID)
	}
	publishedComplete := false
	defer func() {
		if retErr != nil && !publishedComplete {
			m.cleanupRepositoryPrefixBestEffort(context.Background(), store, input.RepositoryPrefix)
		}
	}()
	manifestKey := joinRepositoryKey(input.RepositoryPrefix, clusterbackup.BackupSetManifestName)
	manifestURI := repositoryObjectURI(repo, manifestKey)
	production, err := m.buildProductionBackupSetManifest(input, repo, backupSetID, clusterID, createdAt, completedAt, manifestURI)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	raw, err := marshalProductionJSON(production)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	manifestChecksum := sha256.Sum256(raw)
	marker := clusterbackup.CompleteMarkerV1{Schema: clusterbackup.ProductionBackupCompleteSchema, MarkerVersion: clusterbackup.ProductionCompleteMarkerVersion, BackupSetID: backupSetID, State: production.State, RepositoryID: repo.RepositoryID, ManifestURI: manifestURI, ManifestSizeBytes: decimalInt64(int64(len(raw))), ManifestChecksumSHA256: hex.EncodeToString(manifestChecksum[:]), CompletedAt: completedAt, ArtifactCount: len(production.Artifacts.Nodes) + len(production.Artifacts.AdditionalManifests), TotalArtifactBytes: production.Validation.TotalArtifactBytes}
	doc, err := clusterbackup.ParseBackupSetDocument(raw)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	report := clusterbackup.ValidateBackupSetDocument(ctx, doc, &marker, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, ExpectedNodeCount: len(production.Artifacts.Nodes), AllowS3ArtifactURIs: true})
	if err := report.Error(); err != nil {
		return clusterbackup.Manifest{}, err
	}
	manifestInfo, err := store.PutBytes(ctx, manifestKey, raw, "application/json")
	if err != nil {
		return clusterbackup.Manifest{}, fmt.Errorf("upload backup-set manifest: %w", err)
	}
	marker.ManifestURI = manifestInfo.URI
	marker.ManifestSizeBytes = decimalInt64(manifestInfo.SizeBytes)
	marker.ManifestChecksumSHA256 = manifestInfo.ChecksumSHA256
	markerRaw, err := marshalProductionJSON(marker)
	if err != nil {
		return clusterbackup.Manifest{}, err
	}
	if _, err := store.PutBytes(ctx, joinRepositoryKey(input.RepositoryPrefix, "COMPLETE"), markerRaw, "application/json"); err != nil {
		return clusterbackup.Manifest{}, fmt.Errorf("upload COMPLETE marker: %w", err)
	}
	publishedComplete = true
	summary = productionBackupSetManifestSummary(production)
	summary.ManifestURI = manifestInfo.URI
	return summary, nil
}

func (m *Module) buildProductionBackupSetManifest(input StartClusterBackupInput, repo BackupRepository, backupSetID, clusterID string, createdAt, completedAt time.Time, manifestURI string) (clusterbackup.BackupSetManifestV1, error) {
	_, runs := m.clusterBackupSnapshot()
	run, ok := runs[backupSetID]
	if !ok {
		return clusterbackup.BackupSetManifestV1{}, fmt.Errorf("cluster backup %s not found", backupSetID)
	}
	nodes := make([]clusterbackup.NodeArtifact, 0, len(run.NodeResults))
	for _, artifact := range run.NodeResults {
		nodes = append(nodes, artifact)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Ordinal < nodes[j].Ordinal })
	if len(nodes) == 0 {
		return clusterbackup.BackupSetManifestV1{}, fmt.Errorf("node artifacts are required")
	}
	policy := m.Policy()
	policyHash := policyHashSHA256(policy)
	sourceNodes := make([]clusterbackup.BackupSetSourceNode, 0, len(run.Expected))
	for _, node := range run.Expected {
		sourceNodes = append(sourceNodes, clusterbackup.BackupSetSourceNode{NodeID: node.NodeID, NodeName: firstNonEmptyString(node.NodeID, node.PodName), PodName: node.PodName, Ordinal: node.Ordinal, RaftNodeID: decimalUint64(node.RaftNodeID), PVCRef: "pvc/myceld-data-" + node.PodName})
	}
	sort.Slice(sourceNodes, func(i, j int) bool { return sourceNodes[i].Ordinal < sourceNodes[j].Ordinal })
	storageVersions := map[string]string{"backup_set": "1", "system_raft": "1", "graph": "1", "semantic_records": "1"}
	membershipHash := hashJSON(sourceNodes)
	placementHash := hashJSON(struct {
		ClusterID string                              `json:"cluster_id"`
		Nodes     []clusterbackup.BackupSetSourceNode `json:"nodes"`
	}{ClusterID: clusterID, Nodes: sourceNodes})
	repositoryURI := repositoryObjectURI(repo, input.RepositoryPrefix)
	totalBytes := int64(0)
	artifactNodes := make([]clusterbackup.BackupSetNodeArtifact, 0, len(nodes))
	for _, node := range nodes {
		totalBytes += node.SizeBytes
		artifactNodes = append(artifactNodes, clusterbackup.BackupSetNodeArtifact{ArtifactID: fmt.Sprintf("node:%d", node.Ordinal), PodName: node.PodName, NodeID: node.NodeID, NodeName: firstNonEmptyString(node.NodeID, node.PodName), Ordinal: node.Ordinal, RaftNodeID: decimalUint64(node.RaftNodeID), SourcePVCRef: "pvc/myceld-data-" + node.PodName, SourceDataDir: m.dataDir, ArchiveName: node.ArchiveName, ArchiveURI: node.ArchiveURI, ManifestName: node.ManifestName, ManifestURI: node.ManifestURI, InventoryURI: node.InventoryURI, InventoryChecksumSHA256: node.InventoryChecksumSHA256, SizeBytes: decimalInt64(node.SizeBytes), ChecksumSHA256: node.ChecksumSHA256, CaptureSource: mustJSONRaw(map[string]any{"provider": "FILESYSTEM_IMMUTABLE_COPY", "scratch": "redacted"}), AppliedIndexes: uint64MapToDecimalStrings(node.AppliedIndexes), RaftFreeze: productionNodeRaftFreeze(node.RaftFreeze)})
	}
	barrierSet := m.productionBarrierSet(input, backupSetID, clusterID, createdAt, sourceNodes, artifactNodes, membershipHash, placementHash)
	derivedState := productionDerivedState(policy.DerivedSubsystems, artifactNodes, input.ArchiveFormat)
	manifest := clusterbackup.BackupSetManifestV1{
		Schema:               clusterbackup.ProductionBackupSetSchema,
		ManifestVersion:      clusterbackup.ProductionManifestVersion,
		BackupSetID:          backupSetID,
		State:                "SUCCEEDED",
		Complete:             true,
		CreatedAt:            createdAt,
		CompletedAt:          completedAt,
		Operation:            clusterbackup.BackupSetOperation{BackupOperationID: backupSetID, Trigger: "MANUAL", Reason: input.Reason, IdempotencyKeyHash: hashOptionalString(input.IdempotencyKey), StartedAt: createdAt, CompletedAt: completedAt, PolicyID: "policy-default", PolicyRevision: "1", PolicyHashSHA256: policyHash},
		Source:               clusterbackup.BackupSetSource{ClusterID: clusterID, ClusterName: firstNonEmptyString(input.ClusterName, clusterID), Namespace: strings.TrimSpace(input.Namespace), StatefulSet: firstNonEmptyString(input.StatefulSet, "myceld"), DataDir: m.dataDir, MycelVersion: firstNonEmptyString(strings.TrimSpace(input.Image), "development"), MycelAPIVersion: "v1", Image: strings.TrimSpace(input.Image), ImageDigest: strings.TrimSpace(input.ImageDigest), StorageFormatVersions: storageVersions, Topology: clusterbackup.BackupSetSourceTopology{NodeCount: len(sourceNodes), PartitionCount: maxInt(1, len(run.Barriers)), ReplicaFactor: maxInt(1, len(sourceNodes)), Nodes: sourceNodes}, MembershipHashSHA256: membershipHash, PlacementHashSHA256: placementHash},
		Repository:           clusterbackup.BackupSetRepository{RepositoryID: repo.RepositoryID, Type: "S3_COMPATIBLE", URI: repositoryURI, Bucket: repo.S3.Bucket, Prefix: input.RepositoryPrefix, EndpointFingerprint: repositoryEndpointFingerprint(repo), CredentialRef: credentialReferenceManifestString(repo.CredentialRef), Encryption: clusterbackup.BackupSetEncryptionPolicy{Mode: encryptionModeManifestString(repo.Encryption.Mode), KMSKeyRef: secretReferenceManifestString(repo.Encryption.KeyRef), KeyFingerprint: hashOptionalString(repo.Encryption.KMSKeyID)}, LayoutVersion: 1},
		Policy:               clusterbackup.BackupSetPolicy{PolicyID: "policy-default", PolicyRevision: "1", PolicyHashSHA256: policyHash, RetentionClass: "DEFAULT", Consistency: mustJSONRaw(map[string]any{"quiesce_required": true, "read_allowlist_profile": productionEnum(input.QuiesceReadProfile), "allow_read_only_during_quiesce": false}), Capture: mustJSONRaw(map[string]any{"provider": "FILESYSTEM_IMMUTABLE_COPY", "archive_format": productionArchiveFormat(input.ArchiveFormat)}), Upload: mustJSONRaw(map[string]any{"repository_id": repo.RepositoryID, "layout_version": 1, "complete_marker": true}), Validation: mustJSONRaw(map[string]any{"checksum_algorithm": clusterbackup.ProductionChecksumAlgorithmSHA256, "require_complete_marker": true}), DerivedSubsystems: productionDerivedPolicies(policy.DerivedSubsystems)},
		Compatibility:        clusterbackup.BackupSetCompatibility{RequiredFeatures: []string{}, StorageFormatVersions: storageVersions, MinRestoreMycelVersion: "v0.23.0", SupportedRestoreTargetModes: []string{"NEW_CLUSTER_BOOTSTRAP", "EXISTING_CLUSTER_REPLACE"}, SupportedIdentityModes: []string{"PRESERVE_BACKUP_ID"}, RequiresSameTopology: true, RequiresSourceFencingForPreservedIdentity: true},
		Consistency:          clusterbackup.BackupSetConsistency{Quiescence: mustJSONRaw(map[string]any{"mode": "BACKUP", "read_allowlist_profile": productionEnum(input.QuiesceReadProfile)}), BarrierSet: barrierSet},
		Capture:              clusterbackup.BackupSetCapture{Provider: "FILESYSTEM_IMMUTABLE_COPY", ProviderVersion: "phase-6-mvp", FallbackUsed: false, DataMover: "mycel-daemon-s3-compatible", ArchiveFormat: productionArchiveFormat(input.ArchiveFormat), TemporaryProviderRefsRedacted: true, ProviderEvidence: mustJSONRaw(map[string]any{"repository_id": repo.RepositoryID, "repository_prefix": input.RepositoryPrefix, "scratch": "redacted"})},
		Artifacts:            clusterbackup.BackupSetArtifacts{ArchiveFormat: productionArchiveFormat(input.ArchiveFormat), ChecksumAlgorithm: clusterbackup.ProductionChecksumAlgorithmSHA256, Nodes: artifactNodes, AdditionalManifests: []clusterbackup.BackupSetArtifactRef{}},
		ExternalObjectStores: []clusterbackup.BackupSetExternalObjectStore{},
		DerivedState:         derivedState,
		Validation:           clusterbackup.BackupSetValidation{Result: clusterbackup.ProductionValidationStatePassed, ValidatedAt: completedAt, ValidatorVersion: "mycel-phase-6-mvp", ExpectedNodeCount: len(sourceNodes), NodeArtifactCount: len(artifactNodes), RequiredArtifactCount: len(artifactNodes), ValidatedArtifactCount: len(artifactNodes), TotalArtifactBytes: decimalInt64(totalBytes), ChecksumAlgorithm: clusterbackup.ProductionChecksumAlgorithmSHA256, Checks: []clusterbackup.BackupSetCheckResult{{Name: "repository_manifest_shape", Result: clusterbackup.ProductionValidationStatePassed, CheckedAt: completedAt, Detail: "backup-set manifest satisfies v1 structural validators"}, {Name: "node_artifact_catalog", Result: clusterbackup.ProductionValidationStatePassed, CheckedAt: completedAt, Detail: "node archive, manifest, and inventory URIs are present"}, {Name: "complete_marker", Result: clusterbackup.ProductionValidationStatePassed, CheckedAt: completedAt, Detail: "COMPLETE marker will be published after backup-set.json"}}, Warnings: []string{}, Errors: []string{}},
		Restore:              clusterbackup.BackupSetRestore{SupportedRestoreTargetModes: []string{"NEW_CLUSTER_BOOTSTRAP", "EXISTING_CLUSTER_REPLACE"}, SupportedIdentityModes: []string{"PRESERVE_BACKUP_ID"}, DefaultIdentityMode: "PRESERVE_BACKUP_ID", SupportedAuthSessionPolicies: []string{"PRESERVE_RESTORED_SESSIONS", "REVOKE_ALL_SESSIONS"}, RequiresSameTopology: true, RequiredSecretRefs: []string{credentialReferenceManifestString(repo.CredentialRef)}, RequiredExternalDependencies: []string{}, OperatorNotes: "Phase 6 repository-backed backup set generated by portable filesystem immutable copy."},
		Audit:                clusterbackup.BackupSetAudit{Labels: map[string]string{"capture_provider": "filesystem_immutable_copy"}, Annotations: map[string]string{"manifest_uri": manifestURI}, Redactions: []string{"credential_ref", "temporary_provider_refs"}},
	}
	manifest.DerivedState = clusterbackup.SortBackupSetDerivedState(manifest.DerivedState)
	return manifest, nil
}

func writeClusterBackupSetManifest(outputDir string, manifest clusterbackup.Manifest) error {
	if err := os.MkdirAll(outputDir, fsperm.PrivateDir); err != nil {
		return err
	}
	raw, err := manifest.MarshalDeterministic()
	if err != nil {
		return err
	}
	path := filepath.Join(outputDir, "backup-set.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), fsperm.PrivateFile); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *Module) productionBarrierSet(input StartClusterBackupInput, backupSetID, clusterID string, createdAt time.Time, sourceNodes []clusterbackup.BackupSetSourceNode, artifactNodes []clusterbackup.BackupSetNodeArtifact, membershipHash, placementHash string) clusterbackup.CheckpointBarrierSetV1 {
	barrierGroups := map[string]clusterbackup.CheckpointRaftGroup{}
	for _, node := range artifactNodes {
		for _, group := range node.RaftFreeze.Groups {
			entry := barrierGroups[group.GroupID]
			entry.GroupID = group.GroupID
			if entry.GroupKind == "" {
				entry.GroupKind = productionRaftGroupKind(group.GroupID)
			}
			entry.BarrierIndex = firstNonEmptyString(entry.BarrierIndex, group.BarrierIndex)
			entry.CommitIndex = firstNonEmptyString(entry.CommitIndex, group.CommitIndex, group.BarrierIndex)
			entry.Term = firstNonEmptyString(entry.Term, group.Term, "0")
			entry.LeaderRaftNodeID = firstNonEmptyString(entry.LeaderRaftNodeID, group.Leader, node.RaftNodeID)
			entry.PerNode = append(entry.PerNode, clusterbackup.CheckpointRaftNodeState{NodeID: node.NodeID, PodName: node.PodName, RaftNodeID: node.RaftNodeID, AppliedIndex: firstNonEmptyString(node.AppliedIndexes[group.GroupID], group.AppliedIndex), LastIndex: firstNonEmptyString(group.LastIndex, group.AppliedIndex), SnapshotIndex: firstNonEmptyString(group.SnapshotIndex, "0")})
			if !stringSliceContainsLocal(entry.Members, node.RaftNodeID) {
				entry.Members = append(entry.Members, node.RaftNodeID)
			}
			barrierGroups[group.GroupID] = entry
		}
	}
	groups := make([]clusterbackup.CheckpointRaftGroup, 0, len(barrierGroups))
	for _, group := range barrierGroups {
		sort.Slice(group.PerNode, func(i, j int) bool { return group.PerNode[i].PodName < group.PerNode[j].PodName })
		sort.Strings(group.Members)
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	systemBarrier := "0"
	for _, group := range groups {
		if group.GroupID == string(consensus.SystemGroupID) {
			systemBarrier = group.BarrierIndex
			break
		}
	}
	if systemBarrier == "0" && len(groups) > 0 {
		systemBarrier = groups[0].BarrierIndex
	}
	coordinatorNodeID := ""
	coordinatorRaftNodeID := "0"
	if len(sourceNodes) > 0 {
		coordinatorNodeID = sourceNodes[0].NodeID
		coordinatorRaftNodeID = sourceNodes[0].RaftNodeID
	}
	if strings.TrimSpace(m.localIdentity.NodeID) != "" {
		coordinatorNodeID = m.localIdentity.NodeID
	}
	if m.localIdentity.RaftNodeID != 0 {
		coordinatorRaftNodeID = decimalUint64(m.localIdentity.RaftNodeID)
	}
	quiesceLease := "cluster-backup-" + backupSetID
	if len(artifactNodes) > 0 && strings.TrimSpace(artifactNodes[0].RaftFreeze.LeaseID) != "" {
		quiesceLease = artifactNodes[0].RaftFreeze.LeaseID
	}
	return clusterbackup.CheckpointBarrierSetV1{SchemaVersion: 1, BarrierSetID: "barrier-" + backupSetID, BackupOperationID: backupSetID, BackupSetID: backupSetID, ClusterID: clusterID, QuiesceLeaseID: quiesceLease, EstablishedAt: createdAt, CoordinatorNodeID: coordinatorNodeID, CoordinatorRaftNodeID: coordinatorRaftNodeID, ReadAllowlistProfile: productionEnum(input.QuiesceReadProfile), WriteGateClosedAt: createdAt, Participants: []json.RawMessage{mustJSONRaw(map[string]any{"name": "mycel-runtime", "state": "quiesced"})}, RaftGroups: groups, SystemMetadata: clusterbackup.CheckpointSystemMetadata{ClusterID: clusterID, ClusterName: firstNonEmptyString(input.ClusterName, clusterID), BootstrapEpoch: "1", NodeCount: len(sourceNodes), PartitionCount: maxInt(1, len(groups)), ReplicaFactor: maxInt(1, len(sourceNodes)), SystemRaftGroupID: string(consensus.SystemGroupID), SystemRaftBarrierIndex: systemBarrier, MembershipHashSHA256: membershipHash, PlacementHashSHA256: placementHash, ExpectedNodes: sourceNodes}, Identity: clusterbackup.CheckpointIdentity{IdentityRaftGroupID: string(consensus.SystemGroupID), IdentityRaftBarrierIndex: systemBarrier, AuthorizationRevision: systemBarrier, PrincipalStoreChecksumSHA256: hashString("principal-store:" + backupSetID), RoleBindingChecksumSHA256: hashString("role-bindings:" + backupSetID), CapabilityGrantChecksumSHA256: hashString("capability-grants:" + backupSetID), RefreshSessionStoreChecksumSHA256: hashString("refresh-sessions:" + backupSetID), AuthSessionPolicy: "PRESERVE_RESTORED_SESSIONS"}}
}

func productionNodeRaftFreeze(in clusterbackup.RaftFreezeEvidence) clusterbackup.BackupSetNodeRaftFreeze {
	groups := make([]clusterbackup.BackupSetNodeRaftFreezeGroup, 0, len(in.Groups))
	for groupID, group := range in.Groups {
		groups = append(groups, clusterbackup.BackupSetNodeRaftFreezeGroup{GroupID: firstNonEmptyString(group.GroupID, groupID), BarrierIndex: decimalUint64(group.BarrierIndex), AppliedIndex: decimalUint64(group.AppliedIndex), CommitIndex: decimalUint64(group.CommitIndex), Term: decimalUint64(group.Term), LastIndex: decimalUint64(group.LastIndex), SnapshotIndex: decimalUint64(group.SnapshotIndex), Leader: decimalUint64(group.Leader)})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	return clusterbackup.BackupSetNodeRaftFreeze{LeaseID: in.LeaseID, AcquiredAt: in.AcquiredAt, ReleasedAt: in.ReleasedAt, ExpiresAt: in.ExpiresAt, Groups: groups}
}

func productionRaftGroupKind(groupID string) string {
	if groupID == string(consensus.SystemGroupID) {
		return "SYSTEM"
	}
	return "DATA"
}

func productionDerivedPolicies(in []backupcore.DerivedSubsystemPolicy) []clusterbackup.BackupSetDerivedPolicy {
	out := make([]clusterbackup.BackupSetDerivedPolicy, 0, len(in))
	for _, row := range in {
		out = append(out, clusterbackup.BackupSetDerivedPolicy{Subsystem: productionEnum(row.Subsystem), Mode: productionEnum(row.Mode)})
	}
	return out
}

func productionDerivedState(policies []backupcore.DerivedSubsystemPolicy, nodes []clusterbackup.BackupSetNodeArtifact, format backupcore.ArchiveFormat) []clusterbackup.BackupSetDerivedState {
	out := make([]clusterbackup.BackupSetDerivedState, 0, len(policies))
	var total int64
	checksums := []string{}
	uri := ""
	for _, node := range nodes {
		total += parseSignedDecimalStringLocal(node.SizeBytes)
		checksums = append(checksums, node.ChecksumSHA256)
		if uri == "" {
			uri = node.ArchiveURI
		}
	}
	combinedChecksum := hashString(strings.Join(checksums, ":"))
	for _, policy := range policies {
		mode := productionEnum(policy.Mode)
		subsystem := productionEnum(policy.Subsystem)
		row := clusterbackup.BackupSetDerivedState{Subsystem: subsystem, ArtifactID: strings.ToLower(policy.Subsystem) + ":phase-6", PolicyMode: mode, RestorePolicy: "REBUILD_REQUIRED", ValidationState: "REBUILD_REQUIRED"}
		switch mode {
		case "INCLUDE_REQUIRED":
			row.Included = true
			row.RestorePolicy = "PRESERVED"
			row.ArtifactURI = uri
			row.SizeBytes = decimalInt64(total)
			row.ChecksumSHA256 = combinedChecksum
			row.FormatVersion = "archive:" + productionArchiveFormat(format)
			row.ValidationState = "INCLUDED_AND_VALIDATED"
		case "INCLUDE_OPTIONAL", "REBUILD_ALLOWED":
			row.RestorePolicy = "REBUILD_REQUIRED"
		case "DISCARD":
			row.RestorePolicy = "DISCARD"
			row.ValidationState = "DISCARDED"
		}
		out = append(out, row)
	}
	return clusterbackup.SortBackupSetDerivedState(out)
}

func productionArchiveFormat(format backupcore.ArchiveFormat) string {
	switch format {
	case backupcore.ArchiveFormatZip:
		return "ZIP"
	case backupcore.ArchiveFormatTar:
		return "TAR"
	case backupcore.ArchiveFormatTarGz:
		return "TAR_GZIP"
	case backupcore.ArchiveFormatTarZst:
		return "TAR_ZSTD"
	default:
		return productionEnum(string(format))
	}
}

func productionEnum(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = backupcore.DefaultProductionQuiesceReadProfile
	}
	return strings.ToUpper(strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value))
}

func encryptionModeManifestString(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case EncryptionModeSSEKMS:
		return "SSE_KMS"
	case EncryptionModeSSES3:
		return "SSE_S3"
	case EncryptionModeNone:
		return "NONE"
	case EncryptionModeClientEnvelope:
		return "CLIENT_ENVELOPE"
	default:
		return "PROVIDER_MANAGED"
	}
}

func credentialReferenceManifestString(ref SecretReference) string {
	value := secretReferenceManifestString(ref)
	if value == "" {
		return SecretSchemeAWSIRSA
	}
	return value
}

func secretReferenceManifestString(ref SecretReference) string {
	ref = normalizeSecretReference(ref)
	if ref.Scheme == "" {
		return ""
	}
	parts := []string{ref.Scheme}
	if ref.Namespace != "" || ref.Name != "" {
		if ref.Namespace != "" {
			parts = append(parts, ref.Namespace+"/"+ref.Name)
		} else {
			parts = append(parts, ref.Name)
		}
	}
	if ref.Path != "" {
		parts = append(parts, ref.Path)
	}
	if len(ref.Keys) > 0 {
		keys := make([]string, 0, len(ref.Keys))
		for _, value := range ref.Keys {
			if strings.TrimSpace(value) != "" {
				keys = append(keys, strings.TrimSpace(value))
			}
		}
		sort.Strings(keys)
		parts = append(parts, strings.Join(keys, ","))
	}
	return strings.Join(parts, ":")
}

func repositoryEndpointFingerprint(repo BackupRepository) string {
	endpoint := strings.TrimSpace(repo.S3.EndpointURI)
	if endpoint == "" {
		endpoint = "aws-s3:" + strings.TrimSpace(repo.S3.Region)
	}
	return "sha256:" + hashString(endpoint)
}

func policyHashSHA256(policy backupcore.Policy) string { return hashJSON(policy) }

func hashJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return hashString(fmt.Sprintf("%v", value))
	}
	return hashBytes(raw)
}

func hashOptionalString(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return hashString(value)
}

func hashString(value string) string { return hashBytes([]byte(value)) }

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func mustJSONRaw(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(raw)
}

func marshalProductionJSON(value any) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func decimalUint64(value uint64) string { return fmt.Sprintf("%d", value) }

func decimalInt64(value int64) string {
	if value < 0 {
		value = 0
	}
	return fmt.Sprintf("%d", value)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func stringSliceContainsLocal(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func parseSignedDecimalStringLocal(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
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

func (m *Module) findClusterBackupByIdempotencyKey(key string) (ClusterBackupRunStatus, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return ClusterBackupRunStatus{}, false
	}
	_, runs := m.clusterBackupSnapshot()
	for _, run := range runs {
		if run.IdempotencyKey == key {
			return clusterBackupStatusFromRun(run), true
		}
	}
	return ClusterBackupRunStatus{}, false
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
	return ClusterBackupRunStatus{BackupSetID: run.BackupSetID, ClusterID: run.ClusterID, Reason: run.Reason, Phase: string(run.Phase), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, CompletedAt: completed, Expected: expected, Barriers: cloneStringUint64Map(run.Barriers), Nodes: nodes, ManifestURI: manifestURI, RepositoryID: run.RepositoryID, RepositoryPrefix: run.RepositoryPrefix, CaptureProvider: run.CaptureProvider, FailurePhase: failurePhase, Error: msg, Blockers: append([]ClusterBackupBlocker(nil), run.Blockers...), Cancel: run.Cancel}
}

type ClusterBackupSetValidationResult struct {
	Manifest           clusterbackup.Manifest
	ProductionManifest *clusterbackup.BackupSetManifestV1
	ProductionMarker   *clusterbackup.CompleteMarkerV1
	ProductionReport   *clusterbackup.BackupSetValidationReport
}

func ValidateClusterBackupSet(ctx context.Context, path string) (clusterbackup.Manifest, error) {
	result, err := ValidateClusterBackupSetDetailed(ctx, path)
	return result.Manifest, err
}

func ValidateClusterBackupSetDetailed(ctx context.Context, path string) (ClusterBackupSetValidationResult, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return ClusterBackupSetValidationResult{}, fmt.Errorf("backup_set_path is required")
	}
	manifestPath, err := clusterBackupSetManifestPath(path)
	if err != nil {
		return ClusterBackupSetValidationResult{}, err
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return ClusterBackupSetValidationResult{}, err
	}
	var header struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &header); err == nil && header.Schema == clusterbackup.ProductionBackupSetSchema {
		doc, marker, report, err := clusterbackup.ValidateBackupSetPath(ctx, path, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, ValidateLocalArtifactBytes: true})
		if err != nil {
			return ClusterBackupSetValidationResult{}, err
		}
		summary := productionBackupSetManifestSummary(doc.Manifest)
		result := ClusterBackupSetValidationResult{Manifest: summary, ProductionManifest: &doc.Manifest, ProductionMarker: marker, ProductionReport: &report}
		if err := report.Error(); err != nil {
			return result, err
		}
		return result, nil
	}
	manifest, err := clusterbackup.Parse(raw)
	if err != nil {
		return ClusterBackupSetValidationResult{}, err
	}
	if err := clusterbackup.ValidateArchiveFiles(ctx, manifest); err != nil {
		return ClusterBackupSetValidationResult{Manifest: manifest}, err
	}
	return ClusterBackupSetValidationResult{Manifest: manifest}, nil
}

func clusterBackupSetManifestPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "file://") {
		u, err := url.Parse(path)
		if err != nil || u.Path == "" {
			return "", fmt.Errorf("invalid file backup-set URI %q", path)
		}
		path = u.Path
	} else if strings.Contains(path, "://") {
		return "", fmt.Errorf("backup-set must be a local path or file:// URI")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return filepath.Join(path, clusterbackup.BackupSetManifestName), nil
	}
	return path, nil
}

func productionBackupSetManifestSummary(manifest clusterbackup.BackupSetManifestV1) clusterbackup.Manifest {
	out := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: manifest.BackupSetID, CreatedAt: manifest.CreatedAt, CompletedAt: manifest.CompletedAt, ClusterID: manifest.Source.ClusterID, Complete: manifest.Complete, State: strings.ToLower(manifest.State), Reason: manifest.Operation.Reason, ManifestURI: joinURI(manifest.Repository.URI, clusterbackup.BackupSetManifestName), ExpectedNodes: manifest.Source.Topology.NodeCount, Image: manifest.Source.Image, Namespace: manifest.Source.Namespace, StatefulSet: manifest.Source.StatefulSet, DataDir: manifest.Source.DataDir, ArchiveFormat: archiveFormatFromProductionManifest(manifest.Artifacts.ArchiveFormat)}
	for _, node := range manifest.Artifacts.Nodes {
		inventoryName := ""
		if strings.TrimSpace(node.InventoryURI) != "" {
			if u, err := url.Parse(node.InventoryURI); err == nil && u.Path != "" {
				inventoryName = filepath.Base(u.Path)
			} else {
				inventoryName = filepath.Base(node.InventoryURI)
			}
		}
		out.Nodes = append(out.Nodes, clusterbackup.NodeArtifact{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: decimalStringToUint64(node.RaftNodeID), ArchiveName: node.ArchiveName, ArchiveURI: node.ArchiveURI, ManifestName: node.ManifestName, ManifestURI: node.ManifestURI, InventoryName: inventoryName, InventoryURI: node.InventoryURI, InventoryChecksumSHA256: node.InventoryChecksumSHA256, SizeBytes: decimalStringToInt64(node.SizeBytes), ChecksumSHA256: node.ChecksumSHA256, AppliedIndexes: decimalMapToUint64(node.AppliedIndexes)})
	}
	return out
}

func archiveFormatFromProductionManifest(value string) backupcore.ArchiveFormat {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ZIP":
		return backupcore.ArchiveFormatZip
	case "TAR":
		return backupcore.ArchiveFormatTar
	case "TAR_GZIP", "TAR_GZ":
		return backupcore.ArchiveFormatTarGz
	case "TAR_ZSTD", "TAR_ZST", "TAR_ZST_BYTE_STREAM":
		return backupcore.ArchiveFormatTarZst
	default:
		return backupcore.ArchiveFormat(strings.ToLower(strings.TrimSpace(value)))
	}
}

func decimalMapToUint64(in map[string]string) map[string]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = decimalStringToUint64(value)
	}
	return out
}

func decimalStringToUint64(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	return parsed
}

func decimalStringToInt64(value string) int64 {
	parsed := decimalStringToUint64(value)
	if parsed > uint64(^uint64(0)>>1) {
		return int64(^uint64(0) >> 1)
	}
	return int64(parsed)
}

func joinURI(base, name string) string {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	name = strings.TrimLeft(strings.TrimSpace(name), "/")
	if base == "" {
		return name
	}
	return base + "/" + name
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

func (m *Module) collectCoordinatorReadiness(ctx context.Context, input StartClusterBackupInput, backupSetID, clusterID string, ts time.Time, nodes []ClusterBackupNode) (map[string]coordinatorReadiness, []ClusterBackupPrecheckRaftGroup, error) {
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

func firstNonNilError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("operation failed")
}

func clusterBackupBlockersFromPrecheck(groups []ClusterBackupPrecheckRaftGroup, nodes []ClusterBackupNode) []ClusterBackupBlocker {
	byRaftNode := map[uint64]ClusterBackupNode{}
	for _, node := range nodes {
		byRaftNode[node.RaftNodeID] = node
	}
	blockers := []ClusterBackupBlocker{}
	for _, group := range groups {
		if !group.HasQuorum || group.Leader == 0 {
			blockers = append(blockers, ClusterBackupBlocker{RaftGroup: string(group.GroupID), Reason: "raft_group_has_no_quorum", CommitIndex: group.CommitIndex})
		}
		for raftNodeID, applied := range group.AppliedByNode {
			if applied >= group.CommitIndex {
				continue
			}
			node := byRaftNode[raftNodeID]
			blockers = append(blockers, ClusterBackupBlocker{NodeName: node.PodName, NodeID: node.NodeID, RaftNodeID: raftNodeID, RaftGroup: string(group.GroupID), Reason: "applied_index_behind_commit_index", AppliedIndex: applied, CommitIndex: group.CommitIndex})
		}
	}
	return blockers
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
