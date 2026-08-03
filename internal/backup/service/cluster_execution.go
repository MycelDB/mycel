package service

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
)

var _ backend.ClusterBackupProvider = (*Module)(nil)

func (m *Module) LocalRaftBackupBarriers() (map[string]uint64, error) {
	if m.raftGroups == nil {
		return nil, fmt.Errorf("raft groups are not configured")
	}
	barriers := map[string]uint64{}
	for _, status := range m.raftGroups.Status() {
		if status.GroupID == "" {
			continue
		}
		barriers[string(status.GroupID)] = status.CommitIndex
	}
	if len(barriers) == 0 {
		return nil, fmt.Errorf("raft group status is empty")
	}
	return barriers, nil
}

func (m *Module) WaitLocalRaftBackupBarriers(ctx context.Context, barriers map[string]uint64) (map[string]uint64, error) {
	if len(barriers) == 0 {
		return nil, fmt.Errorf("raft barriers are required")
	}
	if m.raftGroups == nil {
		return nil, fmt.Errorf("raft groups are not configured")
	}
	localStatuses := m.raftGroups.Status()
	if len(localStatuses) == 0 {
		return nil, fmt.Errorf("raft group status is empty")
	}
	for _, status := range localStatuses {
		if _, ok := barriers[string(status.GroupID)]; !ok {
			return nil, fmt.Errorf("raft barrier for group %s is required", status.GroupID)
		}
	}
	applied := map[string]uint64{}
	for groupID, index := range barriers {
		gid := consensus.GroupID(strings.TrimSpace(groupID))
		if gid == "" {
			return nil, fmt.Errorf("raft barrier group_id is required")
		}
		group, ok := m.raftGroups.Group(gid)
		if !ok || group == nil {
			return nil, fmt.Errorf("raft group %s is not available", gid)
		}
		if err := group.WaitApplied(ctx, index); err != nil {
			return nil, fmt.Errorf("wait for raft group %s applied index %d: %w", gid, index, err)
		}
	}
	for _, status := range m.raftGroups.Status() {
		applied[string(status.GroupID)] = status.AppliedIndex
	}
	for groupID, index := range barriers {
		if applied[groupID] < index {
			return nil, fmt.Errorf("raft group %s applied index %d is behind backup barrier %d", groupID, applied[groupID], index)
		}
	}
	return applied, nil
}

func (m *Module) CreateLocalClusterBackupArchive(ctx context.Context, in backend.CreateLocalBackupArchiveInput) (backend.CreateLocalBackupArchiveResult, error) {
	if strings.TrimSpace(in.BackupSetID) == "" {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("backup_set_id is required")
	}
	if strings.TrimSpace(in.ClusterID) == "" {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("cluster_id is required")
	}
	identity := m.localIdentity
	if identity.ClusterID != "" && strings.TrimSpace(in.ClusterID) != identity.ClusterID {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("cluster_id does not match local node")
	}
	if strings.TrimSpace(in.NodeID) == "" {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("node_id is required")
	}
	if identity.NodeID != "" && strings.TrimSpace(in.NodeID) != identity.NodeID {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("node_id does not match local node")
	}
	if strings.TrimSpace(in.PodName) == "" {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("pod_name is required")
	}
	if identity.NodeName != "" && strings.TrimSpace(in.PodName) != identity.NodeName {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("pod_name does not match local node")
	}
	if in.RaftNodeID == 0 {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("raft_node_id is required")
	}
	if identity.RaftNodeID != 0 && in.RaftNodeID != identity.RaftNodeID {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("raft_node_id does not match local node")
	}
	if in.Ordinal < 0 {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("ordinal must be non-negative")
	}
	if strings.TrimSpace(in.OutputDir) == "" {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("output_dir is required")
	}
	if err := validateLocalClusterBackupDestination(m.dataDir, in.OutputDir); err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	format := backupcore.ArchiveFormat(strings.TrimSpace(in.ArchiveFormat))
	if format == "" {
		format = m.Policy().ArchiveFormat
	}
	if !backupcore.IsSupportedArchiveFormat(format) {
		return backend.CreateLocalBackupArchiveResult{}, fmt.Errorf("unsupported archive_format %q", format)
	}
	archiveName, err := clusterbackup.NewArchiveName(in.UTCTimestamp, in.PodName, in.BackupSetID, format)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	manifestName, err := clusterbackup.NewNodeManifestName(archiveName)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	ext, err := backupcore.ArchiveExtension(format)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	backupID := strings.TrimSuffix(archiveName, ext)
	barriers, err := clusterBackupBarriersFromInput(in.Barriers)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	if err := m.validateRecordedLocalClusterBackupRequest(in, barriers); err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	lease, err := m.acquireClusterBackupQuiesce(ctx, in.BackupSetID, in.Reason)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	defer func() {
		if releaseErr := lease.Release(context.Background()); releaseErr != nil && m.logger != nil {
			m.logger.Warn("failed to release cluster backup quiesce lease", "backup_set_id", in.BackupSetID, "error", releaseErr)
		}
	}()
	applied, err := m.WaitLocalRaftBackupBarriers(ctx, barriers)
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	result, err := m.manager.CreateLocalArchive(ctx, backupcore.LocalArchiveInput{
		BackupID:      backupID,
		ArchiveName:   archiveName,
		BackupDir:     in.OutputDir,
		ArchiveFormat: string(format),
		Source:        "cluster-backup",
		Reason:        firstNonEmptyString(in.Reason, "cluster system backup"),
		CreatedAt:     in.UTCTimestamp,
	})
	if err != nil {
		return backend.CreateLocalBackupArchiveResult{}, err
	}
	return backend.CreateLocalBackupArchiveResult{
		ClusterID:      strings.TrimSpace(in.ClusterID),
		PodName:        strings.TrimSpace(in.PodName),
		NodeID:         strings.TrimSpace(in.NodeID),
		RaftNodeID:     in.RaftNodeID,
		Ordinal:        in.Ordinal,
		ArchiveName:    archiveName,
		ArchiveURI:     fileURI(result.ArchivePath),
		ManifestName:   manifestName,
		ManifestURI:    fileURI(result.ManifestPath),
		SizeBytes:      result.Manifest.SizeBytes,
		ChecksumSHA256: result.Manifest.ChecksumSHA256,
		AppliedIndexes: applied,
	}, nil
}

func clusterBackupBarriersFromInput(input []backend.BackupRaftBarrier) (map[string]uint64, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("raft barriers are required")
	}
	barriers := map[string]uint64{}
	for _, barrier := range input {
		groupID := strings.TrimSpace(barrier.GroupID)
		if groupID == "" {
			return nil, fmt.Errorf("raft barrier group_id is required")
		}
		if _, exists := barriers[groupID]; exists {
			return nil, fmt.Errorf("duplicate raft barrier for group %s", groupID)
		}
		barriers[groupID] = barrier.Index
	}
	return barriers, nil
}

func (m *Module) validateRecordedLocalClusterBackupRequest(in backend.CreateLocalBackupArchiveInput, barriers map[string]uint64) error {
	backupSetID := strings.TrimSpace(in.BackupSetID)
	m.mu.Lock()
	run, ok := m.clusterBackups[backupSetID]
	active := m.activeClusterBackupID
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("cluster backup %s is not recorded", backupSetID)
	}
	if active != backupSetID {
		return fmt.Errorf("cluster backup %s is not active", backupSetID)
	}
	if run.Phase.terminal() {
		return fmt.Errorf("cluster backup %s is already terminal", backupSetID)
	}
	if run.Phase != clusterBackupPhaseBarrierWait && run.Phase != clusterBackupPhaseArchiving {
		return fmt.Errorf("cluster backup %s is not ready for local archive; phase=%s", backupSetID, run.Phase)
	}
	if strings.TrimSpace(run.ClusterID) != "" && strings.TrimSpace(run.ClusterID) != strings.TrimSpace(in.ClusterID) {
		return fmt.Errorf("cluster backup cluster_id does not match request")
	}
	var expected *clusterBackupExpectedNode
	for i := range run.Expected {
		if strings.TrimSpace(run.Expected[i].PodName) == strings.TrimSpace(in.PodName) {
			expected = &run.Expected[i]
			break
		}
	}
	if expected == nil {
		return fmt.Errorf("unexpected pod %s for cluster backup %s", strings.TrimSpace(in.PodName), backupSetID)
	}
	if strings.TrimSpace(expected.NodeID) != strings.TrimSpace(in.NodeID) {
		return fmt.Errorf("node_id for pod %s does not match recorded cluster backup membership", expected.PodName)
	}
	if expected.Ordinal != in.Ordinal {
		return fmt.Errorf("ordinal for pod %s does not match recorded cluster backup membership", expected.PodName)
	}
	if expected.RaftNodeID != 0 && expected.RaftNodeID != in.RaftNodeID {
		return fmt.Errorf("raft_node_id for pod %s does not match recorded cluster backup membership", expected.PodName)
	}
	if len(run.Barriers) == 0 {
		return fmt.Errorf("cluster backup %s has no recorded raft barriers", backupSetID)
	}
	if len(barriers) != len(run.Barriers) {
		return fmt.Errorf("request raft barriers do not match recorded cluster backup barriers")
	}
	for groupID, recorded := range run.Barriers {
		got, ok := barriers[groupID]
		if !ok {
			return fmt.Errorf("request missing recorded raft barrier for group %s", groupID)
		}
		if got != recorded {
			return fmt.Errorf("request raft barrier for group %s is %d, want recorded barrier %d", groupID, got, recorded)
		}
	}
	return nil
}

func (m *Module) acquireClusterBackupQuiesce(ctx context.Context, backupSetID, reason string) (*quiesce.CompositeLease, error) {
	if m.quiesce == nil {
		return nil, fmt.Errorf("quiesce coordinator is not configured")
	}
	lease, err := m.quiesce.QuiesceAll(ctx, quiesce.Request{Reason: firstNonEmptyString(reason, "cluster system backup"), Mode: quiesce.ModeBackup, Source: "cluster-backup:" + strings.TrimSpace(backupSetID)})
	if err != nil {
		return nil, fmt.Errorf("quiesce local runtime: %w", err)
	}
	return lease, nil
}

func validateLocalClusterBackupDestination(dataDir, outputDir string) error {
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		return fmt.Errorf("output_dir is required")
	}
	if !filepath.IsAbs(outputDir) {
		return fmt.Errorf("output_dir must be absolute")
	}
	absOutput, err := filepath.Abs(outputDir)
	if err != nil {
		return err
	}
	absData, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	if sameOrChild(absOutput, absData) {
		return fmt.Errorf("output_dir must be outside data dir")
	}
	if err := os.MkdirAll(absOutput, 0o700); err != nil {
		return fmt.Errorf("create output_dir: %w", err)
	}
	probe, err := os.CreateTemp(absOutput, ".mycel-cluster-backup-probe-*")
	if err != nil {
		return fmt.Errorf("output_dir is not writable: %w", err)
	}
	probeName := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(probeName)
		return err
	}
	_ = os.Remove(probeName)
	return nil
}

func sameOrChild(path, parent string) bool {
	path = filepath.Clean(path)
	parent = filepath.Clean(parent)
	if path == parent {
		return true
	}
	rel, err := filepath.Rel(parent, path)
	return err == nil && rel != "." && rel != "" && !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)
}

func fileURI(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	return (&url.URL{Scheme: "file", Path: abs}).String()
}
