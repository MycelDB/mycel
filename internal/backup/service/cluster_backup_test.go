package service

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/wal"
)

func TestBackupRaftStateMachineAppliesClusterBackupLifecycle(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	request := clusterBackupRequestRecord{BackupSetID: "backup-set-1", ClusterID: "cluster-1", Reason: "test", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, request, "request")
	_, runs := m.clusterBackupSnapshot()
	if runs[request.BackupSetID].Phase != clusterBackupPhaseRequested {
		t.Fatalf("phase=%s want requested", runs[request.BackupSetID].Phase)
	}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: request.BackupSetID, Phase: clusterBackupPhasePrechecking, UpdatedAt: time.Now().UTC()}, "phase")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupBarrier, clusterBackupBarrierRecord{BackupSetID: request.BackupSetID, Barriers: map[string]uint64{"system": 10}, UpdatedAt: time.Now().UTC()}, "barrier")
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst", ManifestName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.manifest.json", SizeBytes: 1, ChecksumSHA256: shaHex("a"), AppliedIndexes: map[string]uint64{"system": 10}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: request.BackupSetID, Node: node, UpdatedAt: time.Now().UTC()}, "node-result")
	manifest := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: request.BackupSetID, ClusterID: "cluster-1", Complete: true, State: clusterbackup.StateSucceeded, ExpectedNodes: 1, ArchiveFormat: backupcore.ArchiveFormatTarZst, Nodes: []clusterbackup.NodeArtifact{node}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupComplete, clusterBackupCompleteRecord{BackupSetID: request.BackupSetID, Manifest: manifest, CompletedAt: time.Now().UTC()}, "complete")
	active, runs := m.clusterBackupSnapshot()
	if active != "" {
		t.Fatalf("active=%q want empty", active)
	}
	if runs[request.BackupSetID].Phase != clusterBackupPhaseSucceeded || runs[request.BackupSetID].Manifest == nil {
		t.Fatalf("run=%#v want succeeded with manifest", runs[request.BackupSetID])
	}
}

func TestBackupRaftStateMachineRejectsUnexpectedNodeResult(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}, "request")
	raw, err := json.Marshal(clusterBackupNodeResultRecord{BackupSetID: "backup-set-1", Node: clusterbackup.NodeArtifact{PodName: "myceld-1", NodeID: "node_2", Ordinal: 1, RaftNodeID: 2}, UpdatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupNodeResult, raw, "unexpected-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "unexpected pod") {
		t.Fatalf("ApplyCommand(unexpected node) error=%v, want unexpected pod", err)
	}
}

func TestBackupRaftStateMachineRejectsInvalidCompletionManifest(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", ClusterID: "cluster-1", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}, "request")
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst", ManifestName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.manifest.json", SizeBytes: 1, ChecksumSHA256: shaHex("a")}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: "backup-set-1", Node: node, UpdatedAt: time.Now().UTC()}, "node-result")
	manifest := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: "backup-set-1", ClusterID: "cluster-1", Complete: true, State: clusterbackup.StateSucceeded, ExpectedNodes: 1, ArchiveFormat: backupcore.ArchiveFormatTarZst, Nodes: []clusterbackup.NodeArtifact{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: node.ArchiveName, ManifestName: node.ManifestName, SizeBytes: 1, ChecksumSHA256: strings.Repeat("0", 64)}}}
	raw, err := json.Marshal(clusterBackupCompleteRecord{BackupSetID: "backup-set-1", Manifest: manifest, CompletedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupComplete, raw, "complete")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "does not match recorded node result") {
		t.Fatalf("ApplyCommand(invalid complete) error=%v, want mismatch", err)
	}
}

func TestBackupRaftStateMachineRejectsDuplicateBackupSetIDAfterTerminal(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupFail, clusterBackupFailureRecord{BackupSetID: "backup-set-1", Phase: clusterBackupPhasePrechecking, Message: "failed", UpdatedAt: time.Now().UTC()}, "fail")
	raw, err := json.Marshal(clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupRequest, raw, "duplicate-request")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("ApplyCommand(duplicate request) error=%v, want already exists", err)
	}
}

func TestBackupRaftStateMachineRejectsSecondActiveClusterBackup(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request-1")
	raw, err := json.Marshal(clusterBackupRequestRecord{BackupSetID: "backup-set-2", CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupRequest, raw, "request-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("ApplyCommand(second request) error=%v, want already active", err)
	}
}

func TestClusterBackupIdempotencyKeyFindsPersistedRun(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", IdempotencyKey: "retry-key", CreatedAt: time.Now().UTC()}, "request")
	status, ok := m.findClusterBackupByIdempotencyKey(" retry-key ")
	if !ok {
		t.Fatal("findClusterBackupByIdempotencyKey() ok=false, want true")
	}
	if status.BackupSetID != "backup-set-1" || status.Phase != string(clusterBackupPhaseRequested) {
		t.Fatalf("status=%#v, want backup-set-1 requested", status)
	}
}

func TestClusterBackupSnapshotCaptureRecordAndManifestEvidence(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	createdAt := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	backupSetID := "backup-set-20261122T100000Z-cluster-test"
	request := clusterBackupRequestRecord{BackupSetID: backupSetID, ClusterID: "cluster-test", CaptureProvider: clusterCaptureProviderCSI, CreatedAt: createdAt, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, request, "request")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupBarrier, clusterBackupBarrierRecord{BackupSetID: backupSetID, Barriers: map[string]uint64{"system": 10}, UpdatedAt: createdAt}, "barrier")
	evidence := mustJSONRaw(map[string]any{"schema": "mycel.kubernetes_snapshot_capture_evidence", "version": 1, "provider": "CSI_VOLUME_SNAPSHOT", "snapshot_class_name": "ebs-csi", "snapshots": []map[string]any{{"pod_name": "myceld-0", "source_pvc_ref": "pvc/myceld-data-myceld-0", "snapshot_ref": "volumesnapshot.snapshot.storage.k8s.io/mycel-snap-0", "ready_to_use": true}}})
	nodeSource := mustJSONRaw(map[string]any{"provider": "CSI_VOLUME_SNAPSHOT", "source_pvc_ref": "pvc/myceld-data-myceld-0", "snapshot_ref": "volumesnapshot.snapshot.storage.k8s.io/mycel-snap-0", "ready_to_use": true})
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupCapture, clusterBackupCaptureRecord{BackupSetID: backupSetID, Provider: clusterCaptureProviderCSI, ProviderVersion: "snapshot.storage.k8s.io/v1", SnapshotClassName: "ebs-csi", DataMover: "kubernetes-csi-volumesnapshot-plus-mycel-daemon-s3-compatible", ProviderEvidence: evidence, NodeSources: map[string]json.RawMessage{"myceld-0": nodeSource}, Cleanup: ClusterSnapshotCleanupResult{Attempted: true, Completed: true, Items: []ClusterSnapshotCleanupItem{{PodName: "myceld-0", ResourceURI: "k8s://backup/volumesnapshot/mycel-snap-0", State: "deleted"}}}, UpdatedAt: createdAt}, "capture")
	archiveName, err := clusterbackup.NewArchiveName(createdAt, "myceld-0", backupSetID, backupcore.ArchiveFormatTar)
	if err != nil {
		t.Fatal(err)
	}
	manifestName, err := clusterbackup.NewNodeManifestName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	inventoryName, err := clusterbackup.NewNodeInventoryName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1, ArchiveName: archiveName, ArchiveURI: "s3://mycel-backups/clusters/test/nodes/myceld-0/" + archiveName, ManifestName: manifestName, ManifestURI: "s3://mycel-backups/clusters/test/nodes/myceld-0/" + manifestName, InventoryName: inventoryName, InventoryURI: "s3://mycel-backups/clusters/test/nodes/myceld-0/" + inventoryName, InventoryChecksumSHA256: shaHex("inventory"), SizeBytes: 123, ChecksumSHA256: shaHex("archive"), AppliedIndexes: map[string]uint64{"system": 10}, RaftFreeze: clusterbackup.RaftFreezeEvidence{LeaseID: "lease-1", AcquiredAt: createdAt, ReleasedAt: createdAt.Add(time.Second), ExpiresAt: createdAt.Add(time.Minute), Groups: map[string]clusterbackup.RaftFreezeGroup{"system": {GroupID: "system", BarrierIndex: 10, AppliedIndex: 10, CommitIndex: 10}}}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: backupSetID, Node: node, UpdatedAt: createdAt}, "node-result")
	usageRaw := []byte(`{"version":1,"accounting_state":"ready","updated_at":"2026-11-22T10:00:00Z","sequence":1}`)
	if err := os.MkdirAll(filepath.Join(m.dataDir, "meta", "usage"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(m.dataDir, "meta", "usage", "counters.json"), usageRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-snap", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}, DevOnly: true})
	input := StartClusterBackupInput{ArchiveFormat: backupcore.ArchiveFormatTar, ClusterID: "cluster-test", ClusterName: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: repositoryBackupSetPrefix(repo, backupSetID), CaptureProvider: clusterCaptureProviderCSI, QuiesceReadProfile: backupcore.DefaultProductionQuiesceReadProfile}
	manifest, err := m.buildProductionBackupSetManifest(input, repo, backupSetID, "cluster-test", createdAt, createdAt.Add(time.Minute), "s3://mycel-backups/clusters/test/backup-set.json")
	if err != nil {
		t.Fatalf("buildProductionBackupSetManifest() error = %v", err)
	}
	if manifest.Capture.Provider != "CSI_VOLUME_SNAPSHOT" || manifest.Capture.SnapshotClassName != "ebs-csi" {
		t.Fatalf("capture=%#v, want CSI snapshot evidence", manifest.Capture)
	}
	if len(manifest.Artifacts.Nodes) != 1 || !bytes.Contains(manifest.Artifacts.Nodes[0].CaptureSource, []byte("mycel-snap-0")) {
		t.Fatalf("node capture source=%s, want snapshot ref", string(manifest.Artifacts.Nodes[0].CaptureSource))
	}
	if manifest.Audit.Labels["capture_provider"] != clusterCaptureProviderCSI {
		t.Fatalf("audit labels=%v, want csi capture provider", manifest.Audit.Labels)
	}
	if manifest.Source.StorageFormatVersions["usage_counters"] != "1" || len(manifest.Consistency.BarrierSet.UsageAccounting) != 1 || manifest.Consistency.BarrierSet.UsageAccounting[0].ValidationState != "INCLUDED_AND_VALIDATED" || manifest.Consistency.BarrierSet.UsageAccounting[0].ChecksumSHA256 != hashBytes(usageRaw) {
		t.Fatalf("usage accounting barrier=%#v storage_versions=%v, want included v1 marker", manifest.Consistency.BarrierSet.UsageAccounting, manifest.Source.StorageFormatVersions)
	}
	raw, err := marshalProductionJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := clusterbackup.ParseBackupSetDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	report := clusterbackup.ValidateBackupSetDocument(ctx, doc, nil, clusterbackup.BackupSetValidationOptions{AllowS3ArtifactURIs: true, ExpectedNodeCount: 1})
	if err := report.Error(); err != nil {
		t.Fatalf("snapshot manifest validation error = %v", err)
	}
}

func TestClusterBackupSnapshotProviderRequiresCaptureEvidence(t *testing.T) {
	m := newInitializedBackupModule(t)
	createdAt := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	backupSetID := "backup-set-missing-snapshot-evidence"
	archiveName, err := clusterbackup.NewArchiveName(createdAt, "myceld-0", backupSetID, backupcore.ArchiveFormatTar)
	if err != nil {
		t.Fatal(err)
	}
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1, ArchiveName: archiveName, ArchiveURI: "s3://mycel-backups/archive.tar", ManifestName: "node.manifest.json", ManifestURI: "s3://mycel-backups/node.manifest.json", InventoryName: "node.inventory.json", InventoryURI: "s3://mycel-backups/node.inventory.json", InventoryChecksumSHA256: shaHex("inventory"), SizeBytes: 1, ChecksumSHA256: shaHex("archive"), AppliedIndexes: map[string]uint64{"system": 10}}
	m.restoreClusterBackupState(backupSetID, map[string]clusterBackupRun{backupSetID: {BackupSetID: backupSetID, ClusterID: "cluster-test", RepositoryID: "repo-snap", RepositoryPrefix: "clusters/test/backup-sets/" + backupSetID, CaptureProvider: clusterCaptureProviderCSI, Phase: clusterBackupPhaseValidating, CreatedAt: createdAt, UpdatedAt: createdAt, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}, Barriers: map[string]uint64{"system": 10}, NodeResults: map[string]clusterbackup.NodeArtifact{"myceld-0": node}}})
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-snap", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}, DevOnly: true})
	_, err = m.buildProductionBackupSetManifest(StartClusterBackupInput{ArchiveFormat: backupcore.ArchiveFormatTar, RepositoryID: repo.RepositoryID, RepositoryPrefix: "clusters/test/backup-sets/" + backupSetID, CaptureProvider: clusterCaptureProviderCSI}, repo, backupSetID, "cluster-test", createdAt, createdAt.Add(time.Minute), "s3://mycel-backups/backup-set.json")
	if err == nil || !strings.Contains(err.Error(), "snapshot capture evidence") {
		t.Fatalf("buildProductionBackupSetManifest() error=%v, want snapshot capture evidence", err)
	}
}

func TestBackupRaftStateMachineFailReleasesActiveClusterBackup(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupFail, clusterBackupFailureRecord{BackupSetID: "backup-set-1", Phase: clusterBackupPhasePrechecking, Message: "node missing", UpdatedAt: time.Now().UTC()}, "fail")
	active, runs := m.clusterBackupSnapshot()
	if active != "" {
		t.Fatalf("active=%q want empty", active)
	}
	if runs["backup-set-1"].Phase != clusterBackupPhaseFailed || runs["backup-set-1"].Failure == nil {
		t.Fatalf("run=%#v want failed", runs["backup-set-1"])
	}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-2", CreatedAt: time.Now().UTC()}, "request-2")
}

func TestClusterBackupPrecheckRejectsUnsetExpectedNodeCount(t *testing.T) {
	input := validPrecheckInput()
	input.ExpectedNodeCount = 0
	result := EvaluateClusterBackupPreconditions(input)
	if result.OK || !strings.Contains(strings.Join(result.Failures, "\n"), "expected node count") {
		t.Fatalf("result=%#v, want expected node count failure", result)
	}
}

func TestClusterBackupPrecheckAcceptsHealthyCluster(t *testing.T) {
	result := EvaluateClusterBackupPreconditions(validPrecheckInput())
	if !result.OK {
		t.Fatalf("precheck failed: %v", result.Failures)
	}
}

func TestClusterBackupPrecheckRejectsUnhealthyCluster(t *testing.T) {
	input := validPrecheckInput()
	input.Nodes[1].Ready = false
	input.Nodes[1].CaughtUp = false
	input.RaftGroups[0].AppliedByNode[2] = 9
	input.Destinations[1].Writable = false
	result := EvaluateClusterBackupPreconditions(input)
	if result.OK {
		t.Fatal("precheck succeeded, want failure")
	}
	joined := strings.Join(result.Failures, "\n")
	for _, want := range []string{"not ready", "not caught up", "behind commit index", "not writable"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("failures %q missing %q", joined, want)
		}
	}
}

func TestClusterBackupPrecheckRejectsActiveBackup(t *testing.T) {
	m := newInitializedBackupModule(t)
	m.restoreClusterBackupState("backup-set-active", map[string]clusterBackupRun{"backup-set-active": {BackupSetID: "backup-set-active", Phase: clusterBackupPhaseRequested}})
	input := validPrecheckInput()
	result := m.EvaluateClusterBackupPreconditions(input)
	if result.OK || !strings.Contains(strings.Join(result.Failures, "\n"), "already active") {
		t.Fatalf("result=%#v, want active backup failure", result)
	}
}

func TestClusterBackupLocalBarriersRequireRaftGroups(t *testing.T) {
	m := newInitializedBackupModule(t)
	if _, err := m.LocalRaftBackupBarriers(); err == nil || !strings.Contains(err.Error(), "raft groups") {
		t.Fatalf("LocalRaftBackupBarriers() error=%v, want raft groups", err)
	}
	if _, err := m.WaitLocalRaftBackupBarriers(context.Background(), map[string]uint64{"system": 1}); err == nil || !strings.Contains(err.Error(), "raft groups") {
		t.Fatalf("WaitLocalRaftBackupBarriers() error=%v, want raft groups", err)
	}
}

func TestClusterBackupDestinationMustBeOutsideDataDir(t *testing.T) {
	dataDir := t.TempDir()
	if err := validateLocalClusterBackupDestination(dataDir, filepath.Join(dataDir, "backups")); err == nil || !strings.Contains(err.Error(), "outside data dir") {
		t.Fatalf("validateLocalClusterBackupDestination() error=%v, want outside data dir", err)
	}
}

func TestClusterBackupLocalArchiveRequestRequiresRecordedMembershipAndBarriers(t *testing.T) {
	m := newInitializedBackupModule(t)
	m.restoreClusterBackupState("backup-set-1", map[string]clusterBackupRun{"backup-set-1": {BackupSetID: "backup-set-1", ClusterID: "cluster-1", Phase: clusterBackupPhaseBarrierWait, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}, Barriers: map[string]uint64{"system": 10}}})
	valid := backendArchiveInputForTest()
	if err := m.validateRecordedLocalClusterBackupRequest(valid, map[string]uint64{"system": 10}); err != nil {
		t.Fatalf("validateRecordedLocalClusterBackupRequest(valid) error=%v", err)
	}
	wrongSet := valid
	wrongSet.BackupSetID = "backup-set-2"
	if err := m.validateRecordedLocalClusterBackupRequest(wrongSet, map[string]uint64{"system": 10}); err == nil || !strings.Contains(err.Error(), "not recorded") {
		t.Fatalf("wrong backup set error=%v, want not recorded", err)
	}
	wrongOrdinal := valid
	wrongOrdinal.Ordinal = 1
	if err := m.validateRecordedLocalClusterBackupRequest(wrongOrdinal, map[string]uint64{"system": 10}); err == nil || !strings.Contains(err.Error(), "ordinal") {
		t.Fatalf("wrong ordinal error=%v, want ordinal", err)
	}
	if err := m.validateRecordedLocalClusterBackupRequest(valid, map[string]uint64{"system": 9}); err == nil || !strings.Contains(err.Error(), "want recorded barrier") {
		t.Fatalf("stale barrier error=%v, want recorded barrier", err)
	}
}

func TestClusterBackupBarriersRejectDuplicates(t *testing.T) {
	_, err := clusterBackupBarriersFromInput([]backend.BackupRaftBarrier{{GroupID: "system", Index: 1}, {GroupID: "system", Index: 2}})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("clusterBackupBarriersFromInput() error=%v, want duplicate", err)
	}
}

func TestRepositoryClusterBackupPublishesManifestAndComplete(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	store := newMemoryRepositoryObjectStore()
	m.repositoryObjectStoreFactory = func(context.Context, BackupRepository) (backupRepositoryObjectStore, error) { return store, nil }
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-phase6", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}, Encryption: EncryptionPolicy{Mode: EncryptionModeSSES3}, DevOnly: true, Validation: BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: time.Now().UTC()}})
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	m.repositories[repo.RepositoryID] = repo
	m.mu.Unlock()
	createdAt := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	completedAt := createdAt.Add(time.Minute)
	backupSetID := "backup-set-20261120T120000Z-cluster-test"
	prefix := repositoryBackupSetPrefix(repo, backupSetID)
	archiveName, err := clusterbackup.NewArchiveName(createdAt, "myceld-0", backupSetID, backupcore.ArchiveFormatTar)
	if err != nil {
		t.Fatal(err)
	}
	manifestName, err := clusterbackup.NewNodeManifestName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	inventoryName, err := clusterbackup.NewNodeInventoryName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	nodePrefix := joinRepositoryKey(prefix, "nodes", "myceld-0")
	archiveKey := joinRepositoryKey(nodePrefix, archiveName)
	archiveBytes := minimalRestoreTarBytes(t, "node-1")
	if _, err := store.PutBytes(ctx, archiveKey, archiveBytes, "application/x-tar"); err != nil {
		t.Fatalf("PutBytes(archive) error = %v", err)
	}
	nodeArtifact := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1, ArchiveName: archiveName, ArchiveURI: repositoryObjectURI(repo, archiveKey), ManifestName: manifestName, ManifestURI: repositoryObjectURI(repo, joinRepositoryKey(nodePrefix, manifestName)), InventoryName: inventoryName, InventoryURI: repositoryObjectURI(repo, joinRepositoryKey(nodePrefix, inventoryName)), InventoryChecksumSHA256: shaHex("inventory"), SizeBytes: int64(len(archiveBytes)), ChecksumSHA256: hashBytes(archiveBytes), AppliedIndexes: map[string]uint64{"system": 10}, RaftFreeze: clusterbackup.RaftFreezeEvidence{LeaseID: "lease-1", AcquiredAt: createdAt, ReleasedAt: completedAt, ExpiresAt: completedAt.Add(time.Minute), Groups: map[string]clusterbackup.RaftFreezeGroup{"system": {GroupID: "system", BarrierIndex: 10, AppliedIndex: 10, CommitIndex: 10, Term: 1, LastIndex: 10, SnapshotIndex: 0, Leader: 1}}}}
	m.restoreClusterBackupState(backupSetID, map[string]clusterBackupRun{backupSetID: {BackupSetID: backupSetID, ClusterID: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", Phase: clusterBackupPhaseValidating, CreatedAt: createdAt, UpdatedAt: createdAt, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}, Barriers: map[string]uint64{"system": 10}, NodeResults: map[string]clusterbackup.NodeArtifact{"myceld-0": nodeArtifact}}})
	input := StartClusterBackupInput{Reason: "phase 6 test", ArchiveFormat: backupcore.ArchiveFormatTar, ClusterID: "cluster-test", ClusterName: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", QuiesceReadProfile: backupcore.DefaultProductionQuiesceReadProfile}
	summary, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt)
	if err != nil {
		t.Fatalf("publishRepositoryClusterBackupSet() error = %v", err)
	}
	if summary.ManifestURI == "" || !strings.HasPrefix(summary.ManifestURI, "s3://mycel-backups/") {
		t.Fatalf("summary manifest uri = %q, want repository s3 uri", summary.ManifestURI)
	}
	manifestKey := joinRepositoryKey(prefix, clusterbackup.BackupSetManifestName)
	completeKey := joinRepositoryKey(prefix, "COMPLETE")
	if _, ok := store.objects[manifestKey]; !ok {
		t.Fatalf("backup-set manifest was not uploaded to %s", manifestKey)
	}
	if _, ok := store.objects[completeKey]; !ok {
		t.Fatalf("COMPLETE marker was not uploaded to %s", completeKey)
	}
	listed, err := m.ListRepositoryClusterBackupSets(ctx, repo.RepositoryID)
	if err != nil {
		t.Fatalf("ListRepositoryClusterBackupSets() error = %v", err)
	}
	if len(listed) != 1 || listed[0].Manifest.BackupSetID != backupSetID || listed[0].ProductionReport == nil || !listed[0].ProductionReport.Valid() {
		t.Fatalf("listed repository backup sets = %#v, want one valid backup set", listed)
	}
	validated, err := m.ValidateRepositoryClusterBackupSet(ctx, repo.RepositoryID, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
	if err != nil {
		t.Fatalf("ValidateRepositoryClusterBackupSet() error = %v", err)
	}
	if validated.ProductionMarker == nil || validated.ProductionMarker.ManifestChecksumSHA256 == "" {
		t.Fatalf("validated marker missing checksum: %#v", validated.ProductionMarker)
	}
	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID})
	if err != nil {
		t.Fatalf("PlanClusterRestore() error = %v", err)
	}
	if !plan.Restorable || plan.RestorePlanID == "" || plan.Capacity.RequiredBytes == 0 || len(plan.DerivedActions) == 0 {
		t.Fatalf("restore plan = %#v, want restorable plan with capacity and derived actions", plan)
	}
	blocked, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModeGenerateNewID})
	if err != nil {
		t.Fatalf("PlanClusterRestore(generate identity) error = %v", err)
	}
	if blocked.Restorable || len(blocked.Blockers) == 0 {
		t.Fatalf("blocked restore plan = %#v, want identity-transform blocker", blocked)
	}
	targetDir := filepath.Join(t.TempDir(), "target-node-0")
	stagingDir := filepath.Join(t.TempDir(), "staging")
	target := ClusterRestoreTarget{ExpectedNodeCount: 1, Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, SourcePodName: "myceld-0", SourceNodeID: "node-1", TargetPodName: "myceld-0", TargetDataDir: targetDir}}}
	staging := ClusterRestoreStagingLocation{StagingURI: stagingDir, CleanBeforeUse: true}
	m.mu.Lock()
	run := m.clusterBackups[backupSetID]
	run.Phase = clusterBackupPhaseSucceeded
	m.clusterBackups[backupSetID] = run
	m.activeClusterBackupID = ""
	m.mu.Unlock()
	execPlan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: target, Staging: staging})
	if err != nil {
		t.Fatalf("PlanClusterRestore(execution target) error = %v", err)
	}
	op, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RestorePlanID: execPlan.RestorePlanID, RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: target, Staging: staging, Preconditions: ClusterRestorePreconditionEvidence{CleanTarget: ClusterRestoreCleanTargetProof{TargetEmpty: true}, SourceFencing: ClusterRestoreSourceFencingEvidence{SourceClusterFenced: true}}})
	if err != nil {
		t.Fatalf("StartClusterRestore() error = %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for op.State != "SUCCEEDED" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		var statusErr error
		op, statusErr = m.ClusterRestoreStatus(op.RestoreOperationID)
		if statusErr != nil {
			t.Fatalf("ClusterRestoreStatus() error = %v", statusErr)
		}
	}
	if op.State != "SUCCEEDED" || op.Progress.VerifiedNodes != 1 {
		t.Fatalf("restore operation = %#v, want succeeded with verified node", op)
	}
	status, err := m.ClusterRestoreStatus(op.RestoreOperationID)
	if err != nil || status.RestoreOperationID != op.RestoreOperationID {
		t.Fatalf("ClusterRestoreStatus()=(%#v,%v), want %s", status, err, op.RestoreOperationID)
	}
	if len(status.CleanupTasks) != 1 || status.CleanupTasks[0].State != "deleted" {
		t.Fatalf("cleanup tasks=%#v, want deleted staging directory", status.CleanupTasks)
	}
	if _, err := os.Stat(restoreOperationStagingPath(stagingDir, op.RestoreOperationID)); !os.IsNotExist(err) {
		t.Fatalf("restore staging dir still exists or stat failed: %v", err)
	}
	restoreListed := m.ListClusterRestores()
	if len(restoreListed) != 1 || restoreListed[0].RestoreOperationID != op.RestoreOperationID {
		t.Fatalf("ListClusterRestores()=%#v, want restore operation", restoreListed)
	}
	canceled, err := m.CancelClusterRestore(ctx, op.RestoreOperationID, "terminal replay")
	if err != nil || canceled.State != "SUCCEEDED" {
		t.Fatalf("CancelClusterRestore(terminal)=(%#v,%v), want succeeded unchanged", canceled, err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "meta", "clustering", "node.json")); err != nil {
		t.Fatalf("restored node identity missing: %v", err)
	}
	failed, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, IdempotencyKey: "missing-preconditions", Target: target, Staging: staging})
	if err != nil {
		t.Fatalf("StartClusterRestore(missing preconditions) error = %v", err)
	}
	if failed.State != "FAILED_REQUIRES_OPERATOR" || len(failed.Blockers) == 0 {
		t.Fatalf("missing-precondition restore operation = %#v, want blockers", failed)
	}
	replayed, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, IdempotencyKey: "missing-preconditions", Target: target, Staging: staging})
	if err != nil {
		t.Fatalf("StartClusterRestore(idempotency replay) error = %v", err)
	}
	if replayed.RestoreOperationID != failed.RestoreOperationID || replayed.State != failed.State {
		t.Fatalf("idempotency replay=%#v, want original %#v", replayed, failed)
	}
	pvcTargetDir := filepath.Join(t.TempDir(), "target-pvc-node-0")
	pvcStagingDir := filepath.Join(t.TempDir(), "pvc-staging")
	pvcTarget := ClusterRestoreTarget{ExpectedNodeCount: 1, Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, SourcePodName: "myceld-0", SourceNodeID: "node-1", TargetPodName: "myceld-0", TargetPVCName: "data-myceld-0", TargetDataDir: pvcTargetDir}}}
	pvcStaging := ClusterRestoreStagingLocation{StagingURI: pvcStagingDir, CleanBeforeUse: true}
	pvcPlan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: pvcTarget, Staging: pvcStaging})
	if err != nil || !pvcPlan.Restorable {
		t.Fatalf("PlanClusterRestore(PVC mapping)=(%#v,%v), want restorable", pvcPlan, err)
	}
	pvcOp, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RestorePlanID: pvcPlan.RestorePlanID, RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: pvcTarget, Staging: pvcStaging, Preconditions: ClusterRestorePreconditionEvidence{CleanTarget: ClusterRestoreCleanTargetProof{TargetEmpty: true}, SourceFencing: ClusterRestoreSourceFencingEvidence{SourceClusterFenced: true}}})
	if err != nil {
		t.Fatalf("StartClusterRestore(PVC mapping) error = %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for pvcOp.State != "SUCCEEDED" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		var statusErr error
		pvcOp, statusErr = m.ClusterRestoreStatus(pvcOp.RestoreOperationID)
		if statusErr != nil {
			t.Fatalf("ClusterRestoreStatus(PVC mapping) error = %v", statusErr)
		}
	}
	if pvcOp.State != "SUCCEEDED" || pvcOp.Progress.VerifiedNodes != 1 {
		t.Fatalf("PVC-mapped restore operation = %#v, want succeeded with verified node", pvcOp)
	}

	purePVCTarget := ClusterRestoreTarget{ExpectedNodeCount: 1, Namespace: "restore-ns", StatefulSet: "myceld", Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, SourcePodName: "myceld-0", SourceNodeID: "node-1", TargetPodName: "myceld-0", TargetPVCName: "data-myceld-helper-0"}}}
	purePVCPlan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: purePVCTarget})
	if err != nil || !purePVCPlan.Restorable {
		t.Fatalf("PlanClusterRestore(pure PVC mapping)=(%#v,%v), want restorable", purePVCPlan, err)
	}
	purePVCBlocked, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RestorePlanID: purePVCPlan.RestorePlanID, RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: purePVCTarget, Preconditions: ClusterRestorePreconditionEvidence{CleanTarget: ClusterRestoreCleanTargetProof{TargetEmpty: true}, SourceFencing: ClusterRestoreSourceFencingEvidence{SourceClusterFenced: true}}})
	if err != nil {
		t.Fatalf("StartClusterRestore(pure PVC without helper) error = %v", err)
	}
	if purePVCBlocked.State != clusterRestoreStateFailedRequiresOp || !hasBlockerReason(purePVCBlocked.Blockers, "target_pvc_restore_requires_mounted_data_dir") {
		t.Fatalf("pure PVC without helper = %#v, want mounted-data-dir/helper blocker", purePVCBlocked)
	}

	helperTargetDir := filepath.Join(t.TempDir(), "helper-pvc")
	m.WithClusterRestorePVCHelper(fakePVCRestoreHelper{targetDirs: map[string]string{"data-myceld-helper-0": helperTargetDir}})
	helperOp, err := m.StartClusterRestore(ctx, StartClusterRestoreInput{RestorePlanID: purePVCPlan.RestorePlanID, RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Target: purePVCTarget, Preconditions: ClusterRestorePreconditionEvidence{CleanTarget: ClusterRestoreCleanTargetProof{TargetEmpty: true}, SourceFencing: ClusterRestoreSourceFencingEvidence{SourceClusterFenced: true}}})
	if err != nil {
		t.Fatalf("StartClusterRestore(pure PVC with helper) error = %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for helperOp.State != "SUCCEEDED" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		var statusErr error
		helperOp, statusErr = m.ClusterRestoreStatus(helperOp.RestoreOperationID)
		if statusErr != nil {
			t.Fatalf("ClusterRestoreStatus(pure PVC helper) error = %v", statusErr)
		}
	}
	if helperOp.State != "SUCCEEDED" || helperOp.Progress.VerifiedNodes != 1 || helperOp.Progress.StagedNodes != 1 || helperOp.Progress.BytesCompleted == 0 {
		t.Fatalf("pure PVC helper restore operation = %#v, want succeeded with progress", helperOp)
	}
	if _, err := os.Stat(filepath.Join(helperTargetDir, "meta", "clustering", "node.json")); err != nil {
		t.Fatalf("helper restored node identity missing: %v", err)
	}
	deleted, err := m.DeleteRepositoryClusterBackupSet(ctx, repo.RepositoryID, backupSetID)
	if err != nil {
		t.Fatalf("DeleteRepositoryClusterBackupSet() error = %v", err)
	}
	if deleted == 0 {
		t.Fatalf("DeleteRepositoryClusterBackupSet deleted zero objects")
	}
	listedAfterDelete, err := m.ListRepositoryClusterBackupSets(ctx, repo.RepositoryID)
	if err != nil {
		t.Fatalf("ListRepositoryClusterBackupSets(after delete) error = %v", err)
	}
	if len(listedAfterDelete) != 0 {
		t.Fatalf("listed repository backup sets after delete = %#v, want none", listedAfterDelete)
	}
}

func TestRepositoryClusterBackupCleansIncompletePrefixWhenCompleteUploadFails(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	store := &failingPutRepositoryObjectStore{memoryRepositoryObjectStore: newMemoryRepositoryObjectStore(), failSuffix: "/COMPLETE"}
	m.repositoryObjectStoreFactory = func(context.Context, BackupRepository) (backupRepositoryObjectStore, error) { return store, nil }
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-cleanup", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}, Encryption: EncryptionPolicy{Mode: EncryptionModeSSES3}, DevOnly: true, Validation: BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: time.Now().UTC()}})
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	m.repositories[repo.RepositoryID] = repo
	m.mu.Unlock()
	createdAt := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	completedAt := createdAt.Add(time.Minute)
	backupSetID := "backup-set-cleanup"
	prefix := repositoryBackupSetPrefix(repo, backupSetID)
	archiveName, err := clusterbackup.NewArchiveName(createdAt, "myceld-0", backupSetID, backupcore.ArchiveFormatTar)
	if err != nil {
		t.Fatal(err)
	}
	manifestName, err := clusterbackup.NewNodeManifestName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	inventoryName, err := clusterbackup.NewNodeInventoryName(archiveName)
	if err != nil {
		t.Fatal(err)
	}
	nodePrefix := joinRepositoryKey(prefix, "nodes", "myceld-0")
	archiveKey := joinRepositoryKey(nodePrefix, archiveName)
	archiveBytes := minimalRestoreTarBytes(t, "node-1")
	if _, err := store.PutBytes(ctx, archiveKey, archiveBytes, "application/x-tar"); err != nil {
		t.Fatalf("PutBytes(archive) error = %v", err)
	}
	nodeArtifact := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1, ArchiveName: archiveName, ArchiveURI: repositoryObjectURI(repo, archiveKey), ManifestName: manifestName, ManifestURI: repositoryObjectURI(repo, joinRepositoryKey(nodePrefix, manifestName)), InventoryName: inventoryName, InventoryURI: repositoryObjectURI(repo, joinRepositoryKey(nodePrefix, inventoryName)), InventoryChecksumSHA256: shaHex("inventory"), SizeBytes: int64(len(archiveBytes)), ChecksumSHA256: hashBytes(archiveBytes), AppliedIndexes: map[string]uint64{"system": 10}, RaftFreeze: clusterbackup.RaftFreezeEvidence{LeaseID: "lease-1", AcquiredAt: createdAt, ReleasedAt: completedAt, ExpiresAt: completedAt.Add(time.Minute), Groups: map[string]clusterbackup.RaftFreezeGroup{"system": {GroupID: "system", BarrierIndex: 10, AppliedIndex: 10, CommitIndex: 10, Term: 1, LastIndex: 10, SnapshotIndex: 0, Leader: 1}}}}
	m.restoreClusterBackupState(backupSetID, map[string]clusterBackupRun{backupSetID: {BackupSetID: backupSetID, ClusterID: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", Phase: clusterBackupPhaseValidating, CreatedAt: createdAt, UpdatedAt: createdAt, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}, Barriers: map[string]uint64{"system": 10}, NodeResults: map[string]clusterbackup.NodeArtifact{"myceld-0": nodeArtifact}}})
	input := StartClusterBackupInput{Reason: "cleanup test", ArchiveFormat: backupcore.ArchiveFormatTar, ClusterID: "cluster-test", ClusterName: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", QuiesceReadProfile: backupcore.DefaultProductionQuiesceReadProfile}
	if _, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt); err == nil || !strings.Contains(err.Error(), "upload COMPLETE marker") {
		t.Fatalf("publishRepositoryClusterBackupSet() error=%v, want COMPLETE upload failure", err)
	}
	keys, err := store.ListKeys(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("incomplete repository prefix was not cleaned, remaining keys=%v", keys)
	}
}

func TestClusterRestoreStartReconcilesInterruptedOperation(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	stagingDir := filepath.Join(t.TempDir(), "restore-op-interrupted")
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	targetDir := filepath.Join(t.TempDir(), "target-node-0")
	tmpDir := restoreTempDataDirPath(targetDir, "restore-op-interrupted", 0)
	if err := os.MkdirAll(filepath.Join(tmpDir, "meta", "clustering"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, "meta", "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "meta", "clustering", "node.json"), []byte(`{"node_id":"node-1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "meta", "raft", "hard_state.pb"), []byte("raft"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	m.restoreClusterRestoreState("restore-op-interrupted", map[string]ClusterRestoreOperation{"restore-op-interrupted": {RestoreOperationID: "restore-op-interrupted", RepositoryID: "repo-1", BackupSetID: "backup-set-1", State: clusterRestoreStateApplyingArtifacts, CreatedAt: now, UpdatedAt: now, Target: ClusterRestoreTarget{Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, TargetDataDir: targetDir}}}, CleanupTasks: []CleanupTask{{TaskID: "staging-directory", ResourceKind: "local_directory", ResourceURI: stagingDir, State: "created"}}}})
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() reconcile interrupted restore error = %v", err)
	}
	op, err := m.ClusterRestoreStatus("restore-op-interrupted")
	if err != nil {
		t.Fatalf("ClusterRestoreStatus(interrupted) error = %v", err)
	}
	if op.State != clusterRestoreStateFailedRequiresOp || len(op.Blockers) == 0 {
		t.Fatalf("interrupted restore op=%#v, want failed-requires-operator blocker", op)
	}
	if len(op.CleanupTasks) != 1 || op.CleanupTasks[0].State != "deleted" {
		t.Fatalf("interrupted cleanup tasks=%#v, want deleted", op.CleanupTasks)
	}
	if _, err := os.Stat(stagingDir); !os.IsNotExist(err) {
		t.Fatalf("interrupted staging dir still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "meta", "clustering", "node.json")); err != nil {
		t.Fatalf("interrupted temp restore was not committed to target dir: %v", err)
	}
	if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
		t.Fatalf("interrupted temp restore dir still exists or stat failed: %v", err)
	}
}

func backendArchiveInputForTest() backend.CreateLocalBackupArchiveInput {
	return backend.CreateLocalBackupArchiveInput{ClusterID: "cluster-1", BackupSetID: "backup-set-1", PodName: "myceld-0", NodeID: "node_1", RaftNodeID: 1, Ordinal: 0}
}

func applyClusterRecord(t *testing.T, ctx context.Context, m *Module, typ wal.RecordType, payload any, id string) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(typ, raw, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand(%s) error = %v", id, err)
	}
}

func newInitializedBackupModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule()
	if result := m.Init(context.Background(), &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	m.PrepareExperimentalRaftMode()
	return m
}

func validPrecheckInput() ClusterBackupPrecheckInput {
	return ClusterBackupPrecheckInput{
		BackupSetID:       "backup-set-1",
		ClusterID:         "cluster-1",
		ExpectedNodeCount: 2,
		Nodes: []ClusterBackupPrecheckNode{
			{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, BackendAdvertiseAddr: "myceld-0:9091", ClusterID: "cluster-1", Reachable: true, Ready: true, Admitted: true, CaughtUp: true},
			{PodName: "myceld-1", NodeID: "node_2", Ordinal: 1, RaftNodeID: 2, BackendAdvertiseAddr: "myceld-1:9091", ClusterID: "cluster-1", Reachable: true, Ready: true, Admitted: true, CaughtUp: true},
		},
		RaftGroups: []ClusterBackupPrecheckRaftGroup{{GroupID: consensus.SystemGroupID, Leader: 1, HasQuorum: true, CommitIndex: 10, AppliedByNode: map[uint64]uint64{1: 10, 2: 10}}},
		Destinations: []ClusterBackupDestinationCheck{
			{PodName: "myceld-0", Path: "/mnt/backups/myceld-0", Writable: true, OutsideDataDir: true},
			{PodName: "myceld-1", Path: "/mnt/backups/myceld-1", Writable: true, OutsideDataDir: true},
		},
	}
}

func hasBlockerReason(blockers []ValidationBlocker, reason string) bool {
	for _, blocker := range blockers {
		if blocker.Reason == reason {
			return true
		}
	}
	return false
}

type fakePVCRestoreHelper struct {
	targetDirs map[string]string
}

func (h fakePVCRestoreHelper) RestoreRepositoryBackupNodeToPVC(ctx context.Context, req ClusterRestorePVCHelperRequest) (ClusterRestorePVCHelperResult, error) {
	if req.store == nil {
		return ClusterRestorePVCHelperResult{}, fmt.Errorf("repository object store is required")
	}
	pvcName := strings.TrimSpace(req.Node.TargetPVCName)
	targetDir := strings.TrimSpace(h.targetDirs[pvcName])
	if targetDir == "" {
		return ClusterRestorePVCHelperResult{}, fmt.Errorf("target dir for pvc %s is not configured", pvcName)
	}
	archiveKey, err := repositoryKeyFromS3URI(req.Repository, req.Artifact.ArchiveURI)
	if err != nil {
		return ClusterRestorePVCHelperResult{}, err
	}
	archivePath := filepath.Join(os.TempDir(), repositorySafePathComponent(req.RestoreOperationID)+"-"+req.Artifact.ArchiveName)
	defer os.Remove(archivePath)
	info, err := req.store.GetFile(ctx, archiveKey, archivePath)
	if err != nil {
		return ClusterRestorePVCHelperResult{}, err
	}
	if err := applyRestoreArchiveToMountedDataDir(ctx, req.ArchiveFormat, archivePath, targetDir); err != nil {
		return ClusterRestorePVCHelperResult{}, err
	}
	return ClusterRestorePVCHelperResult{TargetPVCName: pvcName, TargetDataDir: targetDir, Verified: true, BytesCompleted: info.SizeBytes}, nil
}

type memoryRepositoryObjectStore struct {
	objects map[string][]byte
}

func newMemoryRepositoryObjectStore() *memoryRepositoryObjectStore {
	return &memoryRepositoryObjectStore{objects: map[string][]byte{}}
}

type failingPutRepositoryObjectStore struct {
	*memoryRepositoryObjectStore
	failSuffix string
}

func (s *failingPutRepositoryObjectStore) PutBytes(ctx context.Context, key string, data []byte, contentType string) (repositoryObjectInfo, error) {
	if strings.HasSuffix(cleanRepositoryObjectKey(key), strings.TrimPrefix(s.failSuffix, "/")) || strings.HasSuffix(cleanRepositoryObjectKey(key), s.failSuffix) {
		return repositoryObjectInfo{}, fmt.Errorf("injected put failure for %s", key)
	}
	return s.memoryRepositoryObjectStore.PutBytes(ctx, key, data, contentType)
}

func (s *memoryRepositoryObjectStore) PutFile(ctx context.Context, key, path string) (repositoryObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return repositoryObjectInfo{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	return s.PutBytes(ctx, key, raw, "")
}

func (s *memoryRepositoryObjectStore) PutBytes(ctx context.Context, key string, data []byte, contentType string) (repositoryObjectInfo, error) {
	_ = contentType
	if err := ctx.Err(); err != nil {
		return repositoryObjectInfo{}, err
	}
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return repositoryObjectInfo{}, fmt.Errorf("key is required")
	}
	s.objects[key] = append([]byte(nil), data...)
	return repositoryObjectInfo{Key: key, URI: "s3://mycel-backups/" + key, SizeBytes: int64(len(data)), ChecksumSHA256: hashBytes(data)}, nil
}

func (s *memoryRepositoryObjectStore) GetBytes(ctx context.Context, key string, limit int64) ([]byte, repositoryObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, repositoryObjectInfo{}, err
	}
	key = cleanRepositoryObjectKey(key)
	raw, ok := s.objects[key]
	if !ok {
		return nil, repositoryObjectInfo{}, fmt.Errorf("object %s not found", key)
	}
	if limit > 0 && int64(len(raw)) > limit {
		return nil, repositoryObjectInfo{}, fmt.Errorf("object %s exceeds limit", key)
	}
	out := append([]byte(nil), raw...)
	return out, repositoryObjectInfo{Key: key, URI: "s3://mycel-backups/" + key, SizeBytes: int64(len(out)), ChecksumSHA256: hashBytes(out)}, nil
}

func (s *memoryRepositoryObjectStore) GetFile(ctx context.Context, key, path string) (repositoryObjectInfo, error) {
	raw, info, err := s.GetBytes(ctx, key, 0)
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return repositoryObjectInfo{}, err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return repositoryObjectInfo{}, err
	}
	return info, nil
}

func (s *memoryRepositoryObjectStore) Head(ctx context.Context, key string) (repositoryObjectInfo, error) {
	_, info, err := s.GetBytes(ctx, key, 0)
	return info, err
}

func (s *memoryRepositoryObjectStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix = cleanRepositoryObjectKey(prefix)
	keys := []string{}
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *memoryRepositoryObjectStore) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	keys, err := s.ListKeys(ctx, prefix)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range keys {
		delete(s.objects, key)
		deleted++
	}
	return deleted, nil
}

func minimalRestoreTarBytes(t *testing.T, nodeID string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	entries := map[string]string{
		"meta/clustering/node.json": fmt.Sprintf(`{"node_id":%q}`, nodeID),
		"meta/raft/hard_state.pb":   "raft-state",
	}
	for name, body := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body))}); err != nil {
			t.Fatalf("write tar header %s: %v", name, err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("write tar body %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

func shaHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
