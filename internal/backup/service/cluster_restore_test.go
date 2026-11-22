package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/encryption"
)

func TestRestoreArchiveExtractionPathDecryptsEncryptedArchive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	if err := os.MkdirAll(filepath.Join(source, "meta", "clustering"), 0o700); err != nil {
		t.Fatalf("mkdir clustering: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "meta", "clustering", "node.json"), []byte(`{"node_id":"node_1"}`), 0o600); err != nil {
		t.Fatalf("write node identity: %v", err)
	}
	ts := time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC)
	archiveName, err := clusterbackup.NewArchiveName(ts, "myceld-a", "backup-set-1", backupcore.ArchiveFormatTar)
	if err != nil {
		t.Fatalf("NewArchiveName() error = %v", err)
	}
	archivePath := filepath.Join(dir, archiveName)
	if err := backupcore.WriteArchive(ctx, backupcore.ArchiveFormatTar, source, archivePath); err != nil {
		t.Fatalf("WriteArchive() error = %v", err)
	}
	enc, err := encryption.NewService(ctx, encryption.Config{AtRest: encryption.ModeEnabled, KEKProvider: encryption.ProviderStaticEnv, StaticKeyB64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	aad, err := encryptedBackupArchiveAAD(backupcore.ArchiveFormatTar, archiveName)
	if err != nil {
		t.Fatalf("encryptedBackupArchiveAAD() error = %v", err)
	}
	plain, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	ciphertext, err := enc.EncryptRecord(ctx, plain, aad)
	if err != nil {
		t.Fatalf("EncryptRecord() error = %v", err)
	}
	if err := os.WriteFile(archivePath, ciphertext, 0o600); err != nil {
		t.Fatalf("write encrypted archive: %v", err)
	}

	m := &Module{encryption: enc}
	extractPath, cleanup, err := m.restoreArchiveExtractionPath(ctx, backupcore.ArchiveFormatTar, clusterbackup.BackupSetNodeArtifact{ArchiveName: archiveName}, archivePath)
	if err != nil {
		t.Fatalf("restoreArchiveExtractionPath() error = %v", err)
	}
	if cleanup == nil {
		t.Fatalf("restoreArchiveExtractionPath() cleanup=nil, want decrypted temp cleanup")
	}
	defer cleanup()
	if extractPath == archivePath {
		t.Fatalf("restoreArchiveExtractionPath() returned encrypted source path")
	}
	dest := filepath.Join(dir, "dest")
	if err := clusterbackup.ExtractArchive(ctx, backupcore.ArchiveFormatTar, extractPath, dest); err != nil {
		t.Fatalf("ExtractArchive() decrypted archive error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "meta", "clustering", "node.json")); err != nil {
		t.Fatalf("decrypted archive missing restored node metadata: %v", err)
	}
}

func TestRestoreExecutionBlockersAllowControllerManagedPVCRestore(t *testing.T) {
	plan := ClusterRestorePlan{
		RestorePlanID: "plan-1",
		TargetMode:    ClusterRestoreTargetModeNewClusterBootstrap,
		IdentityMode:  ClusterRestoreIdentityModePreserveBackupID,
		Target: ClusterRestoreTarget{StatefulSet: "myceld", Nodes: []ClusterRestoreNodeMapping{
			{Ordinal: 0, TargetPVCName: "data-myceld-0"},
		}},
	}
	input := StartClusterRestoreInput{RestorePlanID: "plan-1"}
	blockers := restoreExecutionBlockersWithPVCHelper(plan, input, true, true, true)
	if hasBlockerReason(blockers, "target_pvc_restore_requires_mounted_data_dir") || hasBlockerReason(blockers, "clean_target_proof_required") || hasBlockerReason(blockers, "source_fencing_required") {
		t.Fatalf("controller-managed PVC restore should not require external PVC mount/clean-target/source-fencing proofs: %#v", blockers)
	}
}

func TestRestoreExecutionBlockersRequireFenceWhenControllerCannotScaleDown(t *testing.T) {
	plan := ClusterRestorePlan{
		RestorePlanID: "plan-1",
		TargetMode:    ClusterRestoreTargetModeNewClusterBootstrap,
		IdentityMode:  ClusterRestoreIdentityModePreserveBackupID,
		Target: ClusterRestoreTarget{StatefulSet: "myceld", Nodes: []ClusterRestoreNodeMapping{
			{Ordinal: 0, TargetPVCName: "data-myceld-0"},
		}},
	}
	input := StartClusterRestoreInput{RestorePlanID: "plan-1"}
	blockers := restoreExecutionBlockersWithPVCHelper(plan, input, true, true, false)
	if !hasBlockerReason(blockers, "source_fencing_required") {
		t.Fatalf("blockers=%#v, want source_fencing_required", blockers)
	}
}

func TestRestoreCleanTargetProofMappingBlockers(t *testing.T) {
	target := ClusterRestoreTarget{Nodes: []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-1", TargetDataDir: "/mnt/restore/1"},
	}}
	proof := ClusterRestoreCleanTargetProof{TargetEmpty: true, CheckedNodes: []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-other", TargetDataDir: "/mnt/restore/other"},
	}}
	blockers := restoreCleanTargetProofMappingBlockers(target, proof)
	if !hasBlockerReason(blockers, "clean_target_proof_mismatch") {
		t.Fatalf("blockers=%#v, want clean_target_proof_mismatch", blockers)
	}

	proof.CheckedNodes = proof.CheckedNodes[:1]
	blockers = restoreCleanTargetProofMappingBlockers(target, proof)
	if !hasBlockerReason(blockers, "clean_target_proof_incomplete") {
		t.Fatalf("blockers=%#v, want clean_target_proof_incomplete", blockers)
	}

	proof.CheckedNodes = []ClusterRestoreNodeMapping{
		{Ordinal: 0, TargetPVCName: "data-myceld-0", TargetDataDir: "/mnt/restore/0"},
		{Ordinal: 1, TargetPVCName: "data-myceld-1", TargetDataDir: "/mnt/restore/1"},
	}
	if blockers := restoreCleanTargetProofMappingBlockers(target, proof); len(blockers) != 0 {
		t.Fatalf("blockers=%#v, want none", blockers)
	}
}

func TestRestoreExecutionCapacityBlockers(t *testing.T) {
	staging := t.TempDir()
	target := filepath.Join(t.TempDir(), "node-0")
	plan := ClusterRestorePlan{Capacity: clusterbackup.BackupSetCapacityEstimate{RequiredBytes: 4096}, Staging: ClusterRestoreStagingLocation{AvailableBytes: 1024}}
	mappings := []ClusterRestoreNodeMapping{{Ordinal: 0, TargetDataDir: target}}
	artifacts := map[int]clusterbackup.BackupSetNodeArtifact{0: {Ordinal: 0, SizeBytes: "2048"}}
	blockers := restoreExecutionCapacityBlockers(plan, mappings, artifacts, staging)
	if !hasBlockerReason(blockers, "restore_staging_capacity_exhausted") {
		t.Fatalf("blockers=%#v, want restore_staging_capacity_exhausted", blockers)
	}
}

func TestClusterRestorePruneIDsKeepsNewestTerminalAndActive(t *testing.T) {
	now := time.Now().UTC()
	ops := map[string]ClusterRestoreOperation{
		"old":    {RestoreOperationID: "old", State: clusterRestoreStateSucceeded, CreatedAt: now.Add(-4 * time.Hour), CompletedAt: now.Add(-3 * time.Hour)},
		"new":    {RestoreOperationID: "new", State: clusterRestoreStateFailed, CreatedAt: now.Add(-2 * time.Hour), CompletedAt: now.Add(-time.Hour)},
		"active": {RestoreOperationID: "active", State: clusterRestoreStateStagingArtifacts, CreatedAt: now},
	}
	ids := clusterRestorePruneIDs(ops, "active", 1)
	if len(ids) != 1 || ids[0] != "old" {
		t.Fatalf("clusterRestorePruneIDs()=%v, want [old]", ids)
	}
}

func TestClusterRestoreCanResumeAfterRestartRequiresOwnerAndSafePhase(t *testing.T) {
	dataDir := t.TempDir()
	m := &Module{dataDir: dataDir}
	executorID, _, _, _ := m.clusterRestoreExecutorIdentity()
	op := ClusterRestoreOperation{RestoreOperationID: "restore-op", ExecutorID: executorID, State: clusterRestoreStateStagingArtifacts, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID, Progress: ClusterRestoreProgress{Nodes: []ClusterRestoreNodeProgress{{Ordinal: 0, State: "staged"}}}}
	if !m.clusterRestoreCanResumeAfterRestart(op) {
		t.Fatalf("clusterRestoreCanResumeAfterRestart()=false, want true")
	}
	op.State = clusterRestoreStateApplyingArtifacts
	if m.clusterRestoreCanResumeAfterRestart(op) {
		t.Fatalf("clusterRestoreCanResumeAfterRestart(applying)=true, want false")
	}
	op.State = clusterRestoreStateStagingArtifacts
	op.ExecutorID = "data-dir:/other"
	if m.clusterRestoreCanResumeAfterRestart(op) {
		t.Fatalf("clusterRestoreCanResumeAfterRestart(wrong owner)=true, want false")
	}
}

func TestCancelClusterRestoreWithoutRunnerRequiresOperatorDuringApply(t *testing.T) {
	m := NewModule(Config{StatusHistoryLimit: 20})
	now := time.Now().UTC()
	m.restoreClusterRestoreState("restore-op", map[string]ClusterRestoreOperation{"restore-op": {RestoreOperationID: "restore-op", State: clusterRestoreStateApplyingArtifacts, CreatedAt: now, UpdatedAt: now, Progress: ClusterRestoreProgress{AppliedNodes: 1}}})
	canceled, err := m.CancelClusterRestore(context.Background(), "restore-op", "operator requested cancel")
	if err != nil {
		t.Fatalf("CancelClusterRestore() error=%v", err)
	}
	if canceled.State != clusterRestoreStateFailedRequiresOp || !hasBlockerReason(canceled.Blockers, "restore_cancel_requires_operator_validation") {
		t.Fatalf("CancelClusterRestore()=%#v, want failed-requires-operator cancellation blocker", canceled)
	}
}

func TestValidateRestoredDataDirForOperationEvidence(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "meta", "clustering"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "meta", "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta", "clustering", "node.json"), []byte(`{"node_id":"node-1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta", "raft", "hard_state.pb"), []byte("raft"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan := ClusterRestorePlan{IdentityMode: ClusterRestoreIdentityModePreserveBackupID, AuthSessionPolicy: ClusterRestoreAuthSessionPolicyPreserveRestoredSessions, ProductionManifest: &clusterbackup.BackupSetManifestV1{Consistency: clusterbackup.BackupSetConsistency{BarrierSet: clusterbackup.CheckpointBarrierSetV1{SystemMetadata: clusterbackup.CheckpointSystemMetadata{ClusterID: "cluster-a", BootstrapEpoch: "1", SystemRaftGroupID: "system", SystemRaftBarrierIndex: "10"}, Identity: clusterbackup.CheckpointIdentity{IdentityRaftGroupID: "system", IdentityRaftBarrierIndex: "10", AuthorizationRevision: "9", RefreshSessionStoreChecksumSHA256: "abc", AuthSessionPolicy: ClusterRestoreAuthSessionPolicyPreserveRestoredSessions}}}, DerivedState: []clusterbackup.BackupSetDerivedState{{Subsystem: "LEXICAL_SEARCH", RestorePolicy: "REBUILD_REQUIRED", ValidationState: "REBUILD_REQUIRED"}, {Subsystem: "SEMANTIC_VECTOR_SEARCH", Included: true, RestorePolicy: "PRESERVED", ValidationState: "PASSED"}}}}
	evidence, err := validateRestoredDataDirForOperation(dataDir, plan, ClusterRestoreNodeMapping{Ordinal: 0, SourceNodeID: "node-1", SourcePodName: "myceld-0"}, clusterbackup.BackupSetNodeArtifact{Ordinal: 0, NodeID: "node-1", PodName: "myceld-0", RaftNodeID: "1"})
	if err != nil {
		t.Fatalf("validateRestoredDataDirForOperation() error=%v", err)
	}
	if evidence["post_restore_validation"] != "passed" || evidence["restored_node_id"] != "node-1" || evidence["refresh_session_checksum_present"] != "true" || evidence["derived_rebuild_required_subsystems"] != "lexical_search" {
		t.Fatalf("evidence=%#v", evidence)
	}
	_, err = validateRestoredDataDirForOperation(dataDir, plan, ClusterRestoreNodeMapping{Ordinal: 0, SourceNodeID: "other-node"}, clusterbackup.BackupSetNodeArtifact{Ordinal: 0, NodeID: "other-node"})
	if err == nil {
		t.Fatalf("validateRestoredDataDirForOperation(node mismatch) error=nil, want error")
	}
}
