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
