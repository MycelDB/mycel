package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

func TestRepositoryRetentionPlanKeepsNewestAndKeepForDeletesOlder(t *testing.T) {
	ctx := context.Background()
	m, repo, store := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-72*time.Hour))
	middle := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-48*time.Hour))
	newest := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-2*time.Hour))
	plan, err := m.PlanRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repo.RepositoryID, Now: now, Policy: backupcore.RetentionPolicy{KeepLastSuccessful: 1, KeepFor: 24 * time.Hour, DeleteIncompleteAfter: 24 * time.Hour}})
	if err != nil {
		t.Fatalf("PlanRepositoryRetention() error = %v", err)
	}
	if plan.DryRun != true || len(plan.Actions) != 3 {
		t.Fatalf("plan=%#v, want dry-run with 3 actions", plan)
	}
	byID := retentionActionsByID(plan.Actions)
	if byID[newest].Action != RetentionActionKeep || byID[newest].Reason != "keep_last_successful" {
		t.Fatalf("newest action=%#v, want keep_last_successful", byID[newest])
	}
	if byID[middle].Action != RetentionActionDelete || byID[middle].Reason != "retention_expired" {
		t.Fatalf("middle action=%#v, want delete expired", byID[middle])
	}
}

func TestRepositoryRetentionPlanNoOpKeepsAllWithinPolicy(t *testing.T) {
	ctx := context.Background()
	m, repo, store := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	older := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-2*time.Hour))
	newer := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-time.Hour))
	plan, err := m.PlanRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repo.RepositoryID, Now: now, Policy: backupcore.RetentionPolicy{KeepLastSuccessful: 5, KeepFor: 24 * time.Hour, DeleteIncompleteAfter: 24 * time.Hour}})
	if err != nil {
		t.Fatalf("PlanRepositoryRetention() error = %v", err)
	}
	if len(plan.Actions) != 2 || plan.DeletedObjects != 0 || len(plan.Warnings) != 0 {
		t.Fatalf("plan=%#v, want two keep actions and no warnings/deletes", plan)
	}
	if plan.Actions[0].BackupSetID != newer || plan.Actions[1].BackupSetID != older {
		t.Fatalf("actions=%#v, want deterministic newest-first completed order", plan.Actions)
	}
	for _, action := range plan.Actions {
		if action.Action != RetentionActionKeep {
			t.Fatalf("action=%#v, want keep", action)
		}
	}
}

func TestRepositoryRetentionApplySkipsActiveRestoreReference(t *testing.T) {
	ctx := context.Background()
	m, repo, store := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	oldID := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-72*time.Hour))
	seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-time.Hour))
	m.clusterRestores = map[string]ClusterRestoreOperation{"restore-1": {RestoreOperationID: "restore-1", RepositoryID: repo.RepositoryID, BackupSetID: oldID, State: clusterRestoreStateStagingArtifacts}}
	plan, err := m.ApplyRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repo.RepositoryID, Now: now, Policy: backupcore.RetentionPolicy{KeepLastSuccessful: 1, KeepFor: time.Hour, DeleteIncompleteAfter: 24 * time.Hour}})
	if err != nil {
		t.Fatalf("ApplyRepositoryRetention() error = %v", err)
	}
	action := retentionActionsByID(plan.Actions)[oldID]
	if action.Action != RetentionActionBlocked || !action.Blocked || plan.DeletedObjects != 0 {
		t.Fatalf("action=%#v deleted=%d, want active blocker", action, plan.DeletedObjects)
	}
	if _, ok := store.objects[joinRepositoryKey(repositoryBackupSetPrefix(repo, oldID), "COMPLETE")]; !ok {
		t.Fatalf("active backup set was deleted")
	}
}

func TestRepositoryRetentionApplyDeletesExpiredAndAgedIncomplete(t *testing.T) {
	ctx := context.Background()
	m, repo, store := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	oldID := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-72*time.Hour))
	newID := seedRepositoryBackupSet(t, ctx, m, repo, store, now.Add(-1*time.Hour))
	incompleteID := "backup-set-20261118T120000Z-cluster-test"
	if _, err := store.PutBytes(ctx, joinRepositoryKey(repositoryBackupSetPrefix(repo, incompleteID), "nodes", "myceld-0", "partial.tar"), []byte("partial"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	plan, err := m.ApplyRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repo.RepositoryID, Now: now, Policy: backupcore.RetentionPolicy{KeepLastSuccessful: 1, KeepFor: 24 * time.Hour, DeleteIncompleteAfter: 24 * time.Hour}})
	if err != nil {
		t.Fatalf("ApplyRepositoryRetention() error = %v", err)
	}
	byID := retentionActionsByID(plan.Actions)
	if byID[oldID].Action != RetentionActionDelete || byID[oldID].DeletedObjects == 0 {
		t.Fatalf("old action=%#v, want delete with objects", byID[oldID])
	}
	if byID[incompleteID].Action != RetentionActionDeleteIncomplete || byID[incompleteID].DeletedObjects == 0 {
		t.Fatalf("incomplete action=%#v, want incomplete delete", byID[incompleteID])
	}
	if byID[newID].Action != RetentionActionKeep {
		t.Fatalf("new action=%#v, want keep", byID[newID])
	}
	for key := range store.objects {
		if strings.Contains(key, oldID) || strings.Contains(key, incompleteID) {
			t.Fatalf("expired key remained after retention: %s", key)
		}
	}
}

func TestRepositoryRetentionApplyCapturesPartialDeletionFailure(t *testing.T) {
	ctx := context.Background()
	m, repo, baseStore := newRetentionRepositoryFixture(t)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	oldID := seedRepositoryBackupSet(t, ctx, m, repo, baseStore, now.Add(-72*time.Hour))
	seedRepositoryBackupSet(t, ctx, m, repo, baseStore, now.Add(-time.Hour))
	store := &partialDeleteRepositoryObjectStore{memoryRepositoryObjectStore: baseStore, failPrefix: repositoryBackupSetPrefix(repo, oldID), failAfter: 1}
	m.repositoryObjectStoreFactory = func(context.Context, BackupRepository) (backupRepositoryObjectStore, error) { return store, nil }
	plan, err := m.ApplyRepositoryRetention(ctx, RepositoryRetentionPlanOptions{RepositoryID: repo.RepositoryID, Now: now, Policy: backupcore.RetentionPolicy{KeepLastSuccessful: 1, KeepFor: time.Hour, DeleteIncompleteAfter: 24 * time.Hour}})
	if err != nil {
		t.Fatalf("ApplyRepositoryRetention() error = %v", err)
	}
	action := retentionActionsByID(plan.Actions)[oldID]
	if action.Action != RetentionActionDelete || action.DeletedObjects != 1 || action.Error == "" || len(plan.Warnings) == 0 {
		t.Fatalf("action=%#v warnings=%v, want partial delete warning", action, plan.Warnings)
	}
}

func newRetentionRepositoryFixture(t *testing.T) (*Module, BackupRepository, *memoryRepositoryObjectStore) {
	t.Helper()
	m := newInitializedBackupModule(t)
	store := newMemoryRepositoryObjectStore()
	m.repositoryObjectStoreFactory = func(context.Context, BackupRepository) (backupRepositoryObjectStore, error) { return store, nil }
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-retention", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}, Encryption: EncryptionPolicy{Mode: EncryptionModeSSES3}, DevOnly: true, Validation: BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: time.Now().UTC()}})
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	m.repositories[repo.RepositoryID] = repo
	m.mu.Unlock()
	return m, repo, store
}

func seedRepositoryBackupSet(t *testing.T, ctx context.Context, m *Module, repo BackupRepository, store *memoryRepositoryObjectStore, createdAt time.Time) string {
	t.Helper()
	completedAt := createdAt.Add(time.Minute)
	backupSetID := newClusterBackupSetID(createdAt, "cluster-test")
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
	input := StartClusterBackupInput{Reason: "retention test", ArchiveFormat: backupcore.ArchiveFormatTar, ClusterID: "cluster-test", ClusterName: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", QuiesceReadProfile: backupcore.DefaultProductionQuiesceReadProfile}
	if _, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt); err != nil {
		t.Fatalf("publishRepositoryClusterBackupSet(%s) error = %v", backupSetID, err)
	}
	m.restoreClusterBackupState("", map[string]clusterBackupRun{})
	return backupSetID
}

func retentionActionsByID(actions []RepositoryRetentionAction) map[string]RepositoryRetentionAction {
	out := map[string]RepositoryRetentionAction{}
	for _, action := range actions {
		out[action.BackupSetID] = action
	}
	return out
}

type partialDeleteRepositoryObjectStore struct {
	*memoryRepositoryObjectStore
	failPrefix string
	failAfter  int
}

func (s *partialDeleteRepositoryObjectStore) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	keys, err := s.ListKeys(ctx, prefix)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range keys {
		if strings.HasPrefix(key, s.failPrefix) && deleted >= s.failAfter {
			return deleted, fmt.Errorf("injected partial delete failure")
		}
		delete(s.objects, key)
		deleted++
	}
	return deleted, nil
}
