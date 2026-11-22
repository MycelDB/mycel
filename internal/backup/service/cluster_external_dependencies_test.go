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

func TestRepositoryClusterBackupPublishesExternalObjectDependencyManifest(t *testing.T) {
	ctx := context.Background()
	m, store, repo, backupSetID, prefix, input, createdAt, completedAt := seedRepositoryClusterBackupForExternalDependencyTest(t)
	provider := fakeExternalObjectDependencyProvider{deps: []clusterbackup.ExternalObjectStoreDependency{testExternalObjectDependency(backupSetID, completedAt)}}
	m.WithExternalObjectDependencyProvider(provider)

	summary, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt)
	if err != nil {
		t.Fatalf("publishRepositoryClusterBackupSet() error = %v", err)
	}
	if summary.ManifestURI == "" {
		t.Fatalf("summary missing manifest URI")
	}
	manifestRaw, _, err := store.GetBytes(ctx, joinRepositoryKey(prefix, clusterbackup.BackupSetManifestName), 0)
	if err != nil {
		t.Fatalf("read published manifest: %v", err)
	}
	doc, err := clusterbackup.ParseBackupSetDocument(manifestRaw)
	if err != nil {
		t.Fatalf("ParseBackupSetDocument() error = %v", err)
	}
	manifest := doc.Manifest
	if len(manifest.ExternalObjectStores) != 1 {
		t.Fatalf("external object stores=%#v, want one", manifest.ExternalObjectStores)
	}
	dep := manifest.ExternalObjectStores[0]
	if dep.RecoveryPointRef != "aws-backup-recovery-point:rp-123" || dep.ExternalStoreRef == "" || dep.ObjectReferenceManifestURI == "" || dep.ObjectReferenceManifestChecksumSHA256 == "" {
		t.Fatalf("external dependency row missing restore evidence: %#v", dep)
	}
	if len(manifest.Consistency.BarrierSet.ObjectReferences) != 1 || manifest.Consistency.BarrierSet.ObjectReferences[0].ExternalRecoveryPointRef != dep.RecoveryPointRef {
		t.Fatalf("barrier object references=%#v, want dependency checkpoint", manifest.Consistency.BarrierSet.ObjectReferences)
	}
	if len(manifest.Artifacts.AdditionalManifests) != 1 || manifest.Artifacts.AdditionalManifests[0].Purpose != "OBJECT_REFERENCE_MANIFEST" {
		t.Fatalf("additional manifests=%#v, want object reference manifest", manifest.Artifacts.AdditionalManifests)
	}
	key, err := repositoryKeyFromS3URI(repo, dep.ObjectReferenceManifestURI)
	if err != nil {
		t.Fatalf("object reference manifest URI invalid: %v", err)
	}
	objectRaw, _, err := store.GetBytes(ctx, key, 0)
	if err != nil {
		t.Fatalf("object reference manifest was not uploaded: %v", err)
	}
	if got := hashBytes(objectRaw); got != dep.ObjectReferenceManifestChecksumSHA256 {
		t.Fatalf("object reference checksum=%s want %s", got, dep.ObjectReferenceManifestChecksumSHA256)
	}
	validated, err := m.ValidateRepositoryClusterBackupSet(ctx, repo.RepositoryID, backupSetID, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
	if err != nil {
		t.Fatalf("ValidateRepositoryClusterBackupSet() error = %v", err)
	}
	if validated.ProductionReport == nil || !validated.ProductionReport.Valid() {
		t.Fatalf("validated report=%#v, want valid", validated.ProductionReport)
	}
}

func TestPlanClusterRestoreBlocksMissingExternalObjectReferenceManifest(t *testing.T) {
	ctx := context.Background()
	m, store, repo, backupSetID, prefix, input, createdAt, completedAt := seedRepositoryClusterBackupForExternalDependencyTest(t)
	provider := fakeExternalObjectDependencyProvider{deps: []clusterbackup.ExternalObjectStoreDependency{testExternalObjectDependency(backupSetID, completedAt)}}
	m.WithExternalObjectDependencyProvider(provider)
	if _, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt); err != nil {
		t.Fatalf("publishRepositoryClusterBackupSet() error = %v", err)
	}
	keys, err := store.ListKeys(ctx, joinRepositoryKey(prefix, "object-references"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("object reference keys=%v, want one", keys)
	}
	delete(store.objects, keys[0])

	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID})
	if err != nil {
		t.Fatalf("PlanClusterRestore() error = %v", err)
	}
	if plan.Restorable || !hasBlockerReason(plan.Blockers, "object_reference_manifest_missing") {
		t.Fatalf("restore plan blockers=%#v, want object_reference_manifest_missing", plan.Blockers)
	}
}

func TestPlanClusterRestoreBlocksMismatchedExternalRecoveryPoint(t *testing.T) {
	ctx := context.Background()
	m, _, repo, backupSetID, _, input, createdAt, completedAt := seedRepositoryClusterBackupForExternalDependencyTest(t)
	provider := fakeExternalObjectDependencyProvider{deps: []clusterbackup.ExternalObjectStoreDependency{testExternalObjectDependency(backupSetID, completedAt)}}
	m.WithExternalObjectDependencyProvider(provider)
	m.WithExternalObjectDependencyValidator(fakeExternalObjectDependencyValidator{errors: []clusterbackup.BackupSetValidationIssue{{Reason: "external_recovery_point_mismatch", Detail: "configured object-store recovery point does not match", FieldPath: "external_object_stores[0].recovery_point_ref"}}})
	if _, err := m.publishRepositoryClusterBackupSet(ctx, input, repo, backupSetID, "cluster-test", createdAt, completedAt); err != nil {
		t.Fatalf("publishRepositoryClusterBackupSet() error = %v", err)
	}

	plan, err := m.PlanClusterRestore(ctx, PlanClusterRestoreInput{RepositoryID: repo.RepositoryID, BackupSetID: backupSetID, TargetMode: ClusterRestoreTargetModeNewClusterBootstrap, IdentityMode: ClusterRestoreIdentityModePreserveBackupID})
	if err != nil {
		t.Fatalf("PlanClusterRestore() error = %v", err)
	}
	if plan.Restorable || !hasBlockerReason(plan.Blockers, "external_recovery_point_mismatch") {
		t.Fatalf("restore plan blockers=%#v, want external_recovery_point_mismatch", plan.Blockers)
	}
}

func seedRepositoryClusterBackupForExternalDependencyTest(t *testing.T) (*Module, *memoryRepositoryObjectStore, BackupRepository, string, string, StartClusterBackupInput, time.Time, time.Time) {
	t.Helper()
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	store := newMemoryRepositoryObjectStore()
	m.repositoryObjectStoreFactory = func(context.Context, BackupRepository) (backupRepositoryObjectStore, error) { return store, nil }
	repo := normalizeBackupRepository(BackupRepository{RepositoryID: "repo-external-deps", Provider: RepositoryProviderS3Compatible, S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test"}, CredentialRef: SecretReference{Scheme: SecretSchemeAWSIRSA}, Encryption: EncryptionPolicy{Mode: EncryptionModeSSES3}, Validation: BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: time.Now().UTC()}})
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
	input := StartClusterBackupInput{Reason: "external dependency test", ArchiveFormat: backupcore.ArchiveFormatTar, ClusterID: "cluster-test", ClusterName: "cluster-test", RepositoryID: repo.RepositoryID, RepositoryPrefix: prefix, CaptureProvider: "filesystem_immutable_copy", QuiesceReadProfile: backupcore.DefaultProductionQuiesceReadProfile}
	return m, store, repo, backupSetID, prefix, input, createdAt, completedAt
}

func testExternalObjectDependency(backupSetID string, ts time.Time) clusterbackup.ExternalObjectStoreDependency {
	object := clusterbackup.ObjectReferenceEntry{SpaceID: "11111111-1111-1111-1111-111111111111", DomainID: "domain-1", BlobID: strings.Repeat("a", 64), PayloadBackend: "object_store", Bucket: "mycel-blobs", Key: "clusters/test/spaces/11111111-1111-1111-1111-111111111111/domains/domain-1/objects/aa/" + strings.Repeat("a", 64), Region: "us-east-1", ETag: "etag-1", SizeBytes: "7", ChecksumAlgorithm: "sha256", ChecksumHex: strings.Repeat("a", 64), PayloadState: "available", MetadataRaftGroupID: "system", MetadataRaftBarrierIndex: "10"}
	manifest := clusterbackup.ObjectReferenceManifestV1{Schema: clusterbackup.ObjectReferenceManifestSchema, ManifestVersion: clusterbackup.ObjectReferenceManifestVersion, BackupSetID: backupSetID, Usage: "BLOB_PAYLOADS", Provider: "S3_COMPATIBLE", PayloadBackend: "object_store", ExternalStoreRef: "s3-compatible:mycel-blobs/clusters/test@sha256:abcdef123456", Bucket: "mycel-blobs", Prefix: "clusters/test", EndpointFingerprint: "sha256:abcdef123456", RecoveryPointRef: "aws-backup-recovery-point:rp-123", MetadataRaftGroupID: "system", MetadataRaftBarrierIndex: "10", ObjectCount: "1", LogicalBytes: "7", GeneratedAt: ts, Objects: []clusterbackup.ObjectReferenceEntry{object}}
	store := clusterbackup.BackupSetExternalObjectStore{Usage: manifest.Usage, Provider: manifest.Provider, IncludePayloadBytes: false, Bucket: manifest.Bucket, Prefix: manifest.Prefix, EndpointFingerprint: manifest.EndpointFingerprint, CredentialRef: "aws-irsa", ExternalStoreRef: manifest.ExternalStoreRef, BackupPolicyRef: "aws-backup-plan:mycel-blobs", RecoveryPointRef: manifest.RecoveryPointRef, ObjectCount: manifest.ObjectCount, LogicalBytes: manifest.LogicalBytes, ValidationState: "VERIFIED"}
	checkpoint := clusterbackup.CheckpointObjectReference{Usage: manifest.Usage, Provider: manifest.Provider, PayloadBackend: manifest.PayloadBackend, MetadataRaftGroupID: manifest.MetadataRaftGroupID, MetadataRaftBarrierIndex: manifest.MetadataRaftBarrierIndex, ObjectCount: manifest.ObjectCount, LogicalBytes: manifest.LogicalBytes, ExternalStoreRef: manifest.ExternalStoreRef, ExternalRecoveryPointRef: manifest.RecoveryPointRef, ValidatedAt: ts}
	return clusterbackup.ExternalObjectStoreDependency{Store: store, Checkpoint: checkpoint, Manifest: manifest}
}

type fakeExternalObjectDependencyProvider struct {
	deps   []clusterbackup.ExternalObjectStoreDependency
	errors []clusterbackup.BackupSetValidationIssue
}

func (p fakeExternalObjectDependencyProvider) BackupExternalObjectStoreDependencies(ctx context.Context, request clusterbackup.ExternalObjectDependencyRequest) ([]clusterbackup.ExternalObjectStoreDependency, error) {
	if len(p.deps) == 0 {
		return nil, nil
	}
	out := append([]clusterbackup.ExternalObjectStoreDependency(nil), p.deps...)
	for i := range out {
		out[i].Manifest.BackupSetID = request.BackupSetID
		out[i].Store.ObjectReferenceManifestURI = ""
		out[i].Store.ObjectReferenceManifestChecksumSHA256 = ""
		out[i].Checkpoint.ObjectReferenceManifestURI = ""
		out[i].Checkpoint.ObjectReferenceManifestChecksumSHA256 = ""
	}
	return out, nil
}

func (p fakeExternalObjectDependencyProvider) ValidateExternalObjectStoreDependency(ctx context.Context, input clusterbackup.ExternalObjectStoreDependencyValidationInput) clusterbackup.ExternalObjectStoreDependencyValidationResult {
	return clusterbackup.ExternalObjectStoreDependencyValidationResult{CheckedAt: time.Now().UTC(), Errors: append([]clusterbackup.BackupSetValidationIssue(nil), p.errors...), Evidence: map[string]string{"validator": "fake"}}
}

type fakeExternalObjectDependencyValidator struct {
	errors []clusterbackup.BackupSetValidationIssue
}

func (v fakeExternalObjectDependencyValidator) ValidateExternalObjectStoreDependency(ctx context.Context, input clusterbackup.ExternalObjectStoreDependencyValidationInput) clusterbackup.ExternalObjectStoreDependencyValidationResult {
	if len(v.errors) == 0 {
		return clusterbackup.ExternalObjectStoreDependencyValidationResult{CheckedAt: time.Now().UTC(), Evidence: map[string]string{"validator": "fake"}}
	}
	for i := range v.errors {
		if v.errors[i].Detail == "" {
			v.errors[i].Detail = fmt.Sprintf("injected validation error for %s", input.Store.ExternalStoreRef)
		}
	}
	return clusterbackup.ExternalObjectStoreDependencyValidationResult{CheckedAt: time.Now().UTC(), Errors: append([]clusterbackup.BackupSetValidationIssue(nil), v.errors...), Evidence: map[string]string{"validator": "fake"}}
}
