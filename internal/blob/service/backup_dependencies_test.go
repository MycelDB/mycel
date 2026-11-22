package service

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestBackupExternalObjectStoreDependenciesBuildsAndValidatesManifest(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3PayloadClient()
	cfg := Config{Backend: "object_store", ObjectStoreProvider: "s3-compatible", S3Bucket: "my-bucket", S3Prefix: "tenant-a", S3Region: "us-east-1", ObjectStoreCredentialRef: "aws-irsa", ObjectStoreBackupPolicyRef: "aws-backup-plan:blobs", ObjectStoreRecoveryPointRef: "aws-backup-recovery-point:rp-123"}
	store, err := newS3PayloadStoreWithClient(cfg, t.TempDir(), fake)
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule(fakeRefCounter{}, cfg)
	m.s3Store = store
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "11111111-1111-1111-1111-111111111111", DomainID: "domain-1", Reader: strings.NewReader("external payload")})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}

	deps, err := m.BackupExternalObjectStoreDependencies(ctx, clusterbackup.ExternalObjectDependencyRequest{BackupSetID: "backup-set-1", ClusterID: "cluster-a", GeneratedAt: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC), RaftBarrierIndexes: map[string]string{"local": "0"}})
	if err != nil {
		t.Fatalf("BackupExternalObjectStoreDependencies() error = %v", err)
	}
	if len(deps) != 1 {
		t.Fatalf("dependencies=%#v, want one", deps)
	}
	dep := deps[0]
	if dep.Store.RecoveryPointRef != cfg.ObjectStoreRecoveryPointRef || dep.Store.CredentialRef != cfg.ObjectStoreCredentialRef || dep.Store.ExternalStoreRef == "" {
		t.Fatalf("dependency store row missing non-secret refs: %#v", dep.Store)
	}
	if dep.Manifest.ObjectCount != "1" || dep.Manifest.LogicalBytes != "16" || len(dep.Manifest.Objects) != 1 || dep.Manifest.Objects[0].BlobID != meta.BlobID {
		t.Fatalf("manifest=%#v, want one uploaded payload", dep.Manifest)
	}
	raw, err := clusterbackup.MarshalObjectReferenceManifest(dep.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if issues := clusterbackup.ValidateObjectReferenceManifest(ctx, raw, dep.Manifest, dep.Store); len(issues) != 0 {
		t.Fatalf("ValidateObjectReferenceManifest() issues=%#v", issues)
	}
	result := m.ValidateExternalObjectStoreDependency(ctx, clusterbackup.ExternalObjectStoreDependencyValidationInput{BackupSetID: "backup-set-1", Store: dep.Store, Manifest: dep.Manifest})
	if len(result.Errors) != 0 {
		t.Fatalf("ValidateExternalObjectStoreDependency() errors=%#v", result.Errors)
	}
}

func TestBackupExternalObjectStoreDependenciesRequiresRecoveryPoint(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3PayloadClient()
	cfg := Config{Backend: "object_store", ObjectStoreProvider: "s3-compatible", S3Bucket: "my-bucket", S3Prefix: "tenant-a"}
	store, err := newS3PayloadStoreWithClient(cfg, t.TempDir(), fake)
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule(fakeRefCounter{}, cfg)
	m.s3Store = store
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if _, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: strings.NewReader("external payload")}); err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	_, err = m.BackupExternalObjectStoreDependencies(ctx, clusterbackup.ExternalObjectDependencyRequest{BackupSetID: "backup-set-1"})
	if err == nil || !strings.Contains(err.Error(), "MYCELD_BLOB_OBJECT_STORE_RECOVERY_POINT_REF") {
		t.Fatalf("BackupExternalObjectStoreDependencies() error=%v, want recovery point requirement", err)
	}
}

func TestValidateExternalObjectStoreDependencyBlocksRecoveryPointMismatch(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3PayloadClient()
	cfg := Config{Backend: "object_store", ObjectStoreProvider: "s3-compatible", S3Bucket: "my-bucket", S3Prefix: "tenant-a", ObjectStoreRecoveryPointRef: "rp-current"}
	store, err := newS3PayloadStoreWithClient(cfg, t.TempDir(), fake)
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule(fakeRefCounter{}, cfg)
	m.s3Store = store
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	manifest := clusterbackup.ObjectReferenceManifestV1{Schema: clusterbackup.ObjectReferenceManifestSchema, ManifestVersion: clusterbackup.ObjectReferenceManifestVersion, BackupSetID: "backup-set-1", Usage: "BLOB_PAYLOADS", Provider: "S3_COMPATIBLE", PayloadBackend: "object_store", ExternalStoreRef: "s3-compatible:my-bucket/tenant-a@sha256:test", Bucket: "my-bucket", Prefix: "tenant-a", EndpointFingerprint: blobEndpointFingerprint(cfg), RecoveryPointRef: "rp-backup", MetadataRaftGroupID: "local", MetadataRaftBarrierIndex: "0", ObjectCount: "0", LogicalBytes: "0", GeneratedAt: time.Now().UTC(), Objects: []clusterbackup.ObjectReferenceEntry{}}
	storeRow := clusterbackup.BackupSetExternalObjectStore{Usage: manifest.Usage, Provider: manifest.Provider, IncludePayloadBytes: false, Bucket: manifest.Bucket, Prefix: manifest.Prefix, EndpointFingerprint: manifest.EndpointFingerprint, CredentialRef: "aws-irsa", ExternalStoreRef: manifest.ExternalStoreRef, RecoveryPointRef: manifest.RecoveryPointRef, ObjectCount: manifest.ObjectCount, LogicalBytes: manifest.LogicalBytes, ValidationState: "VERIFIED"}
	result := m.ValidateExternalObjectStoreDependency(ctx, clusterbackup.ExternalObjectStoreDependencyValidationInput{BackupSetID: "backup-set-1", Store: storeRow, Manifest: manifest})
	if len(result.Errors) == 0 || result.Errors[0].Reason != "external_recovery_point_mismatch" {
		t.Fatalf("ValidateExternalObjectStoreDependency() errors=%#v, want recovery mismatch", result.Errors)
	}
}
