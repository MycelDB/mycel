package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

type fakeBackupRepositoryValidator struct {
	validation BackupRepositoryValidationSummary
}

func (f fakeBackupRepositoryValidator) ValidateBackupRepository(context.Context, BackupRepository) BackupRepositoryValidationSummary {
	return f.validation
}

func TestBackupRepositoryCatalogCRUDPersistsAndRedactsValidation(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	module := NewModule()
	module.dataDir = dataDir
	module.repositoryValidator = fakeBackupRepositoryValidator{validation: BackupRepositoryValidationSummary{
		State:          ValidationStateInvalid,
		CheckedAt:      time.Now().UTC(),
		SanitizedError: "https://minio.example/bucket?X-Amz-Security-Token=secret AKIAABCDEFGHIJKLMNOP",
		Probes: []ValidationProbeResult{{
			Name:      "sentinel_write",
			State:     ValidationStateInvalid,
			Detail:    "failed with AWS_SECRET_ACCESS_KEY=super-secret",
			CheckedAt: time.Now().UTC(),
		}},
		Blockers: []ValidationBlocker{{Reason: "sentinel_write_failed", Detail: "access_key_id=AKIAABCDEFGHIJKLMNOP", Retryable: true}},
	}}

	created, err := module.CreateBackupRepository(ctx, BackupRepository{
		RepositoryID: "repo-prod",
		DisplayName:  "Production backups",
		Provider:     RepositoryProviderS3Compatible,
		S3: S3CompatibleRepositoryConfig{
			Bucket:         "mycel-backups",
			Prefix:         "/clusters/prod/",
			Region:         "us-east-1",
			EndpointURI:    "https://minio.example",
			ForcePathStyle: true,
		},
		CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}},
		Encryption:    EncryptionPolicy{Mode: EncryptionModeSSES3},
		DevOnly:       true,
	})
	if err != nil {
		t.Fatalf("CreateBackupRepository() error = %v", err)
	}
	if created.CreateTime.IsZero() || created.UpdateTime.IsZero() || created.S3.Prefix != "clusters/prod" {
		t.Fatalf("created repository not normalized/timestamped: %#v", created)
	}

	if _, err := module.CreateBackupRepository(ctx, created); err == nil {
		t.Fatalf("CreateBackupRepository() duplicate succeeded")
	}

	created.DisplayName = "Updated backups"
	updated, err := module.UpdateBackupRepository(ctx, created)
	if err != nil {
		t.Fatalf("UpdateBackupRepository() error = %v", err)
	}
	if updated.DisplayName != "Updated backups" || updated.CreateTime.IsZero() || updated.CreateTime != created.CreateTime {
		t.Fatalf("updated repository timestamps/display name = %#v", updated)
	}

	validation, err := module.ValidateBackupRepository(ctx, created.RepositoryID)
	if err != nil {
		t.Fatalf("ValidateBackupRepository() error = %v", err)
	}
	validationText := validation.SanitizedError + " " + validation.Probes[0].Detail + " " + validation.Blockers[0].Detail
	for _, secret := range []string{"super-secret", "AKIAABCDEFGHIJKLMNOP", "X-Amz-Security-Token=secret"} {
		if strings.Contains(validationText, secret) {
			t.Fatalf("validation output leaked %q: %s", secret, validationText)
		}
	}
	if !strings.Contains(validationText, "<redacted>") && !strings.Contains(validationText, "<redacted-access-key>") {
		t.Fatalf("validation output was not redacted: %s", validationText)
	}

	reloaded := NewModule()
	reloaded.dataDir = dataDir
	if err := reloaded.loadBackupRepositoryCatalog(); err != nil {
		t.Fatalf("loadBackupRepositoryCatalog() error = %v", err)
	}
	got, err := reloaded.GetBackupRepository(created.RepositoryID)
	if err != nil {
		t.Fatalf("GetBackupRepository() after reload error = %v", err)
	}
	if got.Validation.State != ValidationStateInvalid || strings.Contains(got.Validation.SanitizedError, "AKIAABCDEFGHIJKLMNOP") {
		t.Fatalf("reloaded validation was not persisted/redacted: %#v", got.Validation)
	}

	if err := module.DeleteBackupRepository(ctx, created.RepositoryID); err != nil {
		t.Fatalf("DeleteBackupRepository() error = %v", err)
	}
	if _, err := module.GetBackupRepository(created.RepositoryID); err == nil {
		t.Fatalf("GetBackupRepository() after delete succeeded")
	}
}

func TestBackupRepositoryDuplicateCreateReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	module.dataDir = t.TempDir()
	repo := BackupRepository{
		RepositoryID:  "repo-replay",
		DisplayName:   "Replay repository",
		Provider:      RepositoryProviderS3Compatible,
		S3:            S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "cluster-a", Region: "us-east-1"},
		CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}},
		Encryption:    EncryptionPolicy{Mode: EncryptionModeNone},
		DevOnly:       true,
	}
	created, err := module.CreateBackupRepository(ctx, repo)
	if err != nil {
		t.Fatalf("CreateBackupRepository() error = %v", err)
	}
	module.repositoryValidator = fakeBackupRepositoryValidator{validation: BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: time.Now().UTC()}}
	if _, err := module.ValidateBackupRepository(ctx, created.RepositoryID); err != nil {
		t.Fatalf("ValidateBackupRepository() error = %v", err)
	}
	raw, err := json.Marshal(backupRepositoryUpsertRecord{Repository: repo})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.applyBackupRepositoryUpsert(ctx, wal.Record{Type: recordTypeBackupRepositoryUpsert, Payload: raw}); err != nil {
		t.Fatalf("duplicate replay apply error = %v", err)
	}
	got, err := module.GetBackupRepository(created.RepositoryID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Validation.State != ValidationStateValid {
		t.Fatalf("duplicate replay downgraded validation: %#v", got.Validation)
	}

	repo.S3.Prefix = "cluster-b"
	raw, err = json.Marshal(backupRepositoryUpsertRecord{Repository: repo})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.applyBackupRepositoryUpsert(ctx, wal.Record{Type: recordTypeBackupRepositoryUpsert, Payload: raw}); !errors.Is(err, ErrBackupRepositoryAlreadyExists) {
		t.Fatalf("different duplicate apply error = %v, want %v", err, ErrBackupRepositoryAlreadyExists)
	}
}

func TestBackupRepositoryRejectsRawCredentialReferenceValues(t *testing.T) {
	module := NewModule()
	module.dataDir = t.TempDir()
	_, err := module.CreateBackupRepository(context.Background(), BackupRepository{
		RepositoryID:  "repo-raw",
		Provider:      RepositoryProviderS3Compatible,
		S3:            S3CompatibleRepositoryConfig{Bucket: "mycel-backups"},
		CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "AKIAABCDEFGHIJKLMNOP", "secret_access_key": "MINIO_SECRET_KEY"}},
		DevOnly:       true,
	})
	if err == nil {
		t.Fatalf("CreateBackupRepository() accepted a raw-looking access key")
	}
}

func TestBackupRepositoryRequiresDevOnlyForDevelopmentCredentialReferences(t *testing.T) {
	module := NewModule()
	module.dataDir = t.TempDir()
	_, err := module.CreateBackupRepository(context.Background(), BackupRepository{
		RepositoryID:  "repo-env",
		Provider:      RepositoryProviderS3Compatible,
		S3:            S3CompatibleRepositoryConfig{Bucket: "mycel-backups"},
		CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}},
	})
	if err == nil || !strings.Contains(err.Error(), "dev_only") {
		t.Fatalf("CreateBackupRepository() error = %v, want dev_only gate", err)
	}
}

func TestBackupRepositoryProjectedFileCredentialResolver(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/access", []byte("minio-access\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/secret", []byte("minio-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := credentialProviderForReference(SecretReference{Scheme: SecretSchemeK8sSecret, Name: "mycel-backup-repository", Path: dir, Keys: map[string]string{"access_key_id": "access", "secret_access_key": "secret"}})
	if err != nil {
		t.Fatalf("credentialProviderForReference() error = %v", err)
	}
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "minio-access" || creds.SecretAccessKey != "minio-secret" {
		t.Fatalf("unexpected credentials resolved from projected files: %#v", creds)
	}
}
