package admin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	backupcore "github.com/myceldb/mycel/internal/backup"
	daemonbackup "github.com/myceldb/mycel/internal/backup/service"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminBackupPolicyGetUpdateAndValidate(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir(), DefaultRepositoryID: "repo-default"}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})

	getRes, err := svc.GetBackupPolicy(authenticatedContext(), &adminv1.GetBackupPolicyRequest{})
	if err != nil {
		t.Fatalf("GetBackupPolicy() error = %v", err)
	}
	if getRes.GetPolicy().GetDefaultRepositoryId() != "repo-default" || getRes.GetPolicy().GetCaptureProvider() == adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_UNSPECIFIED || getRes.GetPolicy().GetQuiesceReadProfile() == adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_UNSPECIFIED {
		t.Fatalf("unexpected default repository policy: %#v", getRes.GetPolicy())
	}

	updateRes, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{
		DefaultRepositoryId:        "repo-prod",
		CaptureProvider:            adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY,
		QuiesceReadProfile:         adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY,
		AllowReadOnlyDuringQuiesce: true,
		RetentionPolicy:            &adminv1.RetentionPolicy{KeepLastSuccessful: 3, DeleteIncompleteAfterSeconds: 3600},
		ValidationOptions:          &adminv1.ValidationOptions{IncludeArtifactChecks: true, RequireCompleteMarker: true},
	}})
	if err != nil {
		t.Fatalf("UpdateBackupPolicy() error = %v", err)
	}
	got := updateRes.GetPolicy()
	if got.GetDefaultRepositoryId() != "repo-prod" || got.GetCaptureProvider() != adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY || got.GetQuiesceReadProfile() != adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY || !got.GetAllowReadOnlyDuringQuiesce() {
		t.Fatalf("repository policy fields not preserved: %#v", got)
	}
	if got.GetRetentionPolicy().GetKeepLastSuccessful() != 3 || !got.GetValidationOptions().GetIncludeArtifactChecks() || !got.GetValidationOptions().GetRequireCompleteMarker() {
		t.Fatalf("retention/validation policy not preserved: %#v", got)
	}
}

func TestAdminBackupPolicyPreservesRepositoryContractFields(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	policy := &adminv1.BackupPolicy{
		DefaultRepositoryId: "repo-prod",
		CaptureProvider:     adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY,
		QuiesceReadProfile:  adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY,
		DerivedSubsystems: []*adminv1.DerivedSubsystemPolicy{{
			Subsystem: adminv1.BackupDerivedSubsystem_BACKUP_DERIVED_SUBSYSTEM_SEMANTIC_VECTOR_SEARCH,
			Mode:      adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_REQUIRED,
		}},
		RetentionPolicy:   &adminv1.RetentionPolicy{KeepLastSuccessful: 3, DeleteIncompleteAfterSeconds: 3600},
		ValidationOptions: &adminv1.ValidationOptions{IncludeArtifactChecks: true, RequireCompleteMarker: true},
	}
	res, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: policy})
	if err != nil {
		t.Fatalf("UpdateBackupPolicy() error = %v", err)
	}
	got := res.GetPolicy()
	if got.GetDefaultRepositoryId() != "repo-prod" || got.GetCaptureProvider() != adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY || got.GetQuiesceReadProfile() != adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY {
		t.Fatalf("repository policy fields not preserved: %#v", got)
	}
	if len(got.GetDerivedSubsystems()) != 1 || got.GetDerivedSubsystems()[0].GetMode() != adminv1.BackupDerivedPolicyMode_BACKUP_DERIVED_POLICY_MODE_INCLUDE_REQUIRED {
		t.Fatalf("derived subsystem policy not preserved: %#v", got.GetDerivedSubsystems())
	}
	if got.GetRetentionPolicy().GetKeepLastSuccessful() != 3 || !got.GetValidationOptions().GetIncludeArtifactChecks() || !got.GetValidationOptions().GetRequireCompleteMarker() {
		t.Fatalf("retention/validation policy not preserved: %#v", got)
	}
}

func TestAdminBackupRepositoryCRUD(t *testing.T) {
	manager := daemonbackup.NewModule()
	if result := manager.Init(context.Background(), runtimetest.New(backupAPIFixtureDataDir(t), nil)); !result.OK {
		t.Fatalf("backup module init failed: %#v", result)
	}
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	repo := &adminv1.BackupRepository{
		RepositoryId: "repo-prod",
		DisplayName:  "Production backups",
		Provider:     adminv1.BackupRepositoryProvider_BACKUP_REPOSITORY_PROVIDER_S3_COMPATIBLE,
		S3:           &adminv1.S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "/clusters/prod/", Region: "us-east-1", EndpointUri: "https://minio.example", ForcePathStyle: true},
		CredentialRef: &adminv1.SecretReference{Scheme: adminv1.SecretReferenceScheme_SECRET_REFERENCE_SCHEME_ENV, Keys: map[string]string{
			"access_key_id":     "MINIO_ACCESS_KEY",
			"secret_access_key": "MINIO_SECRET_KEY",
		}},
		Encryption: &adminv1.EncryptionPolicy{Mode: adminv1.EncryptionMode_ENCRYPTION_MODE_SSE_S3},
		DevOnly:    true,
	}
	created, err := svc.CreateBackupRepository(authenticatedContext(), &adminv1.CreateBackupRepositoryRequest{Repository: repo})
	if err != nil {
		t.Fatalf("CreateBackupRepository() error = %v", err)
	}
	if created.GetRepository().GetS3().GetPrefix() != "clusters/prod" || created.GetRepository().GetCreateTime() == "" {
		t.Fatalf("created repository not normalized/timestamped: %#v", created.GetRepository())
	}
	list, err := svc.ListBackupRepositories(authenticatedContext(), &adminv1.ListBackupRepositoriesRequest{})
	if err != nil || len(list.GetRepositories()) != 1 {
		t.Fatalf("ListBackupRepositories() = (%#v, %v)", list, err)
	}
	created.Repository.DisplayName = "Updated backups"
	updated, err := svc.UpdateBackupRepository(authenticatedContext(), &adminv1.UpdateBackupRepositoryRequest{Repository: created.Repository})
	if err != nil {
		t.Fatalf("UpdateBackupRepository() error = %v", err)
	}
	if updated.GetRepository().GetDisplayName() != "Updated backups" {
		t.Fatalf("display name not updated: %#v", updated.GetRepository())
	}
	deleted, err := svc.DeleteBackupRepository(authenticatedContext(), &adminv1.DeleteBackupRepositoryRequest{RepositoryId: repo.GetRepositoryId()})
	if err != nil {
		t.Fatalf("DeleteBackupRepository() error = %v", err)
	}
	if deleted.GetState() != adminv1.BackupDeletionState_BACKUP_DELETION_STATE_DELETED {
		t.Fatalf("unexpected delete state: %#v", deleted)
	}
}

func TestAdminBackupRepositoryBackedOperationsRequireConfiguredManagers(t *testing.T) {
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: backupAPIFixtureDataDir(t), Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	if _, err := svc.StartBackup(authenticatedContext(), &adminv1.StartBackupRequest{RepositoryId: "repo-prod"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartBackup(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.ListBackupSets(authenticatedContext(), &adminv1.ListBackupSetsRequest{RepositoryId: "repo-prod"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListBackupSets(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.ValidateBackupSet(authenticatedContext(), &adminv1.ValidateBackupSetRequest{RepositoryId: "repo-prod", BackupSetId: "backup-set-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ValidateBackupSet(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.PlanRestore(authenticatedContext(), &adminv1.PlanRestoreRequest{RepositoryId: "repo-prod", BackupSetId: "backup-set-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PlanRestore(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestBackupOperationStateMapsRepositoryPhases(t *testing.T) {
	cases := map[string]adminv1.BackupOperationState{
		"prechecking":         adminv1.BackupOperationState_BACKUP_OPERATION_STATE_PREFLIGHTING,
		"barrier_wait":        adminv1.BackupOperationState_BACKUP_OPERATION_STATE_CHECKPOINTING,
		"archiving":           adminv1.BackupOperationState_BACKUP_OPERATION_STATE_CAPTURING,
		"uploading":           adminv1.BackupOperationState_BACKUP_OPERATION_STATE_UPLOADING,
		"committing_manifest": adminv1.BackupOperationState_BACKUP_OPERATION_STATE_FINALIZING,
		"succeeded":           adminv1.BackupOperationState_BACKUP_OPERATION_STATE_SUCCEEDED,
	}
	for phase, want := range cases {
		if got := mapBackupOperationState(phase); got != want {
			t.Fatalf("mapBackupOperationState(%q) = %s, want %s", phase, got, want)
		}
	}
}

func TestAdminBackupRequiresAuthorization(t *testing.T) {
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: backupAPIFixtureDataDir(t), Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: false})
	if _, err := svc.GetBackupPolicy(context.Background(), &adminv1.GetBackupPolicyRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated code = %v", status.Code(err))
	}
	if _, err := svc.GetBackupPolicy(authenticatedContext(), &adminv1.GetBackupPolicyRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthorized code = %v", status.Code(err))
	}
}

func backupAPIFixtureDataDir(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "meta"), 0o700); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta", "spaces.json"), []byte(`[]`), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return dataDir
}
