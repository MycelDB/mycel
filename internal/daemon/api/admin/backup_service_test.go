package admin

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})

	getRes, err := svc.GetBackupPolicy(authenticatedContext(), &adminv1.GetBackupPolicyRequest{})
	if err != nil {
		t.Fatalf("GetBackupPolicy() error = %v", err)
	}
	if getRes.GetPolicy().GetCompression() != "zip" || getRes.GetPolicy().GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP || getRes.GetPolicy().GetRetentionCount() == 0 {
		t.Fatalf("unexpected default policy: %#v", getRes.GetPolicy())
	}

	backupDir := t.TempDir()
	updateRes, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{Enabled: true, BackupDir: backupDir, IntervalHours: 1, RetentionCount: 2, ArchiveFormat: adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP, QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}})
	if err != nil {
		t.Fatalf("UpdateBackupPolicy() error = %v", err)
	}
	if !updateRes.GetPolicy().GetEnabled() || updateRes.GetPolicy().GetBackupDir() != backupDir || updateRes.GetPolicy().GetRetentionCount() != 2 || updateRes.GetPolicy().GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP || updateRes.GetPolicy().GetCompression() != "zip" {
		t.Fatalf("unexpected updated policy: %#v", updateRes.GetPolicy())
	}

	_, err = svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{BackupDir: filepath.Join(dataDir, "nested"), IntervalHours: 1, RetentionCount: 1, Compression: "zip", QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid backup dir code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestAdminBackupPolicyAcceptsLegacyCompressionFallback(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	backupDir := t.TempDir()
	res, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{Enabled: true, BackupDir: backupDir, IntervalHours: 1, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}})
	if err != nil {
		t.Fatalf("UpdateBackupPolicy(legacy compression) error = %v", err)
	}
	if res.GetPolicy().GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP || res.GetPolicy().GetCompression() != "zip" {
		t.Fatalf("legacy compression was not mapped to archive_format: %#v", res.GetPolicy())
	}
}

func TestAdminBackupPolicyAcceptsArchiveFormatEnums(t *testing.T) {
	formats := []adminv1.BackupArchiveFormat{
		adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP,
		adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR,
		adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ,
		adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST,
	}
	for _, format := range formats {
		t.Run(format.String(), func(t *testing.T) {
			dataDir := backupAPIFixtureDataDir(t)
			manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
			svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
			backupDir := t.TempDir()
			res, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{Enabled: true, BackupDir: backupDir, IntervalHours: 1, RetentionCount: 2, ArchiveFormat: format, QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}})
			if err != nil {
				t.Fatalf("UpdateBackupPolicy() error = %v", err)
			}
			if res.GetPolicy().GetArchiveFormat() != format {
				t.Fatalf("archive_format = %v, want %v", res.GetPolicy().GetArchiveFormat(), format)
			}
		})
	}
}

func TestAdminBackupPolicyRejectsUnknownArchiveFormat(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	backupDir := t.TempDir()
	_, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: &adminv1.BackupPolicy{Enabled: true, BackupDir: backupDir, IntervalHours: 1, RetentionCount: 2, ArchiveFormat: adminv1.BackupArchiveFormat(99), QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown archive format code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestAdminBackupPolicyMapsScheduleFields(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})

	backupDir := t.TempDir()
	policy := &adminv1.BackupPolicy{Enabled: true, BackupDir: backupDir, IntervalHours: 1, RetentionCount: 2, ArchiveFormat: adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP, QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4, ScheduleKind: backupcore.ScheduleKindWeekly, TimeOfDay: "22:00", Timezone: "America/Toronto", Weekdays: []int32{0, 3}, RunMissed: true}
	updateRes, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: policy})
	if err != nil {
		t.Fatalf("UpdateBackupPolicy() error = %v", err)
	}
	got := updateRes.GetPolicy()
	if got.GetScheduleKind() != backupcore.ScheduleKindWeekly || got.GetTimeOfDay() != "22:00" || got.GetTimezone() != "America/Toronto" || len(got.GetWeekdays()) != 2 || got.GetWeekdays()[0] != 0 || got.GetWeekdays()[1] != 3 || !got.GetRunMissed() {
		t.Fatalf("schedule fields not preserved: %#v", got)
	}

	getRes, err := svc.GetBackupPolicy(authenticatedContext(), &adminv1.GetBackupPolicyRequest{})
	if err != nil {
		t.Fatalf("GetBackupPolicy() error = %v", err)
	}
	if getRes.GetPolicy().GetScheduleKind() != backupcore.ScheduleKindWeekly || getRes.GetPolicy().GetTimeOfDay() != "22:00" {
		t.Fatalf("unexpected persisted schedule: %#v", getRes.GetPolicy())
	}
}

func TestAdminBackupPolicyRejectsInvalidSchedule(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})

	base := &adminv1.BackupPolicy{Enabled: true, BackupDir: t.TempDir(), IntervalHours: 1, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeoutSeconds: 5, BackupTimeoutSeconds: 30, RetryAfterSeconds: 10, StatusHistoryLimit: 4}
	tests := []*adminv1.BackupPolicy{
		func() *adminv1.BackupPolicy { cp := *base; cp.ScheduleKind = "hourly"; return &cp }(),
		func() *adminv1.BackupPolicy {
			cp := *base
			cp.ScheduleKind = backupcore.ScheduleKindDaily
			cp.TimeOfDay = "9:00"
			cp.Timezone = "UTC"
			return &cp
		}(),
		func() *adminv1.BackupPolicy {
			cp := *base
			cp.ScheduleKind = backupcore.ScheduleKindDaily
			cp.TimeOfDay = "09:00"
			cp.Timezone = "Mars/Olympus"
			return &cp
		}(),
		func() *adminv1.BackupPolicy {
			cp := *base
			cp.ScheduleKind = backupcore.ScheduleKindWeekly
			cp.TimeOfDay = "09:00"
			cp.Timezone = "UTC"
			return &cp
		}(),
		func() *adminv1.BackupPolicy {
			cp := *base
			cp.ScheduleKind = backupcore.ScheduleKindWeekly
			cp.TimeOfDay = "09:00"
			cp.Timezone = "UTC"
			cp.Weekdays = []int32{7}
			return &cp
		}(),
	}
	for _, tt := range tests {
		_, err := svc.UpdateBackupPolicy(authenticatedContext(), &adminv1.UpdateBackupPolicyRequest{Policy: tt})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("UpdateBackupPolicy(%#v) code = %v, want InvalidArgument (err=%v)", tt, status.Code(err), err)
		}
	}
}

func TestAdminBackupPolicyPreservesRepositoryContractFields(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	backupDir := t.TempDir()
	policy := &adminv1.BackupPolicy{
		Enabled:                    true,
		BackupDir:                  backupDir,
		IntervalHours:              1,
		RetentionCount:             2,
		ArchiveFormat:              adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP,
		QuiesceDrainTimeoutSeconds: 5,
		BackupTimeoutSeconds:       30,
		RetryAfterSeconds:          10,
		StatusHistoryLimit:         4,
		DefaultRepositoryId:        "repo-prod",
		CaptureProvider:            adminv1.BackupCaptureProvider_BACKUP_CAPTURE_PROVIDER_FILESYSTEM_IMMUTABLE_COPY,
		QuiesceReadProfile:         adminv1.BackupQuiesceReadProfile_BACKUP_QUIESCE_READ_PROFILE_CONTROL_ONLY,
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

func TestAdminBackupRepositoryBackedClusterOperationsRequireConfiguredManagers(t *testing.T) {
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: backupAPIFixtureDataDir(t), Policy: backupcore.Policy{BackupDir: t.TempDir()}})
	svc := NewAdminBackupService(manager, quiesce.NewCoordinator(), &fakeOperatorManager{systemAdmin: true})
	if _, err := svc.StartClusterBackup(authenticatedContext(), &adminv1.StartClusterBackupRequest{RepositoryId: "repo-prod"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartClusterBackup(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.ListClusterBackupSets(authenticatedContext(), &adminv1.ListClusterBackupSetsRequest{RepositoryId: "repo-prod"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListClusterBackupSets(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.ValidateClusterBackupSet(authenticatedContext(), &adminv1.ValidateClusterBackupSetRequest{RepositoryId: "repo-prod", BackupSetId: "backup-set-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ValidateClusterBackupSet(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := svc.PlanClusterRestore(authenticatedContext(), &adminv1.PlanClusterRestoreRequest{RepositoryId: "repo-prod", BackupSetId: "backup-set-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PlanClusterRestore(repository) code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestAdminBackupTriggerListStatusAndDelete(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	backupDir := t.TempDir()
	coord := quiesce.NewCoordinator()
	gate := quiesce.NewGate("test-service")
	if err := coord.Register(gate); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: backupDir, IncludeLogs: true}, Quiesce: coord})
	svc := NewAdminBackupService(manager, coord, &fakeOperatorManager{systemAdmin: true})

	triggerRes, err := svc.TriggerBackup(authenticatedContext(), &adminv1.TriggerBackupRequest{Reason: "operator test"})
	if err != nil {
		t.Fatalf("TriggerBackup() error = %v", err)
	}
	backupID := triggerRes.GetBackup().GetBackupId()
	if backupID == "" || triggerRes.GetStatus().GetState() != string(backupcore.RunStateSucceeded) || triggerRes.GetBackup().GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP || triggerRes.GetBackup().GetCompression() != "zip" {
		t.Fatalf("unexpected trigger response: %#v", triggerRes)
	}
	if len(triggerRes.GetStatus().GetParticipants()) != 1 || triggerRes.GetStatus().GetParticipants()[0].GetName() != "test-service" {
		t.Fatalf("expected quiesce participant status, got %#v", triggerRes.GetStatus().GetParticipants())
	}

	statusRes, err := svc.GetBackupStatus(authenticatedContext(), &adminv1.GetBackupStatusRequest{})
	if err != nil {
		t.Fatalf("GetBackupStatus() error = %v", err)
	}
	if statusRes.GetStatus().GetBackupId() != backupID || len(statusRes.GetQuiesce().GetParticipants()) != 1 {
		t.Fatalf("unexpected status response: %#v", statusRes)
	}

	listRes, err := svc.ListBackups(authenticatedContext(), &adminv1.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(listRes.GetBackups()) != 1 || listRes.GetBackups()[0].GetBackupId() != backupID {
		t.Fatalf("unexpected list response: %#v", listRes)
	}

	deleteRes, err := svc.DeleteBackup(authenticatedContext(), &adminv1.DeleteBackupRequest{BackupId: backupID})
	if err != nil {
		t.Fatalf("DeleteBackup() error = %v", err)
	}
	if deleteRes.GetBackupId() != backupID {
		t.Fatalf("unexpected delete response: %#v", deleteRes)
	}
	listRes, err = svc.ListBackups(authenticatedContext(), &adminv1.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("ListBackups() after delete error = %v", err)
	}
	if len(listRes.GetBackups()) != 0 {
		t.Fatalf("expected no backups after delete, got %#v", listRes.GetBackups())
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

func TestAdminBackupConcurrentTriggerReturnsConflict(t *testing.T) {
	dataDir := backupAPIFixtureDataDir(t)
	participant := &blockingBackupParticipant{name: "block", entered: make(chan struct{}), release: make(chan struct{})}
	coord := quiesce.NewCoordinator()
	if err := coord.Register(participant); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	manager := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir(), QuiesceDrainTimeout: time.Minute}, Quiesce: coord})
	svc := NewAdminBackupService(manager, coord, &fakeOperatorManager{systemAdmin: true})
	done := make(chan error, 1)
	go func() {
		_, err := svc.TriggerBackup(authenticatedContext(), &adminv1.TriggerBackupRequest{})
		done <- err
	}()
	select {
	case <-participant.entered:
	case <-time.After(time.Second):
		t.Fatal("first trigger did not enter quiesce participant")
	}
	_, err := svc.TriggerBackup(authenticatedContext(), &adminv1.TriggerBackupRequest{})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("second TriggerBackup() code = %v, want Aborted (err=%v)", status.Code(err), err)
	}
	close(participant.release)
	if err := <-done; err != nil {
		t.Fatalf("first TriggerBackup() error = %v", err)
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

type blockingBackupParticipant struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (p *blockingBackupParticipant) Name() string { return p.name }
func (p *blockingBackupParticipant) Status() quiesce.ParticipantStatus {
	return quiesce.ParticipantStatus{Name: p.name}
}
func (p *blockingBackupParticipant) Quiesce(ctx context.Context, req quiesce.Request) (quiesce.Lease, error) {
	close(p.entered)
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return quiesce.LeaseFunc(func(context.Context) error { return nil }), nil
}
