package admin

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
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
