package backup

import (
	"path/filepath"
	"testing"
)

func TestEffectivePolicyAppliesDefaults(t *testing.T) {
	dataDir := filepath.Join(string(filepath.Separator), "data", "mycel")
	policy := EffectivePolicy(dataDir, Policy{})
	if policy.BackupDir != filepath.Join(string(filepath.Separator), "data", "mycel-backups") {
		t.Fatalf("BackupDir = %q", policy.BackupDir)
	}
	if policy.Interval != DefaultInterval {
		t.Fatalf("Interval = %v, want %v", policy.Interval, DefaultInterval)
	}
	if policy.RetentionCount != DefaultRetentionCount {
		t.Fatalf("RetentionCount = %d, want %d", policy.RetentionCount, DefaultRetentionCount)
	}
	if policy.Compression != DefaultCompression {
		t.Fatalf("Compression = %q, want %q", policy.Compression, DefaultCompression)
	}
	if policy.ArchiveFormat != DefaultArchiveFormat {
		t.Fatalf("ArchiveFormat = %q, want %q", policy.ArchiveFormat, DefaultArchiveFormat)
	}
	if policy.QuiesceDrainTimeout != DefaultQuiesceDrainTimeout {
		t.Fatalf("QuiesceDrainTimeout = %v, want %v", policy.QuiesceDrainTimeout, DefaultQuiesceDrainTimeout)
	}
	if policy.BackupTimeout != DefaultBackupTimeout {
		t.Fatalf("BackupTimeout = %v, want %v", policy.BackupTimeout, DefaultBackupTimeout)
	}
	if policy.RetryAfter != DefaultRetryAfter {
		t.Fatalf("RetryAfter = %v, want %v", policy.RetryAfter, DefaultRetryAfter)
	}
	if policy.StatusHistoryLimit != DefaultStatusHistoryLimit {
		t.Fatalf("StatusHistoryLimit = %d, want %d", policy.StatusHistoryLimit, DefaultStatusHistoryLimit)
	}
	if policy.ScheduleKind != DefaultScheduleKind {
		t.Fatalf("ScheduleKind = %q, want %q", policy.ScheduleKind, DefaultScheduleKind)
	}
	if policy.Timezone != DefaultTimezone {
		t.Fatalf("Timezone = %q, want %q", policy.Timezone, DefaultTimezone)
	}
}

func TestEffectivePolicyPreservesConfiguredValues(t *testing.T) {
	policy := EffectivePolicy("/data/mycel", Policy{BackupDir: "/backups", Interval: 42, RetentionCount: 2, ArchiveFormat: ArchiveFormatZip, QuiesceDrainTimeout: 43, BackupTimeout: 44, RetryAfter: 45, StatusHistoryLimit: 4, ScheduleKind: ScheduleKindWeekly, TimeOfDay: "22:00", Timezone: "America/Toronto", Weekdays: []int{0, 3}, RunMissed: true})
	if policy.BackupDir != "/backups" || policy.Interval != 42 || policy.RetentionCount != 2 || policy.Compression != "zip" || policy.ArchiveFormat != ArchiveFormatZip || policy.QuiesceDrainTimeout != 43 || policy.BackupTimeout != 44 || policy.RetryAfter != 45 || policy.StatusHistoryLimit != 4 || policy.ScheduleKind != ScheduleKindWeekly || policy.TimeOfDay != "22:00" || policy.Timezone != "America/Toronto" || len(policy.Weekdays) != 2 || !policy.RunMissed {
		t.Fatalf("configured policy was not preserved: %#v", policy)
	}
}

func TestEffectivePolicyMigratesLegacyCompressionToArchiveFormat(t *testing.T) {
	policy := EffectivePolicy("/data/mycel", Policy{Compression: "ZIP"})
	if policy.ArchiveFormat != ArchiveFormatZip || policy.Compression != string(ArchiveFormatZip) {
		t.Fatalf("legacy compression was not normalized: %#v", policy)
	}
}
