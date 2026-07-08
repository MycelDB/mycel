package backup

import (
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultInterval            = 24 * time.Hour
	DefaultRetentionCount      = 7
	DefaultCompression         = string(ArchiveFormatZip)
	DefaultArchiveFormat       = ArchiveFormatZip
	DefaultQuiesceDrainTimeout = 2 * time.Minute
	DefaultBackupTimeout       = 30 * time.Minute
	DefaultRetryAfter          = 5 * time.Second
	DefaultStatusHistoryLimit  = 20
	DefaultScheduleKind        = ScheduleKindInterval
	DefaultTimezone            = "UTC"
)

const (
	ScheduleKindInterval = "interval"
	ScheduleKindDaily    = "daily"
	ScheduleKindWeekly   = "weekly"
)

type ArchiveFormat string

const (
	ArchiveFormatZip    ArchiveFormat = "zip"
	ArchiveFormatTar    ArchiveFormat = "tar"
	ArchiveFormatTarGz  ArchiveFormat = "tar.gz"
	ArchiveFormatTarZst ArchiveFormat = "tar.zst"
)

// Policy describes daemon backup scheduling defaults and operator policy.
type Policy struct {
	Enabled        bool
	BackupDir      string
	Interval       time.Duration
	RetentionCount int
	IncludeLogs    bool
	// Compression is the deprecated legacy archive/compression string. Keep it
	// synchronized with ArchiveFormat while persisted policies migrate.
	Compression            string
	ArchiveFormat          ArchiveFormat
	QuiesceDrainTimeout    time.Duration
	BackupTimeout          time.Duration
	RetryAfter             time.Duration
	StatusHistoryLimit     int
	AllowReadsDuringBackup bool
	ScheduleKind           string
	TimeOfDay              string
	Timezone               string
	Weekdays               []int
	RunMissed              bool
}

func normalizeArchiveFormat(format ArchiveFormat, legacyCompression string) ArchiveFormat {
	if strings.TrimSpace(string(format)) != "" {
		return ArchiveFormat(strings.ToLower(strings.TrimSpace(string(format))))
	}
	if strings.TrimSpace(legacyCompression) != "" {
		return ArchiveFormat(strings.ToLower(strings.TrimSpace(legacyCompression)))
	}
	return DefaultArchiveFormat
}

// DefaultBackupDir returns a sibling directory beside the daemon data dir.
func DefaultBackupDir(dataDir string) string {
	dataDir = filepath.Clean(strings.TrimSpace(dataDir))
	if dataDir == "." || dataDir == string(filepath.Separator) {
		return ""
	}
	return filepath.Join(filepath.Dir(dataDir), filepath.Base(dataDir)+"-backups")
}

// EffectivePolicy fills unset policy fields with safe defaults.
func EffectivePolicy(dataDir string, policy Policy) Policy {
	if strings.TrimSpace(policy.BackupDir) == "" {
		policy.BackupDir = DefaultBackupDir(dataDir)
	}
	if policy.Interval <= 0 {
		policy.Interval = DefaultInterval
	}
	if policy.RetentionCount <= 0 {
		policy.RetentionCount = DefaultRetentionCount
	}
	policy.ArchiveFormat = normalizeArchiveFormat(policy.ArchiveFormat, policy.Compression)
	policy.Compression = string(policy.ArchiveFormat)
	if policy.QuiesceDrainTimeout <= 0 {
		policy.QuiesceDrainTimeout = DefaultQuiesceDrainTimeout
	}
	if policy.BackupTimeout <= 0 {
		policy.BackupTimeout = DefaultBackupTimeout
	}
	if policy.RetryAfter <= 0 {
		policy.RetryAfter = DefaultRetryAfter
	}
	if policy.StatusHistoryLimit <= 0 {
		policy.StatusHistoryLimit = DefaultStatusHistoryLimit
	}
	if strings.TrimSpace(policy.ScheduleKind) == "" {
		policy.ScheduleKind = DefaultScheduleKind
	} else {
		policy.ScheduleKind = strings.ToLower(strings.TrimSpace(policy.ScheduleKind))
	}
	if strings.TrimSpace(policy.Timezone) == "" {
		policy.Timezone = DefaultTimezone
	} else {
		policy.Timezone = strings.TrimSpace(policy.Timezone)
	}
	policy.TimeOfDay = strings.TrimSpace(policy.TimeOfDay)
	return policy
}
