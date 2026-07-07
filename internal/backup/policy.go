package backup

import (
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultInterval            = 24 * time.Hour
	DefaultRetentionCount      = 7
	DefaultCompression         = "zip"
	DefaultQuiesceDrainTimeout = 2 * time.Minute
	DefaultBackupTimeout       = 30 * time.Minute
	DefaultRetryAfter          = 5 * time.Second
	DefaultStatusHistoryLimit  = 20
)

// Policy describes daemon backup scheduling defaults and operator policy.
type Policy struct {
	Enabled                bool
	BackupDir              string
	Interval               time.Duration
	RetentionCount         int
	IncludeLogs            bool
	Compression            string
	QuiesceDrainTimeout    time.Duration
	BackupTimeout          time.Duration
	RetryAfter             time.Duration
	StatusHistoryLimit     int
	AllowReadsDuringBackup bool
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
	if strings.TrimSpace(policy.Compression) == "" {
		policy.Compression = DefaultCompression
	}
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
	return policy
}
