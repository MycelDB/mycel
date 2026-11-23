package backup

import (
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultInterval                     = 24 * time.Hour
	DefaultRetentionCount               = 7
	DefaultCompression                  = string(ArchiveFormatZip)
	DefaultArchiveFormat                = ArchiveFormatZip
	DefaultQuiesceDrainTimeout          = 2 * time.Minute
	DefaultBackupTimeout                = 30 * time.Minute
	DefaultRetryAfter                   = 5 * time.Second
	DefaultStatusHistoryLimit           = 20
	DefaultScheduleKind                 = ScheduleKindInterval
	DefaultTimezone                     = "UTC"
	DefaultProductionCaptureProvider    = "auto"
	DefaultProductionQuiesceReadProfile = "control_only"
	DefaultKeepFor                      = 14 * 24 * time.Hour
	DefaultDeleteIncompleteAfter        = 24 * time.Hour
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

type DerivedSubsystemPolicy struct {
	Subsystem string
	Mode      string
}

type RetentionPolicy struct {
	KeepLastSuccessful    int
	KeepFor               time.Duration
	DeleteIncompleteAfter time.Duration
}

type ValidationOptions struct {
	IncludeArtifactChecks           bool
	IncludeExternalDependencyChecks bool
	IncludeCapacityEstimates        bool
	RequireCompleteMarker           bool
}

func DefaultDerivedSubsystemPolicies() []DerivedSubsystemPolicy {
	return []DerivedSubsystemPolicy{
		{Subsystem: "semantic_vector_search", Mode: "include_required"},
		{Subsystem: "lexical_search", Mode: "include_optional"},
		{Subsystem: "graph_checkpoint", Mode: "include_optional"},
		{Subsystem: "persistent_graph_query_index", Mode: "include_optional"},
		{Subsystem: "metadata_adjacency_memory_index", Mode: "discard"},
	}
}

// Policy describes daemon backup scheduling defaults and operator policy.
type Policy struct {
	Enabled        bool
	BackupDir      string
	Interval       time.Duration
	RetentionCount int
	IncludeLogs    bool
	// Compression is the deprecated legacy archive/compression string. Keep it
	// synchronized with ArchiveFormat while persisted policies migrate.
	Compression                string
	ArchiveFormat              ArchiveFormat
	QuiesceDrainTimeout        time.Duration
	BackupTimeout              time.Duration
	RetryAfter                 time.Duration
	StatusHistoryLimit         int
	AllowReadsDuringBackup     bool
	ScheduleKind               string
	TimeOfDay                  string
	Timezone                   string
	Weekdays                   []int
	RunMissed                  bool
	DefaultRepositoryID        string
	CaptureProvider            string
	QuiesceReadProfile         string
	AllowReadOnlyDuringQuiesce bool
	DerivedSubsystems          []DerivedSubsystemPolicy
	Retention                  RetentionPolicy
	Validation                 ValidationOptions
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
	policy.DefaultRepositoryID = strings.TrimSpace(policy.DefaultRepositoryID)
	if strings.TrimSpace(policy.CaptureProvider) == "" {
		policy.CaptureProvider = DefaultProductionCaptureProvider
	} else {
		policy.CaptureProvider = strings.ToLower(strings.TrimSpace(policy.CaptureProvider))
	}
	if strings.TrimSpace(policy.QuiesceReadProfile) == "" {
		policy.QuiesceReadProfile = DefaultProductionQuiesceReadProfile
	} else {
		policy.QuiesceReadProfile = strings.ToLower(strings.TrimSpace(policy.QuiesceReadProfile))
	}
	if policy.Retention.KeepLastSuccessful <= 0 {
		policy.Retention.KeepLastSuccessful = policy.RetentionCount
	}
	if policy.Retention.KeepFor <= 0 {
		policy.Retention.KeepFor = DefaultKeepFor
	}
	if policy.Retention.DeleteIncompleteAfter <= 0 {
		policy.Retention.DeleteIncompleteAfter = DefaultDeleteIncompleteAfter
	}
	if len(policy.DerivedSubsystems) == 0 {
		policy.DerivedSubsystems = DefaultDerivedSubsystemPolicies()
	}
	for i := range policy.DerivedSubsystems {
		policy.DerivedSubsystems[i].Subsystem = strings.ToLower(strings.TrimSpace(policy.DerivedSubsystems[i].Subsystem))
		policy.DerivedSubsystems[i].Mode = strings.ToLower(strings.TrimSpace(policy.DerivedSubsystems[i].Mode))
	}
	return policy
}
