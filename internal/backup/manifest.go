package backup

import "time"

const ManifestVersion = 1

// Manifest describes a completed backup archive. It is written beside the
// archive as <backup-id>.manifest.json after the archive is atomically renamed
// into place.
type Manifest struct {
	Version        int           `json:"version"`
	BackupID       string        `json:"backup_id"`
	ArchiveName    string        `json:"archive_name"`
	CreatedAt      time.Time     `json:"created_at"`
	CompletedAt    time.Time     `json:"completed_at"`
	SizeBytes      int64         `json:"size_bytes"`
	ChecksumSHA256 string        `json:"checksum_sha256"`
	DaemonVersion  string        `json:"daemon_version,omitempty"`
	Policy         PolicySummary `json:"policy"`
}

type PolicySummary struct {
	IncludeLogs            bool   `json:"include_logs"`
	Compression            string `json:"compression"`
	RetentionCount         int    `json:"retention_count"`
	AllowReadsDuringBackup bool   `json:"allow_reads_during_backup"`
}

func policySummary(policy Policy) PolicySummary {
	return PolicySummary{IncludeLogs: policy.IncludeLogs, Compression: policy.Compression, RetentionCount: policy.RetentionCount, AllowReadsDuringBackup: policy.AllowReadsDuringBackup}
}
