package backup

import (
	"fmt"
	"time"

	"github.com/myceldb/mycel/internal/projection"
)

const ManifestVersion = 1

// Manifest describes a completed backup archive. It is written beside the
// archive as <backup-id>.manifest.json after the archive is atomically renamed
// into place.
type Manifest struct {
	Version           int                       `json:"version"`
	BackupID          string                    `json:"backup_id"`
	ArchiveName       string                    `json:"archive_name"`
	CreatedAt         time.Time                 `json:"created_at"`
	CompletedAt       time.Time                 `json:"completed_at"`
	SizeBytes         int64                     `json:"size_bytes"`
	ChecksumSHA256    string                    `json:"checksum_sha256"`
	DaemonVersion     string                    `json:"daemon_version,omitempty"`
	Policy            PolicySummary             `json:"policy"`
	ProjectionCatalog *projection.BackupCatalog `json:"projection_catalog,omitempty"`
}

func (m Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("unsupported backup manifest version %d", m.Version)
	}
	if m.ProjectionCatalog != nil {
		if err := m.ProjectionCatalog.Validate(); err != nil {
			return fmt.Errorf("invalid projection backup catalog: %w", err)
		}
	}
	return nil
}

type PolicySummary struct {
	IncludeLogs            bool   `json:"include_logs"`
	Compression            string `json:"compression"`
	ArchiveFormat          string `json:"archive_format"`
	RetentionCount         int    `json:"retention_count"`
	AllowReadsDuringBackup bool   `json:"allow_reads_during_backup"`
}

func policySummary(policy Policy) PolicySummary {
	policy = EffectivePolicy("", policy)
	return PolicySummary{IncludeLogs: policy.IncludeLogs, Compression: policy.Compression, ArchiveFormat: string(policy.ArchiveFormat), RetentionCount: policy.RetentionCount, AllowReadsDuringBackup: policy.AllowReadsDuringBackup}
}
