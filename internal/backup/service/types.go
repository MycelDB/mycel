package service

import (
	"context"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

const ModuleName = "backup"

type Config struct {
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

type Manager interface {
	Policy() backupcore.Policy
	UpdatePolicy(context.Context, backupcore.Policy) (backupcore.Policy, error)
	RunStatus() backupcore.RunStatus
	ListBackups(context.Context) ([]backupcore.Manifest, error)
	DeleteBackup(context.Context, string) error
	Trigger(context.Context, backupcore.TriggerInput) (backupcore.TriggerResult, error)
}

type BackupRepositoryManager interface {
	ListBackupRepositories() []BackupRepository
	GetBackupRepository(string) (BackupRepository, error)
	CreateBackupRepository(context.Context, BackupRepository) (BackupRepository, error)
	UpdateBackupRepository(context.Context, BackupRepository) (BackupRepository, error)
	ValidateBackupRepository(context.Context, string) (BackupRepositoryValidationSummary, error)
	DeleteBackupRepository(context.Context, string) error
}

type ClusterBackupManager interface {
	StartClusterBackup(context.Context, StartClusterBackupInput) (ClusterBackupRunStatus, error)
	CancelClusterBackup(context.Context, string, string) (ClusterBackupRunStatus, error)
	ClusterBackupStatus(string) (ClusterBackupRunStatus, error)
	ListClusterBackups() []ClusterBackupRunStatus
}

type ClusterRestorePlanner interface {
	PlanClusterRestore(context.Context, PlanClusterRestoreInput) (ClusterRestorePlan, error)
}
