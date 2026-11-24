package service

import (
	"context"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
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

type ClusterRestoreManager interface {
	ClusterRestorePlanner
	StartClusterRestore(context.Context, StartClusterRestoreInput) (ClusterRestoreOperation, error)
	ClusterRestoreStatus(string) (ClusterRestoreOperation, error)
	ListClusterRestores() []ClusterRestoreOperation
	CancelClusterRestore(context.Context, string, string) (ClusterRestoreOperation, error)
}

type ExternalObjectDependencyProvider interface {
	BackupExternalObjectStoreDependencies(context.Context, clusterbackup.ExternalObjectDependencyRequest) ([]clusterbackup.ExternalObjectStoreDependency, error)
}

type ExternalObjectDependencyValidator interface {
	ValidateExternalObjectStoreDependency(context.Context, clusterbackup.ExternalObjectStoreDependencyValidationInput) clusterbackup.ExternalObjectStoreDependencyValidationResult
}
