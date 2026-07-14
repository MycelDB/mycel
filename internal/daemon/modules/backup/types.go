package backup

import (
	"context"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

const ModuleName = "backup"

type Manager interface {
	Policy() backupcore.Policy
	UpdatePolicy(context.Context, backupcore.Policy) (backupcore.Policy, error)
	RunStatus() backupcore.RunStatus
	ListBackups(context.Context) ([]backupcore.Manifest, error)
	DeleteBackup(context.Context, string) error
	Trigger(context.Context, backupcore.TriggerInput) (backupcore.TriggerResult, error)
}
