package service

import (
	"context"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/projection"
	"github.com/myceldb/mycel/internal/wal"
)

func (m *Module) triggerBackup(ctx context.Context, input backupcore.TriggerInput) (backupcore.TriggerResult, error) {
	return m.manager.Trigger(ctx, input)
}

func (m *Module) projectionBackupCatalog(context.Context) (projection.BackupCatalog, error) {
	if m.projectionRegistries == nil {
		return projection.BackupCatalog{}, nil
	}
	catalog := m.projectionRegistries.BackupCatalog()
	if err := catalog.Validate(); err != nil {
		return projection.BackupCatalog{}, err
	}
	return catalog, nil
}

func (m *Module) prepareBackupSnapshot(ctx context.Context) error {
	if m.wal == nil || m.progress == nil || m.checkpoint == nil {
		return nil
	}
	cp, err := wal.CreateCheckpoint(ctx, m.progress, m.checkpoint, 0)
	if err != nil {
		return err
	}
	if cp.LSN > 0 {
		if err := m.wal.RetainFrom(ctx, cp.LSN+1); err != nil {
			return err
		}
	}
	return nil
}
