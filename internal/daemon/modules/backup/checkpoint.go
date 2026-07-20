package backup

import (
	"context"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/wal"
)

func (m *Module) triggerWithWALCheckpoint(ctx context.Context, input backupcore.TriggerInput) (backupcore.TriggerResult, error) {
	if m.wal != nil && m.progress != nil && m.checkpoint != nil {
		cp, err := wal.CreateCheckpoint(ctx, m.progress, m.checkpoint, 0)
		if err != nil {
			return backupcore.TriggerResult{}, err
		}
		if cp.LSN > 0 {
			if err := m.wal.RetainFrom(ctx, cp.LSN+1); err != nil {
				return backupcore.TriggerResult{}, err
			}
		}
	}
	return m.manager.Trigger(ctx, input)
}
