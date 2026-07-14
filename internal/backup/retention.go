package backup

import "context"

// ApplyRetention keeps the newest RetentionCount complete backups and removes
// older complete archive/manifest pairs. Incomplete .tmp files are ignored
// because ListBackups only returns complete manifest+archive pairs.
func (m *Manager) ApplyRetention(ctx context.Context) error {
	policy := m.Policy()
	if policy.RetentionCount <= 0 {
		return nil
	}
	backups, err := m.ListBackups(ctx)
	if err != nil {
		return err
	}
	for i := policy.RetentionCount; i < len(backups); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.DeleteBackup(ctx, backups[i].BackupID); err != nil {
			return err
		}
	}
	return nil
}
