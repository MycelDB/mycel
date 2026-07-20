package backup

import (
	"context"
	"encoding/json"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeBackupPolicyUpdate wal.RecordType = "daemon.backup.policy.update.v1"
	recordTypeBackupDelete       wal.RecordType = "daemon.backup.delete.v1"
)

type backupPolicyRecord struct {
	Policy backupcore.Policy `json:"policy"`
}
type backupDeleteRecord struct {
	BackupID string `json:"backup_id"`
}

func (m *Module) applyBackupPolicyUpdate(ctx context.Context, rec wal.Record) error {
	var payload backupPolicyRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	updated, err := m.manager.UpdatePolicy(ctx, payload.Policy)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.policy = updated
	m.mu.Unlock()
	return nil
}

func (m *Module) applyBackupDelete(ctx context.Context, rec wal.Record) error {
	var payload backupDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.manager.DeleteBackup(ctx, payload.BackupID)
}

func (m *Module) commitWAL(ctx context.Context, typ wal.RecordType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: raw})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if typ == recordTypeBackupPolicyUpdate {
		err = m.applyBackupPolicyUpdate(ctx, wal.Record{Payload: raw})
	} else {
		err = m.applyBackupDelete(ctx, wal.Record{Payload: raw})
	}
	if err != nil {
		return err
	}
	if m.progress != nil {
		if err := m.progress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.waiter != nil {
		m.waiter.SetApplied(lsn)
	}
	return nil
}
