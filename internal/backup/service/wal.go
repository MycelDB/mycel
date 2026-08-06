package service

import (
	"context"
	"encoding/json"
	"errors"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeBackupPolicyUpdate wal.RecordType = "daemon.backup.policy.update.v1"
	recordTypeBackupDelete       wal.RecordType = "daemon.backup.delete.v1"

	recordTypeClusterBackupRequest    wal.RecordType = "daemon.backup.cluster.request.v1"
	recordTypeClusterBackupPhase      wal.RecordType = "daemon.backup.cluster.phase.v1"
	recordTypeClusterBackupBarrier    wal.RecordType = "daemon.backup.cluster.barrier.v1"
	recordTypeClusterBackupNodeResult wal.RecordType = "daemon.backup.cluster.node_result.v1"
	recordTypeClusterBackupComplete   wal.RecordType = "daemon.backup.cluster.complete.v1"
	recordTypeClusterBackupFail       wal.RecordType = "daemon.backup.cluster.fail.v1"
	recordTypeClusterBackupAbort      wal.RecordType = "daemon.backup.cluster.abort.v1"
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
	return m.reconcileSchedulerForPolicy(context.Background(), updated)
}

func (m *Module) applyBackupDelete(ctx context.Context, rec wal.Record) error {
	var payload backupDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	if err := m.manager.DeleteBackup(ctx, payload.BackupID); err != nil && !errors.Is(err, backupcore.ErrBackupNotFound) {
		return err
	}
	return nil
}

func (m *Module) clusterBackupWALAppliers() map[wal.RecordType]wal.Applier {
	return map[wal.RecordType]wal.Applier{
		recordTypeClusterBackupRequest:    wal.ApplierFunc(m.applyClusterBackupRequest),
		recordTypeClusterBackupPhase:      wal.ApplierFunc(m.applyClusterBackupPhase),
		recordTypeClusterBackupBarrier:    wal.ApplierFunc(m.applyClusterBackupBarrier),
		recordTypeClusterBackupNodeResult: wal.ApplierFunc(m.applyClusterBackupNodeResult),
		recordTypeClusterBackupComplete:   wal.ApplierFunc(m.applyClusterBackupComplete),
		recordTypeClusterBackupFail: wal.ApplierFunc(func(ctx context.Context, rec wal.Record) error {
			return m.applyClusterBackupFailure(ctx, rec, clusterBackupPhaseFailed)
		}),
		recordTypeClusterBackupAbort: wal.ApplierFunc(func(ctx context.Context, rec wal.Record) error {
			return m.applyClusterBackupFailure(ctx, rec, clusterBackupPhaseAborted)
		}),
	}
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
		err = m.applyBackupPolicyUpdate(ctx, wal.Record{Type: typ, Payload: raw})
	} else if typ == recordTypeBackupDelete {
		err = m.applyBackupDelete(ctx, wal.Record{Type: typ, Payload: raw})
	} else if applier, ok := m.clusterBackupWALAppliers()[typ]; ok {
		err = applier.ApplyWAL(ctx, wal.Record{Type: typ, Payload: raw})
	} else {
		err = errors.New("unsupported backup WAL record type")
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
