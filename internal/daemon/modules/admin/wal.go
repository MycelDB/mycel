package admin

import (
	"context"
	"encoding/json"

	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeAdminPut wal.RecordType = "identity.admin.put.v1"

type adminPutRecord struct {
	Admin Admin `json:"admin"`
}

func (m *Module) applyAdminPut(ctx context.Context, rec wal.Record) error {
	var payload adminPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.store.ApplyPut(ctx, payload.Admin)
	return err
}

func (m *Module) commitAdminPut(ctx context.Context, admin Admin) (Admin, error) {
	payload, err := json.Marshal(adminPutRecord{Admin: admin})
	if err != nil {
		return Admin{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeAdminPut, SchemaVersion: 1, Timestamp: admin.UpdatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return Admin{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return Admin{}, err
	}
	applied, err := m.store.ApplyPut(ctx, admin)
	if err != nil {
		return Admin{}, err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return Admin{}, err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return applied, nil
}
