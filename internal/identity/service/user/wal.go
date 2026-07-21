package user

import (
	"context"
	"encoding/json"

	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeUserPut wal.RecordType = "identity.user.put.v1"

type userPutRecord struct {
	User User `json:"user"`
}

func (m *Module) applyUserPut(ctx context.Context, rec wal.Record) error {
	var payload userPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.store.ApplyPut(ctx, payload.User)
	return err
}

func (m *Module) commitUserPut(ctx context.Context, user User) (User, error) {
	payload, err := json.Marshal(userPutRecord{User: user})
	if err != nil {
		return User{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeUserPut, SchemaVersion: 1, Timestamp: user.UpdatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return User{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return User{}, err
	}
	applied, err := m.store.ApplyPut(ctx, user)
	if err != nil {
		return User{}, err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return User{}, err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return applied, nil
}
