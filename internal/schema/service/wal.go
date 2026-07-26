package service

import (
	"context"
	"encoding/json"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeSchemaPut    wal.RecordType = "schema.put.v1"
	recordTypeSchemaDelete wal.RecordType = "schema.delete.v1"
)

type schemaPutRecord struct {
	Schema schema.DomainSchema `json:"schema"`
}

type schemaDeleteRecord struct {
	DomainID graph.DomainID `json:"domain_id"`
}

func (m *SchemaManager) WithWAL(manager *wal.Manager, progress wal.AppliedLSNStore, waiter *wal.ApplyWaiter) *SchemaManager {
	m.wal = manager
	m.walProgress = progress
	m.walWaiter = waiter
	return m
}

func (m *SchemaManager) applySchemaPut(ctx context.Context, rec wal.Record) error {
	var payload schemaPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.applyDomainSchema(ctx, payload.Schema)
}

func (m *SchemaManager) applySchemaDelete(ctx context.Context, rec wal.Record) error {
	var payload schemaDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.applyDeleteDomainSchema(ctx, payload.DomainID)
}

func (m *SchemaManager) commitDomainSchema(ctx context.Context, value schema.DomainSchema) error {
	if m.wal == nil {
		return m.applyDomainSchema(ctx, value)
	}
	payload, err := json.Marshal(schemaPutRecord{Schema: value})
	if err != nil {
		return err
	}
	timestamp := value.UpdatedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeSchemaPut, SchemaVersion: 1, Timestamp: timestamp, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.applyDomainSchema(ctx, value); err != nil {
		return err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}

func (m *SchemaManager) commitDeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error {
	if m.wal == nil {
		return m.applyDeleteDomainSchema(ctx, domainID)
	}
	payload, err := json.Marshal(schemaDeleteRecord{DomainID: domainID})
	if err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeSchemaDelete, SchemaVersion: 1, Timestamp: time.Now().UTC(), Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.applyDeleteDomainSchema(ctx, domainID); err != nil {
		return err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}
