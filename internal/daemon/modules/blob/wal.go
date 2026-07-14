package blob

import (
	"context"
	"encoding/json"

	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeBlobMetaPut    wal.RecordType = "blob.meta.put.v1"
	recordTypeBlobMetaDelete wal.RecordType = "blob.meta.delete.v1"
)

type blobMetaPutRecord struct {
	Meta BlobMeta `json:"meta"`
}
type blobMetaDeleteRecord struct {
	SpaceID string `json:"space_id"`
	BlobID  string `json:"blob_id"`
}

func (m *Module) applyBlobMetaPut(ctx context.Context, rec wal.Record) error {
	var payload blobMetaPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.applyMetaPut(ctx, payload.Meta)
}

func (m *Module) applyBlobMetaDelete(ctx context.Context, rec wal.Record) error {
	var payload blobMetaDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.applyMetaDelete(ctx, payload.SpaceID, payload.BlobID)
}

func (m *Module) commitMetaPut(ctx context.Context, meta BlobMeta) (BlobMeta, error) {
	payload, err := json.Marshal(blobMetaPutRecord{Meta: meta})
	if err != nil {
		return BlobMeta{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeBlobMetaPut, SchemaVersion: 1, Timestamp: meta.CreateTime, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return BlobMeta{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return BlobMeta{}, err
	}
	if err := m.applyMetaPut(ctx, meta); err != nil {
		return BlobMeta{}, err
	}
	if err := m.markWALApplied(ctx, lsn); err != nil {
		return BlobMeta{}, err
	}
	return meta, nil
}

func (m *Module) commitMetaDelete(ctx context.Context, spaceID string, blobID string) error {
	payload, err := json.Marshal(blobMetaDeleteRecord{SpaceID: spaceID, BlobID: blobID})
	if err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeBlobMetaDelete, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.applyMetaDelete(ctx, spaceID, blobID); err != nil {
		return err
	}
	return m.markWALApplied(ctx, lsn)
}

func (m *Module) applyMetaPut(ctx context.Context, meta BlobMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(meta.SpaceID)
	if err != nil {
		return err
	}
	metas[meta.BlobID] = meta
	return m.saveSpaceMetaLocked(meta.SpaceID, metas)
}

func (m *Module) applyMetaDelete(ctx context.Context, spaceID string, blobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(spaceID)
	if err != nil {
		return err
	}
	delete(metas, blobID)
	return m.saveSpaceMetaLocked(spaceID, metas)
}

func (m *Module) markWALApplied(ctx context.Context, lsn wal.LSN) error {
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
