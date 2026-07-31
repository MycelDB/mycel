package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	blobstorage "github.com/myceldb/mycel/internal/blob/storage"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeBlobMetaPut    wal.RecordType = "blob.meta.put.v1"
	recordTypeBlobMetaDelete wal.RecordType = "blob.meta.delete.v1"
)

type blobMetaPutRecord struct {
	Meta              BlobMeta          `json:"meta"`
	PayloadDescriptor PayloadDescriptor `json:"payload_descriptor,omitempty"`
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

func (m *Module) commitMetaPutRaft(ctx context.Context, meta BlobMeta) (BlobMeta, error) {
	desc := descriptorFromMeta(meta)
	if err := m.ensureRaftPayloadWritePolicy(ctx, desc); err != nil {
		return BlobMeta{}, err
	}
	payload, err := json.Marshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: desc})
	if err != nil {
		return BlobMeta{}, err
	}
	cmd, err := m.buildBlobRaftCommand(meta.SpaceID, recordTypeBlobMetaPut, payload, "blob-meta-put-"+meta.SpaceID+"-"+meta.BlobID+"-"+uuid.NewString())
	if err != nil {
		return BlobMeta{}, err
	}
	if err := m.proposeBlobRaftCommand(ctx, cmd); err != nil {
		return BlobMeta{}, err
	}
	return meta, nil
}

func (m *Module) commitMetaPut(ctx context.Context, meta BlobMeta) (BlobMeta, error) {
	payload, err := json.Marshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta)})
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

func (m *Module) commitMetaDeleteRaft(ctx context.Context, spaceID string, blobID string) error {
	payload, err := json.Marshal(blobMetaDeleteRecord{SpaceID: spaceID, BlobID: blobID})
	if err != nil {
		return err
	}
	cmd, err := m.buildBlobRaftCommand(spaceID, recordTypeBlobMetaDelete, payload, "blob-meta-delete-"+spaceID+"-"+blobID+"-"+uuid.NewString())
	if err != nil {
		return err
	}
	return m.proposeBlobRaftCommand(ctx, cmd)
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
	if m.refCounter != nil {
		count, err := m.refCounter.BlobRefCount(ctx, spaceID, blobID)
		if err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("%w: %s has %d graph references", ErrReferenced, blobID, count)
		}
	}
	if id := domaingraph.BlobID(blobID); id != "" {
		store, err := m.store(spaceID)
		if err != nil {
			return err
		}
		if err := store.Delete(ctx, id); err != nil && !errors.Is(err, blobstorage.ErrNotFound) {
			return mapStorageError(err)
		}
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
