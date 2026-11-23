package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	blobmodel "github.com/myceldb/mycel/internal/blob/model"
	blobstorage "github.com/myceldb/mycel/internal/blob/storage"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeBlobMetaPut    wal.RecordType = "blob.meta.put.v1"
	recordTypeBlobMetaDelete wal.RecordType = "blob.meta.delete.v1"
)

type blobMetaPutRecord struct {
	Meta              blobmodel.BlobMeta          `json:"meta"`
	PayloadDescriptor blobmodel.PayloadDescriptor `json:"payload_descriptor,omitempty"`
	UsageMutationID   string                      `json:"usage_mutation_id,omitempty"`
}
type blobMetaDeleteRecord struct {
	SpaceID  string `json:"space_id"`
	DomainID string `json:"domain_id,omitempty"`
	BlobID   string `json:"blob_id"`
}

func (m *Module) applyBlobMetaPut(ctx context.Context, rec wal.Record) error {
	var payload blobMetaPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	if payload.Meta.Payload == nil && payload.PayloadDescriptor.BlobID != "" {
		desc := payload.PayloadDescriptor
		payload.Meta.Payload = &desc
	}
	mutationID := strings.TrimSpace(payload.UsageMutationID)
	if mutationID == "" {
		mutationID = blobUsageWALMutationID("put", rec)
	}
	return m.applyMetaPutWithUsage(ctx, payload.Meta, mutationID)
}

func (m *Module) applyBlobMetaDelete(ctx context.Context, rec wal.Record) error {
	var payload blobMetaDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	return m.applyMetaDeleteWithUsage(ctx, payload.SpaceID, payload.DomainID, payload.BlobID, blobUsageWALMutationID("delete", rec))
}

func (m *Module) commitMetaPutRaft(ctx context.Context, meta blobmodel.BlobMeta) (blobmodel.BlobMeta, error) {
	desc := descriptorFromMeta(meta)
	if err := m.ensureRaftPayloadWritePolicy(ctx, desc); err != nil {
		return blobmodel.BlobMeta{}, err
	}
	commandID := "blob-meta-put-" + meta.SpaceID + "-" + meta.BlobID + "-" + uuid.NewString()
	mutationID := blobUsageRaftMutationID("put", commandID)
	release, err := m.reserveBlobUsagePutAdmission(ctx, meta, mutationID)
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	defer release()
	payload, err := json.Marshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: desc, UsageMutationID: mutationID})
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	cmd, err := m.buildBlobRaftCommand(meta.SpaceID, recordTypeBlobMetaPut, payload, commandID)
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	if err := m.proposeBlobRaftCommand(ctx, cmd); err != nil {
		return blobmodel.BlobMeta{}, err
	}
	return meta, nil
}

func (m *Module) commitMetaPut(ctx context.Context, meta blobmodel.BlobMeta) (blobmodel.BlobMeta, error) {
	mutationID := "blob-usage:put:wal-command:" + uuid.NewString()
	release, err := m.reserveBlobUsagePutAdmission(ctx, meta, mutationID)
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	defer release()
	payload, err := json.Marshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta), UsageMutationID: mutationID})
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeBlobMetaPut, SchemaVersion: 1, Timestamp: meta.CreateTime, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return blobmodel.BlobMeta{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return blobmodel.BlobMeta{}, err
	}
	if err := m.applyMetaPutWithUsage(ctx, meta, mutationID); err != nil {
		return blobmodel.BlobMeta{}, err
	}
	if err := m.markWALApplied(ctx, lsn); err != nil {
		return blobmodel.BlobMeta{}, err
	}
	return meta, nil
}

func (m *Module) commitMetaDeleteRaft(ctx context.Context, spaceID string, domainID string, blobID string) error {
	payload, err := json.Marshal(blobMetaDeleteRecord{SpaceID: spaceID, DomainID: domainID, BlobID: blobID})
	if err != nil {
		return err
	}
	cmd, err := m.buildBlobRaftCommand(spaceID, recordTypeBlobMetaDelete, payload, "blob-meta-delete-"+spaceID+"-"+blobID+"-"+uuid.NewString())
	if err != nil {
		return err
	}
	return m.proposeBlobRaftCommand(ctx, cmd)
}

func (m *Module) commitMetaDelete(ctx context.Context, spaceID string, domainID string, blobID string) error {
	payload, err := json.Marshal(blobMetaDeleteRecord{SpaceID: spaceID, DomainID: domainID, BlobID: blobID})
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
	if err := m.applyMetaDeleteWithUsage(ctx, spaceID, domainID, blobID, blobUsageWALLSNMutationID("delete", lsn)); err != nil {
		return err
	}
	return m.markWALApplied(ctx, lsn)
}

func (m *Module) applyMetaPut(ctx context.Context, meta blobmodel.BlobMeta) error {
	return m.applyMetaPutWithUsage(ctx, meta, "")
}

func (m *Module) applyMetaPutWithUsage(ctx context.Context, meta blobmodel.BlobMeta, mutationID string) error {
	return m.applyMetaPutWithUsageInternal(ctx, meta, mutationID, false)
}

func (m *Module) applyMetaPutWithUsageAdmission(ctx context.Context, meta blobmodel.BlobMeta, mutationID string) error {
	return m.applyMetaPutWithUsageInternal(ctx, meta, mutationID, true)
}

func (m *Module) applyMetaPutWithUsageInternal(ctx context.Context, meta blobmodel.BlobMeta, mutationID string, enforceAdmission bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(meta.SpaceID)
	if err != nil {
		return err
	}
	old, hadOld := metas[blobMetaStorageKey(meta)]
	var releaseAdmission func()
	if enforceAdmission {
		var reserveErr error
		releaseAdmission, reserveErr = m.reserveBlobUsagePutAdmissionLocked(ctx, old, hadOld, meta, mutationID)
		if reserveErr != nil {
			return reserveErr
		}
		defer releaseAdmission()
	}
	metas[blobMetaStorageKey(meta)] = meta
	if err := m.saveSpaceMetaLocked(meta.SpaceID, metas); err != nil {
		return err
	}
	if err := m.applyBlobUsagePutDelta(ctx, old, hadOld, meta, mutationID); err != nil {
		return err
	}
	return nil
}

func (m *Module) applyMetaDelete(ctx context.Context, spaceID string, domainID string, blobID string) error {
	return m.applyMetaDeleteWithUsage(ctx, spaceID, domainID, blobID, "")
}

func (m *Module) applyMetaDeleteWithUsage(ctx context.Context, spaceID string, domainID string, blobID string, mutationID string) error {
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
	var meta blobmodel.BlobMeta
	var metaErr error
	if strings.TrimSpace(domainID) != "" {
		meta, metaErr = m.metaInDomain(spaceID, strings.TrimSpace(domainID), blobID)
	} else {
		meta, metaErr = m.meta(spaceID, blobID)
	}
	if metaErr != nil && !errors.Is(metaErr, ErrNotFound) {
		return metaErr
	}
	desc := descriptorFromMeta(meta)
	if metaErr == nil && !isObjectStoreBackend(payloadBackend(desc)) {
		if err := m.deletePayload(ctx, desc); err != nil && !errors.Is(err, blobstorage.ErrNotFound) {
			return mapStorageError(err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(spaceID)
	if err != nil {
		return err
	}
	if metaErr == nil {
		delete(metas, blobMetaStorageKey(meta))
	} else {
		delete(metas, blobID)
	}
	if err := m.saveSpaceMetaLocked(spaceID, metas); err != nil {
		return err
	}
	if metaErr == nil {
		if err := m.applyBlobUsageDeleteDelta(ctx, meta, mutationID); err != nil {
			return err
		}
	}
	if metaErr == nil && isObjectStoreBackend(payloadBackend(desc)) {
		if err := m.deletePayload(ctx, desc); err != nil {
			m.logBestEffortPayloadDeleteFailure(desc, err)
		}
	}
	return nil
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
