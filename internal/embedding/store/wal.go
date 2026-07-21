package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeEmbeddingKeyPut    wal.RecordType = "embedding.provider_key.put.v1"
	recordTypeEmbeddingKeyDelete wal.RecordType = "embedding.provider_key.delete.v1"
)

type keyPutRecord struct {
	Key ProviderKeyRecord `json:"key"`
}
type keyDeleteRecord struct {
	OwnerID identity.UserID               `json:"owner_id"`
	ID      domainembedding.ProviderKeyID `json:"id"`
}

type WALManager struct {
	inner    *defaultManager
	wal      *wal.Manager
	progress wal.AppliedLSNStore
	waiter   *wal.ApplyWaiter
}

func NewWALManager(inner Manager, wm *wal.Manager, progress wal.AppliedLSNStore, waiter *wal.ApplyWaiter, registry *wal.Registry) (Manager, error) {
	dm, ok := inner.(*defaultManager)
	if !ok {
		return nil, fmt.Errorf("embedding WAL manager requires default manager")
	}
	w := &WALManager{inner: dm, wal: wm, progress: progress, waiter: waiter}
	if registry != nil {
		if err := registry.Register(recordTypeEmbeddingKeyPut, wal.ApplierFunc(w.applyKeyPut)); err != nil {
			return nil, err
		}
		if err := registry.Register(recordTypeEmbeddingKeyDelete, wal.ApplierFunc(w.applyKeyDelete)); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *WALManager) Init(ctx context.Context, loc string, key string) error {
	return w.inner.Init(ctx, loc, key)
}
func (w *WALManager) ListKeys(ctx context.Context, owner identity.UserID) ([]domainembedding.ProviderKey, error) {
	return w.inner.ListKeys(ctx, owner)
}
func (w *WALManager) GetKey(ctx context.Context, owner identity.UserID, id domainembedding.ProviderKeyID) (domainembedding.ProviderKey, error) {
	return w.inner.GetKey(ctx, owner, id)
}
func (w *WALManager) ResolveAPIKey(ctx context.Context, owner identity.UserID, provider string, id domainembedding.ProviderKeyID) (domainembedding.ProviderKey, string, error) {
	return w.inner.ResolveAPIKey(ctx, owner, provider, id)
}
func (w *WALManager) ListProfiles(ctx context.Context, owner identity.UserID) ([]domainembedding.Profile, error) {
	return w.inner.ListProfiles(ctx, owner)
}

func (w *WALManager) AddKey(ctx context.Context, in AddKeyInput) (domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	if strings.TrimSpace(in.ProviderID) == "" || strings.TrimSpace(in.Name) == "" {
		return domainembedding.ProviderKey{}, fmt.Errorf("%w: provider_id and name are required", ErrInvalidInput)
	}
	ciphertext := ""
	if strings.TrimSpace(in.APIKey) != "" {
		w.inner.mu.Lock()
		c, err := w.inner.encryptLocked(in.APIKey)
		w.inner.mu.Unlock()
		if err != nil {
			return domainembedding.ProviderKey{}, err
		}
		ciphertext = c
	}
	now := time.Now().UTC()
	id, err := uuid.NewV7()
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	rec := ProviderKeyRecord{ID: id, OwnerID: in.OwnerID, ProviderID: strings.TrimSpace(in.ProviderID), Name: strings.TrimSpace(in.Name), IsDefault: in.IsDefault, Disabled: in.Disabled, APIKeyCiphertext: ciphertext, CreatedAt: now, UpdatedAt: now}
	return w.commitPut(ctx, rec)
}
func (w *WALManager) UpdateKey(ctx context.Context, in UpdateKeyInput) (domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	w.inner.mu.RLock()
	idx := w.inner.findKeyLocked(in.OwnerID, in.ID)
	if idx < 0 {
		w.inner.mu.RUnlock()
		return domainembedding.ProviderKey{}, ErrKeyNotFound
	}
	rec := w.inner.data.Keys[idx]
	w.inner.mu.RUnlock()
	if in.Name != nil {
		if strings.TrimSpace(*in.Name) == "" {
			return domainembedding.ProviderKey{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
		}
		rec.Name = strings.TrimSpace(*in.Name)
	}
	if in.APIKey != nil {
		if strings.TrimSpace(*in.APIKey) == "" {
			rec.APIKeyCiphertext = ""
		} else {
			w.inner.mu.Lock()
			c, err := w.inner.encryptLocked(*in.APIKey)
			w.inner.mu.Unlock()
			if err != nil {
				return domainembedding.ProviderKey{}, err
			}
			rec.APIKeyCiphertext = c
		}
	}
	if in.IsDefault != nil {
		rec.IsDefault = *in.IsDefault
	}
	if in.Disabled != nil {
		rec.Disabled = *in.Disabled
	}
	rec.UpdatedAt = time.Now().UTC()
	return w.commitPut(ctx, rec)
}
func (w *WALManager) DeleteKey(ctx context.Context, in DeleteKeyInput) error {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return err
	}
	p, _ := json.Marshal(keyDeleteRecord{OwnerID: in.OwnerID, ID: in.ID})
	lsn, err := w.wal.Append(ctx, wal.PendingRecord{Type: recordTypeEmbeddingKeyDelete, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: p})
	if err != nil {
		return err
	}
	if err := w.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := w.inner.ApplyDeleteKey(ctx, in.OwnerID, in.ID); err != nil {
		return err
	}
	return w.mark(ctx, lsn)
}
func (w *WALManager) ApplyPutKey(ctx context.Context, rec ProviderKeyRecord) (domainembedding.ProviderKey, error) {
	return w.inner.ApplyPutKey(ctx, rec)
}
func (w *WALManager) ApplyDeleteKey(ctx context.Context, owner identity.UserID, id domainembedding.ProviderKeyID) error {
	return w.inner.ApplyDeleteKey(ctx, owner, id)
}
func (w *WALManager) commitPut(ctx context.Context, rec ProviderKeyRecord) (domainembedding.ProviderKey, error) {
	p, _ := json.Marshal(keyPutRecord{Key: rec})
	lsn, err := w.wal.Append(ctx, wal.PendingRecord{Type: recordTypeEmbeddingKeyPut, SchemaVersion: 1, Timestamp: rec.UpdatedAt, Encoding: wal.PayloadEncodingJSON, Payload: p})
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	if err := w.wal.Sync(ctx, lsn); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	out, err := w.inner.ApplyPutKey(ctx, rec)
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	if err := w.mark(ctx, lsn); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	return out, nil
}
func (w *WALManager) mark(ctx context.Context, lsn wal.LSN) error {
	if w.progress != nil {
		if err := w.progress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if w.waiter != nil {
		w.waiter.SetApplied(lsn)
	}
	return nil
}
func (w *WALManager) applyKeyPut(ctx context.Context, rec wal.Record) error {
	var p keyPutRecord
	if err := json.Unmarshal(rec.Payload, &p); err != nil {
		return err
	}
	_, err := w.inner.ApplyPutKey(ctx, p.Key)
	return err
}
func (w *WALManager) applyKeyDelete(ctx context.Context, rec wal.Record) error {
	var p keyDeleteRecord
	if err := json.Unmarshal(rec.Payload, &p); err != nil {
		return err
	}
	return w.inner.ApplyDeleteKey(ctx, p.OwnerID, p.ID)
}
