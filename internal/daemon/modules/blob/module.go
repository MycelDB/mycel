package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/blob/storage"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/wal"
)

const sniffLen = 512

type Module struct {
	mu          sync.Mutex
	dataDir     string
	metaDir     string
	stores      map[string]*blobstorage.Store
	refCounter  RefCounter
	gate        *quiesce.Gate
	wal         *wal.Manager
	walProgress wal.AppliedLSNStore
	walWaiter   *wal.ApplyWaiter
}

func NewModule(refCounter RefCounter) *Module {
	return &Module{stores: map[string]*blobstorage.Store{}, refCounter: refCounter, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	m.dataDir = filepath.Join(rt.Config.DataDir, "blobs")
	m.metaDir = filepath.Join(rt.Config.DataDir, "blob_meta")
	for _, dir := range []string{m.dataDir, m.metaDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return daemonruntime.Abort(ModuleName, "storage", "create blob data directory", err)
		}
	}
	if m.stores == nil {
		m.stores = map[string]*blobstorage.Store{}
	}
	m.wal = rt.WAL
	m.walProgress = rt.WALProgress
	m.walWaiter = rt.WALWaiter
	if rt.WALRegistry != nil {
		if err := rt.WALRegistry.Register(recordTypeBlobMetaPut, wal.ApplierFunc(m.applyBlobMetaPut)); err != nil {
			return daemonruntime.Abort(ModuleName, "wal", "register blob metadata put WAL applier", err)
		}
		if err := rt.WALRegistry.Register(recordTypeBlobMetaDelete, wal.ApplierFunc(m.applyBlobMetaDelete)); err != nil {
			return daemonruntime.Abort(ModuleName, "wal", "register blob metadata delete WAL applier", err)
		}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register blob quiesce participant", err)
		}
	}
	rt.Logger.Info("blob module initialized", "storage", "file", "path", m.dataDir)
	return daemonruntime.OK(ModuleName)
}

func (m *Module) UploadBlob(ctx context.Context, input UploadInput) (BlobMeta, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return BlobMeta{}, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return BlobMeta{}, err
	}
	spaceID := strings.TrimSpace(input.SpaceID)
	if spaceID == "" || input.Reader == nil {
		return BlobMeta{}, fmt.Errorf("%w: space_id and reader are required", ErrInvalidInput)
	}
	store, err := m.store(spaceID)
	if err != nil {
		return BlobMeta{}, err
	}
	head := make([]byte, sniffLen)
	n, err := io.ReadFull(input.Reader, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return BlobMeta{}, err
	}
	head = head[:n]
	mimeType := normalizeMimeType(http.DetectContentType(head))
	id, size, err := store.Put(ctx, io.MultiReader(bytes.NewReader(head), input.Reader))
	if err != nil {
		return BlobMeta{}, mapStorageError(err)
	}
	blobID := string(id)
	metas, err := m.loadSpaceMeta(spaceID)
	if err != nil {
		return BlobMeta{}, err
	}
	if existing, ok := metas[blobID]; ok {
		// Keep original create time and size/digest, but refresh client-declared metadata.
		existing.MimeType = firstNonEmpty(existing.MimeType, mimeType)
		if strings.TrimSpace(input.DeclaredMimeType) != "" {
			existing.DeclaredMimeType = strings.TrimSpace(input.DeclaredMimeType)
		}
		if strings.TrimSpace(input.OriginalFilename) != "" {
			existing.OriginalFilename = filepath.Base(input.OriginalFilename)
		}
		if m.wal != nil {
			return m.commitMetaPut(ctx, existing)
		}
		if err := m.applyMetaPut(ctx, existing); err != nil {
			return BlobMeta{}, err
		}
		return existing, nil
	}
	meta := BlobMeta{BlobID: blobID, SpaceID: spaceID, Digest: "sha256:" + blobID, SizeBytes: size, MimeType: mimeType, DeclaredMimeType: strings.TrimSpace(input.DeclaredMimeType), OriginalFilename: filepath.Base(strings.TrimSpace(input.OriginalFilename)), CreateTime: time.Now().UTC()}
	if m.wal != nil {
		return m.commitMetaPut(ctx, meta)
	}
	if err := m.applyMetaPut(ctx, meta); err != nil {
		return BlobMeta{}, err
	}
	return meta, nil
}

func (m *Module) GetBlob(ctx context.Context, spaceID string, blobID string) (BlobMeta, error) {
	if err := ctx.Err(); err != nil {
		return BlobMeta{}, err
	}
	meta, err := m.meta(strings.TrimSpace(spaceID), strings.TrimSpace(blobID))
	if err != nil {
		return BlobMeta{}, err
	}
	store, err := m.store(meta.SpaceID)
	if err != nil {
		return BlobMeta{}, err
	}
	exists, err := store.Exists(ctx, domaingraph.BlobID(meta.BlobID))
	if err != nil {
		return BlobMeta{}, mapStorageError(err)
	}
	if !exists {
		return BlobMeta{}, ErrNotFound
	}
	return meta, nil
}

func (m *Module) OpenBlob(ctx context.Context, spaceID string, blobID string) (BlobMeta, io.ReadCloser, error) {
	meta, err := m.GetBlob(ctx, spaceID, blobID)
	if err != nil {
		return BlobMeta{}, nil, err
	}
	store, err := m.store(meta.SpaceID)
	if err != nil {
		return BlobMeta{}, nil, err
	}
	reader, err := store.Open(ctx, domaingraph.BlobID(meta.BlobID))
	if err != nil {
		return BlobMeta{}, nil, mapStorageError(err)
	}
	return meta, reader, nil
}

func (m *Module) DeleteBlob(ctx context.Context, spaceID string, blobID string) (string, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	meta, err := m.GetBlob(ctx, spaceID, blobID)
	if err != nil {
		return "", err
	}
	if m.refCounter != nil {
		count, err := m.refCounter.BlobRefCount(ctx, meta.SpaceID, meta.BlobID)
		if err != nil {
			return "", err
		}
		if count > 0 {
			return "", fmt.Errorf("%w: %s has %d graph references", ErrReferenced, meta.BlobID, count)
		}
	}
	store, err := m.store(meta.SpaceID)
	if err != nil {
		return "", err
	}
	if err := store.Delete(ctx, domaingraph.BlobID(meta.BlobID)); err != nil {
		return "", mapStorageError(err)
	}
	if m.wal != nil {
		if err := m.commitMetaDelete(ctx, meta.SpaceID, meta.BlobID); err != nil {
			return "", err
		}
		return meta.BlobID, nil
	}
	if err := m.applyMetaDelete(ctx, meta.SpaceID, meta.BlobID); err != nil {
		return "", err
	}
	return meta.BlobID, nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) store(spaceID string) (*blobstorage.Store, error) {
	if strings.TrimSpace(spaceID) == "" {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if store := m.stores[spaceID]; store != nil {
		return store, nil
	}
	store, err := blobstorage.Open(filepath.Join(m.dataDir, spaceID))
	if err != nil {
		return nil, err
	}
	m.stores[spaceID] = store
	return store, nil
}

func (m *Module) meta(spaceID string, blobID string) (BlobMeta, error) {
	if spaceID == "" || blobID == "" {
		return BlobMeta{}, fmt.Errorf("%w: space_id and blob_id are required", ErrInvalidInput)
	}
	if _, err := domaingraph.BlobID(blobID).Bytes(); err != nil {
		return BlobMeta{}, fmt.Errorf("%w: invalid blob_id", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(spaceID)
	if err != nil {
		return BlobMeta{}, err
	}
	meta, ok := metas[blobID]
	if !ok {
		return BlobMeta{}, ErrNotFound
	}
	return meta, nil
}

func (m *Module) loadSpaceMeta(spaceID string) (map[string]BlobMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadSpaceMetaLocked(spaceID)
}

func (m *Module) loadSpaceMetaLocked(spaceID string) (map[string]BlobMeta, error) {
	path := filepath.Join(m.metaDir, spaceID+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]BlobMeta{}, nil
		}
		return nil, err
	}
	var metas map[string]BlobMeta
	if err := json.Unmarshal(raw, &metas); err != nil {
		return nil, err
	}
	if metas == nil {
		metas = map[string]BlobMeta{}
	}
	return metas, nil
}

func (m *Module) saveSpaceMetaLocked(spaceID string, metas map[string]BlobMeta) error {
	path := filepath.Join(m.metaDir, spaceID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(metas, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func mapStorageError(err error) error {
	if errors.Is(err, blobstorage.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, blobstorage.ErrInvalidInput) {
		return ErrInvalidInput
	}
	return err
}

func normalizeMimeType(detected string) string {
	if idx := strings.Index(detected, ";"); idx >= 0 {
		detected = detected[:idx]
	}
	return strings.ToLower(strings.TrimSpace(detected))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
