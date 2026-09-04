package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/blob/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

const sniffLen = 512

type Module struct {
	mu                   sync.Mutex
	dataDir              string
	metaDir              string
	stores               map[string]*blobstorage.Store
	config               Config
	s3Store              *s3PayloadStore
	logger               *slog.Logger
	refCounter           RefCounter
	gate                 *quiesce.Gate
	wal                  *wal.Manager
	walProgress          wal.AppliedLSNStore
	walWaiter            *wal.ApplyWaiter
	writeAllowed         func() error
	raftGroups           *consensus.MultiGroup
	raftPartitionCount   uint32
	raftLocalNode        consensus.NodeID
	raftNodeAddrs        []string
	raftBackendAuthToken string
	raftClusterID        string
	raftAppliedCommands  map[string]struct{}
}

func NewModule(refCounter RefCounter, configs ...Config) *Module {
	cfg := Config{}
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return &Module{stores: map[string]*blobstorage.Store{}, config: effectiveBlobConfig(cfg), refCounter: refCounter, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	m.logger = host.Log()
	m.config = effectiveBlobConfig(m.config)
	if err := validateBlobConfig(m.config); err != nil {
		return runtime.Abort(ModuleName, "config", "validate blob storage configuration", err)
	}
	m.dataDir = filepath.Join(host.DataDir(), "blobs")
	m.metaDir = filepath.Join(host.DataDir(), "blob_meta")
	for _, dir := range []string{m.dataDir, m.metaDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return runtime.Abort(ModuleName, "storage", "create blob data directory", err)
		}
	}
	if m.stores == nil {
		m.stores = map[string]*blobstorage.Store{}
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	if m.config.Backend == blobBackendS3 && m.s3Store == nil {
		store, err := newS3PayloadStore(ctx, m.config, filepath.Join(m.dataDir, "_s3_staging"))
		if err != nil {
			return runtime.Abort(ModuleName, "storage", "initialize S3 blob backend", err)
		}
		m.s3Store = store
	}
	m.loadRaftAppliedCommands()
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeBlobMetaPut, wal.ApplierFunc(m.applyBlobMetaPut)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register blob metadata put WAL applier", err)
			}
			if err := registry.Register(recordTypeBlobMetaDelete, wal.ApplierFunc(m.applyBlobMetaDelete)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register blob metadata delete WAL applier", err)
			}
		}
	}
	m.writeAllowed = func() error { return nil }
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if registrar, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register blob quiesce participant", err)
		}
	}
	logBlobBackend(host.Log(), m.config, m.dataDir)
	return runtime.OK(ModuleName)
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
	head := make([]byte, sniffLen)
	n, err := io.ReadFull(input.Reader, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return BlobMeta{}, err
	}
	head = head[:n]
	mimeType := normalizeMimeType(http.DetectContentType(head))
	id, size, payload, err := m.putPayload(ctx, spaceID, mimeType, io.MultiReader(bytes.NewReader(head), input.Reader))
	if err != nil {
		return BlobMeta{}, mapStorageError(err)
	}
	blobID := string(id)
	metas, err := m.loadSpaceMeta(spaceID)
	if err != nil {
		return BlobMeta{}, err
	}
	if existing, ok := metas[blobID]; ok {
		if !samePayloadLocation(descriptorFromMeta(existing), payload) {
			if err := m.deletePayload(ctx, payload); err != nil && payloadBackend(payload) == blobBackendS3 {
				m.logBestEffortPayloadDeleteFailure(payload, err)
			}
		}
		// Keep original create time and size/digest, but refresh client-declared metadata.
		existing.MimeType = firstNonEmpty(existing.MimeType, mimeType)
		if strings.TrimSpace(input.DeclaredMimeType) != "" {
			existing.DeclaredMimeType = strings.TrimSpace(input.DeclaredMimeType)
		}
		if strings.TrimSpace(input.OriginalFilename) != "" {
			existing.OriginalFilename = filepath.Base(input.OriginalFilename)
		}
		if m.raftGroups != nil {
			return m.commitMetaPutRaft(ctx, existing)
		}
		if m.wal != nil {
			return m.commitMetaPut(ctx, existing)
		}
		if err := m.applyMetaPut(ctx, existing); err != nil {
			return BlobMeta{}, err
		}
		return existing, nil
	}
	meta := BlobMeta{BlobID: blobID, SpaceID: spaceID, Digest: "sha256:" + blobID, SizeBytes: size, MimeType: mimeType, DeclaredMimeType: strings.TrimSpace(input.DeclaredMimeType), OriginalFilename: filepath.Base(strings.TrimSpace(input.OriginalFilename)), CreateTime: time.Now().UTC(), Payload: &payload}
	if m.raftGroups != nil {
		return m.commitMetaPutRaft(ctx, meta)
	}
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
	exists, err := m.payloadExists(ctx, descriptorFromMeta(meta))
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
	reader, err := m.openPayload(ctx, descriptorFromMeta(meta))
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
	if m.raftGroups != nil {
		if err := m.commitMetaDeleteRaft(ctx, meta.SpaceID, meta.BlobID); err != nil {
			return "", err
		}
		return meta.BlobID, nil
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
	if m.raftGroups == nil {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return nil, err
		}
	}
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) requireLocalWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
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
