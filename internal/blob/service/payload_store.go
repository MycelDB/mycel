package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	blobstorage "github.com/myceldb/mycel/internal/blob/storage"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
)

const (
	blobBackendLocal       = "local"
	blobBackendS3          = "s3"
	blobBackendObjectStore = "object_store"
	objectStoreProviderS3  = "s3-compatible"
)

func effectiveBlobConfig(cfg Config) Config {
	cfg.Backend = strings.ToLower(strings.TrimSpace(cfg.Backend))
	if cfg.Backend == "" {
		cfg.Backend = blobBackendLocal
	}
	cfg.ObjectStoreProvider = strings.ToLower(strings.TrimSpace(cfg.ObjectStoreProvider))
	if cfg.ObjectStoreProvider == "" && isObjectStoreBackend(cfg.Backend) {
		cfg.ObjectStoreProvider = objectStoreProviderS3
	}
	cfg.S3Bucket = strings.TrimSpace(cfg.S3Bucket)
	cfg.S3Prefix = strings.Trim(strings.TrimSpace(cfg.S3Prefix), "/")
	cfg.S3Region = strings.TrimSpace(cfg.S3Region)
	cfg.S3KMSKeyID = strings.TrimSpace(cfg.S3KMSKeyID)
	cfg.S3EndpointURL = strings.TrimSpace(cfg.S3EndpointURL)
	return cfg
}

func validateBlobConfig(cfg Config) error {
	cfg = effectiveBlobConfig(cfg)
	switch cfg.Backend {
	case blobBackendLocal:
		return nil
	case blobBackendS3, blobBackendObjectStore:
		if provider := strings.TrimSpace(cfg.ObjectStoreProvider); provider != "" && provider != objectStoreProviderS3 {
			return fmt.Errorf("object store blob backend provider must be %s", objectStoreProviderS3)
		}
		if cfg.S3Bucket == "" {
			return fmt.Errorf("object store blob backend requires MYCELD_BLOB_OBJECT_STORE_BUCKET")
		}
		return nil
	default:
		return fmt.Errorf("unsupported blob backend %q", cfg.Backend)
	}
}

func isObjectStoreBackend(backend string) bool {
	backend = strings.ToLower(strings.TrimSpace(backend))
	return backend == blobBackendS3 || backend == blobBackendObjectStore
}

func (m *Module) putPayload(ctx context.Context, spaceID string, mimeType string, r io.Reader) (graphmodel.BlobID, int64, PayloadDescriptor, error) {
	if isObjectStoreBackend(m.config.Backend) {
		if m.s3Store == nil {
			return "", 0, PayloadDescriptor{}, fmt.Errorf("object store blob backend is not initialized")
		}
		return m.s3Store.Put(ctx, spaceID, mimeType, r)
	}
	store, err := m.store(spaceID)
	if err != nil {
		return "", 0, PayloadDescriptor{}, err
	}
	id, size, err := store.Put(ctx, r)
	if err != nil {
		return "", 0, PayloadDescriptor{}, err
	}
	return id, size, PayloadDescriptor{Backend: blobBackendLocal, SpaceID: spaceID, BlobID: string(id), SizeBytes: size, ChecksumAlgorithm: "sha256", ChecksumHex: string(id)}, nil
}

func (m *Module) payloadExists(ctx context.Context, desc PayloadDescriptor) (bool, error) {
	switch payloadBackend(desc) {
	case blobBackendS3, blobBackendObjectStore:
		if m.s3Store == nil {
			return false, fmt.Errorf("object store blob backend is not initialized")
		}
		return m.s3Store.Exists(ctx, desc)
	default:
		store, err := m.store(desc.SpaceID)
		if err != nil {
			return false, err
		}
		return store.Exists(ctx, graphmodel.BlobID(desc.BlobID))
	}
}

func (m *Module) openPayload(ctx context.Context, desc PayloadDescriptor) (io.ReadCloser, error) {
	switch payloadBackend(desc) {
	case blobBackendS3, blobBackendObjectStore:
		if m.s3Store == nil {
			return nil, fmt.Errorf("object store blob backend is not initialized")
		}
		return m.s3Store.Open(ctx, desc)
	default:
		store, err := m.store(desc.SpaceID)
		if err != nil {
			return nil, err
		}
		return store.Open(ctx, graphmodel.BlobID(desc.BlobID))
	}
}

func (m *Module) deletePayload(ctx context.Context, desc PayloadDescriptor) error {
	switch payloadBackend(desc) {
	case blobBackendS3, blobBackendObjectStore:
		if m.s3Store == nil {
			return fmt.Errorf("object store blob backend is not initialized")
		}
		return m.s3Store.Delete(ctx, desc)
	default:
		store, err := m.store(desc.SpaceID)
		if err != nil {
			return err
		}
		return store.Delete(ctx, graphmodel.BlobID(desc.BlobID))
	}
}

func payloadBackend(desc PayloadDescriptor) string {
	backend := strings.ToLower(strings.TrimSpace(desc.Backend))
	if backend == "" {
		return blobBackendLocal
	}
	return backend
}

func samePayloadLocation(a PayloadDescriptor, b PayloadDescriptor) bool {
	if payloadBackend(a) != payloadBackend(b) {
		return false
	}
	if a.BlobID != b.BlobID || a.SpaceID != b.SpaceID {
		return false
	}
	if isObjectStoreBackend(payloadBackend(a)) {
		return strings.TrimSpace(a.S3Bucket) == strings.TrimSpace(b.S3Bucket) && strings.TrimSpace(a.S3Key) == strings.TrimSpace(b.S3Key)
	}
	return true
}

func (m *Module) logBestEffortPayloadDeleteFailure(desc PayloadDescriptor, err error) {
	if err == nil || m.logger == nil {
		return
	}
	m.logger.Warn("object store blob payload delete failed after metadata delete; object may need garbage collection", "space_id", desc.SpaceID, "blob_id", desc.BlobID, "bucket", desc.S3Bucket, "key", desc.S3Key, "error", err)
}

func (m *Module) openLocalStore(spaceID string) (*blobstorage.Store, error) {
	return blobstorage.Open(filepath.Join(m.dataDir, spaceID))
}

func logBlobBackend(logger *slog.Logger, cfg Config, dataDir string) {
	if logger == nil {
		return
	}
	if isObjectStoreBackend(cfg.Backend) {
		logger.Info("blob module initialized", "storage", "object_store", "provider", cfg.ObjectStoreProvider, "bucket", cfg.S3Bucket, "prefix", cfg.S3Prefix, "local_staging_path", dataDir)
		return
	}
	logger.Info("blob module initialized", "storage", "file", "path", dataDir)
}
