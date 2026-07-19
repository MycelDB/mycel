package blob

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/myceldb/mycel/internal/clustering"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/wal"
)

type PayloadDescriptor struct {
	SpaceID           string `json:"space_id"`
	BlobID            string `json:"blob_id"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	ChecksumHex       string `json:"checksum_hex"`
}

func descriptorFromMeta(meta BlobMeta) PayloadDescriptor {
	return PayloadDescriptor{SpaceID: meta.SpaceID, BlobID: meta.BlobID, SizeBytes: meta.SizeBytes, ChecksumAlgorithm: "sha256", ChecksumHex: strings.TrimPrefix(meta.Digest, "sha256:")}
}

func (m *Module) ensurePayloadFromReader(ctx context.Context, desc PayloadDescriptor, r io.Reader) error {
	store, err := m.store(desc.SpaceID)
	if err != nil {
		return err
	}
	if ok, err := store.Exists(ctx, graphmodel.BlobID(desc.BlobID)); err != nil {
		return err
	} else if ok {
		return nil
	}
	id, size, err := store.Put(ctx, r)
	if err != nil {
		return err
	}
	if string(id) != desc.BlobID {
		return fmt.Errorf("blob payload checksum mismatch: got %s want %s", id, desc.BlobID)
	}
	if desc.SizeBytes >= 0 && size != desc.SizeBytes {
		return fmt.Errorf("blob payload size mismatch: got %d want %d", size, desc.SizeBytes)
	}
	return nil
}

type PayloadPreApplyHook struct {
	Module  *Module
	Cluster *clustering.Manager
	Client  clusterbackend.Client
}

func (h PayloadPreApplyHook) BeforeApply(ctx context.Context, rec wal.Record) error {
	if rec.Type != recordTypeBlobMetaPut || h.Module == nil || h.Cluster == nil {
		return nil
	}
	var payload blobMetaPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	desc := descriptorFromMeta(payload.Meta)
	if desc.BlobID == "" || desc.SpaceID == "" {
		return nil
	}
	store, err := h.Module.store(desc.SpaceID)
	if err != nil {
		return err
	}
	if ok, err := store.Exists(ctx, graphmodel.BlobID(desc.BlobID)); err != nil {
		return err
	} else if ok {
		return nil
	}
	authority, ok := h.Cluster.Authority()
	if !ok {
		return fmt.Errorf("blob payload fetch requires cluster authority")
	}
	addr := authority.Primary.BackendAdvertiseAddr
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("blob payload fetch requires primary backend address")
	}
	id := h.Cluster.Identity()
	return h.Client.GetBlobPayload(ctx, addr, &clusterpb.GetBlobPayloadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: id.ClusterID, RequesterNodeId: id.NodeID, SpaceId: desc.SpaceID, BlobId: desc.BlobID, ExpectedSizeBytes: uint64(desc.SizeBytes), ExpectedChecksumAlgorithm: desc.ChecksumAlgorithm, ExpectedChecksumHex: desc.ChecksumHex}, func(r io.Reader) error { return h.Module.ensurePayloadFromReader(ctx, desc, r) })
}
