package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	graphmodel "github.com/myceldb/mycel/internal/graph/model"
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
