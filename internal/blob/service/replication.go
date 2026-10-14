package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	blobmodel "github.com/myceldb/mycel/internal/blob/model"
)

func descriptorFromMeta(meta blobmodel.BlobMeta) blobmodel.PayloadDescriptor {
	if meta.Payload != nil {
		desc := *meta.Payload
		desc.SpaceID = firstNonEmpty(desc.SpaceID, meta.SpaceID)
		desc.DomainID = firstNonEmpty(desc.DomainID, meta.DomainID)
		desc.BlobID = firstNonEmpty(desc.BlobID, meta.BlobID)
		if desc.SizeBytes == 0 {
			desc.SizeBytes = meta.SizeBytes
		}
		desc.ChecksumAlgorithm = firstNonEmpty(desc.ChecksumAlgorithm, "sha256")
		desc.ChecksumHex = firstNonEmpty(desc.ChecksumHex, strings.TrimPrefix(meta.Digest, "sha256:"))
		return desc
	}
	return blobmodel.PayloadDescriptor{Backend: "local", SpaceID: meta.SpaceID, DomainID: meta.DomainID, BlobID: meta.BlobID, SizeBytes: meta.SizeBytes, ChecksumAlgorithm: "sha256", ChecksumHex: strings.TrimPrefix(meta.Digest, "sha256:")}
}

func (m *Module) ensurePayloadFromReader(ctx context.Context, desc blobmodel.PayloadDescriptor, r io.Reader) error {
	if ok, err := m.payloadExists(ctx, desc); err != nil || ok {
		return err
	}
	id, size, stored, err := m.putPayload(ctx, desc.SpaceID, desc.DomainID, "", r)
	if err != nil {
		return err
	}
	_ = stored
	if string(id) != desc.BlobID {
		return fmt.Errorf("blob payload checksum mismatch: got %s want %s", id, desc.BlobID)
	}
	if desc.SizeBytes >= 0 && size != desc.SizeBytes {
		return fmt.Errorf("blob payload size mismatch: got %d want %d", size, desc.SizeBytes)
	}
	return nil
}
