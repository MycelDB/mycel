package service

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type PayloadDescriptor struct {
	Backend           string `json:"backend,omitempty"`
	SpaceID           string `json:"space_id"`
	BlobID            string `json:"blob_id"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	ChecksumHex       string `json:"checksum_hex"`
	S3Bucket          string `json:"s3_bucket,omitempty"`
	S3Key             string `json:"s3_key,omitempty"`
	S3Region          string `json:"s3_region,omitempty"`
	S3ETag            string `json:"s3_etag,omitempty"`
}

func descriptorFromMeta(meta BlobMeta) PayloadDescriptor {
	if meta.Payload != nil {
		desc := *meta.Payload
		desc.SpaceID = firstNonEmpty(desc.SpaceID, meta.SpaceID)
		desc.BlobID = firstNonEmpty(desc.BlobID, meta.BlobID)
		if desc.SizeBytes == 0 {
			desc.SizeBytes = meta.SizeBytes
		}
		desc.ChecksumAlgorithm = firstNonEmpty(desc.ChecksumAlgorithm, "sha256")
		desc.ChecksumHex = firstNonEmpty(desc.ChecksumHex, strings.TrimPrefix(meta.Digest, "sha256:"))
		return desc
	}
	return PayloadDescriptor{Backend: "local", SpaceID: meta.SpaceID, BlobID: meta.BlobID, SizeBytes: meta.SizeBytes, ChecksumAlgorithm: "sha256", ChecksumHex: strings.TrimPrefix(meta.Digest, "sha256:")}
}

func (m *Module) ensurePayloadFromReader(ctx context.Context, desc PayloadDescriptor, r io.Reader) error {
	if ok, err := m.payloadExists(ctx, desc); err != nil || ok {
		return err
	}
	id, size, stored, err := m.putPayload(ctx, desc.SpaceID, "", r)
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
