package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBlobMetaJSONShapeRemainsStorageCompatible(t *testing.T) {
	created := time.Date(2026, time.October, 14, 12, 30, 0, 0, time.UTC)
	meta := BlobMeta{
		BlobID:           "blob-1",
		SpaceID:          "space-1",
		DomainID:         "domain-1",
		Digest:           "sha256:blob-1",
		SizeBytes:        42,
		MimeType:         "image/png",
		DeclaredMimeType: "image/png",
		OriginalFilename: "diagram.png",
		CreateTime:       created,
		Payload: &PayloadDescriptor{
			Backend:           "object_store",
			SpaceID:           "space-1",
			DomainID:          "domain-1",
			BlobID:            "blob-1",
			SizeBytes:         42,
			ChecksumAlgorithm: "sha256",
			ChecksumHex:       "blob-1",
			S3Bucket:          "bucket",
			S3Key:             "objects/blob-1",
			S3Region:          "us-east-1",
			S3ETag:            "etag",
		},
	}

	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	const want = `{"blob_id":"blob-1","space_id":"space-1","domain_id":"domain-1","digest":"sha256:blob-1","size_bytes":42,"mime_type":"image/png","declared_mime_type":"image/png","original_filename":"diagram.png","create_time":"2026-10-14T12:30:00Z","payload":{"backend":"object_store","space_id":"space-1","domain_id":"domain-1","blob_id":"blob-1","size_bytes":42,"checksum_algorithm":"sha256","checksum_hex":"blob-1","s3_bucket":"bucket","s3_key":"objects/blob-1","s3_region":"us-east-1","s3_etag":"etag"}}`
	if string(raw) != want {
		t.Fatalf("Marshal() = %s, want %s", raw, want)
	}

	var decoded BlobMeta
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded.Payload == nil || decoded.Payload.S3Key != meta.Payload.S3Key || !decoded.CreateTime.Equal(created) {
		t.Fatalf("Unmarshal() = %+v", decoded)
	}
}
