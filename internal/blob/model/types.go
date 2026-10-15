package model

import "time"

// PayloadDescriptor describes the durable location and integrity metadata for a blob payload.
type PayloadDescriptor struct {
	Backend           string `json:"backend,omitempty"`
	SpaceID           string `json:"space_id"`
	DomainID          string `json:"domain_id,omitempty"`
	BlobID            string `json:"blob_id"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	ChecksumHex       string `json:"checksum_hex"`
	S3Bucket          string `json:"s3_bucket,omitempty"`
	S3Key             string `json:"s3_key,omitempty"`
	S3Region          string `json:"s3_region,omitempty"`
	S3ETag            string `json:"s3_etag,omitempty"`
}

// BlobMeta is the shared metadata representation for a stored blob.
type BlobMeta struct {
	BlobID           string             `json:"blob_id"`
	SpaceID          string             `json:"space_id"`
	DomainID         string             `json:"domain_id,omitempty"`
	Digest           string             `json:"digest"`
	SizeBytes        int64              `json:"size_bytes"`
	MimeType         string             `json:"mime_type"`
	DeclaredMimeType string             `json:"declared_mime_type,omitempty"`
	OriginalFilename string             `json:"original_filename,omitempty"`
	CreateTime       time.Time          `json:"create_time"`
	Payload          *PayloadDescriptor `json:"payload,omitempty"`
}
