package service

import (
	"context"
	"errors"
	"io"
	"time"
)

const ModuleName = "blob"

var (
	ErrInvalidInput = errors.New("invalid blob input")
	ErrNotFound     = errors.New("blob not found")
	ErrReferenced   = errors.New("blob is referenced")
)

type RefCounter interface {
	BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error)
}

type Manager interface {
	UploadBlob(ctx context.Context, input UploadInput) (BlobMeta, error)
	GetBlob(ctx context.Context, spaceID string, blobID string) (BlobMeta, error)
	OpenBlob(ctx context.Context, spaceID string, blobID string) (BlobMeta, io.ReadCloser, error)
	DeleteBlob(ctx context.Context, spaceID string, blobID string) (string, error)
}

type UploadInput struct {
	SpaceID          string
	DeclaredMimeType string
	OriginalFilename string
	Reader           io.Reader
}

type Config struct {
	Backend          string
	S3Bucket         string
	S3Prefix         string
	S3Region         string
	S3KMSKeyID       string
	S3EndpointURL    string
	S3ForcePathStyle bool
}

type BlobMeta struct {
	BlobID           string             `json:"blob_id"`
	SpaceID          string             `json:"space_id"`
	Digest           string             `json:"digest"`
	SizeBytes        int64              `json:"size_bytes"`
	MimeType         string             `json:"mime_type"`
	DeclaredMimeType string             `json:"declared_mime_type,omitempty"`
	OriginalFilename string             `json:"original_filename,omitempty"`
	CreateTime       time.Time          `json:"create_time"`
	Payload          *PayloadDescriptor `json:"payload,omitempty"`
}
