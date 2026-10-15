package service

import (
	"context"
	"errors"
	"io"

	blobmodel "github.com/myceldb/mycel/internal/blob/model"
)

const ModuleName = "blob"

var (
	ErrInvalidInput = errors.New("invalid blob input")
	ErrNotFound     = blobmodel.ErrNotFound
	ErrReferenced   = errors.New("blob is referenced")
)

type RefCounter interface {
	BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error)
}

type Manager interface {
	UploadBlob(ctx context.Context, input UploadInput) (blobmodel.BlobMeta, error)
	GetBlob(ctx context.Context, spaceID string, blobID string) (blobmodel.BlobMeta, error)
	GetBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (blobmodel.BlobMeta, error)
	OpenBlob(ctx context.Context, spaceID string, blobID string) (blobmodel.BlobMeta, io.ReadCloser, error)
	OpenBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (blobmodel.BlobMeta, io.ReadCloser, error)
	DeleteBlob(ctx context.Context, spaceID string, blobID string) (string, error)
	DeleteBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (string, error)
}

type UploadInput struct {
	SpaceID          string
	DomainID         string
	DeclaredMimeType string
	OriginalFilename string
	Reader           io.Reader
}

type Config struct {
	Backend             string
	ObjectStoreProvider string
	S3Bucket            string
	S3Prefix            string
	S3Region            string
	S3KMSKeyID          string
	S3EndpointURL       string
	S3ForcePathStyle    bool
}
