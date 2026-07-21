package service

import (
	"context"
	"io"
	"strings"
)

type BackendPayloadProvider struct{ Module *Module }

func (p BackendPayloadProvider) OpenBlob(ctx context.Context, spaceID string, blobID string) (int64, string, io.ReadCloser, error) {
	meta, r, err := p.Module.OpenBlob(ctx, spaceID, blobID)
	if err != nil {
		return 0, "", nil, err
	}
	return meta.SizeBytes, strings.TrimPrefix(meta.Digest, "sha256:"), r, nil
}
