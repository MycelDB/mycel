package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BlobPayloadProvider interface {
	OpenBlob(ctx context.Context, spaceID string, blobID string) (sizeBytes int64, checksumHex string, reader io.ReadCloser, err error)
}

func (s *Service) WithBlobPayloadProvider(provider BlobPayloadProvider) *Service {
	s.BlobPayloadProvider = provider
	return s
}

func (s *Service) GetBlobPayload(req *clusterpb.GetBlobPayloadRequest, stream clusterpb.ClusterBackendService_GetBlobPayloadServer) error {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return err
	}
	if s.BlobPayloadProvider == nil {
		return status.Error(codes.Unavailable, "blob payload provider is not configured")
	}
	if !s.Identity.ClusterAdmitted || s.Identity.ClusterID == "" || req.GetClusterId() != s.Identity.ClusterID {
		return status.Error(codes.PermissionDenied, "local node is not admitted to requested cluster")
	}
	if s.Authority == nil || s.Authority.GetPrimary().GetNodeId() != s.Identity.NodeID {
		return status.Error(codes.FailedPrecondition, "node is not cluster primary")
	}
	size, checksum, r, err := s.BlobPayloadProvider.OpenBlob(stream.Context(), req.GetSpaceId(), req.GetBlobId())
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	defer r.Close()
	if req.GetExpectedSizeBytes() != 0 && uint64(size) != req.GetExpectedSizeBytes() {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("blob payload size mismatch: got %d want %d", size, req.GetExpectedSizeBytes()))
	}
	if req.GetExpectedChecksumAlgorithm() != "" && req.GetExpectedChecksumAlgorithm() != "sha256" {
		return status.Error(codes.InvalidArgument, "unsupported blob checksum algorithm")
	}
	if req.GetExpectedChecksumHex() != "" && !strings.EqualFold(checksum, req.GetExpectedChecksumHex()) {
		return status.Error(codes.FailedPrecondition, "blob payload checksum mismatch")
	}
	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			if err := stream.Send(&clusterpb.BlobPayloadChunk{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Data: chunk}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), checksum) {
		return status.Error(codes.FailedPrecondition, "blob payload changed during stream")
	}
	return nil
}
