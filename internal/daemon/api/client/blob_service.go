package client

import (
	"bytes"
	"context"
	"errors"
	"io"

	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const blobDownloadChunkSize = 256 * 1024

type BlobService struct {
	clientv1.UnimplementedBlobServiceServer
	blobs  daemonblob.Manager
	spaces daemonspace.Manager
}

func NewBlobService(blobs daemonblob.Manager, spaces daemonspace.Manager) *BlobService {
	return &BlobService{blobs: blobs, spaces: spaces}
}

func (s *BlobService) UploadBlob(stream clientv1.BlobService_UploadBlobServer) error {
	ctx := stream.Context()
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	var meta *clientv1.UploadBlobMetadata
	var buf bytes.Buffer
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if m := req.GetMetadata(); m != nil {
			if meta != nil {
				return status.Error(codes.InvalidArgument, "upload metadata must be sent once")
			}
			meta = m
			if _, err := s.spaces.GetVisibleSpace(ctx, principal.UserID, meta.GetSpaceId()); err != nil {
				return mapBlobSpaceError(err)
			}
			continue
		}
		chunk := req.GetChunk()
		if meta == nil {
			return status.Error(codes.InvalidArgument, "upload metadata must be sent before chunks")
		}
		if len(chunk) > 0 {
			if _, err := buf.Write(chunk); err != nil {
				return err
			}
		}
	}
	if meta == nil {
		return status.Error(codes.InvalidArgument, "upload metadata is required")
	}
	blob, err := s.blobs.UploadBlob(ctx, daemonblob.UploadInput{SpaceID: meta.GetSpaceId(), DeclaredMimeType: meta.GetDeclaredMimeType(), OriginalFilename: meta.GetOriginalFilename(), Reader: bytes.NewReader(buf.Bytes())})
	if err != nil {
		return mapBlobError(err, "upload blob")
	}
	return stream.SendAndClose(&clientv1.UploadBlobResponse{Blob: mapProtoBlob(blob)})
}

func (s *BlobService) DownloadBlob(req *clientv1.DownloadBlobRequest, stream clientv1.BlobService_DownloadBlobServer) error {
	ctx := stream.Context()
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	if _, err := s.spaces.GetVisibleSpace(ctx, principal.UserID, req.GetSpaceId()); err != nil {
		return mapBlobSpaceError(err)
	}
	meta, reader, err := s.blobs.OpenBlob(ctx, req.GetSpaceId(), req.GetBlobId())
	if err != nil {
		return mapBlobError(err, "download blob")
	}
	defer reader.Close()
	if err := stream.Send(&clientv1.DownloadBlobResponse{Part: &clientv1.DownloadBlobResponse_Blob{Blob: mapProtoBlob(meta)}}); err != nil {
		return err
	}
	buf := make([]byte, blobDownloadChunkSize)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if sendErr := stream.Send(&clientv1.DownloadBlobResponse{Part: &clientv1.DownloadBlobResponse_Chunk{Chunk: chunk}}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *BlobService) GetBlob(ctx context.Context, req *clientv1.GetBlobRequest) (*clientv1.GetBlobResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.spaces.GetVisibleSpace(ctx, principal.UserID, req.GetSpaceId()); err != nil {
		return nil, mapBlobSpaceError(err)
	}
	blob, err := s.blobs.GetBlob(ctx, req.GetSpaceId(), req.GetBlobId())
	if err != nil {
		return nil, mapBlobError(err, "get blob")
	}
	return &clientv1.GetBlobResponse{Blob: mapProtoBlob(blob)}, nil
}

func (s *BlobService) DeleteBlob(ctx context.Context, req *clientv1.DeleteBlobRequest) (*clientv1.DeleteBlobResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.spaces.GetVisibleSpace(ctx, principal.UserID, req.GetSpaceId()); err != nil {
		return nil, mapBlobSpaceError(err)
	}
	id, err := s.blobs.DeleteBlob(ctx, req.GetSpaceId(), req.GetBlobId())
	if err != nil {
		return nil, mapBlobError(err, "delete blob")
	}
	return &clientv1.DeleteBlobResponse{DeletedBlobId: id}, nil
}

func mapProtoBlob(blob daemonblob.BlobMeta) *clientv1.Blob {
	return &clientv1.Blob{BlobId: blob.BlobID, SpaceId: blob.SpaceID, Digest: blob.Digest, SizeBytes: blob.SizeBytes, MimeType: blob.MimeType, DeclaredMimeType: blob.DeclaredMimeType, OriginalFilename: blob.OriginalFilename, CreateTime: timestamppb.New(blob.CreateTime)}
}

func mapBlobSpaceError(err error) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonspace.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonspace.ErrSpaceNotFound) {
		return status.Error(codes.NotFound, "space not found")
	}
	if errors.Is(err, daemonspace.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "space access denied")
	}
	return status.Errorf(codes.Internal, "space access check failed: %v", err)
}

func mapBlobError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonblob.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonblob.ErrNotFound) {
		return status.Error(codes.NotFound, "blob not found")
	}
	if errors.Is(err, daemonblob.ErrReferenced) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
