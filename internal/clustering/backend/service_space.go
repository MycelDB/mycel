package backend

import (
	"context"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SpaceReader interface {
	GetSpace(ctx context.Context, spaceID string) (domainspace.Space, error)
}

type LocalRaftSpaceReader interface {
	GetLocalRaftSpace(ctx context.Context, spaceID string) (domainspace.Space, error)
}

type LocalRaftSpaceLister interface {
	ListLocalRaftSpaces(ctx context.Context, includeArchived bool) ([]domainspace.Space, error)
}

func (s *Service) WithSpaceReader(reader SpaceReader) *Service {
	s.SpaceReader = reader
	return s
}

func (s *Service) GetRaftSpace(ctx context.Context, req *clusterpb.GetRaftSpaceRequest) (*clusterpb.GetRaftSpaceResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if s.SpaceReader == nil {
		return nil, status.Error(codes.FailedPrecondition, "space reader is not configured")
	}
	var sp domainspace.Space
	var err error
	if local, ok := s.SpaceReader.(LocalRaftSpaceReader); ok {
		sp, err = local.GetLocalRaftSpace(ctx, req.GetSpaceId())
	} else {
		sp, err = s.SpaceReader.GetSpace(ctx, req.GetSpaceId())
	}
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &clusterpb.GetRaftSpaceResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Space: SpaceToProto(sp)}, nil
}

func (s *Service) ListRaftSpaces(ctx context.Context, req *clusterpb.ListRaftSpacesRequest) (*clusterpb.ListRaftSpacesResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	lister, ok := s.SpaceReader.(LocalRaftSpaceLister)
	if !ok || lister == nil {
		return nil, status.Error(codes.FailedPrecondition, "space lister is not configured")
	}
	spaces, err := lister.ListLocalRaftSpaces(ctx, req.GetIncludeArchived())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*clusterpb.RaftSpace, 0, len(spaces))
	for _, sp := range spaces {
		out = append(out, SpaceToProto(sp))
	}
	return &clusterpb.ListRaftSpacesResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Spaces: out}, nil
}

func SpaceToProto(sp domainspace.Space) *clusterpb.RaftSpace {
	return &clusterpb.RaftSpace{SpaceId: sp.SpaceID.String(), OwnerId: string(sp.OwnerID), Name: sp.Name, Status: sp.Status, CreatedAt: sp.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: sp.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}
