package backend

import (
	"context"
	"time"

	"github.com/google/uuid"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (c Client) GetRaftSpace(ctx context.Context, addr string, spaceID string) (domainspace.Space, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return domainspace.Space{}, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).GetRaftSpace(c.authContext(ctx), &clusterpb.GetRaftSpaceRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: spaceID})
	if err != nil {
		return domainspace.Space{}, err
	}
	return SpaceFromProto(res.GetSpace())
}

func (c Client) ListRaftSpaces(ctx context.Context, addr string, includeArchived bool) ([]domainspace.Space, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).ListRaftSpaces(c.authContext(ctx), &clusterpb.ListRaftSpacesRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, IncludeArchived: includeArchived})
	if err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(res.GetSpaces()))
	for _, protoSpace := range res.GetSpaces() {
		sp, err := SpaceFromProto(protoSpace)
		if err != nil {
			return nil, err
		}
		out = append(out, sp)
	}
	return out, nil
}

func SpaceFromProto(sp *clusterpb.RaftSpace) (domainspace.Space, error) {
	if sp == nil {
		return domainspace.Space{}, nil
	}
	spaceID, err := uuid.Parse(sp.GetSpaceId())
	if err != nil {
		return domainspace.Space{}, err
	}
	ownerID, err := uuid.Parse(sp.GetOwnerId())
	if err != nil {
		return domainspace.Space{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, sp.GetCreatedAt())
	if err != nil {
		return domainspace.Space{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, sp.GetUpdatedAt())
	if err != nil {
		return domainspace.Space{}, err
	}
	return domainspace.Space{SpaceID: spaceID, OwnerID: ownerID, Name: sp.GetName(), Status: sp.GetStatus(), CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}
