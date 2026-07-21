package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalRaftGraphReader interface {
	ExecuteLocalRaftGraphRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error)
}

func (s *Service) ExecuteRaftGraphRead(ctx context.Context, req *clusterpb.ExecuteRaftGraphReadRequest) (*clusterpb.ExecuteRaftGraphReadResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	reader, ok := s.GraphReader.(LocalRaftGraphReader)
	if !ok || reader == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph reader is not configured")
	}
	payload, err := reader.ExecuteLocalRaftGraphRead(ctx, req.GetSpaceId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &clusterpb.ExecuteRaftGraphReadResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Payload: payload}, nil
}
