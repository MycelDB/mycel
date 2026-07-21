package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalRaftSemanticReader interface {
	ExecuteLocalRaftSemanticRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error)
}

func (s *Service) ExecuteRaftSemanticRead(ctx context.Context, req *clusterpb.ExecuteRaftSemanticReadRequest) (*clusterpb.ExecuteRaftSemanticReadResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	reader, ok := s.SemanticReader.(LocalRaftSemanticReader)
	if !ok || reader == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic reader is not configured")
	}
	payload, err := reader.ExecuteLocalRaftSemanticRead(ctx, req.GetSpaceId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &clusterpb.ExecuteRaftSemanticReadResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Payload: payload}, nil
}
