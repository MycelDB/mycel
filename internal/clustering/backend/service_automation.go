package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalRaftAutomationRuntimeReader interface {
	ExecuteLocalRaftAutomationRuntimeRead(ctx context.Context, partitionID uint32, payload []byte) ([]byte, error)
}

func (s *Service) ExecuteRaftAutomationRuntimeRead(ctx context.Context, req *clusterpb.ExecuteRaftAutomationRuntimeReadRequest) (*clusterpb.ExecuteRaftAutomationRuntimeReadResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	reader, ok := s.AutomationRuntimeReader.(LocalRaftAutomationRuntimeReader)
	if !ok || reader == nil {
		return nil, status.Error(codes.FailedPrecondition, "automation runtime reader is not configured")
	}
	payload, err := reader.ExecuteLocalRaftAutomationRuntimeRead(ctx, req.GetPartitionId(), req.GetPayload())
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
			return nil, err
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &clusterpb.ExecuteRaftAutomationRuntimeReadResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Payload: payload}, nil
}
