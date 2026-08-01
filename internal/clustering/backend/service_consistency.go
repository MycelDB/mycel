package backend

import (
	"context"
	"strconv"
	"strings"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalGraphConsistencyExecutor interface {
	ExecuteLocalGraphConsistency(ctx context.Context, spaceID string, domainID string) ([]byte, error)
}

func (s *Service) GetLocalGraphConsistency(ctx context.Context, req *clusterpb.GetLocalGraphConsistencyRequest) (*clusterpb.GetLocalGraphConsistencyResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetClusterId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id is required")
	}
	if !s.Identity.ClusterAdmitted || strings.TrimSpace(s.Identity.ClusterID) == "" {
		return nil, status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if strings.TrimSpace(req.GetClusterId()) != s.Identity.ClusterID {
		return nil, status.Error(codes.PermissionDenied, "cluster_id mismatch")
	}
	executor, ok := s.GraphReader.(LocalGraphConsistencyExecutor)
	if !ok || executor == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph consistency diagnostics are not configured")
	}
	payload, err := executor.ExecuteLocalGraphConsistency(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
			return nil, err
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	raftNodeID := uint64(0)
	if strings.HasPrefix(s.Identity.NodeID, "node_") {
		if parsed, err := strconv.ParseUint(strings.TrimPrefix(s.Identity.NodeID, "node_"), 10, 64); err == nil {
			raftNodeID = parsed
		}
	}
	return &clusterpb.GetLocalGraphConsistencyResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: s.Identity.ClusterID, NodeId: s.Identity.NodeID, NodeName: s.Identity.NodeName, RaftNodeId: raftNodeID, Payload: payload}, nil
}
