package backend

import (
	"context"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LocalGraphConsistencyInput struct {
	ClusterID     string
	RequesterNode consensus.NodeID
	SpaceID       string
	DomainID      string
}

type LocalGraphConsistencyResult struct {
	ClusterID  string
	NodeID     string
	NodeName   string
	RaftNodeID consensus.NodeID
	Payload    []byte
}

func (c Client) GetLocalGraphConsistency(ctx context.Context, addr string, in LocalGraphConsistencyInput) (LocalGraphConsistencyResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return LocalGraphConsistencyResult{}, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).GetLocalGraphConsistency(c.authContext(ctx), &clusterpb.GetLocalGraphConsistencyRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: in.ClusterID, RequesterNodeId: uint64(in.RequesterNode), SpaceId: in.SpaceID, DomainId: in.DomainID})
	if err != nil {
		return LocalGraphConsistencyResult{}, err
	}
	if res.GetProtocolVersion() != clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1 {
		return LocalGraphConsistencyResult{}, status.Error(codes.FailedPrecondition, "unsupported backend protocol response")
	}
	if strings.TrimSpace(in.ClusterID) != "" && res.GetClusterId() != strings.TrimSpace(in.ClusterID) {
		return LocalGraphConsistencyResult{}, status.Error(codes.PermissionDenied, "cluster_id mismatch in backend response")
	}
	return LocalGraphConsistencyResult{ClusterID: res.GetClusterId(), NodeID: res.GetNodeId(), NodeName: res.GetNodeName(), RaftNodeID: consensus.NodeID(res.GetRaftNodeId()), Payload: append([]byte(nil), res.GetPayload()...)}, nil
}
