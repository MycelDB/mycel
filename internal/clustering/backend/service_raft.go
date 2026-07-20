package backend

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) DeliverRaftMessages(ctx context.Context, req *clusterpb.DeliverRaftMessagesRequest) (*clusterpb.DeliverRaftMessagesResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if s.RaftRouter == nil {
		return nil, status.Error(codes.FailedPrecondition, "raft router is not configured")
	}
	for _, msg := range req.GetMessages() {
		if msg.GetGroupId() == "" || msg.GetFromNodeId() == 0 || len(msg.GetMessage()) == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid raft message envelope")
		}
		env := consensus.MessageEnvelope{GroupID: consensus.GroupID(msg.GetGroupId()), From: consensus.NodeID(msg.GetFromNodeId()), Message: append([]byte(nil), msg.GetMessage()...)}
		decoded, err := consensus.DecodeMessage(consensus.MessageEnvelope{GroupID: env.GroupID, From: env.From, To: 1, Message: env.Message})
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		env.To = consensus.NodeID(decoded.To)
		if err := s.RaftRouter.SendRaftMessage(ctx, env); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}
	return &clusterpb.DeliverRaftMessagesResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1}, nil
}
