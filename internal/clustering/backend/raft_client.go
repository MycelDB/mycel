package backend

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

type RaftMessageSender struct {
	Client Client
	Addr   string
}

func (s RaftMessageSender) SendRaftMessage(ctx context.Context, env consensus.MessageEnvelope) error {
	conn, err := s.Client.dial(ctx, s.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = clusterpb.NewClusterBackendServiceClient(conn).DeliverRaftMessages(s.Client.authContext(ctx), &clusterpb.DeliverRaftMessagesRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		Messages: []*clusterpb.RaftMessageEnvelope{{
			GroupId:    string(env.GroupID),
			FromNodeId: uint64(env.From),
			Message:    append([]byte(nil), env.Message...),
		}},
	})
	return err
}
