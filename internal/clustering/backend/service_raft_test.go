package backend

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type captureRaftSender struct{ envs []consensus.MessageEnvelope }

func (s *captureRaftSender) SendRaftMessage(ctx context.Context, env consensus.MessageEnvelope) error {
	s.envs = append(s.envs, env)
	return nil
}

func TestDeliverRaftMessagesRoutesDecodedEnvelope(t *testing.T) {
	msg := raftpb.Message{From: 1, To: 2, Type: raftpb.MsgHeartbeat, Term: 7}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("marshal raft message: %v", err)
	}
	sender := &captureRaftSender{}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a"}, model.NodeStateClustered, nil).WithRaftRouter(sender)
	_, err = svc.DeliverRaftMessages(context.Background(), &clusterpb.DeliverRaftMessagesRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Messages: []*clusterpb.RaftMessageEnvelope{{GroupId: "space-partition-3", FromNodeId: 1, Message: data}}})
	if err != nil {
		t.Fatalf("DeliverRaftMessages() error = %v", err)
	}
	if len(sender.envs) != 1 || sender.envs[0].GroupID != "space-partition-3" || sender.envs[0].From != 1 || sender.envs[0].To != 2 {
		t.Fatalf("unexpected delivered envelopes: %+v", sender.envs)
	}
}
