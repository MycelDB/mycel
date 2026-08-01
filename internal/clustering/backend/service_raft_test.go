package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type captureRaftSender struct{ envs []consensus.MessageEnvelope }

func (s *captureRaftSender) SendRaftMessage(ctx context.Context, env consensus.MessageEnvelope) error {
	s.envs = append(s.envs, env)
	return nil
}

type fakeLocalRaftGraphReader struct {
	payload []byte
	err     error
}

func (r fakeLocalRaftGraphReader) ExecuteLocalRaftGraphRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error) {
	return r.payload, r.err
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

func TestExecuteRaftGraphReadPreservesStatusError(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a"}, model.NodeStateClustered, nil)
	svc.GraphReader = fakeLocalRaftGraphReader{err: status.Error(codes.FailedPrecondition, "not local leader")}
	_, err := svc.ExecuteRaftGraphRead(context.Background(), &clusterpb.ExecuteRaftGraphReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: "space-1", Payload: []byte("read")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteRaftGraphRead() code=%v want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestExecuteRaftGraphReadWrapsPlainErrorAsInternal(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a"}, model.NodeStateClustered, nil)
	svc.GraphReader = fakeLocalRaftGraphReader{err: errors.New("boom")}
	_, err := svc.ExecuteRaftGraphRead(context.Background(), &clusterpb.ExecuteRaftGraphReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: "space-1", Payload: []byte("read")})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ExecuteRaftGraphRead() code=%v want Internal (err=%v)", status.Code(err), err)
	}
}
