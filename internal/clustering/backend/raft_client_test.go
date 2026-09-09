package backend

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type captureRaftBackendServer struct {
	clusterpb.UnimplementedClusterBackendServiceServer

	mu    sync.Mutex
	calls int
}

func (s *captureRaftBackendServer) DeliverRaftMessages(ctx context.Context, req *clusterpb.DeliverRaftMessagesRequest) (*clusterpb.DeliverRaftMessagesResponse, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return &clusterpb.DeliverRaftMessagesResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1}, nil
}

func (s *captureRaftBackendServer) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type blockingRaftBackendServer struct {
	clusterpb.UnimplementedClusterBackendServiceServer
}

func (s blockingRaftBackendServer) DeliverRaftMessages(ctx context.Context, req *clusterpb.DeliverRaftMessagesRequest) (*clusterpb.DeliverRaftMessagesResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRaftMessageSenderReusesPooledConnection(t *testing.T) {
	backendServer := &captureRaftBackendServer{}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, backendServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	var dials atomic.Int32
	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		dials.Add(1)
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	pool := NewRaftClientConnPool(client)
	defer pool.Close()
	sender := RaftMessageSender{Client: client, Addr: "bufnet", Pool: pool, Timeout: time.Second}
	env := testRaftEnvelope(t)

	for i := 0; i < 2; i++ {
		if err := sender.SendRaftMessage(context.Background(), env); err != nil {
			t.Fatalf("SendRaftMessage(%d) error = %v", i, err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
	if got := backendServer.Calls(); got != 2 {
		t.Fatalf("backend call count = %d, want 2", got)
	}
}

func TestRaftMessageSenderAppliesBoundedTimeout(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, blockingRaftBackendServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	pool := NewRaftClientConnPool(client)
	defer pool.Close()
	sender := RaftMessageSender{Client: client, Addr: "bufnet", Pool: pool, Timeout: 20 * time.Millisecond}

	start := time.Now()
	err := sender.SendRaftMessage(context.Background(), testRaftEnvelope(t))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("SendRaftMessage() code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SendRaftMessage() took %s, want bounded timeout", elapsed)
	}
}

func testRaftEnvelope(t *testing.T) consensus.MessageEnvelope {
	t.Helper()
	msg := raftpb.Message{From: 1, To: 2, Type: raftpb.MsgHeartbeat, Term: 7}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatalf("marshal raft message: %v", err)
	}
	return consensus.MessageEnvelope{GroupID: consensus.SystemGroupID, From: 1, To: 2, Message: data}
}
