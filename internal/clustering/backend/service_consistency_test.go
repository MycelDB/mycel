package backend

import (
	"context"
	"net"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeLocalGraphConsistencyExecutor struct {
	spaceID  string
	domainID string
	payload  []byte
	err      error
}

type mismatchedConsistencyBackend struct {
	clusterpb.UnimplementedClusterBackendServiceServer
}

func (s mismatchedConsistencyBackend) GetLocalGraphConsistency(ctx context.Context, req *clusterpb.GetLocalGraphConsistencyRequest) (*clusterpb.GetLocalGraphConsistencyResponse, error) {
	return &clusterpb.GetLocalGraphConsistencyResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_b", NodeId: "node_2", Payload: []byte("{}")}, nil
}

func (e *fakeLocalGraphConsistencyExecutor) ExecuteLocalGraphConsistency(ctx context.Context, spaceID string, domainID string) ([]byte, error) {
	e.spaceID = spaceID
	e.domainID = domainID
	return append([]byte(nil), e.payload...), e.err
}

func TestGetLocalGraphConsistencyDispatchesToGraphReader(t *testing.T) {
	executor := &fakeLocalGraphConsistencyExecutor{payload: []byte(`{"graph_checksum":"abc"}`)}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", NodeName: "myceld-1", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil)
	svc.GraphReader = executor
	res, err := svc.GetLocalGraphConsistency(context.Background(), &clusterpb.GetLocalGraphConsistencyRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", RequesterNodeId: 1, SpaceId: "space-1", DomainId: "domain-1"})
	if err != nil {
		t.Fatalf("GetLocalGraphConsistency() error = %v", err)
	}
	if executor.spaceID != "space-1" || executor.domainID != "domain-1" {
		t.Fatalf("executor got space=%q domain=%q", executor.spaceID, executor.domainID)
	}
	if res.GetClusterId() != "cluster_a" || res.GetNodeId() != "node_2" || res.GetNodeName() != "myceld-1" || res.GetRaftNodeId() != 2 || string(res.GetPayload()) != `{"graph_checksum":"abc"}` {
		t.Fatalf("unexpected response: %#v", res)
	}
}

func TestGetLocalGraphConsistencyRejectsUnadmittedEmptyCluster(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2"}, model.NodeStateClustered, nil)
	svc.GraphReader = &fakeLocalGraphConsistencyExecutor{payload: []byte("{}")}
	_, err := svc.GetLocalGraphConsistency(context.Background(), &clusterpb.GetLocalGraphConsistencyRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", RequesterNodeId: 1, SpaceId: "space-1", DomainId: "domain-1"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetLocalGraphConsistency() code=%v want PermissionDenied (err=%v)", status.Code(err), err)
	}
}

func TestGetLocalGraphConsistencyRejectsClusterMismatch(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil)
	svc.GraphReader = &fakeLocalGraphConsistencyExecutor{payload: []byte("{}")}
	_, err := svc.GetLocalGraphConsistency(context.Background(), &clusterpb.GetLocalGraphConsistencyRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_b", RequesterNodeId: 1, SpaceId: "space-1", DomainId: "domain-1"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetLocalGraphConsistency() code=%v want PermissionDenied (err=%v)", status.Code(err), err)
	}
}

func TestCollectLocalGraphConsistencyReturnsPeerErrors(t *testing.T) {
	executor := &fakeLocalGraphConsistencyExecutor{payload: []byte(`{"node_count":3}`)}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil)
	svc.GraphReader = executor
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == "bad" {
			return nil, status.Error(codes.Unavailable, "peer unavailable")
		}
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	results := client.CollectLocalGraphConsistency(context.Background(), map[consensus.NodeID]string{2: "good", 3: "bad"}, LocalGraphConsistencyInput{ClusterID: "cluster_a", RequesterNode: 1, SpaceID: "space-1", DomainID: "domain-1"})
	if len(results) != 2 || results[0].TargetNode != 2 || results[1].TargetNode != 3 {
		t.Fatalf("unexpected ordered results: %#v", results)
	}
	if results[0].Err != nil || string(results[0].Result.Payload) != `{"node_count":3}` {
		t.Fatalf("unexpected successful peer result: %#v err=%v", results[0], results[0].Err)
	}
	if results[1].Err == nil {
		t.Fatalf("expected failed peer to retain error: %#v", results[1])
	}
}

func TestClientGetLocalGraphConsistencyRejectsResponseClusterMismatch(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, mismatchedConsistencyBackend{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	_, err := client.GetLocalGraphConsistency(context.Background(), "bufnet", LocalGraphConsistencyInput{ClusterID: "cluster_a", RequesterNode: consensus.NodeID(1), SpaceID: "space-1", DomainID: "domain-1"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("client GetLocalGraphConsistency() code=%v want PermissionDenied (err=%v)", status.Code(err), err)
	}
}

func TestClientGetLocalGraphConsistencyUsesBackendRPC(t *testing.T) {
	executor := &fakeLocalGraphConsistencyExecutor{payload: []byte(`{"node_count":3}`)}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_3", NodeName: "myceld-2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil)
	svc.GraphReader = executor
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, svc)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	defer server.Stop()

	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	res, err := client.GetLocalGraphConsistency(context.Background(), "bufnet", LocalGraphConsistencyInput{ClusterID: "cluster_a", RequesterNode: consensus.NodeID(1), SpaceID: "space-1", DomainID: "domain-1"})
	if err != nil {
		t.Fatalf("client GetLocalGraphConsistency() error = %v", err)
	}
	if res.ClusterID != "cluster_a" || res.NodeID != "node_3" || res.NodeName != "myceld-2" || res.RaftNodeID != 3 || string(res.Payload) != `{"node_count":3}` {
		t.Fatalf("unexpected client response: %#v", res)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("server returned unexpectedly: %v", err)
		}
	default:
	}
}
