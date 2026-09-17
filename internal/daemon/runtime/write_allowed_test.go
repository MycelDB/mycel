package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequireLocalWriteAllowedStandalone(t *testing.T) {
	rt := New(config.Config{Mode: config.DefaultMode}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); err != nil {
		t.Fatalf("RequireLocalWriteAllowed() error = %v", err)
	}
}

func TestRequireLocalWriteAllowedStandaloneRaftConfiguredFailsClosed(t *testing.T) {
	rt := New(config.Config{Mode: config.DefaultMode, Cluster: config.ClusterConfig{RaftNodeCount: 1}}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedMeshFailsClosed(t *testing.T) {
	rt := New(config.Config{Mode: "mesh"}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedMeshRejectsUnwiredExecutor(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	rt := New(config.Config{Mode: "mesh"}, nil, "", nil)
	rt.ClusterManager = mgr
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedRaftRequiresLeaders(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a"}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	groups, err := consensus.StartMultiGroup(context.Background(), consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 1, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 1000, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	rt := New(config.Config{Mode: config.DefaultMode, Cluster: config.ClusterConfig{RaftNodeCount: 1}}, nil, "", nil)
	rt.ClusterManager = mgr
	rt.RaftGroups = groups
	err = rt.RequireLocalWriteAllowed()
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "raft groups without leaders") {
		t.Fatalf("RequireLocalWriteAllowed() error = %v; want no-leader write-ready rejection", err)
	}
}
