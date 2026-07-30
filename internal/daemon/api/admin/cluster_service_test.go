package admin

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clusterAuthz struct{ allow bool }

func (a clusterAuthz) HasCapability(ctx context.Context, operatorID string, capability string) (bool, error) {
	return a.allow, nil
}

func authenticatedClusterContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindOperator, OperatorID: "op_1", Username: "admin"})
}

func newBootstrapClusterManager(t *testing.T) *clustering.Manager {
	t.Helper()
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatalf("new cluster manager: %v", err)
	}
	return mgr
}

func TestAdminClusterServiceGetStatusRequiresAuth(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	_, err := svc.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestAdminClusterServiceGetStatus(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	res, err := svc.GetClusterStatus(authenticatedClusterContext(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("get cluster status: %v", err)
	}
	if res.GetNode().GetNodeName() != "node-a" || !res.GetNode().GetAdmitted() || !res.GetNode().GetBootstrap() {
		t.Fatalf("unexpected node status: %#v", res.GetNode())
	}
	if res.GetCluster().GetClusterName() != "dev" || res.GetCluster().GetMode() != adminv1.ClusterMode_CLUSTER_MODE_CLUSTERED {
		t.Fatalf("unexpected cluster status: %#v", res.GetCluster())
	}
	if len(res.GetPeers()) == 0 || res.GetPeers()[0].GetState() != adminv1.ClusterPeerState_CLUSTER_PEER_STATE_SELF {
		t.Fatalf("expected self peer, got %#v", res.GetPeers())
	}
}

func TestAdminClusterServiceHealthUsesMetadataReadiness(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 3}, nil)
	if err != nil {
		t.Fatalf("new raft manager: %v", err)
	}
	svc := NewAdminClusterService(mgr, clusterAuthz{allow: true})
	res, err := svc.GetClusterHealth(authenticatedClusterContext(), &adminv1.GetClusterHealthRequest{})
	if err != nil {
		t.Fatalf("GetClusterHealth() error = %v", err)
	}
	if res.GetStatus() != "unhealthy" || !containsString(res.GetWarnings(), "system metadata not applied") {
		t.Fatalf("expected metadata readiness unhealthy warning, got %#v", res)
	}
	meta := consensus.SystemMetadata{ClusterID: "cluster_authoritative", ClusterName: "dev", NodeCount: 3, PartitionCount: 16, ReplicaFactor: 3, Nodes: map[string]consensus.SystemNode{"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"}, "node_2": {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b", BackendAdvertiseAddr: "127.0.0.1:9094"}, "node_3": {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c", BackendAdvertiseAddr: "127.0.0.1:9095"}}, Placement: map[uint32]consensus.PartitionPlacement{}}
	if err := mgr.ApplySystemMetadata(context.Background(), meta, 1); err != nil {
		t.Fatalf("ApplySystemMetadata() error = %v", err)
	}
	if err := mgr.MarkPartitionGroupsStarted(16, 16); err != nil {
		t.Fatalf("MarkPartitionGroupsStarted() error = %v", err)
	}
	res, err = svc.GetClusterHealth(authenticatedClusterContext(), &adminv1.GetClusterHealthRequest{})
	if err != nil {
		t.Fatalf("GetClusterHealth() after apply error = %v", err)
	}
	if res.GetStatus() != "healthy" {
		t.Fatalf("expected healthy after metadata apply and partition groups started, got %#v", res)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestAdminClusterServiceRuntimeStatusAndSpaceRoute(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{Name: "dev", RaftNodeCount: 3, RaftPartitionCount: 16, RaftReplicaFactor: 3, RaftLocalNodeID: 2, RaftNodeAddrs: []string{"a:9091", "b:9091", "c:9091"}}, nil)
	ctx := authenticatedClusterContext()
	res, err := svc.GetClusterRuntimeStatus(ctx, &adminv1.GetClusterRuntimeStatusRequest{})
	if err != nil {
		t.Fatalf("runtime status: %v", err)
	}
	if res.GetEngine() != adminv1.ClusterEngine_CLUSTER_ENGINE_RAFT || res.GetRaftPartitionCount() != 16 || res.GetLocalRaftNodeId() != 2 || len(res.GetRaftNodeAddrs()) != 3 {
		t.Fatalf("unexpected runtime status: %#v", res)
	}
	route, err := svc.LookupSpaceRoute(ctx, &adminv1.LookupSpaceRouteRequest{SpaceId: "490851b9-0038-4afc-b1f0-d1bd9e829bc8"})
	if err != nil {
		t.Fatalf("lookup route: %v", err)
	}
	if route.GetPartitionId() >= 16 || len(route.GetReplicaNodeIds()) != 3 {
		t.Fatalf("unexpected route: %#v", route)
	}
	if _, err := svc.LookupSpaceRoute(ctx, &adminv1.LookupSpaceRouteRequest{SpaceId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
