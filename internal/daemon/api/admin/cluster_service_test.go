package admin

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/membership"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", Bootstrap: true}, nil)
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
	if res.GetNode().GetNodeName() != "node-a" || !res.GetNode().GetAdmitted() || !res.GetNode().GetBootstrap() || res.GetNode().GetRole() != adminv1.ClusterNodeRole_CLUSTER_NODE_ROLE_PRIMARY {
		t.Fatalf("unexpected node status: %#v", res.GetNode())
	}
	if res.GetCluster().GetClusterName() != "dev" || res.GetCluster().GetMode() != adminv1.ClusterMode_CLUSTER_MODE_CLUSTERED {
		t.Fatalf("unexpected cluster status: %#v", res.GetCluster())
	}
	if len(res.GetPeers()) == 0 || res.GetPeers()[0].GetState() != adminv1.ClusterPeerState_CLUSTER_PEER_STATE_SELF {
		t.Fatalf("expected self peer, got %#v", res.GetPeers())
	}
	if res.GetAuthority().GetPrimaryNodeId() != res.GetNode().GetNodeId() || res.GetAuthority().GetAuthorityEpoch() != 1 {
		t.Fatalf("unexpected authority: %#v", res.GetAuthority())
	}
}

func TestAdminClusterServiceRuntimeStatusAndSpaceRoute(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true}).WithClusterRuntime(daemonconfig.ClusterConfig{Engine: "raft", Name: "dev", RaftNodeCount: 3, RaftPartitionCount: 16, RaftReplicaFactor: 3, RaftLocalNodeID: 2, RaftNodeAddrs: []string{"a:9091", "b:9091", "c:9091"}}, nil)
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

func TestAdminClusterServiceAddNodeAndListMembersSanitizesToken(t *testing.T) {
	mgr := newBootstrapClusterManager(t)
	svc := NewAdminClusterService(mgr, clusterAuthz{allow: true})
	ctx := authenticatedClusterContext()
	add, err := svc.AddClusterNode(ctx, &adminv1.AddClusterNodeRequest{NodeName: "node-b", TokenTtlSeconds: int64((15 * time.Minute).Seconds())})
	if err != nil {
		t.Fatalf("add cluster node: %v", err)
	}
	if add.GetToken() == "" || add.GetTokenId() == "" || add.GetState() != adminv1.ClusterMemberState_CLUSTER_MEMBER_STATE_PENDING {
		t.Fatalf("unexpected add response: %#v", add)
	}

	list, err := svc.ListClusterMembers(ctx, &adminv1.ListClusterMembersRequest{})
	if err != nil {
		t.Fatalf("list cluster members: %v", err)
	}
	var pending *adminv1.ClusterMember
	for _, member := range list.GetMembers() {
		if member.GetNodeName() == "node-b" {
			pending = member
		}
	}
	if pending == nil || pending.GetTokenId() != add.GetTokenId() || pending.GetTokenExpiresAt() == "" {
		t.Fatalf("pending member missing token metadata: %#v", pending)
	}
	if strings.Contains(pending.String(), add.GetToken()) {
		t.Fatalf("list members leaked plaintext token: %s", pending.String())
	}
	member, ok, err := mgr.Membership().FindByNodeName(ctx, "node-b")
	if err != nil || !ok {
		t.Fatalf("stored member missing ok=%v err=%v", ok, err)
	}
	if member.JoinToken == nil || member.JoinToken.Hash == "" || member.JoinToken.Hash == add.GetToken() || !membership.VerifyToken(member.JoinToken.Hash, add.GetToken()) {
		t.Fatalf("token was not stored as a hash: %#v", member.JoinToken)
	}
}

func TestAdminClusterServiceAddNodeRejectsEmptyNodeName(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	_, err := svc.AddClusterNode(authenticatedClusterContext(), &adminv1.AddClusterNodeRequest{NodeName: "   "})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAdminClusterServiceAddNodeUsesDefaultTTL(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: true})
	before := time.Now().UTC().Add(29 * time.Minute)
	res, err := svc.AddClusterNode(authenticatedClusterContext(), &adminv1.AddClusterNodeRequest{NodeName: "node-b"})
	if err != nil {
		t.Fatalf("add cluster node: %v", err)
	}
	expires, err := time.Parse(time.RFC3339Nano, res.GetExpiresAt())
	if err != nil {
		t.Fatalf("parse expires_at: %v", err)
	}
	after := time.Now().UTC().Add(31 * time.Minute)
	if expires.Before(before) || expires.After(after) {
		t.Fatalf("expected default ttl near 30m, got expires_at=%s before=%s after=%s", expires, before, after)
	}
}

func TestAdminClusterServiceAddNodeRejectsFollower(t *testing.T) {
	ctx := context.Background()
	mgr := newBootstrapClusterManager(t)
	if err := mgr.SetAuthority(ctx, clustering.Authority{Version: clustering.AuthorityVersion, ClusterID: mgr.Identity().ClusterID, Primary: clustering.AuthorityPrimary{NodeID: "node-other", NodeName: "node-other", BackendAdvertiseAddr: "127.0.0.1:9094"}, AuthorityEpoch: 2, Source: clustering.AuthoritySourceManual, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("set authority: %v", err)
	}
	if mgr.LocalRole() != clustering.NodeRoleFollower {
		t.Fatalf("test setup expected follower role, got %s", mgr.LocalRole())
	}
	svc := NewAdminClusterService(mgr, clusterAuthz{allow: true})
	_, err := svc.AddClusterNode(authenticatedClusterContext(), &adminv1.AddClusterNodeRequest{NodeName: "node-c"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition for follower add-node, got %v", err)
	}
	st, _ := status.FromError(err)
	foundHint := false
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetReason() == "MYCEL_CLUSTER_NOT_PRIMARY" && info.GetMetadata()["mycel-primary-node-id"] == "node-other" {
			foundHint = true
		}
	}
	if !foundHint {
		t.Fatalf("expected primary hint detail, got %#v", st.Details())
	}
}

func TestAdminClusterServiceAddNodeRejectsUnauthorizedAndUnadmitted(t *testing.T) {
	svc := NewAdminClusterService(newBootstrapClusterManager(t), clusterAuthz{allow: false})
	_, err := svc.AddClusterNode(authenticatedClusterContext(), &adminv1.AddClusterNodeRequest{NodeName: "node-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}

	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: filepath.Join(t.TempDir(), "node-b"), NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", Bootstrap: false}, nil)
	if err != nil {
		t.Fatalf("new unadmitted manager: %v", err)
	}
	svc = NewAdminClusterService(mgr, clusterAuthz{allow: true})
	_, err = svc.AddClusterNode(authenticatedClusterContext(), &adminv1.AddClusterNodeRequest{NodeName: "node-c"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected unadmitted permission denied, got %v", err)
	}
}
