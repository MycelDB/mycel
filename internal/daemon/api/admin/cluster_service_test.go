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
