package backend

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegisterNodeUpdatesTopologyAndReturnsView(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, CreatedAt: now, UpdatedAt: now}
	reg, err := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, ClusterID: self.ClusterID, ClusterName: self.ClusterName, BackendAdvertiseAddr: self.BackendAdvertiseAddr, State: model.PeerStateSelf, Source: model.PeerSourceSelf})
	if err != nil {
		t.Fatal(err)
	}
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	if err := store.UpsertMember(ctx, membership.Member{NodeName: "node-b", NodeID: "node_b", State: membership.MemberStateActive}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store)
	remote := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_b", NodeName: "node-b", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", CreatedAt: now, UpdatedAt: now}
	res, err := svc.RegisterNode(ctx, &clusterpb.RegisterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Identity: IdentityToProto(remote), State: NodeStateToProto(model.NodeStateClustered)})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetAccepted() {
		t.Fatalf("not accepted: %s", res.GetReason())
	}
	found := false
	for _, p := range reg.RemotePeers() {
		if p.NodeID == "node_b" && p.State == model.PeerStateActive && p.ClusterID == self.ClusterID {
			found = true
		}
	}
	if !found {
		t.Fatalf("remote peer not registered: %#v", reg.List())
	}
	if len(res.GetClusterView().GetPeers()) < 2 {
		t.Fatalf("cluster view missing peers: %#v", res.GetClusterView().GetPeers())
	}
}

func TestRegisterNodeRejectsClusterNameMismatch(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, CreatedAt: now, UpdatedAt: now}
	reg, _ := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, BackendAdvertiseAddr: self.BackendAdvertiseAddr})
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store)
	remote := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_b", ClusterID: "cluster_b", ClusterName: "prod", BackendAdvertiseAddr: "127.0.0.1:9094", CreatedAt: now, UpdatedAt: now}
	res, err := svc.RegisterNode(ctx, &clusterpb.RegisterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Identity: IdentityToProto(remote)})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetAccepted() {
		t.Fatal("expected rejection")
	}
}

func TestRegisterNodeWithTokenPromotesPendingMember(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", NodeName: "node-a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, CreatedAt: now, UpdatedAt: now}
	reg, _ := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, BackendAdvertiseAddr: self.BackendAdvertiseAddr, State: model.PeerStateSelf})
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	token := "mycel_join_v1_test"
	if err := store.UpsertMember(ctx, membership.Member{NodeName: "node-b", State: membership.MemberStatePending, JoinToken: &membership.JoinToken{TokenID: "join_tok_1", Hash: membership.HashToken(token), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store)
	remote := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_b", NodeName: "node-b", ClusterID: "cluster_b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", CreatedAt: now, UpdatedAt: now}
	res, err := svc.RegisterNode(ctx, &clusterpb.RegisterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Identity: IdentityToProto(remote), JoinToken: token, KnownPeers: []*clusterpb.Peer{PeerToProto(model.Peer{NodeID: remote.NodeID, ClusterID: remote.ClusterID, BackendAdvertiseAddr: remote.BackendAdvertiseAddr, State: model.PeerStateSelf, Source: model.PeerSourceSelf})}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetAccepted() {
		t.Fatalf("not accepted: %s", res.GetReason())
	}
	member, ok, err := store.FindByNodeID(ctx, "node_b")
	if err != nil || !ok {
		t.Fatalf("member ok=%v err=%v", ok, err)
	}
	if member.State != membership.MemberStateActive || member.JoinToken == nil || member.JoinToken.ConsumedAt == nil || member.JoinToken.Hash != "" {
		t.Fatalf("member=%#v", member)
	}
	for _, p := range reg.RemotePeers() {
		if p.NodeID == remote.NodeID && p.ClusterID != self.ClusterID {
			t.Fatalf("registered peer has non-authoritative cluster id: %#v", p)
		}
	}
}

func TestRegisterNodeRejectsWrongToken(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", NodeName: "node-a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, CreatedAt: now, UpdatedAt: now}
	reg, _ := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, BackendAdvertiseAddr: self.BackendAdvertiseAddr, State: model.PeerStateSelf})
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	if err := store.UpsertMember(ctx, membership.Member{NodeName: "node-b", State: membership.MemberStatePending, JoinToken: &membership.JoinToken{TokenID: "join_tok_1", Hash: membership.HashToken("mycel_join_v1_good"), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store)
	remote := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_b", NodeName: "node-b", ClusterID: "cluster_b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", CreatedAt: now, UpdatedAt: now}
	res, err := svc.RegisterNode(ctx, &clusterpb.RegisterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Identity: IdentityToProto(remote), JoinToken: "mycel_join_v1_bad"})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetAccepted() {
		t.Fatal("expected rejection")
	}
}

func TestAddClusterNodeCreatesPendingMembership(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, ClusterBootstrap: true, CreatedAt: now, UpdatedAt: now}
	reg, _ := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, BackendAdvertiseAddr: self.BackendAdvertiseAddr})
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store).WithAuthority(&clusterpb.ClusterAuthority{ClusterId: self.ClusterID, Primary: &clusterpb.AuthorityPrimary{NodeId: self.NodeID}, AuthorityEpoch: 1})
	res, err := svc.AddClusterNode(ctx, &clusterpb.AddClusterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, NodeName: "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetToken() == "" || res.GetState() != string(membership.MemberStatePending) {
		t.Fatalf("response=%#v", res)
	}
	member, ok, err := store.FindByNodeName(ctx, "node-b")
	if err != nil || !ok {
		t.Fatalf("member ok=%v err=%v", ok, err)
	}
	if member.JoinToken == nil || member.JoinToken.Hash == "" || member.JoinToken.Hash == res.GetToken() {
		t.Fatalf("token not stored hashed: %#v", member.JoinToken)
	}
}

func TestAddClusterNodeRejectsFollower(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_b", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", ClusterAdmitted: true, CreatedAt: now, UpdatedAt: now}
	reg, _ := topology.NewRegistry(ctx, nil, model.Peer{NodeID: self.NodeID, BackendAdvertiseAddr: self.BackendAdvertiseAddr})
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), self.ClusterID, self.ClusterName)
	svc := NewService(self, model.NodeStateClustered, reg).WithMembership(store).WithAuthority(&clusterpb.ClusterAuthority{ClusterId: self.ClusterID, Primary: &clusterpb.AuthorityPrimary{NodeId: "node_a"}, AuthorityEpoch: 1})
	_, err := svc.AddClusterNode(ctx, &clusterpb.AddClusterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, NodeName: "node-c"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", err)
	}
	st, _ := status.FromError(err)
	foundHint := false
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetReason() == "MYCEL_CLUSTER_NOT_PRIMARY" && info.GetMetadata()["mycel-primary-node-id"] == "node_a" {
			foundHint = true
		}
	}
	if !foundHint {
		t.Fatalf("expected primary hint detail, got %#v", st.Details())
	}
}

func TestAddClusterNodeRejectsUnadmittedNode(t *testing.T) {
	svc := NewService(model.NodeIdentity{}, model.NodeStateStandalone, nil)
	_, err := svc.AddClusterNode(context.Background(), &clusterpb.AddClusterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, NodeName: "node-b"})
	if err == nil {
		t.Fatal("expected permission denied")
	}
}

func TestInvalidProtocolRejected(t *testing.T) {
	svc := NewService(model.NodeIdentity{}, model.NodeStateStandalone, nil)
	_, err := svc.GetClusterView(context.Background(), &clusterpb.GetClusterViewRequest{})
	if err == nil {
		t.Fatal("expected invalid protocol error")
	}
}
