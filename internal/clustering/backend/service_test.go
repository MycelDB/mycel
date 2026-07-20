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

func TestInvalidProtocolRejected(t *testing.T) {
	svc := NewService(model.NodeIdentity{}, model.NodeStateStandalone, nil)
	_, err := svc.GetClusterView(context.Background(), &clusterpb.GetClusterViewRequest{})
	if err == nil {
		t.Fatal("expected invalid protocol error")
	}
}
