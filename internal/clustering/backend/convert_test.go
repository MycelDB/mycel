package backend

import (
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

func TestIdentityConversionRoundTripIncludesAdmissionFields(t *testing.T) {
	now := time.Now().UTC()
	id := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", NodeName: "node-a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", ClusterAdmitted: true, ClusterBootstrap: true, NodePublicKeyFingerprint: "sha256:abc", CreatedAt: now, UpdatedAt: now}
	got, err := IdentityFromProto(IdentityToProto(id))
	if err != nil {
		t.Fatal(err)
	}
	if !got.ClusterAdmitted || !got.ClusterBootstrap || got.NodePublicKeyFingerprint != id.NodePublicKeyFingerprint {
		t.Fatalf("got=%#v", got)
	}
}

func TestPeerConversionRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	peer := model.Peer{NodeID: "node_a", NodeName: "node-a", ClusterID: "cluster_a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", State: model.PeerStateActive, Source: model.PeerSourceDiscovered, LastSeenAt: &now}
	got, err := PeerFromProto(PeerToProto(peer))
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != peer.NodeID || got.State != peer.State || got.Source != peer.Source || got.BackendAdvertiseAddr != peer.BackendAdvertiseAddr {
		t.Fatalf("got=%#v want=%#v", got, peer)
	}
}

func TestSnapshotConversionRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	id := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", ClusterID: "cluster_a", CreatedAt: now, UpdatedAt: now}
	snap := model.Snapshot{Version: model.PeerStoreVersion, UpdatedAt: now, Peers: []model.Peer{{NodeID: "node_a", BackendAdvertiseAddr: "127.0.0.1:9093", State: model.PeerStateSelf, Source: model.PeerSourceSelf}}}
	got, err := SnapshotFromProto(SnapshotToProto(snap, id, model.NodeStateClustered))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Peers) != 1 || got.Peers[0].NodeID != "node_a" {
		t.Fatalf("got=%#v", got)
	}
}
