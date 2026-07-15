package clustering

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
)

func TestManagerBootstrapCreatesMembership(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !m.IsAdmitted() || !m.IsBootstrap() {
		t.Fatalf("expected admitted bootstrap")
	}
	member, ok, err := m.Membership().FindByNodeID(ctx, m.Identity().NodeID)
	if err != nil || !ok {
		t.Fatalf("membership self missing ok=%v err=%v", ok, err)
	}
	if member.State != membership.MemberStateActive || !member.ClusterBootstrap {
		t.Fatalf("member=%#v", member)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "membership.json")); err != nil {
		t.Fatal(err)
	}
}

func TestManagerNonBootstrapDoesNotCreateSelfMembership(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.IsAdmitted() || m.IsBootstrap() {
		t.Fatalf("unexpected admitted/bootstrap")
	}
	_, ok, err := m.Membership().FindByNodeID(ctx, m.Identity().NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("non-bootstrap self should not be active membership")
	}
}

func TestManagerFirstBootCreatesIdentityStateAndTopology(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Identity().NodeID == "" || m.Identity().ClusterID == "" {
		t.Fatalf("missing identity: %#v", m.Identity())
	}
	if m.State() != model.NodeStateClustered {
		t.Fatalf("state=%s", m.State())
	}
	self, ok := m.Topology().Self()
	if !ok || self.NodeID != m.Identity().NodeID || self.State != model.PeerStateSelf {
		t.Fatalf("self=%#v ok=%v", self, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "node.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "local_state.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "peers.json")); err != nil {
		t.Fatal(err)
	}
}

func TestManagerRestartPreservesIDs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first, err := NewManager(ctx, Options{DataDir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewManager(ctx, Options{DataDir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity().NodeID != second.Identity().NodeID || first.Identity().ClusterID != second.Identity().ClusterID {
		t.Fatalf("ids changed: first=%#v second=%#v", first.Identity(), second.Identity())
	}
}

func TestManagerSeedConfigDoesNotCreateTopologyPeers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, BackendAdvertiseAddr: "127.0.0.1:9093", SeedPeers: []string{"127.0.0.1:9094"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Topology().RemotePeers() {
		if p.BackendAdvertiseAddr == "127.0.0.1:9094" {
			t.Fatalf("seed should not be a topology peer: %#v", m.Topology().List())
		}
	}
}

func TestManagerStopWritesStoppedState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "meta", "clustering", "local_state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state LocalState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state.State != model.NodeStateStopped {
		t.Fatalf("state=%s", state.State)
	}
}
