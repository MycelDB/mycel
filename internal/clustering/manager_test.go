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

func TestManagerBootstrapCreatesMembershipAndAuthority(t *testing.T) {
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
	authority, ok := m.Authority()
	if !ok {
		t.Fatal("bootstrap authority missing")
	}
	if authority.Primary.NodeID != m.Identity().NodeID || authority.AuthorityEpoch != 1 || authority.Source != AuthoritySourceBootstrap {
		t.Fatalf("unexpected authority: %#v", authority)
	}
	if m.LocalRole() != NodeRolePrimary {
		t.Fatalf("expected primary role, got %s", m.LocalRole())
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "authority.json")); err != nil {
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
	if _, ok := m.Authority(); ok {
		t.Fatal("non-bootstrap manager should not create authority")
	}
	if m.LocalRole() != NodeRoleNone {
		t.Fatalf("expected no role, got %s", m.LocalRole())
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

func TestManagerSetAuthorityDerivesFollowerRole(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := AdmitLocalNode(ctx, dir, m.Identity().ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	m.identity = id
	authority := Authority{Version: AuthorityVersion, ClusterID: m.Identity().ClusterID, Primary: AuthorityPrimary{NodeID: "node-a", NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"}, AuthorityEpoch: 1, Source: AuthoritySourceBootstrap}
	if err := m.SetAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if m.LocalRole() != NodeRoleFollower {
		t.Fatalf("expected follower role, got %s", m.LocalRole())
	}
	loaded, ok, err := LoadAuthority(ctx, AuthorityPath(dir))
	if err != nil || !ok || loaded.Primary.NodeID != "node-a" {
		t.Fatalf("authority not persisted loaded=%#v ok=%v err=%v", loaded, ok, err)
	}
}

func TestManagerSetAuthorityRejectsClusterMismatch(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(ctx, Options{DataDir: t.TempDir(), NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetAuthority(ctx, Authority{ClusterID: "other", Primary: AuthorityPrimary{NodeID: "node-a"}, AuthorityEpoch: 1}); err == nil {
		t.Fatal("expected cluster mismatch to fail")
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
