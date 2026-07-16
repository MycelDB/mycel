package clustering

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

func TestSaveLoadAuthority(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta", "clustering", "authority.json")
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	authority := Authority{Version: AuthorityVersion, ClusterID: "cluster-a", Primary: AuthorityPrimary{NodeID: "node-a", NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"}, AuthorityEpoch: 7, Source: AuthoritySourceManual, UpdatedAt: now}
	if err := SaveAuthority(ctx, path, authority); err != nil {
		t.Fatalf("SaveAuthority: %v", err)
	}
	loaded, ok, err := LoadAuthority(ctx, path)
	if err != nil || !ok {
		t.Fatalf("LoadAuthority ok=%v err=%v", ok, err)
	}
	if loaded.ClusterID != authority.ClusterID || loaded.Primary.NodeID != authority.Primary.NodeID || loaded.AuthorityEpoch != authority.AuthorityEpoch || loaded.Source != authority.Source {
		t.Fatalf("unexpected authority: %#v", loaded)
	}
}

func TestLoadAuthorityMissing(t *testing.T) {
	_, ok, err := LoadAuthority(context.Background(), filepath.Join(t.TempDir(), "missing.json"))
	if err != nil || ok {
		t.Fatalf("expected missing authority ok=false err=nil, got ok=%v err=%v", ok, err)
	}
}

func TestSaveAuthorityValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.json")
	if err := SaveAuthority(context.Background(), path, Authority{Primary: AuthorityPrimary{NodeID: "node-a"}, AuthorityEpoch: 1}); err == nil {
		t.Fatal("expected missing cluster_id to fail")
	}
	if err := SaveAuthority(context.Background(), path, Authority{ClusterID: "cluster-a", AuthorityEpoch: 1}); err == nil {
		t.Fatal("expected missing primary node_id to fail")
	}
	if err := SaveAuthority(context.Background(), path, Authority{ClusterID: "cluster-a", Primary: AuthorityPrimary{NodeID: "node-a"}}); err == nil {
		t.Fatal("expected non-positive epoch to fail")
	}
}

func TestInitBootstrapAuthorityCreatesAndPreserves(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	id := model.NodeIdentity{NodeID: "node-a", NodeName: "node-a", ClusterID: "cluster-a", BackendAdvertiseAddr: "127.0.0.1:9093"}
	authority, err := InitBootstrapAuthority(ctx, dir, id, now)
	if err != nil {
		t.Fatalf("InitBootstrapAuthority: %v", err)
	}
	if authority.Primary.NodeID != "node-a" || authority.AuthorityEpoch != 1 || authority.Source != AuthoritySourceBootstrap {
		t.Fatalf("unexpected bootstrap authority: %#v", authority)
	}
	second, err := InitBootstrapAuthority(ctx, dir, model.NodeIdentity{NodeID: "node-b", ClusterID: "cluster-a"}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("second InitBootstrapAuthority: %v", err)
	}
	if second.Primary.NodeID != "node-a" || !second.UpdatedAt.Equal(now) {
		t.Fatalf("expected existing authority to be preserved: %#v", second)
	}
}

func TestInitBootstrapAuthorityRejectsClusterMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	id := model.NodeIdentity{NodeID: "node-a", ClusterID: "cluster-a"}
	if _, err := InitBootstrapAuthority(ctx, dir, id, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := InitBootstrapAuthority(ctx, dir, model.NodeIdentity{NodeID: "node-a", ClusterID: "cluster-b"}, time.Now()); err == nil {
		t.Fatal("expected cluster mismatch to fail")
	}
}

func TestDeriveLocalRole(t *testing.T) {
	authority := Authority{ClusterID: "cluster-a", Primary: AuthorityPrimary{NodeID: "node-a"}, AuthorityEpoch: 1}
	primary := model.NodeIdentity{NodeID: "node-a", ClusterAdmitted: true}
	follower := model.NodeIdentity{NodeID: "node-b", ClusterAdmitted: true}
	unadmitted := model.NodeIdentity{NodeID: "node-c"}
	if role := DeriveLocalRole(model.NodeStateClustered, primary, authority, true); role != NodeRolePrimary {
		t.Fatalf("expected primary, got %s", role)
	}
	if role := DeriveLocalRole(model.NodeStateClustered, follower, authority, true); role != NodeRoleFollower {
		t.Fatalf("expected follower, got %s", role)
	}
	if role := DeriveLocalRole(model.NodeStateStandalone, primary, authority, true); role != NodeRoleNone {
		t.Fatalf("expected standalone none, got %s", role)
	}
	if role := DeriveLocalRole(model.NodeStateClustered, unadmitted, authority, true); role != NodeRoleNone {
		t.Fatalf("expected unadmitted none, got %s", role)
	}
	if role := DeriveLocalRole(model.NodeStateClustered, follower, Authority{}, false); role != NodeRoleNone {
		t.Fatalf("expected unknown authority none, got %s", role)
	}
}
