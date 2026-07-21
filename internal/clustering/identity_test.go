package clustering

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadOrCreateBootstrapWritesAdmissionFields(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"})
	if err != nil {
		t.Fatal(err)
	}
	if !node.Identity.ClusterAdmitted || !node.Identity.ClusterBootstrap {
		t.Fatalf("bootstrap/admitted not set: %#v", node.Identity)
	}
}

func TestLoadOrCreateCreatesAndPreservesIdentity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	first, err := LoadOrCreate(ctx, Options{DataDir: dir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("LoadOrCreate first: %v", err)
	}
	if first.State != NodeStateStandalone {
		t.Fatalf("state=%s want %s", first.State, NodeStateStandalone)
	}
	if first.Identity.NodeID == "" || first.Identity.ClusterID == "" {
		t.Fatalf("missing ids: %#v", first.Identity)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "clustering", "node.json")); err != nil {
		t.Fatalf("node.json not written: %v", err)
	}
	var local LocalState
	raw, err := os.ReadFile(filepath.Join(dir, "meta", "clustering", "local_state.json"))
	if err != nil {
		t.Fatalf("local_state.json not written: %v", err)
	}
	if err := json.Unmarshal(raw, &local); err != nil {
		t.Fatalf("unmarshal local state: %v", err)
	}
	if local.Mode != ClusterModeStandalone || local.State != NodeStateStandalone {
		t.Fatalf("local state=%#v want standalone", local)
	}
	second, err := LoadOrCreate(ctx, Options{DataDir: dir})
	if err != nil {
		t.Fatalf("LoadOrCreate second: %v", err)
	}
	if second.Identity.NodeID != first.Identity.NodeID || second.Identity.ClusterID != first.Identity.ClusterID {
		t.Fatalf("ids changed: first=%#v second=%#v", first.Identity, second.Identity)
	}
}

func TestLoadOrCreatePersistsAndUpdatesMutableFields(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "cluster-a", BackendAdvertiseAddr: "127.0.0.1:9091"})
	if err != nil {
		t.Fatalf("LoadOrCreate first: %v", err)
	}
	if first.State != NodeStateClustered {
		t.Fatalf("state=%s want %s", first.State, NodeStateClustered)
	}
	var local LocalState
	raw, err := os.ReadFile(filepath.Join(dir, "meta", "clustering", "local_state.json"))
	if err != nil {
		t.Fatalf("local_state.json not written: %v", err)
	}
	if err := json.Unmarshal(raw, &local); err != nil {
		t.Fatalf("unmarshal local state: %v", err)
	}
	if local.Mode != ClusterModeClustered || local.State != NodeStateClustered {
		t.Fatalf("local state=%#v want clustered", local)
	}
	second, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "node-b", ClusterName: "cluster-b", BackendAdvertiseAddr: "10.0.0.5:9091"})
	if err != nil {
		t.Fatalf("LoadOrCreate second: %v", err)
	}
	if second.Identity.NodeID != first.Identity.NodeID || second.Identity.ClusterID != first.Identity.ClusterID {
		t.Fatalf("immutable ids changed")
	}
	if second.Identity.NodeName != "node-b" || second.Identity.ClusterName != "cluster-b" || second.Identity.BackendAdvertiseAddr != "10.0.0.5:9091" {
		t.Fatalf("mutable fields not updated: %#v", second.Identity)
	}
}

func TestLoadOrCreateWritesPeers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	local, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9191"})
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "meta", "clustering", "peers.json"))
	if err != nil {
		t.Fatalf("peers.json not written: %v", err)
	}
	var peers PeerStore
	if err := json.Unmarshal(raw, &peers); err != nil {
		t.Fatalf("unmarshal peers: %v", err)
	}
	if len(peers.Peers) != 1 {
		t.Fatalf("peers len=%d want 1 self only: %#v", len(peers.Peers), peers.Peers)
	}
	if peers.Peers[0].NodeID != local.Identity.NodeID || peers.Peers[0].State != PeerStateSelf {
		t.Fatalf("self peer not first/matching: %#v", peers.Peers[0])
	}
}

func TestLoadOrCreateRejectsInvalidAdvertiseAddress(t *testing.T) {
	_, err := LoadOrCreate(context.Background(), Options{DataDir: t.TempDir(), BackendAdvertiseAddr: "0.0.0.0:9091"})
	if err == nil {
		t.Fatal("expected invalid wildcard advertise address")
	}
}

func TestLoadOrCreateRejectsMalformedIdentity(t *testing.T) {
	dir := t.TempDir()
	meta := filepath.Join(dir, "meta", "clustering")
	if err := os.MkdirAll(meta, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(meta, "node.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(context.Background(), Options{DataDir: dir}); err == nil {
		t.Fatal("expected malformed node.json error")
	}
}

func TestValidateIdentityRequiresIDs(t *testing.T) {
	raw := []byte(`{"version":1,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`)
	var id NodeIdentity
	if err := json.Unmarshal(raw, &id); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIdentity(id); err == nil {
		t.Fatal("expected missing ids error")
	}
}
