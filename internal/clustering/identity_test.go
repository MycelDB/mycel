package clustering

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
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

func TestLoadOrCreateRaftModeCreatesPendingIdentity(t *testing.T) {
	ctx := context.Background()
	clusterIDs := map[string]bool{}
	for i := 1; i <= 3; i++ {
		node, err := LoadOrCreate(ctx, Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", RaftMode: true, RaftLocalNodeID: uint64(i), RaftNodeCount: 3})
		if err != nil {
			t.Fatalf("LoadOrCreate(%d): %v", i, err)
		}
		if node.Identity.ClusterID != "" || node.Identity.ClusterAdmitted || node.Identity.ClusterBootstrap {
			t.Fatalf("raft-mode identity should be pending, got %#v", node.Identity)
		}
		if node.Identity.NodeID != "node_"+strconv.Itoa(i) {
			t.Fatalf("node_id=%q want deterministic raft node id", node.Identity.NodeID)
		}
		if node.State != NodeStateInitializing {
			t.Fatalf("state=%s want %s", node.State, NodeStateInitializing)
		}
		clusterIDs[node.Identity.ClusterID] = true
	}
	if len(clusterIDs) != 1 || !clusterIDs[""] {
		t.Fatalf("raft-mode fresh identities should not generate cluster IDs: %#v", clusterIDs)
	}
}

func TestLoadOrCreateRaftModeConvertsExistingUnadmittedIdentityToPending(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	standalone, err := LoadOrCreate(ctx, Options{DataDir: dir})
	if err != nil {
		t.Fatalf("standalone LoadOrCreate: %v", err)
	}
	if standalone.Identity.ClusterID == "" || standalone.Identity.NodeID == "node_2" || standalone.Identity.ClusterAdmitted {
		t.Fatalf("unexpected standalone baseline: %#v", standalone.Identity)
	}
	raft, err := LoadOrCreate(ctx, Options{DataDir: dir, ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", RaftMode: true, RaftLocalNodeID: 2, RaftNodeCount: 3})
	if err != nil {
		t.Fatalf("raft LoadOrCreate: %v", err)
	}
	if raft.Identity.ClusterID != "" || raft.Identity.NodeID != "node_2" || raft.Identity.ClusterAdmitted || raft.Identity.ClusterBootstrap {
		t.Fatalf("existing unadmitted identity not converted to pending raft cache: %#v", raft.Identity)
	}
}

func TestLoadOrCreateRaftModeTreatsExistingAdmittedIdentityAsUnvalidatedCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clustered, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "old", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"})
	if err != nil {
		t.Fatalf("clustered LoadOrCreate: %v", err)
	}
	if clustered.Identity.ClusterID == "" || !clustered.Identity.ClusterAdmitted {
		t.Fatalf("unexpected clustered baseline: %#v", clustered.Identity)
	}
	raft, err := LoadOrCreate(ctx, Options{DataDir: dir, ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094", RaftMode: true, RaftLocalNodeID: 2, RaftNodeCount: 3})
	if err != nil {
		t.Fatalf("raft LoadOrCreate: %v", err)
	}
	if raft.Identity.ClusterID != clustered.Identity.ClusterID || raft.Identity.ClusterAdmitted || raft.Identity.ClusterBootstrap || raft.State != NodeStateInitializing {
		t.Fatalf("existing admitted identity should become unvalidated raft cache retaining cluster_id: %#v state=%s", raft.Identity, raft.State)
	}
}

func TestManagerApplySystemMetadataCachesAuthoritativeIdentity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mgr, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 3}, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr.Identity().ClusterID != "" || mgr.IsAdmitted() {
		t.Fatalf("expected pending manager identity: %#v", mgr.Identity())
	}
	meta := consensus.SystemMetadata{ClusterID: "cluster_authoritative", ClusterName: "dev", NodeCount: 3, PartitionCount: 64, ReplicaFactor: 3, Nodes: map[string]consensus.SystemNode{"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:9093"}}, Placement: map[uint32]consensus.PartitionPlacement{}}
	if err := mgr.ApplySystemMetadata(ctx, meta, 1); err != nil {
		t.Fatalf("ApplySystemMetadata: %v", err)
	}
	if mgr.Identity().ClusterID != "cluster_authoritative" || !mgr.Identity().ClusterAdmitted || !mgr.Identity().ClusterBootstrap || mgr.State() != NodeStateInitializing {
		t.Fatalf("metadata not cached in manager identity/state: identity=%#v state=%s", mgr.Identity(), mgr.State())
	}
	if err := mgr.MarkPartitionGroupsStarted(64, 64); err != nil {
		t.Fatalf("MarkPartitionGroupsStarted: %v", err)
	}
	if mgr.State() != NodeStateClustered || !mgr.Readiness().ClientReady {
		t.Fatalf("partition-ready manager state/readiness mismatch: state=%s readiness=%#v", mgr.State(), mgr.Readiness())
	}
	second, err := LoadOrCreate(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 3})
	if err != nil {
		t.Fatalf("reload LoadOrCreate: %v", err)
	}
	if second.Identity.ClusterID != "cluster_authoritative" || second.Identity.ClusterAdmitted || second.State != NodeStateInitializing {
		t.Fatalf("authoritative identity cache should reload as unvalidated pending raft cache: %#v state=%s", second.Identity, second.State)
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
