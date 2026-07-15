package topology

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

func TestRegistrySelfAndPersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "peers.json")
	self := model.Peer{NodeID: "node_a", BackendAdvertiseAddr: "127.0.0.1:9093"}
	reg, err := NewRegistry(ctx, NewFileStore(path), self)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reg.Self()
	if !ok || got.NodeID != "node_a" || got.State != model.PeerStateSelf || got.Source != model.PeerSourceSelf {
		t.Fatalf("self=%#v ok=%v", got, ok)
	}
	loaded, err := NewFileStore(path).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Peers) != 1 || loaded.Peers[0].State != model.PeerStateSelf {
		t.Fatalf("loaded=%#v", loaded.Peers)
	}
}

func TestRegistryUpsertMergeAndEvents(t *testing.T) {
	ctx := context.Background()
	reg, err := NewRegistry(ctx, nil, model.Peer{NodeID: "node_a", BackendAdvertiseAddr: "127.0.0.1:9093"})
	if err != nil {
		t.Fatal(err)
	}
	events, unsub := reg.Subscribe(4)
	defer unsub()
	peer := model.Peer{NodeID: "node_b", BackendAdvertiseAddr: "127.0.0.1:9094", State: model.PeerStateSeed, Source: model.PeerSourceSeed}
	if err := reg.Upsert(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if ev := <-events; ev.Type != model.EventPeerAdded {
		t.Fatalf("event=%s", ev.Type)
	}
	peer.State = model.PeerStateActive
	if err := reg.Upsert(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if ev := <-events; ev.Type != model.EventPeerStateChanged {
		t.Fatalf("event=%s", ev.Type)
	}
	if err := reg.Merge(ctx, model.Snapshot{Peers: []model.Peer{{NodeID: "node_a", BackendAdvertiseAddr: "other", State: model.PeerStateActive}, {NodeID: "node_c", BackendAdvertiseAddr: "127.0.0.1:9095", State: model.PeerStateSelf, Source: model.PeerSourceSelf}}}); err != nil {
		t.Fatal(err)
	}
	self, _ := reg.Self()
	if self.BackendAdvertiseAddr != "127.0.0.1:9093" {
		t.Fatalf("self overwritten: %#v", self)
	}
	foundC := false
	for _, p := range reg.RemotePeers() {
		if p.NodeID == "node_c" && p.State == model.PeerStateActive {
			foundC = true
		}
	}
	if !foundC {
		t.Fatalf("merged peers=%#v", reg.List())
	}
}

func TestRegistryMarkUnreachable(t *testing.T) {
	ctx := context.Background()
	reg, err := NewRegistry(ctx, nil, model.Peer{NodeID: "node_a", BackendAdvertiseAddr: "127.0.0.1:9093"})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.MarkUnreachable(ctx, "127.0.0.1:9094"); err != nil {
		t.Fatal(err)
	}
	peers := reg.RemotePeers()
	if len(peers) != 1 || peers[0].State != model.PeerStateUnreachable {
		t.Fatalf("peers=%#v", peers)
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "peers.json")
	now := time.Now().UTC()
	store := NewFileStore(path)
	if err := store.Save(ctx, model.Snapshot{Version: model.PeerStoreVersion, UpdatedAt: now, Peers: []model.Peer{{BackendAdvertiseAddr: "127.0.0.1:9094", State: model.PeerStateSeed, Source: model.PeerSourceSeed}}}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Peers) != 1 || got.Peers[0].BackendAdvertiseAddr != "127.0.0.1:9094" {
		t.Fatalf("got=%#v", got)
	}
}
