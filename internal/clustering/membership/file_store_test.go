package membership

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreMissingLoadAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "membership.json"), "cluster-a", "dev")
	data, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if data.ClusterID != "cluster-a" || len(data.Members) != 0 {
		t.Fatalf("unexpected data: %#v", data)
	}
	now := time.Now().UTC()
	data.Members = append(data.Members, Member{NodeName: "node-a", NodeID: "node_a", State: MemberStateActive, CreatedAt: now, UpdatedAt: now})
	if err := store.Save(ctx, data); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Members) != 1 || loaded.Members[0].NodeName != "node-a" {
		t.Fatalf("loaded=%#v", loaded)
	}
}

func TestFileStoreUpsertAndFind(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "membership.json"), "cluster-a", "dev")
	member := Member{NodeName: "node-b", State: MemberStatePending, Role: "member", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := store.UpsertMember(ctx, member); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.FindByNodeName(ctx, "node-b")
	if err != nil || !ok {
		t.Fatalf("find by name ok=%v err=%v", ok, err)
	}
	joined := time.Now().UTC()
	got.NodeID = "node_b"
	got.State = MemberStateActive
	got.JoinedAt = &joined
	if err := store.UpsertMember(ctx, got); err != nil {
		t.Fatal(err)
	}
	byID, ok, err := store.FindByNodeID(ctx, "node_b")
	if err != nil || !ok || byID.State != MemberStateActive {
		t.Fatalf("find by id=%#v ok=%v err=%v", byID, ok, err)
	}
}
