package membership

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
	member := Member{NodeName: "node-b", State: MemberStatePending, Role: "member", JoinToken: &JoinToken{TokenID: "join_tok_1", Hash: HashToken("secret"), CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}}
	if err := store.UpsertMember(ctx, member); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.FindByNodeName(ctx, "node-b")
	if err != nil || !ok {
		t.Fatalf("find by name ok=%v err=%v", ok, err)
	}
	if got.JoinToken == nil || got.JoinToken.Hash == "" {
		t.Fatalf("missing token: %#v", got)
	}
	joined := time.Now().UTC()
	got.NodeID = "node_b"
	got.State = MemberStateActive
	got.JoinedAt = &joined
	got.JoinToken.Hash = ""
	if err := store.UpsertMember(ctx, got); err != nil {
		t.Fatal(err)
	}
	byID, ok, err := store.FindByNodeID(ctx, "node_b")
	if err != nil || !ok || byID.State != MemberStateActive {
		t.Fatalf("find by id=%#v ok=%v err=%v", byID, ok, err)
	}
}

func TestFileStoreDoesNotPersistPlaintextToken(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "membership.json")
	store := NewFileStore(path, "cluster-a", "dev")
	plain := "mycel_join_v1_plaintext"
	if err := store.UpsertMember(ctx, Member{NodeName: "node-b", State: MemberStatePending, JoinToken: &JoinToken{TokenID: "join_tok_1", Hash: HashToken(plain), CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), plain) {
		t.Fatal("plaintext token persisted")
	}
	var data StoreData
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	if data.Members[0].JoinToken.Hash == "" {
		t.Fatal("token hash not persisted")
	}
}
