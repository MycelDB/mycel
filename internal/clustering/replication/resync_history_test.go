package replication

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResyncHistoryStoreMissingFileReturnsEmpty(t *testing.T) {
	store := NewResyncHistoryStore(filepath.Join(t.TempDir(), "history.json"))
	ops, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		t.Fatalf("ops=%d", len(ops))
	}
}

func TestResyncHistoryStoreUpsertInsertAndUpdate(t *testing.T) {
	ctx := context.Background()
	store := NewResyncHistoryStore(filepath.Join(t.TempDir(), "history.json"))
	op := ResyncOperation{OperationID: "op-1", TargetNodeName: "node-b", StartedAt: time.Now(), Status: ResyncOperationRunning}
	if err := store.Upsert(ctx, op); err != nil {
		t.Fatal(err)
	}
	op.Status = ResyncOperationSucceeded
	op.SnapshotBaseLSN = 42
	if err := store.Upsert(ctx, op); err != nil {
		t.Fatal(err)
	}
	ops, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("ops=%d", len(ops))
	}
	if ops[0].Status != ResyncOperationSucceeded || ops[0].SnapshotBaseLSN != 42 {
		t.Fatalf("op=%#v", ops[0])
	}
}

func TestResyncHistoryStoreBoundsHistory(t *testing.T) {
	ctx := context.Background()
	store := NewResyncHistoryStore(filepath.Join(t.TempDir(), "history.json"))
	store.Limit = 3
	for i := 0; i < 5; i++ {
		if err := store.Upsert(ctx, ResyncOperation{OperationID: string(rune('a' + i)), StartedAt: time.Now(), Status: ResyncOperationSucceeded}); err != nil {
			t.Fatal(err)
		}
	}
	ops, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 3 {
		t.Fatalf("ops=%d", len(ops))
	}
	if ops[0].OperationID != "e" || ops[2].OperationID != "c" {
		t.Fatalf("unexpected order: %#v", ops)
	}
}

func TestResyncHistoryStoreCorruptJSONReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewResyncHistoryStore(path).List(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}
