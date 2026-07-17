package replication

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/wal"
)

func TestProgressStoreMissingSaveLoadAndError(t *testing.T) {
	ctx := context.Background()
	store := NewProgressStore(filepath.Join(t.TempDir(), "nested", "progress.json"))
	p, err := store.Load(ctx)
	if err != nil || p.Version != ProgressVersion {
		t.Fatalf("Load=%#v err=%v", p, err)
	}
	p.ClusterID = "cluster_a"
	p.PrimaryNodeID = "node_a"
	p.ReceivedLSN = 2
	p.AppliedLSN = 1
	if err := store.Save(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateError(ctx, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastError != "boom" || got.ReceivedLSN != 2 || got.AppliedLSN != 1 {
		t.Fatalf("got=%#v", got)
	}
	if err := store.UpdateError(ctx, nil); err != nil {
		t.Fatal(err)
	}
	got, _ = store.Load(ctx)
	if got.LastError != "" {
		t.Fatalf("error not cleared: %#v", got)
	}
}

func TestProgressStoreRejectsInvalidLSN(t *testing.T) {
	store := NewProgressStore(filepath.Join(t.TempDir(), "progress.json"))
	if err := store.Save(context.Background(), Progress{ReceivedLSN: wal.LSN(1), AppliedLSN: wal.LSN(2)}); err == nil {
		t.Fatal("expected invalid lsn error")
	}
}
