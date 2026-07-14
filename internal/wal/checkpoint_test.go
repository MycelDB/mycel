package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointDoesNotAdvancePastApplied(t *testing.T) {
	ctx := context.Background()
	progress := &memoryProgress{lsn: 5}
	store := NewCheckpointStore(filepath.Join(t.TempDir(), "meta", "wal", "checkpoint.json"))
	cp, err := CreateCheckpoint(ctx, progress, store, 10)
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 5 {
		t.Fatalf("checkpoint lsn=%v want 5", cp.LSN)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LSN != 5 {
		t.Fatalf("loaded lsn=%v want 5", loaded.LSN)
	}
}

func TestCheckpointUsesRequestedAppliedTarget(t *testing.T) {
	ctx := context.Background()
	progress := &memoryProgress{lsn: 5}
	store := NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json"))
	cp, err := CreateCheckpoint(ctx, progress, store, 3)
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 3 {
		t.Fatalf("checkpoint lsn=%v want 3", cp.LSN)
	}
}

func TestRetainFromKeepsNeededSegments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := Open(ctx, Options{Dir: dir, SegmentBytes: 150})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{"x":"xxxxxxxxxxxxxxxxxxxxxxxx"}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Sync(ctx, lsn); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	segsBefore, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segsBefore) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(segsBefore))
	}
	m, err = Open(ctx, Options{Dir: dir, SegmentBytes: 150})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.RetainFrom(ctx, segsBefore[len(segsBefore)-1].start); err != nil {
		t.Fatal(err)
	}
	segsAfter, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segsAfter) >= len(segsBefore) {
		t.Fatalf("retention did not remove old segments: before=%d after=%d", len(segsBefore), len(segsAfter))
	}
	if _, err := os.Stat(segsAfter[len(segsAfter)-1].path); err != nil {
		t.Fatal(err)
	}
}
