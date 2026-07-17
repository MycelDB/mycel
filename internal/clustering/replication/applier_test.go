package replication

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/wal"
)

func newTestApplier(t *testing.T, fail bool) (*Applier, *ProgressStore, *ReceiveLog, *int) {
	t.Helper()
	dir := t.TempDir()
	log := NewReceiveLog(filepath.Join(dir, "log"))
	progress := NewProgressStore(filepath.Join(dir, "progress.json"))
	registry := wal.NewRegistry()
	count := 0
	_ = registry.Register("test.v1", wal.ApplierFunc(func(ctx context.Context, rec wal.Record) error {
		if fail {
			return errors.New("apply failed")
		}
		count++
		return nil
	}))
	return &Applier{Log: log, Progress: progress, Registry: registry}, progress, log, &count
}

func TestApplierApplySkipGapAndFailure(t *testing.T) {
	ctx := context.Background()
	a, p, _, count := newTestApplier(t, false)
	if err := a.ApplyReceived(ctx, "cluster", "node", 1, testRecord(1)); err != nil {
		t.Fatal(err)
	}
	got, _ := p.Load(ctx)
	if got.AppliedLSN != 1 || got.ReceivedLSN != 1 || *count != 1 {
		t.Fatalf("progress=%#v count=%d", got, *count)
	}
	if err := a.ApplyReceived(ctx, "cluster", "node", 1, testRecord(1)); err != nil || *count != 1 {
		t.Fatalf("skip err=%v count=%d", err, *count)
	}
	if err := a.ApplyReceived(ctx, "cluster", "node", 1, testRecord(3)); err == nil {
		t.Fatal("expected gap")
	}
	bad, progress, _, _ := newTestApplier(t, true)
	err := bad.ApplyReceived(ctx, "cluster", "node", 1, testRecord(1))
	if err == nil {
		t.Fatal("expected apply failure")
	}
	pg, _ := progress.Load(ctx)
	if pg.AppliedLSN != 0 || pg.ReceivedLSN != 1 {
		t.Fatalf("progress advanced incorrectly: %#v", pg)
	}
}

func TestApplierReplayAndAuthorityMismatch(t *testing.T) {
	ctx := context.Background()
	a, p, log, count := newTestApplier(t, false)
	if err := log.Put(ctx, testRecord(1)); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(ctx, Progress{ClusterID: "cluster", PrimaryNodeID: "node", AuthorityEpoch: 1, ReceivedLSN: 1}); err != nil {
		t.Fatal(err)
	}
	if err := a.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	if *count != 1 {
		t.Fatalf("count=%d", *count)
	}
	if err := a.ApplyReceived(ctx, "other", "node", 1, testRecord(2)); err == nil {
		t.Fatal("expected authority mismatch")
	}
}
