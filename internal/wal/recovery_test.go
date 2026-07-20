package wal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type memoryProgress struct{ lsn LSN }

func (m *memoryProgress) AppliedLSN(context.Context) (LSN, error)        { return m.lsn, nil }
func (m *memoryProgress) SetAppliedLSN(_ context.Context, lsn LSN) error { m.lsn = lsn; return nil }

func TestRecoveryAppliesInOrderAndTracksProgress(t *testing.T) {
	ctx := context.Background()
	mgr, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	for i := 0; i < 3; i++ {
		lsn, err := mgr.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := mgr.Sync(ctx, lsn); err != nil {
			t.Fatal(err)
		}
	}
	seen := []LSN{}
	reg := NewRegistry()
	if err := reg.Register("test.v1", ApplierFunc(func(_ context.Context, rec Record) error {
		seen = append(seen, rec.LSN)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	progress := &memoryProgress{}
	rec := NewRecovery(mgr, reg, progress)
	applied, err := rec.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 3 || progress.lsn != 3 {
		t.Fatalf("applied=%v progress=%v want 3", applied, progress.lsn)
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 2 || seen[2] != 3 {
		t.Fatalf("seen=%v", seen)
	}
	if err := rec.Waiter().WaitUntilApplied(ctx, 3); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryStartsAfterAppliedLSN(t *testing.T) {
	ctx := context.Background()
	mgr, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	for i := 0; i < 3; i++ {
		lsn, _ := mgr.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
		_ = mgr.Sync(ctx, lsn)
	}
	seen := []LSN{}
	reg := NewRegistry()
	_ = reg.Register("test.v1", ApplierFunc(func(_ context.Context, rec Record) error { seen = append(seen, rec.LSN); return nil }))
	progress := &memoryProgress{lsn: 2}
	applied, err := NewRecovery(mgr, reg, progress).Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 3 || len(seen) != 1 || seen[0] != 3 {
		t.Fatalf("applied=%v seen=%v", applied, seen)
	}
}

func TestRecoveryUnknownRecordTypeFails(t *testing.T) {
	ctx := context.Background()
	mgr, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	lsn, _ := mgr.Append(ctx, PendingRecord{Type: "unknown.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	_ = mgr.Sync(ctx, lsn)
	_, err = NewRecovery(mgr, NewRegistry(), &memoryProgress{}).Recover(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRecoveryReportsApplyFailureLSN(t *testing.T) {
	ctx := context.Background()
	mgr, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	lsn, _ := mgr.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	_ = mgr.Sync(ctx, lsn)
	boom := errors.New("boom")
	reg := NewRegistry()
	_ = reg.Register("test.v1", ApplierFunc(func(context.Context, Record) error { return boom }))
	_, err = NewRecovery(mgr, reg, &memoryProgress{}).Recover(ctx)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v want boom", err)
	}
}

func TestFileProgressStore(t *testing.T) {
	ctx := context.Background()
	store := NewFileProgressStore(t.TempDir() + "/meta/wal/progress.json")
	lsn, err := store.AppliedLSN(ctx)
	if err != nil || lsn != 0 {
		t.Fatalf("initial lsn=%v err=%v", lsn, err)
	}
	if err := store.SetAppliedLSN(ctx, 42); err != nil {
		t.Fatal(err)
	}
	lsn, err = store.AppliedLSN(ctx)
	if err != nil || lsn != 42 {
		t.Fatalf("lsn=%v err=%v", lsn, err)
	}
}

func TestWaitUntilAppliedHonorsContext(t *testing.T) {
	w := NewApplyWaiter()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.WaitUntilApplied(ctx, 1); err == nil {
		t.Fatal("expected timeout")
	}
	w.SetApplied(1)
	if err := w.WaitUntilApplied(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRejectsDuplicate(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("x", ApplierFunc(func(context.Context, Record) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("x", ApplierFunc(func(context.Context, Record) error { return nil })); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestProgressJSONShape(t *testing.T) {
	store := NewFileProgressStore(t.TempDir() + "/progress.json")
	if err := store.SetAppliedLSN(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	rawLSN, err := store.AppliedLSN(context.Background())
	if err != nil || rawLSN != 7 {
		t.Fatal(rawLSN, err)
	}
	// Keep the persisted field name stable for docs and future migrations.
	b, _ := json.Marshal(progressDocument{AppliedLSN: 7})
	if string(b) == `{}` {
		t.Fatal("progress json missing fields")
	}
}
