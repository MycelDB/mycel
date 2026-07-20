package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendReadReopenAndRotate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := Open(ctx, Options{Dir: dir, SegmentBytes: 180})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{"n":1}`)})
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if lsn != LSN(i+1) {
			t.Fatalf("lsn=%v want %v", lsn, i+1)
		}
		if err := m.Sync(ctx, lsn); err != nil {
			t.Fatalf("Sync() error = %v", err)
		}
	}
	if got := m.LastCommittedLSN(); got != 5 {
		t.Fatalf("last=%v want 5", got)
	}
	_ = m.Close()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected rotation, got %d segments", len(segs))
	}
	m, err = Open(ctx, Options{Dir: dir, SegmentBytes: 180})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	if got := m.LastCommittedLSN(); got != 5 {
		t.Fatalf("last after reopen=%v want 5", got)
	}
	it, err := m.ReadFrom(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	want := LSN(3)
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if rec.LSN != want {
			t.Fatalf("got lsn %v want %v", rec.LSN, want)
		}
		want++
	}
	if want != 6 {
		t.Fatalf("ended at %v", want)
	}
}

func TestStatusAndWaitUntilCommitted(t *testing.T) {
	ctx := context.Background()
	m, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.WaitUntilCommitted(waitCtx, 1) }()
	select {
	case err := <-done:
		t.Fatalf("wait returned before append: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(ctx, lsn); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("WaitUntilCommitted() error = %v", err)
	}
	status, err := m.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.LastCommittedLSN != 1 || status.OldestRetainedLSN != 1 || status.CurrentSegmentStart != 1 {
		t.Fatalf("status=%#v", status)
	}
}

func TestReadNextBlockingWaitsForFutureRecord(t *testing.T) {
	ctx := context.Background()
	m, err := Open(ctx, Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	type result struct {
		rec Record
		ok  bool
		err error
	}
	ch := make(chan result, 1)
	go func() { rec, ok, err := m.ReadNextBlocking(readCtx, 1); ch <- result{rec: rec, ok: ok, err: err} }()
	select {
	case r := <-ch:
		t.Fatalf("read returned before append: %#v", r)
	case <-time.After(20 * time.Millisecond):
	}
	lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(ctx, lsn); err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil || !r.ok || r.rec.LSN != 1 {
		t.Fatalf("result=%#v", r)
	}
}

func TestChecksumCorruptionFailsOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := Open(ctx, Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(ctx, lsn); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	path := filepath.Join(dir, segmentName(1))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xff
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, Options{Dir: dir}); err == nil {
		t.Fatal("expected corruption error")
	}
}

func TestTornFinalFrameIsTruncated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := Open(ctx, Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := m.Append(ctx, PendingRecord{Type: "test.v1", SchemaVersion: 1, Payload: []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(ctx, lsn); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	path := filepath.Join(dir, segmentName(1))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0x4d, 0x57, 0x41})
	_ = f.Close()
	m, err = Open(ctx, Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open with torn final frame error = %v", err)
	}
	if got := m.LastCommittedLSN(); got != 1 {
		t.Fatalf("last=%v want 1", got)
	}
	b, _ := os.ReadFile(path)
	if bytes.HasSuffix(b, []byte{0x4d, 0x57, 0x41}) {
		t.Fatal("torn bytes were not truncated")
	}
}
