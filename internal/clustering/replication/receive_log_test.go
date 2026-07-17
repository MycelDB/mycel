package replication

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

func testRecord(lsn wal.LSN) Record {
	return Record{LSN: lsn, Type: "test.v1", SchemaVersion: 1, Timestamp: time.Unix(int64(lsn), 0).UTC(), Encoding: wal.PayloadEncodingJSON, Payload: []byte(`{"ok":true}`)}
}

func TestReceiveLogPutGetScanTruncate(t *testing.T) {
	ctx := context.Background()
	log := NewReceiveLog(t.TempDir())
	r1 := testRecord(1)
	r2 := testRecord(2)
	if err := log.Put(ctx, r1); err != nil {
		t.Fatal(err)
	}
	if err := log.Put(ctx, r1); err != nil {
		t.Fatalf("duplicate put: %v", err)
	}
	got, err := log.Get(ctx, 1)
	if err != nil || got.LSN != 1 {
		t.Fatalf("Get=%#v err=%v", got, err)
	}
	if err := log.Put(ctx, r2); err != nil {
		t.Fatal(err)
	}
	recs, err := log.ScanAfter(ctx, 0)
	if err != nil || len(recs) != 2 || recs[0].LSN != 1 || recs[1].LSN != 2 {
		t.Fatalf("ScanAfter=%#v err=%v", recs, err)
	}
	recs, err = log.ScanAfter(ctx, 1)
	if err != nil || len(recs) != 1 || recs[0].LSN != 2 {
		t.Fatalf("ScanAfter(1)=%#v err=%v", recs, err)
	}
	if err := log.TruncateBefore(ctx, 2); err != nil {
		t.Fatal(err)
	}
	recs, err = log.ScanAfter(ctx, 0)
	if err != nil || len(recs) != 1 || recs[0].LSN != 2 {
		t.Fatalf("after truncate=%#v err=%v", recs, err)
	}
}

func TestReceiveLogConflictAndCorrupt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	log := NewReceiveLog(dir)
	r := testRecord(1)
	if err := log.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Payload = []byte(`{"ok":false}`)
	if err := log.Put(ctx, r); err == nil {
		t.Fatal("expected conflict")
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000003.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := log.ScanAfter(ctx, 2); err == nil {
		t.Fatal("expected corrupt file error")
	}
}

func TestReceiveLogMissingScanEmpty(t *testing.T) {
	recs, err := NewReceiveLog(filepath.Join(t.TempDir(), "missing")).ScanAfter(context.Background(), 0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("recs=%v err=%v", recs, err)
	}
}
