package graphstorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
)

func TestLocalStoreTransactionsAndIndexRebuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	tmpl := graph.TemplateID(uuid.New())
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := createdAt.Add(time.Hour)
	parent := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &tmpl, Content: "parent", Props: map[string]any{"journal_day": 20260102}, CreatedAt: createdAt, UpdatedAt: updatedAt}
	child := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &tmpl, Content: "child", Props: map[string]any{}}
	edge := graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: parent.ID, ToID: child.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 0}}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.PutNode(parent); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutNode(child); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutEdge(edge); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer store.Close()
	got, err := store.GetNode(ctx, parent.ID)
	if err != nil || got.Content != "parent" {
		t.Fatalf("unexpected node=%+v err=%v", got, err)
	}
	if !got.CreatedAt.Equal(createdAt) || !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("timestamps did not round trip: got created_at=%s updated_at=%s", got.CreatedAt, got.UpdatedAt)
	}
	children, err := store.Children(ctx, parent.ID)
	if err != nil || len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("unexpected children=%+v err=%v", children, err)
	}
	ids, err := store.NodesByTemplate(ctx, tmpl)
	if err != nil || len(ids) != 2 {
		t.Fatalf("unexpected template ids=%+v err=%v", ids, err)
	}
	journalIDs, err := store.JournalNodesByDayRange(ctx, 20260101, 20260107)
	if err != nil || len(journalIDs) != 1 || journalIDs[0] != parent.ID {
		t.Fatalf("unexpected journal ids=%+v err=%v", journalIDs, err)
	}
}

func TestLocalStoreIgnoresUncommittedRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "uncommitted", Props: map[string]any{}}
	txnID := uuid.New()
	payload, err := encodeNode(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.txns.appendRecord(RecordKindTxnBegin, txnID, uuid.Nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.nodes.appendRecord(RecordKindNodePut, txnID, node.ID, payload); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.GetNode(ctx, node.ID); err != ErrNotFound {
		t.Fatalf("expected uncommitted record ignored, got %v", err)
	}
}

func TestScanSegmentRejectsCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(filepath.Join(dir, "nodes.kseg"), SegmentKindNode)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	loc, err := seg.appendRecord(RecordKindNodePut, uuid.New(), id, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "nodes.kseg"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, loc.Offset+recordHeaderLen); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := scanSegment(filepath.Join(dir, "nodes.kseg"), SegmentKindNode, func(scannedRecord) error { return nil }); err == nil {
		t.Fatal("expected crc error")
	}
}

func TestLocalStoreRevisionAndConflict(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	if got := store.Revision(); got != 0 {
		t.Fatalf("initial revision = %d, want 0", got)
	}
	tx1, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1 failed: %v", err)
	}
	tx1.ExpectRevision(store.Revision())
	node1 := graph.Node{ID: graph.NodeID(uuid.New()), Content: "one", Props: map[string]any{}}
	if err := tx1.PutNode(node1); err != nil {
		t.Fatalf("put node1 failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1 failed: %v", err)
	}
	if got := store.Revision(); got != 1 {
		t.Fatalf("revision after tx1 = %d, want 1", got)
	}
	tx2, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2 failed: %v", err)
	}
	tx2.ExpectRevision(0)
	if err := tx2.PutNode(graph.Node{ID: graph.NodeID(uuid.New()), Content: "two", Props: map[string]any{}}); err != nil {
		t.Fatalf("put node2 failed: %v", err)
	}
	if err := tx2.Commit(); err != ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestLocalStoreRevisionRebuildsFromCommittedTransactions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d failed: %v", i, err)
		}
		if err := tx.PutNode(graph.Node{ID: graph.NodeID(uuid.New()), Content: "node", Props: map[string]any{}}); err != nil {
			t.Fatalf("put %d failed: %v", i, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer store.Close()
	if got := store.Revision(); got != 2 {
		t.Fatalf("rebuilt revision = %d, want 2", got)
	}
}
