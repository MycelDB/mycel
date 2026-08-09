package graphstorage

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

func TestLocalStoreTransactionsAndIndexRebuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := createdAt.Add(time.Hour)
	parent := graph.Node{ID: graph.NodeID(uuid.New()), Content: "parent", Props: map[string]any{"journal_day": 20260102}, CreatedAt: createdAt, UpdatedAt: updatedAt}
	child := graph.Node{ID: graph.NodeID(uuid.New()), Content: "child", Props: map[string]any{}}
	edgeCreatedAt := createdAt.Add(2 * time.Hour)
	edgeUpdatedAt := edgeCreatedAt.Add(time.Hour)
	edge := graph.Edge{ID: graph.EdgeID(uuid.New()), DomainID: graph.DomainID(uuid.New()), FromID: parent.ID, ToID: child.ID, Labels: []string{"contains", "primary"}, Properties: map[string]any{"order": int64(0), "source": "test"}, Payload: map[string]any{"text": "edge payload"}, Meta: map[string]any{"system": "store-test"}, CreatedAt: edgeCreatedAt, UpdatedAt: edgeUpdatedAt}
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
	gotEdge := children[0]
	if gotEdge.DomainID != edge.DomainID || !reflect.DeepEqual(gotEdge.Labels, edge.Labels) || !reflect.DeepEqual(gotEdge.Properties, edge.Properties) || !reflect.DeepEqual(gotEdge.Payload, edge.Payload) || !reflect.DeepEqual(gotEdge.Meta, edge.Meta) || !gotEdge.CreatedAt.Equal(edgeCreatedAt) || !gotEdge.UpdatedAt.Equal(edgeUpdatedAt) {
		t.Fatalf("edge did not round trip through store: got %+v want %+v", gotEdge, edge)
	}
	journalIDs, err := store.JournalNodesByDayRange(ctx, 20260101, 20260107)
	if err != nil || len(journalIDs) != 1 || journalIDs[0] != parent.ID {
		t.Fatalf("unexpected journal ids=%+v err=%v", journalIDs, err)
	}
}

func TestLocalStoreIncomingOutgoingEdgesSurviveReopenAndReplacement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	fromA := graph.Node{ID: graph.NodeID(uuid.New()), Content: "from-a"}
	fromB := graph.Node{ID: graph.NodeID(uuid.New()), Content: "from-b"}
	toA := graph.Node{ID: graph.NodeID(uuid.New()), Content: "to-a"}
	toB := graph.Node{ID: graph.NodeID(uuid.New()), Content: "to-b"}
	edgeID := graph.EdgeID(uuid.New())
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed failed: %v", err)
	}
	for _, node := range []graph.Node{fromA, fromB, toA, toB} {
		if err := seed.PutNode(node); err != nil {
			t.Fatalf("put seed node failed: %v", err)
		}
	}
	if err := seed.PutEdge(graph.Edge{ID: edgeID, FromID: fromA.ID, ToID: toA.ID, Labels: []string{"references"}, Properties: map[string]any{"phase": "seed"}}); err != nil {
		t.Fatalf("put seed edge failed: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}
	assertEndpointEdges(t, ctx, store, fromA.ID, toA.ID, edgeID)

	move, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin move failed: %v", err)
	}
	move.ExpectRevision(store.Revision())
	if err := move.PutEdge(graph.Edge{ID: edgeID, FromID: fromB.ID, ToID: toB.ID, Labels: []string{"references"}, Properties: map[string]any{"phase": "moved"}}); err != nil {
		t.Fatalf("move edge failed: %v", err)
	}
	if err := move.Commit(); err != nil {
		t.Fatalf("move commit failed: %v", err)
	}
	assertNoEndpointEdges(t, ctx, store, fromA.ID, toA.ID)
	assertEndpointEdges(t, ctx, store, fromB.ID, toB.ID, edgeID)

	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer store.Close()
	assertNoEndpointEdges(t, ctx, store, fromA.ID, toA.ID)
	assertEndpointEdges(t, ctx, store, fromB.ID, toB.ID, edgeID)

	incoming, err := store.IncomingEdges(ctx, toB.ID)
	if err != nil {
		t.Fatalf("IncomingEdges() error = %v", err)
	}
	incoming[0].Properties["phase"] = "mutated"
	got, err := store.GetEdge(ctx, edgeID)
	if err != nil {
		t.Fatalf("GetEdge() error = %v", err)
	}
	if got.Properties["phase"] != "moved" {
		t.Fatalf("IncomingEdges returned mutable edge; stored phase=%v", got.Properties["phase"])
	}
}

func TestLocalStoreIncomingOutgoingEdgesRemoveDeletedEdges(t *testing.T) {
	ctx := context.Background()
	store, parent, child := newStoreWithParentChild(t, ctx)
	defer store.Close()
	edgeID := graph.EdgeID(uuid.New())
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed failed: %v", err)
	}
	seed.ExpectRevision(store.Revision())
	if err := seed.PutEdge(graph.Edge{ID: edgeID, FromID: parent.ID, ToID: child.ID, Labels: []string{"references"}}); err != nil {
		t.Fatalf("put edge failed: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}
	assertEndpointEdges(t, ctx, store, parent.ID, child.ID, edgeID)
	deleteTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin delete failed: %v", err)
	}
	deleteTx.ExpectRevision(store.Revision())
	if err := deleteTx.DeleteEdge(edgeID); err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("delete commit failed: %v", err)
	}
	assertNoEndpointEdges(t, ctx, store, parent.ID, child.ID)
}

func assertEndpointEdges(t *testing.T, ctx context.Context, store *LocalStore, from graph.NodeID, to graph.NodeID, edgeID graph.EdgeID) {
	t.Helper()
	outgoing, err := store.OutgoingEdges(ctx, from)
	if err != nil {
		t.Fatalf("OutgoingEdges() error = %v", err)
	}
	if len(outgoing) != 1 || outgoing[0].ID != edgeID || outgoing[0].FromID != from || outgoing[0].ToID != to {
		t.Fatalf("OutgoingEdges() = %+v, want edge %s %s->%s", outgoing, edgeID, from, to)
	}
	incoming, err := store.IncomingEdges(ctx, to)
	if err != nil {
		t.Fatalf("IncomingEdges() error = %v", err)
	}
	if len(incoming) != 1 || incoming[0].ID != edgeID || incoming[0].FromID != from || incoming[0].ToID != to {
		t.Fatalf("IncomingEdges() = %+v, want edge %s %s->%s", incoming, edgeID, from, to)
	}
}

func assertNoEndpointEdges(t *testing.T, ctx context.Context, store *LocalStore, from graph.NodeID, to graph.NodeID) {
	t.Helper()
	outgoing, err := store.OutgoingEdges(ctx, from)
	if err != nil {
		t.Fatalf("OutgoingEdges() error = %v", err)
	}
	for _, edge := range outgoing {
		if edge.ToID == to {
			t.Fatalf("OutgoingEdges(%s) still contains edge to %s: %+v", from, to, outgoing)
		}
	}
	incoming, err := store.IncomingEdges(ctx, to)
	if err != nil {
		t.Fatalf("IncomingEdges() error = %v", err)
	}
	for _, edge := range incoming {
		if edge.FromID == from {
			t.Fatalf("IncomingEdges(%s) still contains edge from %s: %+v", to, from, incoming)
		}
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
	// A concurrent transaction with a stale base revision that writes a DISJOINT
	// (brand-new) node must NOT conflict: conflict detection is per write-set,
	// not whole-graph.
	tx2, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2 failed: %v", err)
	}
	tx2.ExpectRevision(0)
	if err := tx2.PutNode(graph.Node{ID: graph.NodeID(uuid.New()), Content: "two", Props: map[string]any{}}); err != nil {
		t.Fatalf("put node2 failed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("expected disjoint write to succeed, got %v", err)
	}
	if got := store.Revision(); got != 2 {
		t.Fatalf("revision after tx2 = %d, want 2", got)
	}

	// A transaction with a stale base revision that writes an OVERLAPPING entity
	// (node1, last modified at revision 1) must still conflict.
	tx3, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx3 failed: %v", err)
	}
	tx3.ExpectRevision(0)
	if err := tx3.PutNode(graph.Node{ID: node1.ID, Content: "one-updated", Props: map[string]any{}}); err != nil {
		t.Fatalf("put node1 update failed: %v", err)
	}
	if err := tx3.Commit(); err != ErrConflict {
		t.Fatalf("expected ErrConflict on overlapping write, got %v", err)
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

func TestLocalStoreWriteSetConflictSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "one", Props: map[string]any{}}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	tx.ExpectRevision(0)
	if err := tx.PutNode(node); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer store.Close()

	disjoint, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin disjoint failed: %v", err)
	}
	disjoint.ExpectRevision(0)
	if err := disjoint.PutNode(graph.Node{ID: graph.NodeID(uuid.New()), Content: "two", Props: map[string]any{}}); err != nil {
		t.Fatalf("put disjoint failed: %v", err)
	}
	if err := disjoint.Commit(); err != nil {
		t.Fatalf("stale disjoint write after reopen should succeed, got %v", err)
	}

	overlap, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin overlap failed: %v", err)
	}
	overlap.ExpectRevision(0)
	node.Content = "updated"
	if err := overlap.PutNode(node); err != nil {
		t.Fatalf("put overlap failed: %v", err)
	}
	if err := overlap.Commit(); err != ErrConflict {
		t.Fatalf("stale overlapping write after reopen = %v, want ErrConflict", err)
	}
}

func TestLocalStoreRejectsEdgeToConcurrentlyDeletedEndpoint(t *testing.T) {
	ctx := context.Background()
	store, parent, child := newStoreWithParentChild(t, ctx)
	defer store.Close()
	base := store.Revision()

	edgeTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin edge tx failed: %v", err)
	}
	edgeTx.ExpectRevision(base)
	edge := graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: parent.ID, ToID: child.ID, Labels: []string{"references"}, Properties: map[string]any{}}
	if err := edgeTx.PutEdge(edge); err != nil {
		t.Fatalf("put edge failed: %v", err)
	}
	deleteTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin delete tx failed: %v", err)
	}
	deleteTx.ExpectRevision(base)
	if err := deleteTx.DeleteNode(child.ID); err != nil {
		t.Fatalf("delete child failed: %v", err)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("delete commit failed: %v", err)
	}
	if err := edgeTx.Commit(); err != ErrConflict {
		t.Fatalf("edge commit after endpoint delete = %v, want ErrConflict", err)
	}
}

func TestLocalStoreRejectsNodeDeleteAfterConcurrentEdgeAdd(t *testing.T) {
	ctx := context.Background()
	store, parent, child := newStoreWithParentChild(t, ctx)
	defer store.Close()
	base := store.Revision()

	deleteTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin delete tx failed: %v", err)
	}
	deleteTx.ExpectRevision(base)
	if err := deleteTx.DeleteNode(child.ID); err != nil {
		t.Fatalf("delete child failed: %v", err)
	}
	edgeTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin edge tx failed: %v", err)
	}
	edgeTx.ExpectRevision(base)
	if err := edgeTx.PutEdge(graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: parent.ID, ToID: child.ID, Labels: []string{"references"}, Properties: map[string]any{}}); err != nil {
		t.Fatalf("put edge failed: %v", err)
	}
	if err := edgeTx.Commit(); err != nil {
		t.Fatalf("edge commit failed: %v", err)
	}
	if err := deleteTx.Commit(); err != ErrConflict {
		t.Fatalf("delete after concurrent edge add = %v, want ErrConflict", err)
	}
}

func TestLocalStoreRejectsConcurrentContainsParents(t *testing.T) {
	ctx := context.Background()
	store, parent, child := newStoreWithParentChild(t, ctx)
	defer store.Close()
	secondParent := graph.Node{ID: graph.NodeID(uuid.New()), Content: "parent-two", Props: map[string]any{}}
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed failed: %v", err)
	}
	seed.ExpectRevision(store.Revision())
	if err := seed.PutNode(secondParent); err != nil {
		t.Fatalf("put second parent failed: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}
	base := store.Revision()

	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first failed: %v", err)
	}
	first.ExpectRevision(base)
	if err := first.PutEdge(graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: parent.ID, ToID: child.ID, Labels: []string{"contains"}, Properties: map[string]any{}}); err != nil {
		t.Fatalf("put first contains failed: %v", err)
	}
	second, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin second failed: %v", err)
	}
	second.ExpectRevision(base)
	if err := second.PutEdge(graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: secondParent.ID, ToID: child.ID, Labels: []string{"contains"}, Properties: map[string]any{}}); err != nil {
		t.Fatalf("put second contains failed: %v", err)
	}
	if err := first.Commit(); err != nil {
		t.Fatalf("first commit failed: %v", err)
	}
	if err := second.Commit(); err != ErrConflict {
		t.Fatalf("second contains parent commit = %v, want ErrConflict", err)
	}
}

func newStoreWithParentChild(t *testing.T, ctx context.Context) (*LocalStore, graph.Node, graph.Node) {
	t.Helper()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	parent := graph.Node{ID: graph.NodeID(uuid.New()), Content: "parent", Props: map[string]any{}}
	child := graph.Node{ID: graph.NodeID(uuid.New()), Content: "child", Props: map[string]any{}}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed failed: %v", err)
	}
	tx.ExpectRevision(0)
	if err := tx.PutNode(parent); err != nil {
		t.Fatalf("put parent failed: %v", err)
	}
	if err := tx.PutNode(child); err != nil {
		t.Fatalf("put child failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}
	return store, parent, child
}

func TestLocalStoreRejectsFutureExpectedRevision(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	tx.ExpectRevision(store.Revision() + 1)
	if err := tx.PutNode(graph.Node{ID: graph.NodeID(uuid.New()), Content: "future", Props: map[string]any{}}); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := tx.Commit(); err != ErrConflict {
		t.Fatalf("future expected revision commit = %v, want ErrConflict", err)
	}
}

func TestLocalStoreAllowsEdgeMoveAwayBeforeNodeDelete(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()
	oldParent := graph.Node{ID: graph.NodeID(uuid.New()), Content: "old-parent", Props: map[string]any{}}
	newParent := graph.Node{ID: graph.NodeID(uuid.New()), Content: "new-parent", Props: map[string]any{}}
	child := graph.Node{ID: graph.NodeID(uuid.New()), Content: "child", Props: map[string]any{}}
	edgeID := graph.EdgeID(uuid.New())
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed failed: %v", err)
	}
	seed.ExpectRevision(0)
	for _, node := range []graph.Node{oldParent, newParent, child} {
		if err := seed.PutNode(node); err != nil {
			t.Fatalf("put seed node failed: %v", err)
		}
	}
	if err := seed.PutEdge(graph.Edge{ID: edgeID, FromID: oldParent.ID, ToID: child.ID, Labels: []string{"contains"}, Properties: map[string]any{}}); err != nil {
		t.Fatalf("put seed edge failed: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin move/delete failed: %v", err)
	}
	tx.ExpectRevision(store.Revision())
	if err := tx.PutEdge(graph.Edge{ID: edgeID, FromID: newParent.ID, ToID: child.ID, Labels: []string{"contains"}, Properties: map[string]any{}}); err != nil {
		t.Fatalf("move edge failed: %v", err)
	}
	if err := tx.DeleteNode(oldParent.ID); err != nil {
		t.Fatalf("delete old parent failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("edge move away before node delete should commit, got %v", err)
	}
	if _, err := store.GetNode(ctx, oldParent.ID); err != ErrNotFound {
		t.Fatalf("old parent lookup = %v, want ErrNotFound", err)
	}
	moved, err := store.GetEdge(ctx, edgeID)
	if err != nil {
		t.Fatalf("moved edge lookup failed: %v", err)
	}
	if moved.FromID != newParent.ID || moved.ToID != child.ID {
		t.Fatalf("moved edge = %#v", moved)
	}
}
