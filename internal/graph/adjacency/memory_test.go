package adjacency

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestMemoryEdgeIndexRebuildIncomingOutgoing(t *testing.T) {
	ctx := context.Background()
	fromA := nodeID("from-a")
	fromB := nodeID("from-b")
	toA := nodeID("to-a")
	idx := NewMemoryEdgeIndex()
	edgeA := edgeID("edge-a")
	edgeB := edgeID("edge-b")
	if err := idx.Rebuild(ctx, []graph.Edge{{ID: edgeB, FromID: fromB, ToID: toA}, {ID: edgeA, FromID: fromA, ToID: toA}}); err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}
	incoming, err := idx.Incoming(ctx, toA)
	if err != nil {
		t.Fatalf("Incoming() error = %v", err)
	}
	if want := []graph.EdgeID{edgeA, edgeB}; !reflect.DeepEqual(incoming, want) {
		t.Fatalf("Incoming() = %v, want %v", incoming, want)
	}
	outgoing, err := idx.Outgoing(ctx, fromA)
	if err != nil {
		t.Fatalf("Outgoing() error = %v", err)
	}
	if want := []graph.EdgeID{edgeA}; !reflect.DeepEqual(outgoing, want) {
		t.Fatalf("Outgoing() = %v, want %v", outgoing, want)
	}
}

func TestMemoryEdgeIndexPutReplacesEndpoints(t *testing.T) {
	ctx := context.Background()
	idx := NewMemoryEdgeIndex()
	id := edgeID("edge")
	oldFrom := nodeID("old-from")
	oldTo := nodeID("old-to")
	newFrom := nodeID("new-from")
	newTo := nodeID("new-to")
	if err := idx.Put(ctx, graph.Edge{ID: id, FromID: oldFrom, ToID: oldTo}); err != nil {
		t.Fatalf("Put(old) error = %v", err)
	}
	if err := idx.Put(ctx, graph.Edge{ID: id, FromID: newFrom, ToID: newTo}); err != nil {
		t.Fatalf("Put(new) error = %v", err)
	}
	if got, _ := idx.Outgoing(ctx, oldFrom); len(got) != 0 {
		t.Fatalf("old outgoing = %v, want empty", got)
	}
	if got, _ := idx.Incoming(ctx, oldTo); len(got) != 0 {
		t.Fatalf("old incoming = %v, want empty", got)
	}
	if got, _ := idx.Outgoing(ctx, newFrom); !reflect.DeepEqual(got, []graph.EdgeID{id}) {
		t.Fatalf("new outgoing = %v, want [%s]", got, id)
	}
	if got, _ := idx.Incoming(ctx, newTo); !reflect.DeepEqual(got, []graph.EdgeID{id}) {
		t.Fatalf("new incoming = %v, want [%s]", got, id)
	}
}

func TestMemoryEdgeIndexDelete(t *testing.T) {
	ctx := context.Background()
	idx := NewMemoryEdgeIndex()
	id := edgeID("edge")
	from := nodeID("from")
	to := nodeID("to")
	edge := graph.Edge{ID: id, FromID: from, ToID: to}
	if err := idx.Put(ctx, edge); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := idx.Delete(ctx, edge); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := idx.Delete(ctx, edge); err != nil {
		t.Fatalf("Delete(absent) error = %v", err)
	}
	if got, _ := idx.Outgoing(ctx, from); len(got) != 0 {
		t.Fatalf("outgoing after delete = %v, want empty", got)
	}
	if got, _ := idx.Incoming(ctx, to); len(got) != 0 {
		t.Fatalf("incoming after delete = %v, want empty", got)
	}
}

func TestMemoryEdgeIndexReturnedSlicesAreDefensive(t *testing.T) {
	ctx := context.Background()
	idx := NewMemoryEdgeIndex()
	id := edgeID("edge")
	from := nodeID("from")
	to := nodeID("to")
	if err := idx.Put(ctx, graph.Edge{ID: id, FromID: from, ToID: to}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := idx.Outgoing(ctx, from)
	if err != nil {
		t.Fatalf("Outgoing() error = %v", err)
	}
	got[0] = edgeID("other")
	again, err := idx.Outgoing(ctx, from)
	if err != nil {
		t.Fatalf("Outgoing(again) error = %v", err)
	}
	if !reflect.DeepEqual(again, []graph.EdgeID{id}) {
		t.Fatalf("Outgoing after caller mutation = %v, want [%s]", again, id)
	}
}

func nodeID(name string) graph.NodeID {
	return graph.NodeID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("adjacency-test/node/"+name)))
}

func edgeID(name string) graph.EdgeID {
	return graph.EdgeID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("adjacency-test/edge/"+name)))
}
