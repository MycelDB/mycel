package graphstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestRevisionFenceRejectsChangedDomainAtomically(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	domainID := uuid.New()
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.PutNode(graph.Node{ID: uuid.New(), DomainID: domainID, Content: "before"}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	observed := store.Revision()

	mutate, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := mutate.PutNode(graph.Node{ID: uuid.New(), DomainID: domainID, Content: "changed"}); err != nil {
		t.Fatal(err)
	}
	if err := mutate.Commit(); err != nil {
		t.Fatal(err)
	}

	stale, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale.ExpectRevisionFence(RevisionFence{Kind: "domain", ID: domainID, Revision: observed})
	if err := stale.PutNode(graph.Node{ID: uuid.New(), DomainID: domainID, Content: "stale"}); err != nil {
		t.Fatal(err)
	}
	if err := stale.Commit(); !errors.Is(err, ErrRevisionFenceConflict) {
		t.Fatalf("stale commit error = %v, want ErrRevisionFenceConflict", err)
	}
}

func TestRevisionFenceRejectsChangedEntityAtomically(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	domainID := uuid.New()
	nodeID := uuid.New()
	seed, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.PutNode(graph.Node{ID: nodeID, DomainID: domainID, Content: "before"}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	observed := store.NodeRevision(nodeID)

	mutate, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mutate.ExpectRevision(store.Revision())
	if err := mutate.PutNode(graph.Node{ID: nodeID, DomainID: domainID, Content: "changed"}); err != nil {
		t.Fatal(err)
	}
	if err := mutate.Commit(); err != nil {
		t.Fatal(err)
	}

	stale, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale.ExpectRevisionFence(RevisionFence{Kind: "node", ID: nodeID, Revision: observed})
	if err := stale.PutNode(graph.Node{ID: nodeID, DomainID: domainID, Content: "stale"}); err != nil {
		t.Fatal(err)
	}
	if err := stale.Commit(); !errors.Is(err, ErrRevisionFenceConflict) {
		t.Fatalf("stale commit error = %v, want ErrRevisionFenceConflict", err)
	}
	got, err := store.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "changed" {
		t.Fatalf("node content = %q, want changed", got.Content)
	}
}
