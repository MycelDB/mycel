package embeddingstore

import (
	"context"
	"testing"

	"github.com/google/uuid"
	domainembedding "martinbeauvais.com/mbgit/knotbase/knotdb/domain/embedding"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

func TestStoreAppendReopenAndSearch(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	graphsDir := t.TempDir()
	store, err := Open(graphsDir, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	nodeA := graph.NodeID(uuid.New())
	nodeB := graph.NodeID(uuid.New())
	if _, err := store.Append(ctx, domainembedding.EmbeddingRecord{NodeID: nodeA, ProviderID: "test", ModelID: "m", SourceMode: domainembedding.SourceModeSelf, SourceHash: "a", Vector: []float64{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, domainembedding.EmbeddingRecord{NodeID: nodeB, ProviderID: "test", ModelID: "m", SourceMode: domainembedding.SourceModeSelf, SourceHash: "b", Vector: []float64{0, 1}}); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(graphsDir, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	results, err := reopened.Search(ctx, []float64{1, 0}, "test", "m", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].NodeID != nodeA || results[0].Score <= results[1].Score {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestExistingFindsMatchingSourceHash(t *testing.T) {
	ctx := context.Background()
	store, err := Open(t.TempDir(), domainspace.SpaceID(uuid.New()))
	if err != nil {
		t.Fatal(err)
	}
	nodeID := graph.NodeID(uuid.New())
	if _, err := store.Append(ctx, domainembedding.EmbeddingRecord{NodeID: nodeID, ProviderID: "p", ModelID: "m", SourceMode: domainembedding.SourceModeSubtree, SourceHash: "hash", Vector: []float64{1}}); err != nil {
		t.Fatal(err)
	}
	existing, err := store.Existing(ctx, nodeID, nil, "p", "m", domainembedding.SourceModeSubtree, "hash")
	if err != nil {
		t.Fatal(err)
	}
	if existing == nil {
		t.Fatalf("expected existing record")
	}
}
