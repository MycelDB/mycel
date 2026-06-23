package embeddingstore

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
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

func TestSearchUsesLatestRecordPerNodeProfileAndSourceMode(t *testing.T) {
	ctx := context.Background()
	store, err := Open(t.TempDir(), domainspace.SpaceID(uuid.New()))
	if err != nil {
		t.Fatal(err)
	}
	nodeID := graph.NodeID(uuid.New())
	oldTime := time.Now().UTC().Add(-time.Minute)
	newTime := oldTime.Add(time.Second)
	if _, err := store.Append(ctx, domainembedding.EmbeddingRecord{NodeID: nodeID, ProviderID: "p", ModelID: "m", SourceMode: domainembedding.SourceModeSubtree, SourceHash: "old", Vector: []float64{1, 0}, CreatedAt: oldTime}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, domainembedding.EmbeddingRecord{NodeID: nodeID, ProviderID: "p", ModelID: "m", SourceMode: domainembedding.SourceModeSubtree, SourceHash: "new", Vector: []float64{0, 1}, CreatedAt: newTime}); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(ctx, []float64{1, 0}, "p", "m", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected only latest logical embedding, got %#v", results)
	}
	if results[0].SourceHash != "new" {
		t.Fatalf("expected latest source hash, got %#v", results[0])
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
