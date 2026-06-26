package vectorstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
)

func TestMycelFileVectorBackendUpsertSearchDelete(t *testing.T) {
	ctx := context.Background()
	graphsDir := filepath.Join(t.TempDir(), "graphs")
	backend := MycelFileBackend{GraphsDir: graphsDir}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	node1 := graph.NodeID(uuid.New())
	node2 := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	old, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:old", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatalf("upsert old failed: %v", err)
	}
	latest, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:new", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{0, 1, 0}, CreatedAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatalf("upsert latest failed: %v", err)
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node2, SourceHash: "sha256:two", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{0, 0, 1}, CreatedAt: now.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("upsert second node failed: %v", err)
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:subtree", SourceMode: "subtree", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("upsert subtree source failed: %v", err)
	}

	manifestPath := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "manifest.ksem")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected manifest: %v", err)
	}
	segmentPath := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "records", "embeddings-000001.kvec")
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("expected segment: %v", err)
	}

	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{0, 1, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 3 || results[0].Record.ID != latest.ID || results[0].Record.SourceHash != "sha256:new" {
		t.Fatalf("expected latest node1 first, got %+v (old=%s)", results, old.ID)
	}

	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: latest.ID, Reason: "manual_delete", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, CreatedAt: now.Add(3 * time.Minute)}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	deleted, err := backend.VerifyDeleted(ctx, VerifyDeletedInput{SpaceID: spaceID, SemanticIndexID: indexID, NodeID: node1, SourceMode: "self", TargetRecordID: latest.ID})
	if err != nil || !deleted {
		t.Fatalf("expected verified deleted, deleted=%v err=%v", deleted, err)
	}
	results, err = backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{0, 0, 1}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search after delete failed: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != node2 {
		t.Fatalf("expected tombstoned self source excluded, got %+v", results)
	}
	results, err = backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search subtree after delete failed: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != node1 || results[0].Record.SourceMode != "subtree" {
		t.Fatalf("expected subtree source to remain searchable, got %+v", results)
	}
}

func TestMycelFileVectorBackendValidation(t *testing.T) {
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	_, err := backend.Upsert(context.Background(), domainsemantic.AdvancedEmbeddingRecord{SpaceID: uuid.New(), DomainID: uuid.New(), SemanticIndexID: uuid.New(), NodeID: uuid.New(), ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Dimensions: 3, Vector: []float64{1}})
	if err == nil {
		t.Fatal("expected dimension validation error")
	}
}
