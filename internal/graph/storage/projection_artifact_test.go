package graphstorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/projection"
)

func TestProjectionArtifactManifestRoundTripAndCorruptionFailsClosed(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id, err := projection.IDFor(projection.KindMetadata, projection.Scope{SpaceID: "space", DomainID: "domain"}, "graph-checkpoint")
	if err != nil {
		t.Fatalf("IDFor() error = %v", err)
	}
	manifest := projection.ArtifactManifest{ProjectionID: id, Kind: projection.KindMetadata, Scope: projection.Scope{SpaceID: "space", DomainID: "domain"}, ConfigurationHash: "sha256:config", ConfigurationEpoch: 1, MaterializerVersion: "graph-checkpoint-projection-v1", Source: projection.Frontier{Revision: 7}, FencingEpoch: 3, Generation: "gchk_1", Format: "graph-checkpoint-v1", Checksum: "sha256:artifact"}
	if err := store.WriteProjectionArtifactManifest(ctx, manifest); err != nil {
		t.Fatalf("WriteProjectionArtifactManifest() error = %v", err)
	}
	got, err := store.ProjectionArtifactManifests(ctx)
	if err != nil {
		t.Fatalf("ProjectionArtifactManifests() error = %v", err)
	}
	if len(got) != 1 || got[0] != manifest {
		t.Fatalf("ProjectionArtifactManifests() = %+v, want %+v", got, manifest)
	}

	entries, err := os.ReadDir(filepath.Join(store.path, graphProjectionArtifactManifestDir))
	if err != nil || len(entries) != 1 {
		t.Fatalf("read artifact manifest directory: entries=%d err=%v", len(entries), err)
	}
	path := filepath.Join(store.path, graphProjectionArtifactManifestDir, entries[0].Name())
	if err := os.WriteFile(path, []byte(`{"projection_id":`), 0o600); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	if _, err := store.ProjectionArtifactManifests(ctx); err == nil {
		t.Fatal("ProjectionArtifactManifests() accepted corrupt manifest")
	}
	if err := store.RemoveProjectionArtifactManifest(ctx, id); err != nil {
		t.Fatalf("RemoveProjectionArtifactManifest() error = %v", err)
	}
}
