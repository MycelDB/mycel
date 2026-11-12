package service

import (
	"testing"

	"github.com/google/uuid"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/projection"
)

func TestGraphQueryIndexProjectionIdentityUsesStableName(t *testing.T) {
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	first, scope, key, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "title")
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, _, secondKey, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "title")
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first != second || key != secondKey || key != "query-index:title" {
		t.Fatalf("identity changed: %q/%q/%q", first, second, key)
	}
	other, _, _, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "other")
	if err != nil {
		t.Fatalf("other identity: %v", err)
	}
	if first == other {
		t.Fatal("different index names collided")
	}
	if scope.SpaceID != spaceID || scope.DomainID != domainID {
		t.Fatalf("scope = %+v", scope)
	}
	base := GraphPersistentQueryIndexStatus{SchemaHash: "schema-1", DefinitionFingerprint: "definition-1", KeyEncodingVersion: 1}
	changed := base
	changed.SchemaHash = "schema-2"
	if graphQueryIndexConfigurationHash(base) == graphQueryIndexConfigurationHash(changed) {
		t.Fatal("schema changes did not change query-index configuration hash")
	}
}

func TestGraphProjectionArtifactManifestsTrackValidatedCheckpointState(t *testing.T) {
	store, err := graphstorage.Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	scope := projection.Scope{SpaceID: spaceID, DomainID: domainID}
	checkpointID, _, err := graphCheckpointProjectionIdentity(spaceID, domainID)
	if err != nil {
		t.Fatalf("checkpoint identity: %v", err)
	}
	status := GraphCheckpointStatus{CheckpointPresent: true, CheckpointRevision: 7, GraphChecksum: "abc123", PersistentIndex: GraphPersistentIndexStatus{IndexSetID: "set-1"}}
	module := &Module{}
	if err := module.persistGraphCheckpointArtifactManifest(t.Context(), store, checkpointID, scope, graphCheckpointConfigurationHash(), 4, status); err != nil {
		t.Fatalf("persist checkpoint manifest: %v", err)
	}
	queryID, queryScope, _, err := graphQueryIndexProjectionIdentity(spaceID, domainID, "title")
	if err != nil {
		t.Fatalf("query identity: %v", err)
	}
	query := GraphPersistentQueryIndexStatus{Identity: "domain:title", Name: "title", SchemaHash: "schema", DefinitionFingerprint: "definition", BuildState: "ready", LastIndexedGraphRevision: 7, KeyEncodingVersion: 1, EntryCount: 3}
	if err := module.persistGraphQueryIndexArtifactManifest(t.Context(), store, queryID, queryScope, graphQueryIndexConfigurationHash(query), 2, 4, "set-1", query); err != nil {
		t.Fatalf("persist query manifest: %v", err)
	}
	manifests, err := store.ProjectionArtifactManifests(t.Context())
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("manifest count = %d, want 2", len(manifests))
	}
	status.CheckpointPresent = false
	if err := module.persistGraphCheckpointArtifactManifest(t.Context(), store, checkpointID, scope, graphCheckpointConfigurationHash(), 4, status); err != nil {
		t.Fatalf("remove checkpoint manifest: %v", err)
	}
	manifests, err = store.ProjectionArtifactManifests(t.Context())
	if err != nil {
		t.Fatalf("list manifests after removal: %v", err)
	}
	if len(manifests) != 1 || manifests[0].ProjectionID != queryID {
		t.Fatalf("manifests after removal = %+v", manifests)
	}
}

func TestGraphCheckpointProjectionIdentityIsStableAndScoped(t *testing.T) {
	spaceID := uuid.NewString()
	firstDomain := uuid.NewString()
	secondDomain := uuid.NewString()
	first, firstScope, err := graphCheckpointProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, secondScope, err := graphCheckpointProjectionIdentity(spaceID, firstDomain)
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first != second || firstScope != secondScope {
		t.Fatalf("identity changed across calls: %q/%+v vs %q/%+v", first, firstScope, second, secondScope)
	}
	other, _, err := graphCheckpointProjectionIdentity(spaceID, secondDomain)
	if err != nil {
		t.Fatalf("second domain identity: %v", err)
	}
	if first == other {
		t.Fatalf("domain identities collided: %q", first)
	}
	if got := firstScope; got.SpaceID != spaceID || got.DomainID != firstDomain {
		t.Fatalf("scope = %+v", got)
	}
}
