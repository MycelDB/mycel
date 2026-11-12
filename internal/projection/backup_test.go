package projection

import (
	"context"
	"strings"
	"testing"
)

func TestBackupCatalogCapturesDeterministicRegistryEvidence(t *testing.T) {
	manager := NewRegistryManager(2)
	registry := manager.NewRaftStateMachine(1).Registry
	definition := Definition{ID: ProjectionID("prj_test"), Kind: KindMetadata, Scope: Scope{SpaceID: "space", DomainID: "domain"}, Key: "checkpoint", ConfigurationHash: "sha256:config", ConfigurationEpoch: 1, MaterializerVersion: "test-v1"}
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	if err := registry.ApplyRecord(context.Background(), NewDesiredFrontierRecord(DesiredFrontier{ProjectionID: definition.ID, Required: Frontier{Revision: 4}, ConfigurationEpoch: 1})); err != nil {
		t.Fatalf("apply frontier: %v", err)
	}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(Checkpoint{ProjectionID: definition.ID, Published: Frontier{Revision: 4}, ConfigurationEpoch: 1, ConfigurationHash: "sha256:config", MaterializerVersion: "test-v1", FencingEpoch: 3, ArtifactGeneration: "artifact-1", State: StateReady})); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	catalog := manager.BackupCatalog()
	if err := catalog.Validate(); err != nil {
		t.Fatalf("validate catalog: %v", err)
	}
	if len(catalog.Partitions) != 1 || len(catalog.Partitions[0].Projections) != 1 {
		t.Fatalf("unexpected catalog: %+v", catalog)
	}
	evidence := catalog.Partitions[0].Projections[0]
	if evidence.ProjectionID != definition.ID || evidence.Kind != definition.Kind || evidence.Scope != definition.Scope || evidence.ConfigurationHash != definition.ConfigurationHash || evidence.MaterializerVersion != definition.MaterializerVersion || evidence.RequiredRevision != 4 || evidence.PublishedRevision != 4 || evidence.FencingEpoch != 3 || evidence.ArtifactGeneration != "artifact-1" {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
	first := catalog.Partitions[0].SnapshotHash
	second := manager.BackupCatalog().Partitions[0].SnapshotHash
	if first != second || !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("snapshot hash is not deterministic: %q/%q", first, second)
	}
}

func TestBackupCatalogRequiresArtifactCheckpointAgreement(t *testing.T) {
	catalog := BackupCatalog{Version: BackupCatalogVersion, PartitionCount: 1, Partitions: []BackupPartitionCatalog{{PartitionID: 0, SnapshotHash: "sha256:" + strings.Repeat("0", 64), Projections: []BackupProjectionCatalog{{ProjectionID: "prj_one", Kind: KindMetadata, Scope: Scope{SpaceID: "space", DomainID: "domain"}, ConfigurationHash: "sha256:config", MaterializerVersion: "test-v1", ConfigurationEpoch: 2, PublishedRevision: 7, FencingEpoch: 3, ArtifactGeneration: "generation-1"}}, Artifacts: []ArtifactManifest{{ProjectionID: "prj_one", Kind: KindMetadata, Scope: Scope{SpaceID: "space", DomainID: "domain"}, ConfigurationHash: "sha256:config", ConfigurationEpoch: 2, MaterializerVersion: "test-v1", Source: Frontier{Revision: 7}, FencingEpoch: 3, Generation: "generation-1", Format: "test-v1", Checksum: "sha256:artifact"}}}}}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("matching artifact rejected: %v", err)
	}
	catalog.Partitions[0].Artifacts[0].Generation = "stale-generation"
	if err := catalog.Validate(); err == nil {
		t.Fatal("artifact with mismatched checkpoint generation accepted")
	}
}

func TestBackupCatalogRejectsDuplicatePartitionsAndProjections(t *testing.T) {
	base := BackupCatalog{Version: BackupCatalogVersion, PartitionCount: 1, Partitions: []BackupPartitionCatalog{{PartitionID: 0, SnapshotHash: "sha256:" + strings.Repeat("0", 64), Projections: []BackupProjectionCatalog{{ProjectionID: "prj_one"}}}}}
	duplicatePartition := base
	duplicatePartition.Partitions = append(append([]BackupPartitionCatalog(nil), base.Partitions...), base.Partitions[0])
	if err := duplicatePartition.Validate(); err == nil {
		t.Fatal("duplicate partition accepted")
	}
	duplicateProjection := base
	duplicateProjection.Partitions = []BackupPartitionCatalog{{PartitionID: 0, SnapshotHash: "sha256:" + strings.Repeat("0", 64), Projections: []BackupProjectionCatalog{{ProjectionID: "prj_one"}, {ProjectionID: "prj_one"}}}}
	if err := duplicateProjection.Validate(); err == nil {
		t.Fatal("duplicate projection accepted")
	}
}
