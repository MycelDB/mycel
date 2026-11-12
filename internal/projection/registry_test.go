package projection

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func testDefinition(id ProjectionID) Definition {
	return Definition{ID: id, Kind: KindSemantic, Scope: Scope{SpaceID: "space-a", DomainID: "domain-a"}, Key: "rules", ConfigurationHash: "config-v1", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
}

func TestRegistryApplyIsIdempotentAndHashFenced(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("prj_rules")
	if err := registry.Apply(context.Background(), "command-1", []byte("hash-1"), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	if err := registry.Apply(context.Background(), "command-1", []byte("hash-1"), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("replay same command: %v", err)
	}
	if err := registry.Apply(context.Background(), "command-1", []byte("hash-2"), NewDefinitionRecord(definition)); err == nil {
		t.Fatal("replay with different hash unexpectedly succeeded")
	}
	if got, ok := registry.Definition(definition.ID); !ok || got != definition {
		t.Fatalf("definition = %+v, ok=%v", got, ok)
	}
}

func TestRegistryStoresDesiredFrontierAndRejectsRegression(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("prj_rules")
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	frontier := DesiredFrontier{ProjectionID: definition.ID, Required: Frontier{Revision: 8, Cursor: "cursor-8"}, ConfigurationEpoch: 1}
	if err := registry.ApplyRecord(context.Background(), NewDesiredFrontierRecord(frontier)); err != nil {
		t.Fatalf("apply desired frontier: %v", err)
	}
	if got, ok := registry.DesiredFrontier(definition.ID); !ok || got != frontier {
		t.Fatalf("desired frontier = %+v, ok=%v", got, ok)
	}
	stale := frontier
	stale.Required.Revision = 7
	if err := registry.ApplyRecord(context.Background(), NewDesiredFrontierRecord(stale)); err == nil {
		t.Fatal("stale desired frontier unexpectedly succeeded")
	}
}

func TestRegistryRejectsCheckpointWithoutDefinitionAndRegression(t *testing.T) {
	registry := NewRegistry()
	checkpoint := Checkpoint{ProjectionID: "prj_rules", Published: Frontier{Revision: 1}, ConfigurationEpoch: 1, ConfigurationHash: "config-v1", MaterializerVersion: "semantic-v1", ArtifactGeneration: "artifact-1", State: StateReady}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(checkpoint)); err == nil {
		t.Fatal("checkpoint without definition unexpectedly succeeded")
	}
	definition := testDefinition(checkpoint.ProjectionID)
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(checkpoint)); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}
	checkpoint.Published.Revision = 0
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(checkpoint)); err == nil || !strings.Contains(err.Error(), "regressed") {
		t.Fatalf("regressed checkpoint error = %v", err)
	}
}

func TestRegistryCheckpointCASRejectsStaleWorker(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("prj_rules")
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	first := Checkpoint{ProjectionID: definition.ID, Published: Frontier{Revision: 10, Cursor: "cursor-10"}, ConfigurationEpoch: 1, ConfigurationHash: definition.ConfigurationHash, MaterializerVersion: definition.MaterializerVersion, FencingEpoch: 4, ArtifactGeneration: "artifact-1", State: StateReady}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointCASRecord(first, CheckpointExpectation{ProjectionID: definition.ID})); err != nil {
		t.Fatalf("apply first CAS checkpoint: %v", err)
	}
	stale := first
	stale.FencingEpoch = 3
	stale.ArtifactGeneration = "artifact-stale"
	err := registry.ApplyRecord(context.Background(), NewCheckpointCASRecord(stale, CheckpointExpectation{ProjectionID: definition.ID, Exists: true, Published: first.Published, ConfigurationEpoch: 1, FencingEpoch: 3, ArtifactGeneration: "artifact-stale"}))
	if !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("stale CAS error = %v, want checkpoint conflict", err)
	}
	got, ok := registry.Checkpoint(definition.ID)
	if !ok || got != first {
		t.Fatalf("checkpoint after stale CAS = %+v, ok=%v", got, ok)
	}
}

func TestRegistryRejectsStaleFencingAndGenerationUpdates(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("prj_rules")
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	first := Checkpoint{ProjectionID: definition.ID, Published: Frontier{Revision: 10, Cursor: "cursor-10"}, ConfigurationEpoch: 1, ConfigurationHash: definition.ConfigurationHash, MaterializerVersion: definition.MaterializerVersion, FencingEpoch: 4, ArtifactGeneration: "artifact-1", State: StateReady}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(first)); err != nil {
		t.Fatalf("apply first checkpoint: %v", err)
	}
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(first)); err != nil {
		t.Fatalf("replay first checkpoint: %v", err)
	}
	stale := first
	stale.FencingEpoch = 3
	if err := registry.ApplyRecord(context.Background(), NewCheckpointRecord(stale)); err == nil {
		t.Fatal("stale fencing checkpoint unexpectedly succeeded")
	}
	updated := definition
	updated.ConfigurationEpoch = 2
	updated.ConfigurationHash = "config-v2"
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(updated)); err != nil {
		t.Fatalf("apply updated definition: %v", err)
	}
	if _, ok := registry.Checkpoint(definition.ID); ok {
		t.Fatal("old checkpoint survived a configuration generation change")
	}
	oldGeneration := updated
	oldGeneration.ConfigurationHash = "config-v2b"
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(oldGeneration)); err == nil {
		t.Fatal("changed definition without generation advance unexpectedly succeeded")
	}
}

func TestRegistryReadinessAndUnknownBootstrapFailClosed(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("prj_rules")
	if err := registry.BootstrapUnknown(context.Background(), definition, "node-a"); err != nil {
		t.Fatalf("bootstrap unknown: %v", err)
	}
	readiness, err := registry.Readiness(definition.ID, "node-a", ReadinessRequirement{Required: Frontier{}, ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"})
	if err != nil {
		t.Fatalf("readiness: %v", err)
	}
	if readiness.State != StateRebuildRequired || !containsReason(readiness.Reasons, ReasonMissingManifest) {
		t.Fatalf("unknown readiness = %+v", readiness)
	}
	status := ReplicaStatus{ProjectionID: definition.ID, NodeID: "node-a", State: StateReady, Required: Frontier{Revision: 4, Cursor: "cursor-4"}, Published: Frontier{Revision: 4, Cursor: "cursor-4"}, ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1", FencingEpoch: 7, ArtifactGeneration: "artifact-2"}
	if err := registry.ApplyRecord(context.Background(), NewReplicaStatusRecord(status)); err != nil {
		t.Fatalf("apply ready status: %v", err)
	}
	readiness, err = registry.Readiness(definition.ID, "node-a", ReadinessRequirement{Required: status.Required, ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1", FencingEpoch: 7})
	if err != nil {
		t.Fatalf("ready readiness: %v", err)
	}
	if readiness.State != StateReady || len(readiness.Reasons) != 0 {
		t.Fatalf("ready readiness = %+v", readiness)
	}
}

func containsReason(reasons []Reason, wanted Reason) bool {
	for _, reason := range reasons {
		if reason == wanted {
			return true
		}
	}
	return false
}
