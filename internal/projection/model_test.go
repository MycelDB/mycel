package projection

import (
	"context"
	"strings"
	"testing"
)

func TestIDForIsDeterministicAndScopeBound(t *testing.T) {
	scope := Scope{SpaceID: "space-a", DomainID: "domain-a"}
	first, err := IDFor(KindSemantic, scope, "search")
	if err != nil {
		t.Fatalf("IDFor() error = %v", err)
	}
	second, err := IDFor(KindSemantic, scope, "search")
	if err != nil {
		t.Fatalf("IDFor() second error = %v", err)
	}
	if first != second {
		t.Fatalf("IDFor() is not deterministic: %q != %q", first, second)
	}
	other, err := IDFor(KindSemantic, Scope{SpaceID: "space-b", DomainID: "domain-a"}, "search")
	if err != nil {
		t.Fatalf("IDFor() other error = %v", err)
	}
	if first == other {
		t.Fatalf("IDs for different scopes unexpectedly match: %q", first)
	}
	if !strings.HasPrefix(first.String(), "prj_") {
		t.Fatalf("projection ID %q is not opaque/stable-formatted", first)
	}
}

func TestRecordValidationBoundsOpaqueCursor(t *testing.T) {
	definition := Definition{ID: "prj_test", Kind: KindSemantic, Scope: Scope{SpaceID: "space", DomainID: "domain"}, Key: "search", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
	if err := definition.Validate(); err != nil {
		t.Fatalf("definition.Validate() error = %v", err)
	}
	record := NewCheckpointRecord(Checkpoint{ProjectionID: definition.ID, Published: Frontier{Revision: 1, Cursor: strings.Repeat("x", MaxCursorBytes+1)}, ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1", ArtifactGeneration: "artifact-1", State: StateReady})
	if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("record.Validate() error = %v, want cursor bound failure", err)
	}
}

func TestReadinessSerializationIsBounded(t *testing.T) {
	readiness := Readiness{ProjectionID: "prj_test", NodeID: "node-a", State: StateDegraded, MaterializerVersion: "semantic-v1"}
	for i := 0; i < MaxReadinessReasons+1; i++ {
		readiness.Reasons = append(readiness.Reasons, ReasonSourceLag)
	}
	if _, err := readiness.Marshal(); err == nil {
		t.Fatal("readiness with too many reasons unexpectedly serialized")
	}
}

func TestSnapshotOrderingIsDeterministic(t *testing.T) {
	registry := NewRegistry()
	definitionA := Definition{ID: "prj_a", Kind: KindSemantic, Scope: Scope{SpaceID: "space", DomainID: "domain"}, Key: "a", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
	definitionB := Definition{ID: "prj_b", Kind: KindLexical, Scope: Scope{SpaceID: "space", DomainID: "domain"}, Key: "b", ConfigurationEpoch: 1, MaterializerVersion: "lexical-v1"}
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definitionB)); err != nil {
		t.Fatalf("apply definition B: %v", err)
	}
	if err := registry.ApplyRecord(context.Background(), NewDefinitionRecord(definitionA)); err != nil {
		t.Fatalf("apply definition A: %v", err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot.Definitions) != 2 || snapshot.Definitions[0].ID != definitionA.ID || snapshot.Definitions[1].ID != definitionB.ID {
		t.Fatalf("snapshot definitions are not sorted: %+v", snapshot.Definitions)
	}
}
