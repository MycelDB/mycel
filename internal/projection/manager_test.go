package projection

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestRegistryManagerSharesPartitionStateMachineAndReadOnlyView(t *testing.T) {
	manager := NewRegistryManager(4)
	first := manager.NewRaftStateMachine(2)
	second := manager.NewRaftStateMachine(2)
	if first != second {
		t.Fatal("manager created different state machines for one partition")
	}
	definition := Definition{ID: "prj_rules", Kind: KindSemantic, Scope: Scope{SpaceID: uuid.NewString(), DomainID: "domain-a"}, Key: "rules", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
	if err := first.Registry.ApplyRecord(context.Background(), NewDefinitionRecord(definition)); err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	reader, ok := manager.Reader(2)
	if !ok {
		t.Fatal("manager did not expose a reader for the registered partition")
	}
	if got, ok := reader.Definition(definition.ID); !ok || got != definition {
		t.Fatalf("reader definition = %+v, ok=%v", got, ok)
	}
	if _, ok := manager.Reader(3); ok {
		t.Fatal("manager exposed an unregistered partition reader")
	}
}
