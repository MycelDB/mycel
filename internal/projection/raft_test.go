package projection

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestRaftStateMachineAppliesAndRestoresRegistry(t *testing.T) {
	spaceID := uuid.New()
	definitionID := ProjectionID("prj_rules")
	definition := Definition{ID: definitionID, Kind: KindSemantic, Scope: Scope{SpaceID: spaceID.String(), DomainID: "domain-a"}, Key: "rules", ConfigurationHash: "config-v1", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
	stateMachine := NewRaftStateMachine(0, 4)
	apply := consensus.ApplyContext{}
	definitionCommand, err := NewPartitionCommand(domainspace.SpaceID(spaceID), 4, NewDefinitionRecord(definition), "definition-1")
	if err != nil {
		t.Fatalf("build definition command: %v", err)
	}
	if err := stateMachine.ApplyCommand(context.Background(), apply, definitionCommand); err != nil {
		t.Fatalf("apply definition command: %v", err)
	}
	frontierCommand, err := NewPartitionCommand(domainspace.SpaceID(spaceID), 4, NewDesiredFrontierRecord(DesiredFrontier{ProjectionID: definitionID, Required: Frontier{Revision: 3}, ConfigurationEpoch: 1}), "frontier-1")
	if err != nil {
		t.Fatalf("build frontier command: %v", err)
	}
	if err := stateMachine.ApplyCommand(context.Background(), apply, frontierCommand); err != nil {
		t.Fatalf("apply frontier command: %v", err)
	}
	status := ReplicaStatus{ProjectionID: definitionID, NodeID: "node-a", State: StateReady, Required: Frontier{Revision: 3}, Published: Frontier{Revision: 3}, ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1", FencingEpoch: 2, ArtifactGeneration: "artifact-1"}
	statusCommand, err := NewPartitionCommand(domainspace.SpaceID(spaceID), 4, NewReplicaStatusRecord(status), "status-1")
	if err != nil {
		t.Fatalf("build status command: %v", err)
	}
	if err := stateMachine.ApplyCommand(context.Background(), apply, statusCommand); err != nil {
		t.Fatalf("apply status command: %v", err)
	}
	data, err := stateMachine.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewRaftStateMachine(0, 4)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if got, ok := restored.Registry.Definition(definitionID); !ok || got != definition {
		t.Fatalf("restored definition = %+v, ok=%v", got, ok)
	}
	if got, ok := restored.Registry.DesiredFrontier(definitionID); !ok || got.Required.Revision != 3 {
		t.Fatalf("restored desired frontier = %+v, ok=%v", got, ok)
	}
	if got, ok := restored.Registry.ReplicaStatus(definitionID, "node-a"); !ok || !reflect.DeepEqual(got, status) {
		t.Fatalf("restored status = %+v, ok=%v", got, ok)
	}
}

func TestRaftStateMachineRejectsWrongScope(t *testing.T) {
	stateMachine := NewRaftStateMachine(0, 4)
	definition := Definition{ID: "prj_rules", Kind: KindSemantic, Scope: Scope{SpaceID: uuid.NewString(), DomainID: "domain-a"}, Key: "rules", ConfigurationEpoch: 1, MaterializerVersion: "semantic-v1"}
	record, err := NewDefinitionRecord(definition).Marshal()
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	cmd := consensus.RaftCommand{Scope: consensus.CommandScopeSystem, RecordType: recordTypeProjectionDefinition, Payload: record, CommandID: "wrong-scope", CommandHash: []byte("hash")}
	if err := stateMachine.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err == nil {
		t.Fatal("system-scoped projection command unexpectedly succeeded")
	}
}
