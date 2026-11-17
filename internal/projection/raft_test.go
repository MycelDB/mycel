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

func TestRaftStateMachineRejectsRegressedReplicaStatusAsCommandRejection(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.New())
	stateMachine := NewRaftStateMachine(0, 4)
	definition := Definition{ID: "prj_rules", Kind: KindSemantic, Scope: Scope{SpaceID: spaceID.String(), DomainID: "domain-a"}, Key: "rules", ConfigurationEpoch: 2, MaterializerVersion: "semantic-v1"}
	definitionCommand, err := NewPartitionCommand(spaceID, 4, NewDefinitionRecord(definition), "definition-replica-status")
	if err != nil {
		t.Fatalf("build definition command: %v", err)
	}
	if err := stateMachine.ApplyCommand(context.Background(), consensus.ApplyContext{}, definitionCommand); err != nil {
		t.Fatalf("apply definition command: %v", err)
	}
	initial := ReplicaStatus{
		ProjectionID:        "prj_rules",
		NodeID:              "node-a",
		State:               StateReady,
		Required:            Frontier{Revision: 10},
		Published:           Frontier{Revision: 10, Cursor: "cursor-a"},
		ConfigurationEpoch:  2,
		MaterializerVersion: "semantic-v1",
		FencingEpoch:        4,
	}
	initialCommand, err := NewPartitionCommand(spaceID, 4, NewReplicaStatusRecord(initial), "status-initial")
	if err != nil {
		t.Fatalf("build initial status command: %v", err)
	}
	if err := stateMachine.ApplyCommand(context.Background(), consensus.ApplyContext{}, initialCommand); err != nil {
		t.Fatalf("apply initial status command: %v", err)
	}

	tests := []struct {
		name   string
		status ReplicaStatus
	}{
		{
			name:   "configuration regression",
			status: ReplicaStatus{ProjectionID: initial.ProjectionID, NodeID: initial.NodeID, State: StateReady, Required: initial.Required, Published: initial.Published, ConfigurationEpoch: 1, MaterializerVersion: initial.MaterializerVersion, FencingEpoch: initial.FencingEpoch},
		},
		{
			name:   "fencing regression",
			status: ReplicaStatus{ProjectionID: initial.ProjectionID, NodeID: initial.NodeID, State: StateReady, Required: initial.Required, Published: initial.Published, ConfigurationEpoch: initial.ConfigurationEpoch, MaterializerVersion: initial.MaterializerVersion, FencingEpoch: 3},
		},
		{
			name:   "source frontier regression",
			status: ReplicaStatus{ProjectionID: initial.ProjectionID, NodeID: initial.NodeID, State: StateReady, Required: initial.Required, Published: Frontier{Revision: 9, Cursor: "cursor-a"}, ConfigurationEpoch: initial.ConfigurationEpoch, MaterializerVersion: initial.MaterializerVersion, FencingEpoch: initial.FencingEpoch},
		},
		{
			name:   "same revision conflicting cursor",
			status: ReplicaStatus{ProjectionID: initial.ProjectionID, NodeID: initial.NodeID, State: StateReady, Required: initial.Required, Published: Frontier{Revision: 10, Cursor: "cursor-b"}, ConfigurationEpoch: initial.ConfigurationEpoch, MaterializerVersion: initial.MaterializerVersion, FencingEpoch: initial.FencingEpoch},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commandID := "status-stale-" + tt.name
			command, err := NewPartitionCommand(spaceID, 4, NewReplicaStatusRecord(tt.status), commandID)
			if err != nil {
				t.Fatalf("build stale status command: %v", err)
			}
			err = stateMachine.ApplyCommand(context.Background(), consensus.ApplyContext{}, command)
			if !consensus.IsCommandRejection(err) {
				t.Fatalf("stale status error = %v, want command rejection", err)
			}
			if consensus.IsFatalApplyError(err) {
				t.Fatalf("stale status was classified as fatal: %v", err)
			}
			if err := stateMachine.ApplyCommand(context.Background(), consensus.ApplyContext{}, command); err != nil {
				t.Fatalf("replaying rejected status command: %v", err)
			}
		})
	}
	snapshot, err := stateMachine.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after rejected status commands: %v", err)
	}
	restored := NewRaftStateMachine(0, 4)
	if err := restored.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("restore after rejected status commands: %v", err)
	}
	last := tests[len(tests)-1]
	lastCommand, err := NewPartitionCommand(spaceID, 4, NewReplicaStatusRecord(last.status), "status-stale-"+last.name)
	if err != nil {
		t.Fatalf("build replayed stale status command: %v", err)
	}
	if err := restored.ApplyCommand(context.Background(), consensus.ApplyContext{Replay: true}, lastCommand); err != nil {
		t.Fatalf("replay rejected status after restore: %v", err)
	}
	if got, ok := restored.Registry.ReplicaStatus(initial.ProjectionID, initial.NodeID); !ok || !reflect.DeepEqual(got, initial) {
		t.Fatalf("restored replica status changed after rejected update: got=%+v ok=%v", got, ok)
	}
	if got, ok := stateMachine.Registry.ReplicaStatus(initial.ProjectionID, initial.NodeID); !ok || !reflect.DeepEqual(got, initial) {
		t.Fatalf("replica status changed after rejected updates: got=%+v ok=%v", got, ok)
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
