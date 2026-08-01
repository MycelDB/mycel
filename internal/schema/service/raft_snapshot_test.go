package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/schema/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSchemaRaftStateMachineSnapshotRestorePartition(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager(storage.NewMemoryStore())
	domainID := graph.DomainID(uuid.New())
	value := mustSchemaRaftTestValue(t, domainID)
	cmd, err := mgr.buildSchemaPutRaftCommand(schemaPutRecord{Schema: value}, 4, "schema-snapshot-put")
	if err != nil {
		t.Fatalf("buildSchemaPutRaftCommand() error = %v", err)
	}
	sm := RaftStateMachine{Manager: mgr, PartitionID: cmd.PartitionID, PartitionCount: 4}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewManager(storage.NewMemoryStore())
	restoredSM := RaftStateMachine{Manager: restored, PartitionID: cmd.PartitionID, PartitionCount: 4}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	got, err := restored.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("GetDomainSchema() error = %v", err)
	}
	if got.DomainID != domainID || len(got.NodeTypes) == 0 {
		t.Fatalf("unexpected restored schema: %+v", got)
	}
	labels, err := restored.ResolveNodeLabel(ctx, domainID, got.NodeTypes[0].Name)
	if err != nil || len(labels) == 0 {
		t.Fatalf("ResolveNodeLabel() = %+v, %v; want restored cache/schema", labels, err)
	}

	otherDomainID := graph.DomainID(uuid.New())
	otherPartition, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(otherDomainID), 4)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	if otherPartition.Uint32() == cmd.PartitionID {
		t.Skip("random other domain landed in same partition")
	}
	if _, err := restored.GetDomainSchema(ctx, otherDomainID); err == nil {
		t.Fatal("unexpected schema for other domain")
	}
}
