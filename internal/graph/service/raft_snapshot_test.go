package service

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGraphRaftStateMachineSnapshotRestorePartition(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	parentID := uuid.NewString()
	childID := uuid.NewString()
	edgeID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, tx, NodeInput{NodeID: parentID, Labels: []string{"SnapshotParent"}, Properties: map[string]any{"role": "parent"}}); err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	if _, err := m.CreateNode(ctx, tx, NodeInput{NodeID: childID, Labels: []string{"SnapshotChild"}, Properties: map[string]any{"role": "child"}}); err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{EdgeID: edgeID, FromNodeID: parentID, ToNodeID: childID, Labels: []string{"contains"}, Properties: map[string]any{"kind": "snapshot"}}); err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(uuid.MustParse(spaceID)), 4)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	sm := RaftStateMachine{Module: m, PartitionID: pid.Uint32(), PartitionCount: 4}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	restoredSM := RaftStateMachine{Module: restored, PartitionID: pid.Uint32(), PartitionCount: 4}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	got, err := restored.GetNode(ctx, graphTx(spaceID, domainID, 1), parentID)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.ID.String() != parentID {
		t.Fatalf("restored node id=%s", got.ID)
	}
	if _, err := restored.GetEdge(ctx, graphTx(spaceID, domainID, 1), edgeID); err != nil {
		t.Fatalf("GetEdge() error = %v", err)
	}
	rev, err := restored.CurrentRevision(ctx, spaceID)
	if err != nil {
		t.Fatalf("CurrentRevision() error = %v", err)
	}
	if rev != 1 {
		t.Fatalf("restored revision=%d want 1", rev)
	}
	stats, err := restored.LocalGraphConsistencyStats(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("LocalGraphConsistencyStats() error = %v", err)
	}
	if stats.NodeCount != 2 || stats.EdgeCount != 1 {
		t.Fatalf("restored stats=%+v", stats)
	}
}
