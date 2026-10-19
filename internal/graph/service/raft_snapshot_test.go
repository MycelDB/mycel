package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGraphRaftSnapshotPreservesDeliveredIdempotencyState(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatal(result.Error)
	}
	initialSinkCalls := 0
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		initialSinkCalls++
		return nil
	}))
	spaceID, domainID, principalID, nodeID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	tx := idempotencyGraphTx(spaceID, domainID, principalID, 0, "delivered-snapshot")
	input := domaingraph.NodeInput{NodeID: nodeID, Content: "delivered"}
	if _, err := m.UpsertNode(ctx, tx, input); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if initialSinkCalls != 1 {
		t.Fatalf("initial sink calls = %d", initialSinkCalls)
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(uuid.MustParse(spaceID)), 4)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (RaftStateMachine{Module: m, PartitionID: pid.Uint32(), PartitionCount: 4}).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatal(result.Error)
	}
	restoredSinkCalls := 0
	restored.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		restoredSinkCalls++
		return nil
	}))
	if err := (RaftStateMachine{Module: restored, PartitionID: pid.Uint32(), PartitionCount: 4}).RestoreSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	retry := idempotencyGraphTx(spaceID, domainID, principalID, 1, "delivered-snapshot")
	if _, err := restored.UpsertNode(ctx, retry, input); err != nil {
		t.Fatal(err)
	}
	result, err := restored.CommitTransactionGraph(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IdempotentReplay || result.CommittedRevision != 1 || restoredSinkCalls != 0 {
		t.Fatalf("restored delivered replay = %#v, sink calls = %d", result, restoredSinkCalls)
	}
}

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
	principalID := uuid.NewString()
	tx := idempotencyGraphTx(spaceID, domainID, principalID, 0, "snapshot-capture")
	if _, err := m.UpsertNode(ctx, tx, domaingraph.NodeInput{NodeID: parentID, Labels: []string{"SnapshotParent"}, Properties: map[string]any{"role": "parent"}, Content: "parent"}); err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	graphStateTx := graphTx(spaceID, domainID, 1)
	if _, err := m.UpsertNode(ctx, graphStateTx, domaingraph.NodeInput{NodeID: childID, Labels: []string{"SnapshotChild"}, Properties: map[string]any{"role": "child"}}); err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, graphStateTx, domaingraph.EdgeInput{EdgeID: edgeID, FromNodeID: parentID, ToNodeID: childID, Labels: []string{"contains"}, Properties: map[string]any{"kind": "snapshot"}}); err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, graphStateTx); err != nil {
		t.Fatalf("CommitTransactionGraph(graph) error = %v", err)
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
	sinkCalls := 0
	restored.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		sinkCalls++
		return nil
	}))
	restoredSM := RaftStateMachine{Module: restored, PartitionID: pid.Uint32(), PartitionCount: 4}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	metrics := restored.IdempotencyMetrics()
	if metrics.ActiveRecords != 1 || metrics.CapacityRecords != graphstorage.MaxIdempotencyRecordsPerStore {
		t.Fatalf("restored idempotency metrics = %+v", metrics)
	}
	got, err := restored.GetNode(ctx, graphTx(spaceID, domainID, 2), parentID)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.ID.String() != parentID {
		t.Fatalf("restored node id=%s", got.ID)
	}
	if _, err := restored.GetEdge(ctx, graphTx(spaceID, domainID, 2), edgeID); err != nil {
		t.Fatalf("GetEdge() error = %v", err)
	}
	rev, err := restored.CurrentRevision(ctx, spaceID)
	if err != nil {
		t.Fatalf("CurrentRevision() error = %v", err)
	}
	if rev != 2 {
		t.Fatalf("restored revision=%d want 2", rev)
	}
	stats, err := restored.LocalGraphConsistencyStats(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("LocalGraphConsistencyStats() error = %v", err)
	}
	if stats.NodeCount != 2 || stats.EdgeCount != 1 {
		t.Fatalf("restored stats=%+v", stats)
	}
	retry := idempotencyGraphTx(spaceID, domainID, principalID, 2, "snapshot-capture")
	if _, err := restored.UpsertNode(ctx, retry, domaingraph.NodeInput{NodeID: parentID, Labels: []string{"SnapshotParent"}, Properties: map[string]any{"role": "parent"}, Content: "parent"}); err != nil {
		t.Fatal(err)
	}
	replay, err := restored.CommitTransactionGraph(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.CommittedRevision != 1 || sinkCalls != 1 {
		t.Fatalf("snapshot replay = %#v, sink calls = %d", replay, sinkCalls)
	}
	secondRetry := idempotencyGraphTx(spaceID, domainID, principalID, 2, "snapshot-capture")
	if _, err := restored.UpsertNode(ctx, secondRetry, domaingraph.NodeInput{NodeID: parentID, Labels: []string{"SnapshotParent"}, Properties: map[string]any{"role": "parent"}, Content: "parent"}); err != nil {
		t.Fatal(err)
	}
	if secondReplay, err := restored.CommitTransactionGraph(ctx, secondRetry); err != nil || !secondReplay.IdempotentReplay || sinkCalls != 1 {
		t.Fatalf("second snapshot replay = %#v, err = %v, sink calls = %d", secondReplay, err, sinkCalls)
	}
	mismatch := idempotencyGraphTx(spaceID, domainID, principalID, 2, "snapshot-capture")
	if _, err := restored.UpsertNode(ctx, mismatch, domaingraph.NodeInput{NodeID: parentID, Labels: []string{"Different"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.CommitTransactionGraph(ctx, mismatch); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("snapshot mismatch error = %v, want ErrIdempotencyConflict", err)
	}
	if rev, err := restored.CurrentDomainRevision(ctx, spaceID, domainID); err != nil || rev != 2 {
		t.Fatalf("revision after snapshot replay/mismatch = %d, %v; want 2", rev, err)
	}
}
