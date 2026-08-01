package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSemanticSystemRaftStateMachineSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	source := NewModule()
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("source init failed: %v", result.Error)
	}
	vectorStore, err := source.globalBase.UpsertVectorStore(ctx, semanticVectorStore("snapshot-system"))
	if err != nil {
		t.Fatalf("UpsertVectorStore() error = %v", err)
	}
	usage := semanticUsageEvent()
	if _, err := source.accountingBase.Append(ctx, usage); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	snapshot, err := (RaftStateMachine{Module: source, System: true, PartitionCount: 4}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	restored.raftAppliedCommands = map[string]struct{}{"semantic-global-stale-post-snapshot": {}}
	if err := (RaftStateMachine{Module: restored, System: true, PartitionCount: 4}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	stores, err := restored.globalBase.ListVectorStores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasVectorStoreKey(stores, vectorStore.Key) {
		t.Fatalf("vector stores=%#v want key %q", stores, vectorStore.Key)
	}
	events, err := restored.accountingBase.List(ctx, storeaccounting.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != usage.ID {
		t.Fatalf("events=%#v want %s", events, usage.ID)
	}
	if restored.raftCommandApplied("semantic-global-stale-post-snapshot") {
		t.Fatal("RestoreSnapshot should trim stale system semantic applied command IDs")
	}
}

func TestSemanticPartitionRaftStateMachineSnapshotRestoreResetsRunningWork(t *testing.T) {
	ctx := context.Background()
	source := NewModule()
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("source init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	pid, err := partitioning.PartitionForSpaceID(spaceID, 4)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	idx := semanticIndex(spaceID)
	spaceMgr, err := source.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	maintMgr, err := source.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	event := semanticDirtyEvent(spaceID)
	if _, err := maintMgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("AppendGraphDirtyEvent() error = %v", err)
	}
	if err := maintMgr.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: "snapshot-test", SpaceID: spaceID, LastGraphRevision: 7, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	claimedUntil := time.Now().UTC().Add(time.Hour)
	work := domainsemantic.SemanticDirtyWorkItem{ID: domainsemantic.SemanticDirtyWorkItemID(uuid.New()), SemanticIndexID: idx.ID, SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), TargetNodeID: graphmodel.NodeID(uuid.New()), Reason: "snapshot", Action: domainsemantic.SemanticDirtyWorkActionRefresh, Status: domainsemantic.SemanticDirtyWorkStatusRunning, ClaimedBy: "old-node", ClaimedUntil: &claimedUntil, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if _, err := maintMgr.UpsertDirtyWorkItem(ctx, work); err != nil {
		t.Fatalf("UpsertDirtyWorkItem() error = %v", err)
	}
	snapshot, err := (RaftStateMachine{Module: source, PartitionID: pid.Uint32(), PartitionCount: 4}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	restored.raftAppliedCommands = map[string]struct{}{"semantic-maintenance-" + spaceID.String() + "-stale-post-snapshot": {}}
	if err := (RaftStateMachine{Module: restored, PartitionID: pid.Uint32(), PartitionCount: 4}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	restoredSpace, err := restored.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := restoredSpace.ListSemanticIndexes(ctx)
	if err != nil || len(indexes) != 1 || indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v err=%v want %s", indexes, err, idx.ID)
	}
	restoredMaint, err := restored.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := restoredMaint.ListGraphDirtyEvents(ctx)
	if err != nil || len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("events=%#v err=%v want %s", events, err, event.ID)
	}
	checkpoint, err := restoredMaint.GetCheckpoint(ctx, "snapshot-test")
	if err != nil || checkpoint.LastGraphRevision != 7 {
		t.Fatalf("checkpoint=%+v err=%v want revision 7", checkpoint, err)
	}
	items, err := restoredMaint.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("work items=%#v err=%v want one", items, err)
	}
	if items[0].Status != domainsemantic.SemanticDirtyWorkStatusPending || items[0].ClaimedBy != "" || items[0].ClaimedUntil != nil {
		t.Fatalf("restored running work not reset: %+v", items[0])
	}
	if restored.raftCommandApplied("semantic-maintenance-" + spaceID.String() + "-stale-post-snapshot") {
		t.Fatal("RestoreSnapshot should trim stale same-partition semantic applied command IDs")
	}
}
