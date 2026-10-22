package service

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
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
	if _, err := spaceMgr.UpsertIndexState(ctx, domainsemantic.SemanticIndexState{SemanticIndexID: idx.ID, State: "ready", RecordCount: 4, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("UpsertIndexState() error = %v", err)
	}
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	reliability := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: "search", SemanticGeneration: 3, LatestObserved: 12, AnalyzedThrough: 11, FreshThrough: 8, UnresolvedRanges: []domainsemantic.SemanticRevisionRange{{FirstRevision: 9, LastRevision: 11}}, CapacityState: "exhausted", CapacityReason: "test capacity", RebuildState: "required"}
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "degraded", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": reliability}, UpdatedAt: time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("UpsertSemanticRuleState() error = %v", err)
	}
	if _, err := spaceMgr.UpsertSearchIndexState(ctx, domainsemantic.SemanticSearchIndexState{ID: domainsemantic.SearchIndexID(uuid.New()), SemanticRuleID: ruleID, EmbeddingBindingKey: "search", State: "ready", LiveRecordCount: 4, UpdatedAt: time.Date(2026, 10, 20, 10, 1, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("UpsertSearchIndexState() error = %v", err)
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
	var decoded semanticRaftSnapshot
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Spaces) != 1 || len(decoded.Spaces[0].WorkItems) != 1 {
		t.Fatalf("unexpected snapshot shape: %+v", decoded.Spaces)
	}
	decoded.Spaces[0].WorkItems[0].CreatedAt = time.Time{}
	decoded.Spaces[0].WorkItems[0].UpdatedAt = time.Time{}
	pendingWorkID := domainsemantic.SemanticDirtyWorkItemID(uuid.New())
	decoded.Spaces[0].WorkItems = append(decoded.Spaces[0].WorkItems, domainsemantic.SemanticDirtyWorkItem{ID: pendingWorkID, SemanticIndexID: idx.ID, SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), TargetNodeID: graphmodel.NodeID(uuid.New()), Reason: "legacy pending", Action: domainsemantic.SemanticDirtyWorkActionRefresh, Status: domainsemantic.SemanticDirtyWorkStatusPending, FirstGraphRevision: 8, LastGraphRevision: 8})
	decoded.Spaces[0].DirtyEvents[0].CommittedAt = time.Time{}
	decoded.Spaces[0].Checkpoints[0].UpdatedAt = time.Time{}
	decoded.Spaces[0].SearchIndexStates[0].UpdatedAt = time.Time{}
	snapshot, err = json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
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
	if err != nil || len(items) != 2 {
		t.Fatalf("work items=%#v err=%v want two", items, err)
	}
	for _, item := range items {
		if item.Status != domainsemantic.SemanticDirtyWorkStatusPending || item.ClaimedBy != "" || item.ClaimedUntil != nil || !item.CreatedAt.Equal(time.Unix(0, 0).UTC()) || !item.UpdatedAt.Equal(time.Unix(0, 0).UTC()) {
			t.Fatalf("restored zero-time work was not normalized deterministically: %+v", item)
		}
	}
	if items[0].ID != work.ID && items[1].ID != work.ID {
		t.Fatalf("restored running work %s missing: %+v", work.ID, items)
	}
	if items[0].ID != pendingWorkID && items[1].ID != pendingWorkID {
		t.Fatalf("restored pending work %s missing: %+v", pendingWorkID, items)
	}
	restoredAgain := NewModule()
	if result := restoredAgain.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("second restored init failed: %v", result.Error)
	}
	if err := (RaftStateMachine{Module: restoredAgain, PartitionID: pid.Uint32(), PartitionCount: 4}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("second RestoreSnapshot() error = %v", err)
	}
	restoredAgainMaint, err := restoredAgain.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	itemsAgain, err := restoredAgainMaint.ListDirtyWorkItems(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(items, itemsAgain) {
		t.Fatalf("identical snapshot produced different work state:\nfirst=%+v\nsecond=%+v", items, itemsAgain)
	}
	workLog := filepath.Join("work", "work-000001.ksem")
	firstLog, err := os.ReadFile(filepath.Join(restored.maintenanceDir(spaceID), workLog))
	if err != nil {
		t.Fatal(err)
	}
	secondLog, err := os.ReadFile(filepath.Join(restoredAgain.maintenanceDir(spaceID), workLog))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstLog, secondLog) {
		t.Fatal("identical snapshot produced different durable work-log bytes")
	}
	states, err := restoredSpace.ListSemanticRuleStates(ctx)
	if err != nil || len(states) != 1 {
		t.Fatalf("rule states=%#v err=%v want one", states, err)
	}
	gotReliability := states[0].ReliabilityByBinding["search"]
	if gotReliability.SemanticGeneration != 3 || gotReliability.FreshThrough != 0 || gotReliability.RebuildState != "required" || gotReliability.CapacityState != "exhausted" || len(gotReliability.UnresolvedRanges) != 1 {
		t.Fatalf("restored reliability state mismatch: %+v", gotReliability)
	}
	searchStates, err := restoredSpace.ListSearchIndexStates(ctx)
	if err != nil || len(searchStates) != 1 || searchStates[0].State != "missing" || !searchStates[0].UpdatedAt.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("restored search index state should require rebuild with deterministic time: states=%+v err=%v", searchStates, err)
	}
	indexStates, err := restoredSpace.ListIndexStates(ctx)
	if err != nil || len(indexStates) != 1 || indexStates[0].State != "missing" || indexStates[0].RecordCount != 0 {
		t.Fatalf("restored legacy index state should require rebuild: states=%+v err=%v", indexStates, err)
	}
	restoredAgainSpace, err := restoredAgain.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	statesAgain, err := restoredAgainSpace.ListSemanticRuleStates(ctx)
	if err != nil || !reflect.DeepEqual(states, statesAgain) {
		t.Fatalf("identical snapshot produced different rule state: first=%+v second=%+v err=%v", states, statesAgain, err)
	}
	searchStatesAgain, err := restoredAgainSpace.ListSearchIndexStates(ctx)
	if err != nil || !reflect.DeepEqual(searchStates, searchStatesAgain) {
		t.Fatalf("identical snapshot produced different search-index state: first=%+v second=%+v err=%v", searchStates, searchStatesAgain, err)
	}
	indexStatesAgain, err := restoredAgainSpace.ListIndexStates(ctx)
	if err != nil || !reflect.DeepEqual(indexStates, indexStatesAgain) {
		t.Fatalf("identical snapshot produced different index state: first=%+v second=%+v err=%v", indexStates, indexStatesAgain, err)
	}
	eventsAgain, err := restoredAgainMaint.ListGraphDirtyEvents(ctx)
	if err != nil || !reflect.DeepEqual(events, eventsAgain) {
		t.Fatalf("identical snapshot produced different dirty-event state: first=%+v second=%+v err=%v", events, eventsAgain, err)
	}
	checkpointAgain, err := restoredAgainMaint.GetCheckpoint(ctx, "snapshot-test")
	if err != nil || !reflect.DeepEqual(checkpoint, checkpointAgain) || !checkpointAgain.UpdatedAt.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("identical snapshot produced different checkpoint state: first=%+v second=%+v err=%v", checkpoint, checkpointAgain, err)
	}
	firstFiles, err := semanticSnapshotFileTree(restored.spaceSemanticDir(spaceID))
	if err != nil {
		t.Fatal(err)
	}
	secondFiles, err := semanticSnapshotFileTree(restoredAgain.spaceSemanticDir(spaceID))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstFiles, secondFiles) {
		t.Fatalf("identical snapshot produced different durable semantic files: first=%v second=%v", fileTreeSizes(firstFiles), fileTreeSizes(secondFiles))
	}
	if restored.raftCommandApplied("semantic-maintenance-" + spaceID.String() + "-stale-post-snapshot") {
		t.Fatal("RestoreSnapshot should trim stale same-partition semantic applied command IDs")
	}
}

func semanticSnapshotFileTree(root string) (map[string][]byte, error) {
	out := map[string][]byte{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[relative] = raw
		return nil
	})
	return out, err
}

func fileTreeSizes(tree map[string][]byte) map[string]int {
	out := make(map[string]int, len(tree))
	for path, raw := range tree {
		out[path] = len(raw)
	}
	return out
}
