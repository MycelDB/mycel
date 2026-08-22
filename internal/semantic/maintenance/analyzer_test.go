package maintenance

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestAnalyzerCoalescesGraphDirtyEvents(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	root := t.TempDir()
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(root, "semantic"), spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	maintenanceMgr := storesemantic.NewMaintenanceManager()
	if err := maintenanceMgr.Init(ctx, filepath.Join(root, "semantic", "maintenance"), spaceID); err != nil {
		t.Fatalf("maintenance init failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Content: "node"}}}
	idx, err := mgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf, RecordTypes: []string{"note"}}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	txnID := uuid.New()
	event := domainsemantic.GraphDirtyEvent{TxnID: txnID, GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("append duplicate failed: %v", err)
	}
	events, err := maintenanceMgr.ListGraphDirtyEvents(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected idempotent event list, events=%+v err=%v", events, err)
	}
	res, err := (Analyzer{SpaceManager: mgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 1 || res.EnqueuedItems != 1 {
		t.Fatalf("unexpected analyze result %+v", res)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected one dirty item, items=%+v err=%v", items, err)
	}
	if items[0].SemanticIndexID != idx.ID || items[0].TargetNodeID != nodeID || items[0].Reason != "node_created" || items[0].Status != domainsemantic.SemanticDirtyWorkStatusPending {
		t.Fatalf("unexpected dirty item %+v", items[0])
	}
	states, err := mgr.ListIndexStates(ctx)
	if err != nil || len(states) != 1 || states[0].GraphDirtyCheckpointRevision != 1 || states[0].DirtyCount != 1 {
		t.Fatalf("unexpected states %+v err=%v", states, err)
	}
	res, err = (Analyzer{SpaceManager: mgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("second analyze failed: %v", err)
	}
	if res.ProcessedEvents != 0 {
		t.Fatalf("expected checkpoint skip, got %+v", res)
	}
}

func TestAnalyzerResolvesSubtreeTargetsAndCooldown(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	rootID := graph.NodeID(uuid.New())
	childID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	idx, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree, RecordTypes: []string{"page"}}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{
		nodes: map[graph.NodeID]graph.Node{
			rootID:  {ID: rootID, DomainID: domainID, Content: "root"},
			childID: {ID: childID, DomainID: domainID, Content: "child"},
		},
		parents: map[graph.NodeID]graph.NodeID{childID: rootID},
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{childID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	now := time.Now().UTC()
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, DirtyCooldown: time.Minute}).AnalyzeOnce(ctx, AnalyzeInput{Now: now})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 1 || res.EnqueuedItems != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected one item, got %+v err=%v", items, err)
	}
	if items[0].SemanticIndexID != idx.ID || items[0].TargetNodeID != childID || items[0].Action != domainsemantic.SemanticDirtyWorkActionRefresh || items[0].Reason != "node_updated" {
		t.Fatalf("unexpected subtree item: %+v", items[0])
	}
	if items[0].EarliestRunAt == nil || !items[0].EarliestRunAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected cooldown: %+v", items[0].EarliestRunAt)
	}
}

func TestAnalyzerUsesTargetCooldownOverride(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Content: "content"}}}
	now := time.Now().UTC()
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: now}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	baseCooldown := time.Minute
	overrideCooldown := 3 * time.Minute
	_, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, DirtyCooldown: baseCooldown, DirtyCooldownForTarget: func(_ context.Context, _ domainsemantic.SemanticIndex, targetID graph.NodeID, fallback time.Duration) (time.Duration, error) {
		if targetID != nodeID || fallback != baseCooldown {
			t.Fatalf("unexpected resolver input target=%s fallback=%s", targetID, fallback)
		}
		return overrideCooldown, nil
	}}).AnalyzeOnce(ctx, AnalyzeInput{Now: now})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 || items[0].EarliestRunAt == nil {
		t.Fatalf("unexpected items: %+v err=%v", items, err)
	}
	if !items[0].EarliestRunAt.Equal(now.Add(overrideCooldown)) {
		t.Fatalf("expected override cooldown %s, got %+v", overrideCooldown, items[0].EarliestRunAt)
	}
}

func TestAnalyzerRepeatedDirtyEventsPushOutCooldown(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Content: "content"}}}
	firstNow := time.Now().UTC()
	cooldown := time.Minute
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: firstNow}); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if _, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, DirtyCooldown: cooldown}).AnalyzeOnce(ctx, AnalyzeInput{Now: firstNow}); err != nil {
		t.Fatalf("first analyze failed: %v", err)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 || items[0].EarliestRunAt == nil {
		t.Fatalf("unexpected first items: %+v err=%v", items, err)
	}
	firstItem := items[0]

	secondNow := firstNow.Add(30 * time.Second)
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 2, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: secondNow}); err != nil {
		t.Fatalf("append second event: %v", err)
	}
	if _, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, DirtyCooldown: cooldown}).AnalyzeOnce(ctx, AnalyzeInput{Now: secondNow}); err != nil {
		t.Fatalf("second analyze failed: %v", err)
	}
	items, err = maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("unexpected second items: %+v err=%v", items, err)
	}
	secondItem := items[0]
	if secondItem.ID != firstItem.ID || secondItem.Generation <= firstItem.Generation || secondItem.EarliestRunAt == nil || !secondItem.EarliestRunAt.Equal(secondNow.Add(cooldown)) {
		t.Fatalf("cooldown not pushed out: first=%+v second=%+v", firstItem, secondItem)
	}
}

func TestAnalyzerMoveDirtiesOldAndNewSubtreeTargets(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	oldRootID := graph.NodeID(uuid.New())
	newRootID := graph.NodeID(uuid.New())
	childID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree, RecordTypes: []string{"page"}}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{
		nodes: map[graph.NodeID]graph.Node{
			oldRootID: {ID: oldRootID, DomainID: domainID, Content: "old"},
			newRootID: {ID: newRootID, DomainID: domainID, Content: "new"},
			childID:   {ID: childID, DomainID: domainID, Content: "child"},
		},
		parents: map[graph.NodeID]graph.NodeID{childID: newRootID},
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, ChangedEdges: []domainsemantic.GraphDirtyEdgeChange{{Labels: []string{"contains"}, Change: "updated", FromID: newRootID, ToID: childID}}, OldParentByNodeID: map[graph.NodeID]graph.NodeID{childID: oldRootID}, NewParentByNodeID: map[graph.NodeID]graph.NodeID{childID: newRootID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append move event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.EnqueuedItems != 3 {
		t.Fatalf("expected three enqueued targets, got %+v", res)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	seen := map[graph.NodeID]bool{}
	for _, item := range items {
		seen[item.TargetNodeID] = true
	}
	if !seen[oldRootID] || !seen[newRootID] {
		t.Fatalf("expected old and new roots, got %+v", items)
	}
}

func TestAnalyzerDeleteRefreshesContainingSubtreeRoot(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	rootID := graph.NodeID(uuid.New())
	deletedID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree, RecordTypes: []string{"page"}}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{rootID: {ID: rootID, DomainID: domainID, Content: "root"}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, DeletedNodeIDs: []graph.NodeID{deletedID}, OldParentByNodeID: map[graph.NodeID]graph.NodeID{deletedID: rootID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append delete event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.EnqueuedItems != 1 {
		t.Fatalf("expected one item, got %+v", res)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	if items[0].TargetNodeID != rootID || items[0].Action != domainsemantic.SemanticDirtyWorkActionRefresh || items[0].Reason != "node_deleted" {
		t.Fatalf("unexpected delete item: %+v", items[0])
	}
}

func TestAnalyzerDropsIrrelevantSelfPolicyNode(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf, RecordTypes: []string{"page"}}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Content: "block"}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.EnqueuedItems != 1 {
		t.Fatalf("expected unfiltered node to be enqueued, got %+v", res)
	}
}

func TestAnalyzerBatchesDirtyWorkUpserts(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	if _, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}); err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	nodes := map[graph.NodeID]graph.Node{}
	created := []graph.NodeID{}
	for i := 0; i < 5; i++ {
		nodeID := graph.NodeID(uuid.New())
		created = append(created, nodeID)
		nodes[nodeID] = graph.Node{ID: nodeID, DomainID: domainID, Content: fmt.Sprintf("node %d", i)}
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: created, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	recordingMgr := &recordingBatchMaintenanceManager{MaintenanceManager: maintenanceMgr}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: recordingMgr, GraphReader: fakeGraphReader{nodes: nodes}, MaxBatchSize: 2}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.EnqueuedItems != 5 {
		t.Fatalf("enqueued items = %d, want 5", res.EnqueuedItems)
	}
	if recordingMgr.singleCalls != 0 {
		t.Fatalf("single upsert calls = %d, want 0", recordingMgr.singleCalls)
	}
	want := []int{2, 2, 1}
	if fmt.Sprint(recordingMgr.batchSizes) != fmt.Sprint(want) {
		t.Fatalf("batch sizes = %v, want %v", recordingMgr.batchSizes, want)
	}
}

func TestAnalyzerSkipIndexDoesNotEnqueueDirtyWork(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	idx, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Content: "node"}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, SkipIndex: func(context.Context, domainsemantic.SemanticIndex) (bool, error) { return true, nil }}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 0 || res.EnqueuedItems != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		t.Fatalf("list dirty work failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no dirty work for skipped index %s, got %+v", idx.ID, items)
	}
}

func newAnalyzerManagers(t *testing.T, ctx context.Context, spaceID domainspace.SpaceID) (storesemantic.SpaceManager, storesemantic.MaintenanceManager) {
	t.Helper()
	root := t.TempDir()
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(root, "semantic"), spaceID); err != nil {
		t.Fatalf("space manager init failed: %v", err)
	}
	maintenanceMgr := storesemantic.NewMaintenanceManager()
	if err := maintenanceMgr.Init(ctx, filepath.Join(root, "semantic", "maintenance"), spaceID); err != nil {
		t.Fatalf("maintenance manager init failed: %v", err)
	}
	return spaceMgr, maintenanceMgr
}

type recordingBatchMaintenanceManager struct {
	storesemantic.MaintenanceManager
	batchSizes  []int
	singleCalls int
}

func (m *recordingBatchMaintenanceManager) UpsertDirtyWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	m.singleCalls++
	return m.MaintenanceManager.UpsertDirtyWorkItem(ctx, item)
}

func (m *recordingBatchMaintenanceManager) UpsertDirtyWorkItems(ctx context.Context, items []domainsemantic.SemanticDirtyWorkItem) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	m.batchSizes = append(m.batchSizes, len(items))
	if batcher, ok := m.MaintenanceManager.(dirtyWorkBatchUpserter); ok {
		return batcher.UpsertDirtyWorkItems(ctx, items)
	}
	out := make([]domainsemantic.SemanticDirtyWorkItem, 0, len(items))
	for _, item := range items {
		updated, err := m.MaintenanceManager.UpsertDirtyWorkItem(ctx, item)
		if err != nil {
			return out, err
		}
		out = append(out, updated)
	}
	return out, nil
}

type fakeGraphReader struct {
	nodes   map[graph.NodeID]graph.Node
	parents map[graph.NodeID]graph.NodeID
}

func (r fakeGraphReader) GetNode(_ context.Context, _ graph.DomainID, id graph.NodeID) (graph.Node, error) {
	if n, ok := r.nodes[id]; ok {
		return n, nil
	}
	return graph.Node{}, fmt.Errorf("not found")
}

func (r fakeGraphReader) Parent(_ context.Context, _ graph.DomainID, childID graph.NodeID) (*graph.Edge, error) {
	parentID, ok := r.parents[childID]
	if !ok || parentID == uuid.Nil {
		return nil, nil
	}
	return &graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: parentID, ToID: childID, Labels: []string{"contains"}}, nil
}

func TestAnalyzerSemanticRuleEnqueuesOneWorkItemPerBinding(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	rule := analyzerRule(spaceID, domainID, []string{"Note"}, []domainsemantic.SemanticEmbeddingBinding{
		analyzerBinding("search", true),
		analyzerBinding("summary", true),
		analyzerBinding("disabled", false),
	})
	rule, err := spaceMgr.UpsertSemanticRule(ctx, rule)
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Labels: []string{"Note"}}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 2 || res.EnqueuedItems != 2 {
		t.Fatalf("unexpected result %+v", res)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 2 {
		t.Fatalf("expected two work items, got %+v err=%v", items, err)
	}
	seen := map[string]bool{}
	for _, item := range items {
		if item.SemanticRuleID != rule.ID || item.SemanticIndexID != domainsemantic.SemanticIndexID(rule.ID) || item.TargetNodeID != nodeID {
			t.Fatalf("unexpected work item %+v", item)
		}
		seen[item.EmbeddingBindingKey] = true
	}
	if !seen["search"] || !seen["summary"] || seen["disabled"] {
		t.Fatalf("unexpected binding keys %+v", seen)
	}
}

func TestAnalyzerSemanticRuleFiltersLabelsAndBinding(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	noteID := graph.NodeID(uuid.New())
	taskID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	rule, err := spaceMgr.UpsertSemanticRule(ctx, analyzerRule(spaceID, domainID, []string{"Note"}, []domainsemantic.SemanticEmbeddingBinding{analyzerBinding("search", true), analyzerBinding("summary", true)}))
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{
		noteID: {ID: noteID, DomainID: domainID, Labels: []string{"Note"}},
		taskID: {ID: taskID, DomainID: domainID, Labels: []string{"Task"}},
	}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{noteID, taskID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{SemanticRuleID: rule.ID, EmbeddingBindingKey: "summary"})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.EnqueuedItems != 1 {
		t.Fatalf("expected one enqueued item, got %+v", res)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected one work item, got %+v err=%v", items, err)
	}
	if items[0].TargetNodeID != noteID || items[0].EmbeddingBindingKey != "summary" {
		t.Fatalf("unexpected filtered item %+v", items[0])
	}
}

func TestAnalyzerSemanticRuleTriggerAndCheckpointsAreBindingAware(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	rule := analyzerRule(spaceID, domainID, []string{"Note"}, []domainsemantic.SemanticEmbeddingBinding{analyzerBinding("search", true), analyzerBinding("summary", true)})
	rule.Trigger = domainsemantic.SemanticTriggerPolicy{Events: []string{"node_created"}}
	rule, err := spaceMgr.UpsertSemanticRule(ctx, rule)
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Labels: []string{"Note"}}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, UpdatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append update event failed: %v", err)
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 2, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("append create event failed: %v", err)
	}
	res, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 2 || res.EnqueuedItems != 2 {
		t.Fatalf("expected create event once per binding, got %+v", res)
	}
	for _, binding := range []string{"search", "summary"} {
		checkpoint, err := maintenanceMgr.GetCheckpoint(ctx, analyzerRuleBindingConsumer(rule.ID, binding))
		if err != nil {
			t.Fatalf("checkpoint %s error: %v", binding, err)
		}
		if checkpoint.LastGraphRevision != 2 {
			t.Fatalf("checkpoint %s revision = %d, want 2", binding, checkpoint.LastGraphRevision)
		}
	}
}

func analyzerRule(spaceID domainspace.SpaceID, domainID graph.DomainID, labels []string, bindings []domainsemantic.SemanticEmbeddingBinding) domainsemantic.SemanticGenerationRule {
	return domainsemantic.SemanticGenerationRule{
		SpaceID:    spaceID,
		DomainID:   domainID,
		Key:        "rule-" + uuid.NewString(),
		Enabled:    true,
		Selector:   domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: labels},
		Source:     domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf},
		Storage:    domainsemantic.DefaultSemanticStoragePolicy(),
		Embeddings: bindings,
	}
}

func analyzerBinding(key string, enabled bool) domainsemantic.SemanticEmbeddingBinding {
	return domainsemantic.SemanticEmbeddingBinding{Key: key, Purpose: "semantic_search", IntelligenceProfile: "default-embeddings", VectorStore: "local", Enabled: enabled}
}
