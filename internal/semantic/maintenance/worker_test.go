package maintenance

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestWorkerCompletesRefreshWithNonForcedBackfill(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newWorkerManagers(t, ctx, spaceID, domainID, indexID)
	item, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, Action: domainsemantic.SemanticDirtyWorkActionRefresh})
	if err != nil {
		t.Fatalf("upsert work: %v", err)
	}
	runner := &fakeBackfillRunner{}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: runner, Config: WorkerConfig{MaxBatchSize: 10}}).ProcessOnce(ctx, 0)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Processed != 1 || res.Completed != 1 || res.Failed != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(runner.inputs) != 1 || runner.inputs[0].Force || len(runner.inputs[0].NodeIDs) != 1 || runner.inputs[0].NodeIDs[0] != nodeID {
		t.Fatalf("unexpected backfill input: %+v", runner.inputs)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	if len(items) != 1 || items[0].ID != item.ID || items[0].Status != domainsemantic.SemanticDirtyWorkStatusComplete || items[0].CompletedAt == nil {
		t.Fatalf("work not completed: %+v", items)
	}
}

func TestWorkerRetryableFailureUsesBackoff(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newWorkerManagers(t, ctx, spaceID, domainID, indexID)
	item, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, Action: domainsemantic.SemanticDirtyWorkActionRefresh})
	if err != nil {
		t.Fatalf("upsert work: %v", err)
	}
	runner := &fakeBackfillRunner{err: errors.New("rate limit exceeded")}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: runner, Config: WorkerConfig{RetryBaseDelay: time.Minute, RetryMaxDelay: time.Hour}}).ProcessOnce(ctx, 1)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Processed != 1 || res.Completed != 0 || res.Failed != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	got := findWorkItem(items, item.ID)
	if got.Status != domainsemantic.SemanticDirtyWorkStatusPending || got.LastErrorCategory != "rate_limited" || got.EarliestRunAt == nil || got.Attempts != 1 {
		t.Fatalf("unexpected retry item: %+v", got)
	}
}

func TestWorkerPermanentFailureMarksFailed(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newWorkerManagers(t, ctx, spaceID, domainID, indexID)
	item, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, Action: domainsemantic.SemanticDirtyWorkActionRefresh})
	if err != nil {
		t.Fatalf("upsert work: %v", err)
	}
	runner := &fakeBackfillRunner{err: errors.New("semantic index disabled")}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: runner}).ProcessOnce(ctx, 1)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Failed != 1 {
		t.Fatalf("expected failure result, got %+v", res)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	got := findWorkItem(items, item.ID)
	if got.Status != domainsemantic.SemanticDirtyWorkStatusFailed || got.LastErrorCategory != "configuration_error" || got.FailedAt == nil {
		t.Fatalf("unexpected failed item: %+v", got)
	}
}

func TestWorkerPoolProcessesMultipleItems(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	spaceMgr, maintenanceMgr := newWorkerManagers(t, ctx, spaceID, domainID, indexID)
	for i := 0; i < 3; i++ {
		if _, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: graph.NodeID(uuid.New()), Action: domainsemantic.SemanticDirtyWorkActionRefresh}); err != nil {
			t.Fatalf("upsert work %d: %v", i, err)
		}
	}
	runner := &fakeBackfillRunner{}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: runner, Config: WorkerConfig{WorkerCount: 2, MaxBatchSize: 3}}).ProcessOnce(ctx, 0)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Processed != 3 || res.Completed != 3 || runner.callCount() != 3 {
		t.Fatalf("unexpected pool result=%+v calls=%d", res, runner.callCount())
	}
}

func TestWorkerDeleteUsesVectorBackend(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	spaceMgr, maintenanceMgr := newWorkerManagers(t, ctx, spaceID, domainID, indexID)
	item, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, Action: domainsemantic.SemanticDirtyWorkActionDelete, Reason: "node.deleted"})
	if err != nil {
		t.Fatalf("upsert delete work: %v", err)
	}
	backend := &fakeVectorBackend{}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: &fakeBackfillRunner{}, VectorBackend: backend}).ProcessOnce(ctx, 1)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Completed != 1 || len(backend.deletes) != 1 || backend.deletes[0].NodeID != nodeID || backend.deletes[0].Reason != "node.deleted" {
		t.Fatalf("unexpected delete result=%+v deletes=%+v", res, backend.deletes)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	if got := findWorkItem(items, item.ID); got.Status != domainsemantic.SemanticDirtyWorkStatusComplete {
		t.Fatalf("delete item not complete: %+v", got)
	}
}

func TestWorkerDeleteRemovesRuleBindingsIncludingKeyOnlyStore(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	parentID := graph.NodeID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	globalMgr := storesemantic.NewGlobalManager()
	if err := globalMgr.Init(ctx, filepath.Join(root, "meta")); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := globalMgr.UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatal(err)
	}
	bindings := []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStore: "mycel-file", Enabled: true}, {Key: "summary", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSubtree}, Embeddings: bindings}); err != nil {
		t.Fatal(err)
	}
	maintenanceMgr := storesemantic.NewMaintenanceManager()
	if err := maintenanceMgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic", "maintenance"), spaceID); err != nil {
		t.Fatal(err)
	}
	backend := vectorstore.MycelFileBackend{GraphsDir: filepath.Join(root, "graphs")}
	for _, binding := range bindings {
		for _, selectedNodeID := range []graph.NodeID{parentID, nodeID} {
			if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: selectedNodeID, TargetNodeID: selectedNodeID, SourceMode: "subtree", SourceGraphRevision: 4, SemanticGeneration: 1, SemanticConfigurationHash: "config", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	reader := fakeGraphReader{nodes: map[graph.NodeID]graph.Node{parentID: {ID: parentID, DomainID: domainID, Labels: []string{"Document"}}}}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{TxnID: uuid.New(), GraphRevision: 5, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, DeletedNodeIDs: []graph.NodeID{nodeID}, OldParentByNodeID: map[graph.NodeID]graph.NodeID{nodeID: parentID}, CommittedAt: now}); err != nil {
		t.Fatal(err)
	}
	analyzed, err := (Analyzer{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, GraphReader: reader}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil || analyzed.EnqueuedItems != 4 {
		t.Fatalf("subtree delete classification: result=%+v err=%v", analyzed, err)
	}
	runner := &fakeBackfillRunner{}
	injectedFailures := 0
	backend.AfterSegmentSync = func(record domainsemantic.AdvancedEmbeddingRecord) error {
		if record.Tombstone && injectedFailures == 0 {
			injectedFailures++
			return errors.New("vector store temporarily unavailable during tombstone publication")
		}
		return nil
	}
	instrumentedBackend := &countingLatestRecordBackend{Backend: backend, Latest: backend, Lister: backend}
	worker := Worker{GlobalManager: globalMgr, SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: runner, VectorBackend: instrumentedBackend, Config: WorkerConfig{MaxBatchSize: 4, RetryBaseDelay: time.Nanosecond, RetryMaxDelay: time.Nanosecond}}
	first, err := worker.ProcessOnce(ctx, 4)
	if err != nil || first.Completed != 3 || first.Failed != 1 {
		t.Fatalf("first delete processing: result=%+v err=%v", first, err)
	}
	time.Sleep(time.Millisecond)
	result, err := worker.ProcessOnce(ctx, 4)
	if err != nil || result.Completed != 1 || result.Failed != 0 || runner.callCount() != 2 {
		t.Fatalf("delete retry processing: result=%+v refresh_calls=%d err=%v", result, runner.callCount(), err)
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 4 {
		t.Fatalf("list completed subtree work: items=%+v err=%v", items, err)
	}
	for _, item := range items {
		if item.Status != domainsemantic.SemanticDirtyWorkStatusComplete {
			t.Fatalf("physical repair left freshness barrier unresolved: %+v", item)
		}
	}
	if instrumentedBackend.HistoricalLists != 0 {
		t.Fatalf("ordinary delete performed %d historical record scans", instrumentedBackend.HistoricalLists)
	}
	history, err := backend.ListRecords(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID))
	if err != nil || len(history) != 6 {
		t.Fatalf("delete retry duplicated append history: records=%+v err=%v", history, err)
	}
	tombstones := 0
	for _, record := range history {
		if record.Tombstone {
			tombstones++
		}
	}
	if tombstones != 2 {
		t.Fatalf("expected one tombstone per binding, got %d in %+v", tombstones, history)
	}
	for _, binding := range bindings {
		search := func(stage string) {
			t.Helper()
			results, err := backend.Search(ctx, vectorstore.SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), VectorStoreID: storeID, VectorSpaceKey: "test/2", Query: []float64{1, 0}, Limit: 10})
			if err != nil || len(results) != 1 || results[0].NodeID != parentID {
				t.Fatalf("binding %q did not retain only the parent %s delete: results=%+v err=%v", binding.Key, stage, results, err)
			}
		}
		search("immediately after")
		key := vectorstore.SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: binding.Key, VectorStoreID: storeID, VectorSpaceKey: "test/2"}
		if _, err := backend.RebuildSearchIndex(ctx, key, vectorstore.RebuildLimit{}); err != nil {
			t.Fatal(err)
		}
		search("after physical index reconstruction following")
	}
}

type countingLatestRecordBackend struct {
	vectorstore.Backend
	Latest          vectorstore.LatestRecordStore
	Lister          vectorstore.RecordLister
	HistoricalLists int
}

func (b *countingLatestRecordBackend) LatestRecord(ctx context.Context, in vectorstore.LatestRecordInput) (domainsemantic.AdvancedEmbeddingRecord, bool, error) {
	return b.Latest.LatestRecord(ctx, in)
}

func (b *countingLatestRecordBackend) EnsureSearchIndexRecord(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) error {
	return b.Latest.EnsureSearchIndexRecord(ctx, record)
}

func (b *countingLatestRecordBackend) ListRecords(ctx context.Context, spaceID domainspace.SpaceID, semanticIndexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	b.HistoricalLists++
	return b.Lister.ListRecords(ctx, spaceID, semanticIndexID)
}

func newWorkerManagers(t *testing.T, ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, indexID domainsemantic.SemanticIndexID) (storesemantic.SpaceManager, storesemantic.MaintenanceManager) {
	t.Helper()
	spaceMgr, maintenanceMgr := newAnalyzerManagers(t, ctx, spaceID)
	_, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{ID: indexID, SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true})
	if err != nil {
		t.Fatalf("upsert index: %v", err)
	}
	return spaceMgr, maintenanceMgr
}

func findWorkItem(items []domainsemantic.SemanticDirtyWorkItem, id uuid.UUID) domainsemantic.SemanticDirtyWorkItem {
	for _, item := range items {
		if item.ID == id {
			return item
		}
	}
	return domainsemantic.SemanticDirtyWorkItem{}
}

type fakeBackfillRunner struct {
	mu     sync.Mutex
	inputs []backfill.Input
	err    error
}

func (r *fakeBackfillRunner) Run(_ context.Context, in backfill.Input) (backfill.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, in)
	return backfill.Result{}, r.err
}

func (r *fakeBackfillRunner) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.inputs)
}

type fakeVectorBackend struct {
	deletes []vectorstore.DeleteInput
}

func (b *fakeVectorBackend) Upsert(_ context.Context, rec domainsemantic.AdvancedEmbeddingRecord) (domainsemantic.AdvancedEmbeddingRecord, error) {
	return rec, nil
}
func (b *fakeVectorBackend) Search(context.Context, vectorstore.SearchInput) ([]vectorstore.SearchResult, error) {
	return nil, nil
}
func (b *fakeVectorBackend) Delete(_ context.Context, in vectorstore.DeleteInput) (domainsemantic.AdvancedEmbeddingRecord, error) {
	b.deletes = append(b.deletes, in)
	return domainsemantic.AdvancedEmbeddingRecord{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, Tombstone: true}, nil
}
func (b *fakeVectorBackend) VerifyDeleted(context.Context, vectorstore.VerifyDeletedInput) (bool, error) {
	return true, nil
}
