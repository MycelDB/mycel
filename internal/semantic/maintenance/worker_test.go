package maintenance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storesemantic "github.com/myceldb/mycel/store/semantic"
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
	item, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, Action: domainsemantic.SemanticDirtyWorkActionDelete, Reason: "node_deleted"})
	if err != nil {
		t.Fatalf("upsert delete work: %v", err)
	}
	backend := &fakeVectorBackend{}
	res, err := (Worker{SpaceManager: spaceMgr, MaintenanceManager: maintenanceMgr, Backfill: &fakeBackfillRunner{}, VectorBackend: backend}).ProcessOnce(ctx, 1)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Completed != 1 || len(backend.deletes) != 1 || backend.deletes[0].NodeID != nodeID || backend.deletes[0].Reason != "node_deleted" {
		t.Fatalf("unexpected delete result=%+v deletes=%+v", res, backend.deletes)
	}
	items, _ := maintenanceMgr.ListDirtyWorkItems(ctx)
	if got := findWorkItem(items, item.ID); got.Status != domainsemantic.SemanticDirtyWorkStatusComplete {
		t.Fatalf("delete item not complete: %+v", got)
	}
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
