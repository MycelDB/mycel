package service

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSemanticProjectionLeaseFailsClosedAcrossLeadershipTerms(t *testing.T) {
	ctx := context.Background()
	module := NewModule(Config{GraphReader: &recordingGraphReader{}})
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}, CreatedAt: now, UpdatedAt: now}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildEpoch: "epoch-1", RebuildState: "completed"}}}); err != nil {
		t.Fatal(err)
	}
	var term atomic.Uint64
	term.Store(7)
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) {
		return 1, term.Load(), true, nil
	}
	leasedCtx, err := module.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.saveProjectionLease(leasedCtx, spaceID, ruleID, "search", 2, "epoch-1", 5); err != nil {
		t.Fatal(err)
	}
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"}
	if err := module.validateLocalProjectionForSearch(ctx, search); err != nil {
		t.Fatalf("current-term projection rejected: %v", err)
	}
	maintenance, err := module.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: reliabilityAnalyzerConsumer(ruleID, "search"), SpaceID: spaceID, LastGraphRevision: 6}); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("stale-frontier search error=%v, want fail-closed readiness error", err)
	}
	if _, err := module.ProcessDirtyWork(ctx, ProcessInput{SpaceID: spaceID, Limit: 1}); err != nil {
		t.Fatalf("advance projection after maintenance progression: %v", err)
	}
	if err := module.validateLocalProjectionForSearch(ctx, search); err != nil {
		t.Fatalf("advanced projection readiness rejected: %v", err)
	}
	snapshot, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, true, nil)
	if err != nil {
		t.Fatalf("advanced projection snapshot rejected: %v", err)
	}
	key := semanticProjectionBindingKey{ruleID: ruleID, bindingKey: "search"}
	if snapshot[key].freshThrough != 6 {
		t.Fatalf("advanced projection snapshot freshness=%d, want authoritative checkpoint 6", snapshot[key].freshThrough)
	}
	if _, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, false, snapshot); err != nil {
		t.Fatalf("unchanged advanced projection failed post-search validation: %v", err)
	}
	term.Store(8)
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("stale-term search error=%v, want fail-closed readiness error", err)
	}
}

func TestSemanticProjectionLeaseAllowsFailClosedEmptySearchWhileRebuildBlocksResults(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	store, err := module.GlobalManager().UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: domainsemantic.VectorStoreID(uuid.New()), Key: "lease-store", Name: "Lease store", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	storeID := store.ID
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	reliability := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildState: "required"}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "stale", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": reliability}}); err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 1, 7, true, nil }
	leasedCtx, err := module.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.saveProjectionLease(leasedCtx, spaceID, ruleID, "search", 1, "", 8); err != nil {
		t.Fatal(err)
	}
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search", Text: "query", Limit: 3}
	if _, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, true, nil); err != nil {
		t.Fatalf("rebuild-blocked empty search required a projection lease: %v", err)
	}
	searched := map[semanticProjectionBindingKey]semanticProjectionObservation{{ruleID: ruleID, bindingKey: "search"}: {generation: 1, freshThrough: 8}}
	if _, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, false, searched); err == nil || !strings.Contains(err.Error(), "changed rebuild state") {
		t.Fatalf("searched projection did not reject concurrent rebuild state: %v", err)
	}
	result, err := module.Search(ctx, search)
	if err != nil || len(result.Results) != 0 || len(result.Warnings) == 0 {
		t.Fatalf("rebuild-blocked search result=%+v err=%v, want empty warning result", result, err)
	}
	reliability.RebuildState = "completed"
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": reliability}}); err != nil {
		t.Fatal(err)
	}
	if err := module.validateLocalProjectionForSearch(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("servable projection without lease error=%v, want readiness rejection", err)
	}
	reliability.RebuildEpoch = "epoch-b"
	reliability.FreshThrough = 9
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": reliability}}); err != nil {
		t.Fatal(err)
	}
	if err := module.saveProjectionLease(leasedCtx, spaceID, ruleID, "search", 2, "epoch-b", 9); err != nil {
		t.Fatal(err)
	}
	if _, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, false, searched); err == nil || !strings.Contains(err.Error(), "changed provenance") {
		t.Fatalf("completed projection ABA error=%v, want provenance rejection", err)
	}
	searchedB := map[semanticProjectionBindingKey]semanticProjectionObservation{{ruleID: ruleID, bindingKey: "search"}: {generation: 2, rebuildEpoch: "epoch-b", freshThrough: 9}}
	rule.Enabled = false
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := module.validateLocalProjectionForSearchWithRebuildBlock(ctx, search, false, searchedB); err == nil || !strings.Contains(err.Error(), "changed eligibility") {
		t.Fatalf("disabled searched binding error=%v, want eligibility rejection", err)
	}
}

func TestForcedBackfillRequiresReconciliationAndResolvesCurrentProvenanceForProjectionReadiness(t *testing.T) {
	ctx := context.Background()
	module := NewModule(Config{GraphReader: &recordingGraphReader{}, GraphRevisionReader: fixedGraphRevisionReader{revision: 5}})
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	now := time.Now().UTC()
	if _, err := module.GlobalManager().UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "stale-lease-store", Name: "Stale lease store", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}, CreatedAt: now, UpdatedAt: now}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 3, RebuildEpoch: "epoch-3", RebuildState: "completed", BaselineFreshThrough: 5}}}); err != nil {
		t.Fatal(err)
	}
	backend := module.localVectorBackend()
	stale, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, TargetNodeID: nodeID, SourceHash: "sha256:stale", SourceMode: "self", SourceGraphRevision: 1, SemanticGeneration: 3, RebuildEpoch: "epoch-3", SemanticConfigurationHash: "stale-config", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 1, 7, true, nil }
	input := semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", Force: true, Limit: 100, SourceGraphRevision: 5, PinSourceGraphRevision: true}
	if _, err := module.BackfillIndex(ctx, input); err != nil {
		t.Fatalf("non-reconciling forced backfill: %v", err)
	}
	module.projectionLeasesMu.Lock()
	leases, err := module.loadProjectionLeasesLocked(spaceID)
	module.projectionLeasesMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(leases.Entries) != 0 {
		t.Fatalf("non-reconciling backfill certified projection: %+v", leases.Entries)
	}
	input.Reconcile = true
	staleInput := input
	staleInput.SemanticGeneration = 2
	if _, err := module.BackfillIndex(ctx, staleInput); err == nil || !strings.Contains(err.Error(), "stale semantic generation") {
		t.Fatalf("explicit stale provenance error=%v, want rejection", err)
	}
	if _, err := module.BackfillIndex(ctx, input); err != nil {
		t.Fatalf("reconciling forced backfill: %v", err)
	}
	latest, found, err := backend.LatestRecord(ctx, vectorstore.LatestRecordInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID})
	if err != nil || !found || !latest.Tombstone || latest.DeleteTargetRecordID != stale.ID {
		t.Fatalf("stale record was not reconciled: found=%t record=%+v err=%v", found, latest, err)
	}
	module.projectionLeasesMu.Lock()
	leases, err = module.loadProjectionLeasesLocked(spaceID)
	module.projectionLeasesMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(leases.Entries) != 1 || leases.Entries[0].SemanticGeneration != 3 || leases.Entries[0].RebuildEpoch != "epoch-3" || leases.Entries[0].FreshThrough != 5 {
		t.Fatalf("projection lease did not use current provenance: %+v", leases.Entries)
	}
	status, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Rebuild.RebuildState != "completed" || status.Rebuild.RebuildOwnerNode != 1 || status.Rebuild.RebuildOwnerTerm != 7 || status.SemanticGeneration != 3 {
		t.Fatalf("forced baseline did not complete current reliability provenance: %+v", status.Rebuild)
	}
	rotated := status.Rebuild
	rotated.SemanticGeneration = 4
	rotated.RebuildEpoch = ""
	rotated.RebuildState = "required"
	rotated.RebuildErrorSanitized = "configuration changed"
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "stale", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": rotated}}); err != nil {
		t.Fatal(err)
	}
	boundCtx, err := module.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.completeSemanticBackfillBaseline(boundCtx, semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticGeneration: 3, RebuildEpoch: "epoch-3", SourceGraphRevision: 5}); err == nil || !strings.Contains(err.Error(), "stale semantic generation") {
		t.Fatalf("stale completion after configuration rotation error=%v", err)
	}
	status, err = module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if status.SemanticGeneration != 4 || status.Rebuild.RebuildState != "required" {
		t.Fatalf("stale completion overwrote rotated reliability state: %+v", status.Rebuild)
	}
}

func TestSemanticProjectionLeaseRequiresLocalBaselineOnPromotedNode(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 1}}}); err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 2
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 2, 9, true, nil }
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"}
	if _, err := module.Search(ctx, search); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("empty promoted-node search error=%v, want fail-closed readiness error", err)
	}
}
