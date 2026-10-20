package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestStaleSemanticWorkAndGenerationPublicationAreRejected(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	spaceID := domainspace.SpaceID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	bindingKey := "search"
	module := NewModule()
	if result := module.Init(ctx, runtime); !result.OK {
		t.Fatal(result.Error)
	}
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{bindingKey: {EmbeddingBindingKey: bindingKey, SemanticGeneration: 2}}}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := module.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	item, err := maintenance.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, TargetNodeID: graph.NodeID(uuid.New())})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := maintenance.ClaimReadyWork(ctx, storesemantic.ClaimReadyWorkInput{ClaimedBy: "worker", Now: time.Now().UTC(), Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim work: items=%v err=%v", claimed, err)
	}
	if err := module.authorizeSemanticWorkItem(ctx, claimed[0]); err != nil {
		t.Fatalf("current work was rejected: %v", err)
	}
	newer, err := maintenance.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, TargetNodeID: item.TargetNodeID})
	if err != nil {
		t.Fatal(err)
	}
	if newer.Generation <= claimed[0].Generation || module.authorizeSemanticWorkItem(ctx, claimed[0]) == nil {
		t.Fatal("stale claimed generation was not rejected")
	}
	if err := module.authorizeSemanticPublication(ctx, semanticbackfill.Publication{Input: semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, SemanticGeneration: 1}}); err == nil {
		t.Fatal("stale semantic configuration generation was not rejected")
	}
}

func TestMarkRuleConfigurationChangedRotatesGenerationAndRequiresRebuild(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	spaceID := domainspace.SpaceID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	module := NewModule()
	if result := module.Init(ctx, runtime); !result.OK {
		t.Fatal(result.Error)
	}
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 4, BaselineFreshThrough: 22, RebuildState: "completed"}}}); err != nil {
		t.Fatal(err)
	}
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Enabled: true}}}
	if err := module.MarkRuleConfigurationChanged(ctx, spaceID, rule); err != nil {
		t.Fatal(err)
	}
	reloadedMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	states, err := reloadedMgr.ListSemanticRuleStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := states[0].ReliabilityByBinding["search"]
	if got.SemanticGeneration != 5 || got.BaselineFreshThrough != 0 || got.RebuildState != "required" || got.RebuildErrorSanitized == "" {
		t.Fatalf("unexpected rotated reliability state: %+v", got)
	}
}

func TestReliabilityStatusKeepsAnalyzedAndFreshFrontiersDistinctAcrossReload(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	bindingKey := "search"
	newModule := func() *Module {
		module := NewModule(Config{GraphRevisionReader: fixedGraphRevisionReader{revision: 15}})
		if result := module.Init(ctx, runtime); !result.OK {
			t.Fatalf("init module: %v", result.Error)
		}
		return module
	}
	module := newModule()
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: bindingKey, Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{bindingKey: {EmbeddingBindingKey: bindingKey, SemanticGeneration: 3, BaselineFreshThrough: 9, RebuildEpoch: "epoch-3", RebuildState: "completed", PinnedTargetRevision: 9, DeltaCatchupRevision: 9, RebuildStartedAt: &started, RebuildCompletedAt: &started}}}); err != nil {
		t.Fatal(err)
	}
	maintenanceMgr, err := module.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenanceMgr.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: reliabilityAnalyzerConsumer(ruleID, bindingKey), SpaceID: spaceID, LastGraphRevision: 12}); err != nil {
		t.Fatal(err)
	}
	if _, err := maintenanceMgr.AppendGraphDirtyEvent(ctx, domainsemantic.GraphDirtyEvent{ID: domainsemantic.GraphDirtyEventID(uuid.New()), TxnID: uuid.New(), GraphRevision: 15, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CommittedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, SpaceID: spaceID, DomainID: domainID, TargetNodeID: graph.NodeID(uuid.New()), FirstGraphRevision: 10, LastGraphRevision: 10, Status: domainsemantic.SemanticDirtyWorkStatusComplete}); err != nil {
		t.Fatal(err)
	}
	pending, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, SpaceID: spaceID, DomainID: domainID, TargetNodeID: graph.NodeID(uuid.New()), FirstGraphRevision: 11, LastGraphRevision: 14, Status: domainsemantic.SemanticDirtyWorkStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	assertStatus := func(module *Module, wantFresh uint64, wantGaps uint64) {
		t.Helper()
		status, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey})
		if err != nil {
			t.Fatal(err)
		}
		if status.LatestObservedGraphRevision != 15 || status.AnalyzedThroughGraphRevision != 12 || status.FreshThroughGraphRevision != wantFresh || status.UnresolvedGapCount != wantGaps || status.SemanticGeneration < 3 || status.ConfiguredFrontierLimit == 0 {
			t.Fatalf("unexpected reliability status: %+v", status)
		}
		if wantGaps > 0 && status.OldestUnresolvedGraphRevision != 11 {
			t.Fatalf("oldest unresolved revision = %d, want 11", status.OldestUnresolvedGraphRevision)
		}
	}
	assertStatus(module, 10, 1)

	reloaded := NewModule(Config{GraphRevisionReader: fixedGraphRevisionReader{revision: 15}})
	reloaded.dataDir = module.dataDir
	assertStatus(reloaded, 10, 1)
	reloadedMaintenance, err := reloaded.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloadedMaintenance.CompleteWork(ctx, pending.ID, storesemantic.WorkResult{Generation: pending.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := reloadedMaintenance.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: reliabilityAnalyzerConsumer(ruleID, bindingKey), SpaceID: spaceID, LastGraphRevision: 15}); err != nil {
		t.Fatal(err)
	}
	status, err := reloaded.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey})
	if err != nil {
		t.Fatal(err)
	}
	if status.FreshThroughGraphRevision != 15 || status.UnresolvedGapCount != 0 || status.AnalyzedThroughGraphRevision != 15 {
		t.Fatalf("unexpected converged status: %+v", status)
	}
}
