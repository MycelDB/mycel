package service

import (
	"context"
	"errors"
	"strings"
	"sync"
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
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestRuleScopedPublicationFencesDoNotSerializeIndependentRules(t *testing.T) {
	module := NewModule()
	spaceID := domainspace.SpaceID(uuid.New())
	unlockFirst := module.lockSemanticPublication(spaceID, domainsemantic.SemanticRuleID(uuid.New()))
	acquiredSecond := make(chan struct{}, 1)
	go func() {
		unlockSecond := module.lockSemanticPublication(spaceID, domainsemantic.SemanticRuleID(uuid.New()))
		acquiredSecond <- struct{}{}
		unlockSecond()
	}()
	select {
	case <-acquiredSecond:
	case <-time.After(time.Second):
		unlockFirst()
		t.Fatal("independent semantic rules were serialized by publication fencing")
	}
	unlockFirst()
	module.publicationFencesMu.Lock()
	defer module.publicationFencesMu.Unlock()
	if len(module.publicationFences) != 0 {
		t.Fatalf("released publication fences retained %d entries", len(module.publicationFences))
	}
}

func TestReliabilityTransitionRejectsNewerGenerationState(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 3, RebuildEpoch: "new", RebuildState: "required"}}}); err != nil {
		t.Fatal(err)
	}
	err = module.transitionReliabilityState(ctx, spaceID, ruleID, "search", 2, "old", "running", func(state *domainsemantic.SemanticReliabilityState) { state.RebuildState = "completed" })
	if err == nil || err.Error() != "stale semantic reliability transition rejected" {
		t.Fatalf("stale transition error = %v", err)
	}
	states, err := spaceMgr.ListSemanticRuleStates(ctx)
	if err != nil || len(states) != 1 {
		t.Fatalf("list states: count=%d err=%v", len(states), err)
	}
	if got := states[0].ReliabilityByBinding["search"]; got.SemanticGeneration != 3 || got.RebuildEpoch != "new" || got.RebuildState != "required" {
		t.Fatalf("stale transition overwrote newer state: %+v", got)
	}
}

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

func TestSemanticPublicationRejectsOlderSameEpochRecord(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildEpoch: "epoch", RebuildState: "running"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := module.localVectorBackend().Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, TargetNodeID: nodeID, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}, SourceGraphRevision: 11, SemanticGeneration: 2, RebuildEpoch: "epoch", SemanticConfigurationHash: "config"}); err != nil {
		t.Fatal(err)
	}
	publication := semanticbackfill.Publication{Input: semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SourceGraphRevision: 10, PinSourceGraphRevision: true, SemanticGeneration: 2, RebuildEpoch: "epoch"}, NodeID: nodeID}
	release, err := module.acquireSemanticPublication(ctx, publication)
	if release != nil {
		release()
	}
	if !errors.Is(err, semanticbackfill.ErrSupersededPublication) {
		t.Fatalf("older publication error = %v", err)
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

func TestReliabilityStatusRotatesOnlyWhenSelectedRouteChanges(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	profileID := domainsemantic.IntelligenceProfileID(uuid.New())
	unusedProfileID := domainsemantic.IntelligenceProfileID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	capabilityID := domainsemantic.ModelEndpointCapabilityID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	now := time.Now().UTC()
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertIntelligenceProfile(ctx, domainsemantic.IntelligenceProfile{ID: profileID, SpaceID: spaceID, Key: "profile", Operation: domainsemantic.OperationEmbeddings, EndpointRefs: []string{endpointID.String()}, ModelRefs: []string{modelID.String()}, CapabilityRefs: []string{capabilityID.String()}, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertIntelligenceProfile(ctx, domainsemantic.IntelligenceProfile{ID: unusedProfileID, SpaceID: spaceID, Key: "unused", Operation: domainsemantic.OperationEmbeddings, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	global := module.GlobalManager()
	if _, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{ID: endpointID, Key: "selected-route", Name: "selected-route", ConnectorType: domainsemantic.ConnectorCustomHTTP, NetworkClass: domainsemantic.NetworkClassPrivateNetwork, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{ID: modelID, Key: "selected-model", Kind: domainsemantic.ModelKindEmbedding, ModelName: "selected-model", Dimensions: 8, VectorSpaceKey: "selected/8", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := global.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ID: capabilityID, ModelEndpointID: endpointID, ModelID: modelID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := global.UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: profileID, IntelligenceProfile: "unused", VectorStoreID: storeID, Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 1, RebuildState: "completed"}}}); err != nil {
		t.Fatal(err)
	}
	initial, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil || initial.Rebuild.ResolvedConfigurationHash == "" {
		t.Fatalf("unexpected initial status: %+v err=%v", initial, err)
	}
	if _, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{ID: endpointID, Key: "selected-route", Name: "renamed selected route", ConnectorType: domainsemantic.ConnectorCustomHTTP, NetworkClass: domainsemantic.NetworkClassPrivateNetwork, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	presentationOnly, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil || presentationOnly.SemanticGeneration != 1 || presentationOnly.RebuildRequired {
		t.Fatalf("presentation-only route change rotated generation: %+v err=%v", presentationOnly, err)
	}
	if _, err := spaceMgr.UpsertIntelligenceProfile(ctx, domainsemantic.IntelligenceProfile{ID: unusedProfileID, SpaceID: spaceID, Key: "unused", Operation: domainsemantic.OperationEmbeddings, RequiredFeatures: []string{"changed-but-unused"}, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{ID: domainsemantic.ModelEndpointID(uuid.New()), Key: "unrelated-route", Name: "unrelated-route", ConnectorType: domainsemantic.ConnectorCustomHTTP, NetworkClass: domainsemantic.NetworkClassPrivateNetwork, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	unchanged, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil || unchanged.SemanticGeneration != 1 || unchanged.RebuildRequired {
		t.Fatalf("unrelated catalog change rotated generation: %+v err=%v", unchanged, err)
	}
	selectedModel := domainsemantic.InferenceModel{ID: modelID, Key: "selected-model", Kind: domainsemantic.ModelKindEmbedding, ModelName: "selected-model", Dimensions: 16, VectorSpaceKey: "selected/16", CreatedAt: now, UpdatedAt: now.Add(time.Second)}
	if _, err := global.UpsertModel(ctx, selectedModel); err != nil {
		t.Fatal(err)
	}
	changed, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil || changed.SemanticGeneration != 2 || !changed.RebuildRequired || changed.Rebuild.RebuildState != "required" {
		t.Fatalf("selected route change did not require rebuild: %+v err=%v", changed, err)
	}
}

func TestRebuildSemanticRuleCompletesAtNonzeroPinnedRevision(t *testing.T) {
	ctx := context.Background()
	module := NewModule(Config{GraphReader: &recordingGraphReader{}, GraphRevisionReader: fixedGraphRevisionReader{revision: 5}})
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	now := time.Now().UTC()
	if _, err := module.GlobalManager().UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "stale", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildState: "required"}}}); err != nil {
		t.Fatal(err)
	}
	maintenanceMgr, err := module.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", TargetNodeID: graph.NodeID(uuid.New()), FirstGraphRevision: 5, LastGraphRevision: 5, Status: domainsemantic.SemanticDirtyWorkStatusCancelled}); err != nil {
		t.Fatal(err)
	}
	before, err := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err != nil || before.UnresolvedGapCount != 1 {
		t.Fatalf("cancelled work did not remain a reliability gap: status=%+v err=%v", before, err)
	}
	result, err := module.RebuildSemanticRule(ctx, RebuildInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", BackfillLimit: 1, MaintenanceBatchLimit: 1, MaxCatchupIterations: 2})
	if err != nil {
		t.Fatalf("RebuildSemanticRule() error = %v", err)
	}
	if result.Reliability.Rebuild.RebuildState != "completed" || result.Reliability.FreshThroughGraphRevision != 5 || result.Reliability.Rebuild.PinnedTargetRevision != 5 {
		t.Fatalf("unexpected rebuild result: %+v", result)
	}
	states, err := spaceMgr.ListSemanticRuleStates(ctx)
	if err != nil || len(states) != 1 {
		t.Fatalf("list rule states: count=%d err=%v", len(states), err)
	}
	running := states[0].ReliabilityByBinding["search"]
	running.RebuildState = "running"
	states[0].ReliabilityByBinding["search"] = running
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, states[0]); err != nil {
		t.Fatal(err)
	}
	_, err = module.RebuildSemanticRule(ctx, RebuildInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if err == nil || err.Error() != "semantic rebuild is already running" {
		t.Fatalf("concurrent rebuild error = %v", err)
	}
}

func TestRebuildSemanticRuleFailsWhenLeadershipChangesDuringBackfill(t *testing.T) {
	ctx := context.Background()
	var term atomic.Uint64
	term.Store(7)
	graphs := &leadershipChangingGraphReader{change: func() { term.Store(8) }}
	module := NewModule(Config{GraphReader: graphs, GraphRevisionReader: fixedGraphRevisionReader{revision: 5}})
	if result := module.Init(ctx, testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(got domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) {
		if got != spaceID {
			t.Fatalf("leadership probe space=%s, want %s", got, spaceID)
		}
		return 1, term.Load(), true, nil
	}
	now := time.Now().UTC()
	if _, err := module.GlobalManager().UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "stale", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildState: "required"}}}); err != nil {
		t.Fatal(err)
	}
	_, err = module.RebuildSemanticRule(ctx, RebuildInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", BackfillLimit: 1, MaintenanceBatchLimit: 1, MaxCatchupIterations: 1})
	if err == nil || !strings.Contains(err.Error(), "leadership changed") {
		t.Fatalf("RebuildSemanticRule() error=%v, want leadership rejection", err)
	}
	status, statusErr := module.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if statusErr != nil {
		t.Fatalf("get reliability status: %v", statusErr)
	}
	if got := status.Rebuild.RebuildState; got != "failed" {
		t.Fatalf("rebuild state=%q, want failed", got)
	}
	staleRunning := status.Rebuild
	staleRunning.RebuildState = "running"
	staleRunning.RebuildEpoch = "abandoned-term-7"
	staleRunning.RebuildOwnerNode = 1
	staleRunning.RebuildOwnerTerm = 7
	if err := module.saveReliabilityState(ctx, spaceID, ruleID, staleRunning); err != nil {
		t.Fatal(err)
	}
	takenOver, err := module.RebuildSemanticRule(ctx, RebuildInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", BackfillLimit: 1, MaintenanceBatchLimit: 1, MaxCatchupIterations: 1})
	if err != nil {
		t.Fatalf("new-term rebuild takeover failed: %v", err)
	}
	if takenOver.Reliability.Rebuild.RebuildState != "completed" || takenOver.Reliability.Rebuild.RebuildOwnerNode != 1 || takenOver.Reliability.Rebuild.RebuildOwnerTerm != 8 {
		t.Fatalf("unexpected takeover result: %+v", takenOver.Reliability.Rebuild)
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

type leadershipChangingGraphReader struct {
	once   sync.Once
	change func()
}

func (r *leadershipChangingGraphReader) GetNode(context.Context, sessionmodel.GraphTransaction, string) (graph.Node, error) {
	return graph.Node{}, nil
}

func (r *leadershipChangingGraphReader) ListNodes(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Node, string, error) {
	r.once.Do(r.change)
	return nil, "", nil
}

func (r *leadershipChangingGraphReader) ListEdges(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Edge, string, error) {
	return nil, "", nil
}

func (r *leadershipChangingGraphReader) GetParent(context.Context, sessionmodel.GraphTransaction, string) (*graph.Edge, error) {
	return nil, nil
}
