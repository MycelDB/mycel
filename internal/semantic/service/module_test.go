package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveryBudgetInvoker struct {
	completed   []domaininference.BudgetScopeID
	current     map[string]domaininference.BudgetScope
	completeErr map[domaininference.BudgetScopeID]error
	reactivated []domaininference.BudgetScopeID
}

func (f *recoveryBudgetInvoker) Invoke(context.Context, inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	return inferenceruntime.InvokeResponse{}, nil
}
func (f *recoveryBudgetInvoker) ValidateSemanticBudgetScope(context.Context, inferenceruntime.SemanticBudgetScopeRequest) error {
	return nil
}

func (f *recoveryBudgetInvoker) EnsureSemanticBudgetScope(context.Context, inferenceruntime.SemanticBudgetScopeRequest) (domaininference.BudgetScope, error) {
	return domaininference.BudgetScope{}, nil
}
func (f *recoveryBudgetInvoker) CurrentSemanticBudgetScope(_ context.Context, _, _, binding string) (domaininference.BudgetScope, error) {
	if scope, ok := f.current[binding]; ok {
		return scope, nil
	}
	return domaininference.BudgetScope{}, inferencestorage.ErrBudgetScopeNotFound
}
func (f *recoveryBudgetInvoker) GetBudgetScope(_ context.Context, id domaininference.BudgetScopeID) (domaininference.BudgetScope, error) {
	for _, scope := range f.current {
		if scope.ID == id {
			return scope, nil
		}
	}
	return domaininference.BudgetScope{}, inferencestorage.ErrBudgetScopeNotFound
}
func (f *recoveryBudgetInvoker) CompleteBudgetScope(_ context.Context, id domaininference.BudgetScopeID, _ time.Time) (domaininference.BudgetScope, error) {
	f.completed = append(f.completed, id)
	if err := f.completeErr[id]; err != nil {
		return domaininference.BudgetScope{}, err
	}
	for binding, scope := range f.current {
		if scope.ID == id {
			scope.State = domaininference.BudgetScopeCompleted
			f.current[binding] = scope
			return scope, nil
		}
	}
	return domaininference.BudgetScope{ID: id}, nil
}
func (f *recoveryBudgetInvoker) ReactivateBudgetScope(_ context.Context, id domaininference.BudgetScopeID, _ time.Time) (domaininference.BudgetScope, error) {
	f.reactivated = append(f.reactivated, id)
	for binding, scope := range f.current {
		if scope.ID == id {
			scope.State = domaininference.BudgetScopeActive
			f.current[binding] = scope
			return scope, nil
		}
	}
	return domaininference.BudgetScope{ID: id}, nil
}

func TestModuleInitRejectsDuplicatePersistedSemanticRuleDisplayNames(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	rules := map[string]any{"rules": []domainsemantic.SemanticGenerationRule{
		{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: domainID, Key: "first", DisplayName: "Duplicate"},
		{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: domainID, Key: "second", DisplayName: " duplicate "},
	}}
	data, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	semanticDir := filepath.Join(dataDir, "graphs", spaceID.String(), "semantic")
	if err := os.MkdirAll(semanticDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(semanticDir, "rules.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	module := NewModule()
	result := module.Init(ctx, &daemonruntime.Runtime{Config: daemonconfig.Config{DataDir: dataDir}, LoggerValue: slog.Default()})
	if result.OK || !errors.Is(result.Error, storesemantic.ErrConflict) {
		t.Fatalf("Init() result=%+v, want semantic catalog conflict", result)
	}
}

func TestCompleteSemanticProviderBudgetsPreflightsAllBindingsAndCompensates(t *testing.T) {
	ctx := context.Background()
	budget := &domainsemantic.SemanticProviderBudgetPolicy{}
	rule := domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: domainspace.SpaceID(uuid.New()), Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "a", ProviderBudget: budget}, {Key: "b", ProviderBudget: budget}}}
	firstID, secondID := domaininference.BudgetScopeID("scope-a"), domaininference.BudgetScopeID("scope-b")
	invoker := &recoveryBudgetInvoker{current: map[string]domaininference.BudgetScope{"a": {ID: firstID, State: domaininference.BudgetScopeActive}, "b": {ID: secondID, State: domaininference.BudgetScopeActive, Usage: domaininference.BudgetUsage{AmbiguousAttempts: 1}}}}
	module := NewModule(Config{InferenceInvoker: invoker})
	if err := module.CompleteSemanticProviderBudgets(ctx, rule, true); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("quiescent completion error=%v code=%v", err, status.Code(err))
	}
	if len(invoker.completed) != 0 {
		t.Fatalf("preflight partially completed scopes=%v", invoker.completed)
	}
	invoker.current["b"] = domaininference.BudgetScope{ID: secondID, State: domaininference.BudgetScopeActive}
	invoker.completeErr = map[domaininference.BudgetScopeID]error{secondID: errors.New("persist failed")}
	if err := module.CompleteSemanticProviderBudgets(ctx, rule, true); err == nil {
		t.Fatal("completion unexpectedly succeeded")
	}
	if len(invoker.completed) != 2 || len(invoker.reactivated) != 1 || invoker.reactivated[0] != firstID {
		t.Fatalf("completed=%v reactivated=%v", invoker.completed, invoker.reactivated)
	}
}

func TestReconcileProviderBudgetPrepareToleratesPrunedOldScope(t *testing.T) {
	ctx := context.Background()
	budgets := &recoveryBudgetInvoker{completeErr: map[domaininference.BudgetScopeID]error{"pre-crash-ambiguous-scope": inferencestorage.ErrBudgetScopeNotFound}}
	module := NewModule(Config{InferenceInvoker: budgets})
	if result := module.Init(ctx, daemonruntime.New(t.TempDir(), slog.Default())); !result.OK {
		t.Fatalf("Init()=%+v", result)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	manager, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	scopeID := domaininference.BudgetScopeID("pre-crash-ambiguous-scope")
	rule, err := manager.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), Key: "pending-prepare", Enabled: false, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionPrepare, ProviderBudgetTransitionScopeIDs: []string{string(scopeID)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.reconcileProviderBudgetTransitions(ctx); err != nil {
		t.Fatalf("reconcileProviderBudgetTransitions() error=%v", err)
	}
	if len(budgets.completed) != 1 || budgets.completed[0] != scopeID {
		t.Fatalf("completed scopes=%v", budgets.completed)
	}
	manager, err = module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := manager.ListSemanticRules(ctx)
	if err != nil || len(rules) != 1 || rules[0].ID != rule.ID || rules[0].ProviderBudgetTransition != "" {
		t.Fatalf("reconciled rules=%+v error=%v", rules, err)
	}
}

func TestReconcileProviderBudgetDeleteRollsForwardDurablePurgeIntent(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, daemonruntime.New(t.TempDir(), slog.Default())); !result.OK {
		t.Fatalf("Init()=%+v", result)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	manager, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), Key: "pending-delete", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionDelete, ProviderBudgetTransitionID: "delete-a", ProviderBudgetTransitionPurgeVectors: true}); err != nil {
		t.Fatal(err)
	}
	if err := module.reconcileProviderBudgetTransitions(ctx); err != nil {
		t.Fatalf("reconcileProviderBudgetTransitions() error=%v", err)
	}
	manager, err = module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := manager.ListSemanticRules(ctx)
	if err != nil || len(rules) != 0 {
		t.Fatalf("delete journal was not rolled forward: rules=%+v error=%v", rules, err)
	}
}

func TestReconcileProviderBudgetTransitionRollsForwardDurableJournal(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, daemonruntime.New(t.TempDir(), slog.Default())); !result.OK {
		t.Fatalf("Init()=%+v", result)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	manager, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rule, err := manager.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: domainID, Key: "pending-budget-transition", Enabled: false, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionComplete})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.reconcileProviderBudgetTransitions(ctx); err != nil {
		t.Fatalf("reconcileProviderBudgetTransitions() error=%v", err)
	}
	manager, err = module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := manager.ListSemanticRules(ctx)
	if err != nil || len(rules) != 1 || rules[0].ID != rule.ID || rules[0].ProviderBudgetTransition != "" {
		t.Fatalf("reconciled rules=%+v error=%v", rules, err)
	}
}

func TestReconcileReliabilityTransitionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, daemonruntime.New(t.TempDir(), slog.Default())); !result.OK {
		t.Fatalf("Init()=%+v", result)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	manager, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	if _, err := manager.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 4}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), Key: "pending-reliability-transition", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), ProviderBudgetTransition: domainsemantic.SemanticProviderBudgetTransitionReliability, ProviderBudgetTransitionID: "transition-reliability-a", ProviderBudgetTransitionRequiresRebuild: true}); err != nil {
		t.Fatal(err)
	}
	pending, err := manager.ListSemanticRules(ctx)
	if err != nil || len(pending) != 1 || pending[0].ProviderBudgetTransition != domainsemantic.SemanticProviderBudgetTransitionReliability || pending[0].ProviderBudgetTransitionID != "transition-reliability-a" || !pending[0].ProviderBudgetTransitionRequiresRebuild {
		t.Fatalf("pending reliability transition=%+v error=%v", pending, err)
	}
	if err := module.reconcileProviderBudgetTransitions(ctx); err != nil {
		t.Fatal(err)
	}
	if err := module.reconcileProviderBudgetTransitions(ctx); err != nil {
		t.Fatalf("second reconciliation: %v", err)
	}
	manager, err = module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	states, err := manager.ListSemanticRuleStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := states[0].ReliabilityByBinding["search"]
	if got.SemanticGeneration != 5 || got.LastConfigurationTransitionID != "transition-reliability-a" {
		t.Fatalf("reconciled reliability=%+v", got)
	}
}

func TestInitRebuildsInferenceProjectionFromSemanticProfiles(t *testing.T) {
	ctx := context.Background()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	spaceID := domainspace.SpaceID(uuid.New())
	profileStore := storesemantic.NewSpaceManager()
	if err := profileStore.Init(ctx, filepath.Join(rt.Config.DataDir, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatalf("init semantic profile store: %v", err)
	}
	profile, err := profileStore.UpsertIntelligenceProfile(ctx, domainsemantic.IntelligenceProfile{SpaceID: spaceID, Key: "semantic", Operation: domainsemantic.OperationEmbeddings, Enabled: true})
	if err != nil {
		t.Fatalf("upsert semantic profile: %v", err)
	}

	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, rt); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	staleMgr, err := inference.SpaceManager(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("open stale inference manager: %v", err)
	}
	if _, err := staleMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: spaceID.String(), Key: "stale", Operation: domaininference.OperationEmbeddings, Enabled: true}); err != nil {
		t.Fatalf("seed stale projected profile: %v", err)
	}
	semantic := NewModule(Config{InferenceInvoker: inference, InferenceProjection: inference})
	if result := semantic.Init(ctx, rt); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	projectedMgr, err := inference.SpaceManager(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("open projected inference manager: %v", err)
	}
	projected, err := projectedMgr.ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list projected profiles: %v", err)
	}
	if len(projected) != 1 || projected[0].ID.String() != profile.ID.String() || projected[0].Key != "semantic" {
		t.Fatalf("unexpected projected profiles: %+v", projected)
	}
}

func TestPhase8BackupWaitsForSemanticWorkAndBlocksManualMutation(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	rt.Quiesce = coordinator
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: rt.Config.DataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}, Quiesce: coordinator})
	releaseActive, err := m.gate.Enter(ctx)
	if err != nil {
		t.Fatalf("enter semantic gate: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "test"})
		done <- err
	}()
	waitForSemanticGateQuiesced(t, m.gate)
	select {
	case err := <-done:
		t.Fatalf("backup completed before active semantic work drained: %v", err)
	default:
	}
	releaseActive()
	if err := <-done; err != nil {
		t.Fatalf("backup trigger after semantic drain: %v", err)
	}

	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("quiesce semantic gate: %v", err)
	}
	_, err = m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: domainspace.SpaceID(uuid.New())})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("AnalyzeDirtyWork() code = %v, want unavailable (err=%v)", status.Code(err), err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release semantic gate: %v", err)
	}
	if err := m.PurgeVectorIndex(ctx, domainspace.SpaceID(uuid.New()), uuid.New()); err != nil {
		t.Fatalf("semantic mutation after release failed: %v", err)
	}
}

func TestSchemaDirtyCooldownForTargetUsesSemanticIndexingPolicy(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	cooldown := 5 * time.Minute
	m := NewModule(Config{SchemaReader: fakeSchemaReader{schema: schemamodel.DomainSchema{
		DomainID: domainID,
		NodeTypes: []schemamodel.NodeType{{
			Name:   "Doc",
			Labels: []string{"doc"},
			Indexing: schemamodel.IndexPolicy{
				Semantic:              true,
				SemanticDirtyCooldown: cooldown,
			},
		}},
	}}})
	resolver := m.schemaDirtyCooldownForTarget(fakeServiceGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Labels: []string{"doc"}}}})
	if resolver == nil {
		t.Fatal("expected resolver")
	}
	got, err := resolver(ctx, domainsemantic.SemanticIndex{DomainID: domainID}, nodeID, time.Minute)
	if err != nil {
		t.Fatalf("resolver failed: %v", err)
	}
	if got != cooldown {
		t.Fatalf("cooldown = %s, want %s", got, cooldown)
	}
}

func waitForSemanticGateQuiesced(t *testing.T, gate *quiesce.Gate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if gate.Status().Quiesced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for semantic gate to quiesce")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestSemanticQuiesceRejectsManualMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: domainspace.SpaceID(uuid.New())})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("AnalyzeDirtyWork() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
	_, err = m.RebuildSemanticRule(ctx, RebuildInput{SpaceID: domainspace.SpaceID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("RebuildSemanticRule() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestSemanticMaintenanceDisabledDoesNotStartLoops(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should not be running when disabled")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticMaintenanceEnabledDoesNotStartBeforeStart(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should not run until Start")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticServiceStatus(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	status := m.Status(ctx)
	if status.Name != ModuleName || status.State != "stopped" || status.Started {
		t.Fatalf("unexpected stopped status: %#v", status)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	status = m.Status(ctx)
	if status.Name != ModuleName || status.State != "running" || !status.Started || status.StartedAt.IsZero() {
		t.Fatalf("unexpected running status: %#v", status)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticMaintenanceEnabledStartsLoopsAndStops(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !m.MaintenanceRunning() {
		t.Fatalf("maintenance should be running")
	}
	waitForStats(t, m, func(stats MaintenanceStats) bool { return stats.AnalyzerRuns > 0 && stats.WorkerRuns > 0 })
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should stop after close")
	}
}

func TestRuntimeCloseStopsSemanticMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if err := rt.InitServices(ctx, []daemonruntime.Service{m}); err != nil {
		t.Fatalf("init services failed: %v", err)
	}
	if err := rt.StartServices(ctx); err != nil {
		t.Fatalf("start services failed: %v", err)
	}
	waitForStats(t, m, func(stats MaintenanceStats) bool { return stats.AnalyzerRuns > 0 && stats.WorkerRuns > 0 })
	if err := rt.Close(); err != nil {
		t.Fatalf("runtime close failed: %v", err)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("runtime close should stop semantic maintenance")
	}
}

func TestMaintenanceManagerReusesLoadedBasePerSpace(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	first, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("first base manager failed: %v", err)
	}
	second, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("second base manager failed: %v", err)
	}
	if first != second {
		t.Fatal("baseMaintenanceManager did not reuse loaded manager for same space")
	}
	other, err := m.baseMaintenanceManager(ctx, domainspace.SpaceID(uuid.New()))
	if err != nil {
		t.Fatalf("other base manager failed: %v", err)
	}
	if other == first {
		t.Fatal("distinct spaces should not share maintenance managers")
	}
}

func TestMaintenanceManagerCacheClearedOnCloseAndReload(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	mgr, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("base manager failed: %v", err)
	}
	if len(m.maintenanceManagers) != 1 {
		t.Fatalf("manager cache length = %d, want 1", len(m.maintenanceManagers))
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if len(m.maintenanceManagers) != 0 {
		t.Fatalf("manager cache length after close = %d, want 0", len(m.maintenanceManagers))
	}
	mgr2, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("base manager after close failed: %v", err)
	}
	if mgr2 == mgr {
		t.Fatal("base manager should reload after close/reinit")
	}
	if err := m.ReloadAfterSnapshot(ctx); err != nil {
		t.Fatalf("reload after snapshot failed: %v", err)
	}
	if len(m.maintenanceManagers) != 0 {
		t.Fatalf("manager cache length after snapshot reload = %d, want 0", len(m.maintenanceManagers))
	}
}

func TestApplyMaintenanceMutationUpsertBatch(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatalf("maintenance manager init failed: %v", err)
	}
	items := []domainsemantic.SemanticDirtyWorkItem{
		{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: graph.NodeID(uuid.New())},
		{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: graph.NodeID(uuid.New())},
	}
	if err := applyMaintenanceMutation(ctx, mgr, maintenanceMutationRecord{Kind: "work.upsert_batch", SpaceID: spaceID, Payload: raw(items)}); err != nil {
		t.Fatalf("apply batch mutation failed: %v", err)
	}
	stored, err := mgr.ListDirtyWorkItems(ctx)
	if err != nil {
		t.Fatalf("list dirty work failed: %v", err)
	}
	if len(stored) != len(items) {
		t.Fatalf("stored items = %d, want %d: %+v", len(stored), len(items), stored)
	}
}

func TestListRulesIncludesDisabledRulesForClientPreconditionMapping(t *testing.T) {
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
	if _, err := mgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "disabled-rule", Enabled: false, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}); err != nil {
		t.Fatal(err)
	}
	rules, err := module.ListRules(ctx, spaceID, domainID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 || rules[0].ID != ruleID || rules[0].Enabled {
		t.Fatalf("ListRules()=%+v, want disabled rule", rules)
	}
}

func TestBackfillIndexRejectsSemanticDisabledDomain(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainMgr := storedomains.NewManager()
	if err := domainMgr.Init(ctx, rt.Config.DataDir+"/meta"); err != nil {
		t.Fatalf("domain store init failed: %v", err)
	}
	domain, err := domainMgr.Create(ctx, storedomains.CreateInput{SpaceID: spaceID, Key: "private", DiscoveryMode: graph.DomainDiscoveryModeExplicitOnly, SearchMode: graph.DomainSearchModeDisabled, SemanticMode: graph.DomainSemanticModeDisabled})
	if err != nil {
		t.Fatalf("create domain failed: %v", err)
	}
	spaceMgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager failed: %v", err)
	}
	idx, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domain.ID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	res, err := m.BackfillIndex(ctx, semanticbackfill.Input{SpaceID: spaceID, SemanticIndexID: idx.ID})
	if err == nil {
		t.Fatal("expected semantic disabled backfill error")
	}
	if len(res.Records) != 0 || res.GeneratedCount != 0 {
		t.Fatalf("expected no generated records, got %+v", res)
	}
}

func TestSemanticGraphReaderPinsReadOnlyTransactionToCurrentRevision(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	graphs := &recordingGraphReader{}
	revisions := fixedGraphRevisionReader{revision: 42}
	reader := semanticGraphReader{reader: graphs, revisions: revisions, spaceID: spaceID}

	if _, err := reader.GetNode(ctx, domainID, graph.NodeID(uuid.New())); err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if _, err := reader.ListNodes(ctx, domainID); err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(graphs.txs) != 2 {
		t.Fatalf("recorded tx count = %d, want 2", len(graphs.txs))
	}
	for _, tx := range graphs.txs {
		if tx.Mode != sessionmodel.TransactionModeReadOnly {
			t.Fatalf("tx mode = %s, want read-only", tx.Mode)
		}
		if tx.BaseRevision != 42 {
			t.Fatalf("tx base revision = %d, want 42", tx.BaseRevision)
		}
		if tx.SpaceID != spaceID.String() || tx.DomainID != domainID.String() {
			t.Fatalf("unexpected tx scope: %+v", tx)
		}
	}
}

func testRuntime(t *testing.T, sem daemonconfig.SemanticMaintenanceConfig) *daemonruntime.Runtime {
	t.Helper()
	cfg := daemonconfig.Config{DataDir: t.TempDir(), Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0", SemanticMaintenance: sem}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	return daemonruntime.New(cfg, logger, "", nil)
}

func waitForStats(t *testing.T, m *Module, ok func(MaintenanceStats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok(m.MaintenanceStats()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met; stats=%+v", m.MaintenanceStats())
}

type fixedGraphRevisionReader struct {
	revision int64
}

func (r fixedGraphRevisionReader) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	return r.revision, nil
}

type recordingGraphReader struct {
	txs []sessionmodel.GraphTransaction
}

func (m *recordingGraphReader) GetNode(_ context.Context, tx sessionmodel.GraphTransaction, _ string) (graph.Node, error) {
	m.txs = append(m.txs, tx)
	return graph.Node{}, nil
}

func (m *recordingGraphReader) ListNodes(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Node, string, error) {
	m.txs = append(m.txs, tx)
	return nil, "", nil
}

func (m *recordingGraphReader) ListEdges(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Edge, string, error) {
	m.txs = append(m.txs, tx)
	return nil, "", nil
}

func (m *recordingGraphReader) GetParent(_ context.Context, tx sessionmodel.GraphTransaction, _ string) (*graph.Edge, error) {
	m.txs = append(m.txs, tx)
	return nil, nil
}

func TestSyncSemanticDirtyCountsRefreshesRuleAndIndexState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(ruleID)
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatalf("init space manager: %v", err)
	}
	maintenanceMgr := storesemantic.NewMaintenanceManager()
	if err := maintenanceMgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic", "maintenance"), spaceID); err != nil {
		t.Fatalf("init maintenance manager: %v", err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", DirtyCount: 22}); err != nil {
		t.Fatalf("seed rule state: %v", err)
	}
	if _, err := mgr.UpsertIndexState(ctx, domainsemantic.SemanticIndexState{SemanticRuleID: ruleID, SemanticIndexID: indexID, State: "active", DirtyCount: 22}); err != nil {
		t.Fatalf("seed index state: %v", err)
	}
	if _, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), TargetNodeID: graph.NodeID(uuid.New()), Action: domainsemantic.SemanticDirtyWorkActionRefresh, Status: domainsemantic.SemanticDirtyWorkStatusComplete}); err != nil {
		t.Fatalf("seed completed work: %v", err)
	}
	if err := NewModule().syncSemanticDirtyCounts(ctx, spaceID, mgr, maintenanceMgr); err != nil {
		t.Fatalf("syncSemanticDirtyCounts() error = %v", err)
	}
	ruleStates, err := mgr.ListSemanticRuleStates(ctx)
	if err != nil {
		t.Fatalf("ListSemanticRuleStates() error = %v", err)
	}
	if len(ruleStates) != 1 || ruleStates[0].DirtyCount != 0 {
		t.Fatalf("unexpected rule states: %+v", ruleStates)
	}
	indexStates, err := mgr.ListIndexStates(ctx)
	if err != nil {
		t.Fatalf("ListIndexStates() error = %v", err)
	}
	if len(indexStates) != 1 || indexStates[0].DirtyCount != 0 {
		t.Fatalf("unexpected index states: %+v", indexStates)
	}
}

type fakeSchemaReader struct {
	schema schemamodel.DomainSchema
	err    error
}

func (m fakeSchemaReader) GetDomainSchema(context.Context, graph.DomainID) (schemamodel.DomainSchema, error) {
	if m.err != nil {
		return schemamodel.DomainSchema{}, m.err
	}
	return m.schema, nil
}

type fakeServiceGraphReader struct {
	nodes map[graph.NodeID]graph.Node
}

func (r fakeServiceGraphReader) GetNode(_ context.Context, _ graph.DomainID, id graph.NodeID) (graph.Node, error) {
	if node, ok := r.nodes[id]; ok {
		return node, nil
	}
	return graph.Node{}, context.Canceled
}

func (r fakeServiceGraphReader) Parent(context.Context, graph.DomainID, graph.NodeID) (*graph.Edge, error) {
	return nil, nil
}
