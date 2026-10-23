package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	model "github.com/myceldb/mycel/internal/inference/model"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInferenceWALWrapsGlobalSpaceAndUsageStores(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wm.Close()
	host := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	host.WAL = wm
	host.WALRegistry = wal.NewRegistry()
	host.WALProgress = wal.NewFileProgressStore(filepath.Join(dataDir, "wal-progress", "applied.json"))
	host.WALWaiter = wal.NewApplyWaiter()

	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	endpoint, err := module.GlobalManager().UpsertEndpoint(ctx, model.Endpoint{Key: "OpenAI", ConnectorType: model.ConnectorOpenAICompatible, Enabled: true})
	if err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	spaceMgr, err := module.SpaceManager(ctx, "space-1")
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	profile, err := spaceMgr.UpsertProfile(ctx, model.Profile{Key: "Summarize", Operation: model.OperationChat, Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	profilesAfterWrite, err := spaceMgr.ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list profiles after wal write: %v", err)
	}
	if len(profilesAfterWrite) != 1 || profilesAfterWrite[0].ID != profile.ID {
		t.Fatalf("wal space manager did not observe own write: %#v", profilesAfterWrite)
	}
	profileAgain, err := spaceMgr.UpsertProfile(ctx, model.Profile{Key: "summarize", Operation: model.OperationChat, Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile again: %v", err)
	}
	if profileAgain.ID != profile.ID {
		t.Fatalf("same-key WAL profile upsert changed ID: %s -> %s", profile.ID, profileAgain.ID)
	}
	usage, err := module.UsageLedger().AppendUsageEvent(ctx, model.UsageEvent{SpaceID: "space-1", ProfileID: profile.ID, EndpointID: endpoint.ID, Operation: model.OperationChat, Status: model.UsageStatusSucceeded})
	if err != nil {
		t.Fatalf("append usage: %v", err)
	}
	now := time.Now().UTC()
	budget, err := module.CreateBudgetScope(ctx, model.BudgetScope{ID: "wal-budget", Route: model.BudgetRoute{EndpointID: endpoint.ID, ModelID: uuid.New(), CapabilityID: uuid.New(), Operation: model.OperationChat}, Limits: model.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, MaxConcurrentProviderCalls: 1, BatchingMode: model.BatchingModeSingle, MaxBatchItems: 1, MaxProviderMillisPerRequest: 1000, MaxProviderMillis: 1000, Deadline: now.Add(time.Hour)}, ExpiresAt: now.Add(time.Hour), RetentionUntil: now.Add(2 * time.Hour)})
	if err != nil {
		t.Fatalf("create budget: %v", err)
	}
	if _, _, err := module.ReserveProviderAttempt(ctx, model.BudgetReservationRequest{ScopeID: budget.ID, AttemptID: "wal-attempt", RequestID: "wal-request", Route: budget.Route, BatchItems: 1, ProviderMillis: 1000, Now: now}); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}
	if endpoint.ID == uuid.Nil || profile.ID == uuid.Nil || usage.ID == uuid.Nil {
		t.Fatalf("expected ids from wal wrappers: endpoint=%s profile=%s usage=%s", endpoint.ID, profile.ID, usage.ID)
	}
	if host.WALWaiter.AppliedLSN() == 0 {
		t.Fatalf("expected wal waiter to advance")
	}

	module2 := NewModule()
	host2 := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if result := module2.Init(ctx, host2); !result.OK {
		t.Fatalf("init second module: %#v", result)
	}
	endpoints, err := module2.GlobalManager().ListEndpoints(ctx)
	if err != nil {
		t.Fatalf("list endpoints: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0].ID != endpoint.ID || endpoints[0].Key != "openai" {
		t.Fatalf("unexpected endpoints after reload: %#v", endpoints)
	}
	profiles, err := mustInferenceSpace(t, module2, "space-1").ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list profiles: %v", err)
	}
	if len(profiles) != 1 || profiles[0].ID != profile.ID || profiles[0].Key != "summarize" {
		t.Fatalf("unexpected profiles after reload: %#v", profiles)
	}
	events, err := module2.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		t.Fatalf("list usage: %v", err)
	}
	if len(events) != 1 || events[0].ID != usage.ID {
		t.Fatalf("unexpected usage after reload: %#v", events)
	}
	budgetAfterReload, err := module2.GetBudgetScope(ctx, budget.ID)
	if err != nil || budgetAfterReload.Usage.Attempts != 1 || len(budgetAfterReload.Attempts) != 1 || budgetAfterReload.Attempts[0].ID != "wal-attempt" {
		t.Fatalf("unexpected budget after reload: %#v err=%v", budgetAfterReload, err)
	}
}

func TestInferenceMutationRespectsLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	hostWithGate := &rejectingWriteHost{Host: host}
	module := NewModule()
	if result := module.Init(ctx, hostWithGate); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, model.Endpoint{Key: "openai"}); err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected local write gate rejection, got %v", err)
	}
}

func TestInferenceBudgetAccountingRespectsLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	module := NewModule()
	if result := module.Init(ctx, &rejectingWriteHost{Host: host}); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	now := time.Now().UTC()
	_, err := module.CreateBudgetScope(ctx, model.BudgetScope{ID: "runtime-budget", Route: model.BudgetRoute{EndpointID: uuid.New(), ModelID: uuid.New(), CapabilityID: uuid.New(), Operation: model.OperationChat}, Limits: model.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, MaxConcurrentProviderCalls: 1, BatchingMode: model.BatchingModeSingle, MaxBatchItems: 1, MaxProviderMillisPerRequest: 100, MaxProviderMillis: 100, Deadline: now.Add(time.Minute)}, ExpiresAt: now.Add(time.Minute), RetentionUntil: now.Add(time.Hour)})
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("CreateBudgetScope() error=%v, want local write rejection", err)
	}
}

func TestInferenceQuiesceRejectsDurableMutations(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	lease, err := host.QuiesceCoordinator().QuiesceAll(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error=%v", err)
	}
	defer lease.Release(context.Background())
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, model.Endpoint{Key: "openai"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("UpsertEndpoint() error=%v, want Unavailable", err)
	}
	spaceMgr, err := module.SpaceManager(ctx, "space-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, model.Profile{Key: "summarize", Operation: model.OperationChat}); status.Code(err) != codes.Unavailable {
		t.Fatalf("UpsertProfile() error=%v, want Unavailable", err)
	}
	if _, err := module.UsageLedger().AppendUsageEvent(ctx, model.UsageEvent{SpaceID: "space-1", Operation: model.OperationChat, Status: model.UsageStatusSucceeded}); status.Code(err) != codes.Unavailable {
		t.Fatalf("AppendUsageEvent() error=%v, want Unavailable", err)
	}
}

func TestInferenceRuntimeEvidenceBypassesLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	hostWithGate := &rejectingWriteHost{Host: host}
	module := NewModule()
	if result := module.Init(ctx, hostWithGate); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	spaceMgr, err := module.SpaceManager(ctx, "space-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spaceMgr.UpsertPolicyDecision(ctx, model.PolicyDecision{SpaceID: "space-1", Action: model.PolicyDecisionAllowed}); err != nil {
		t.Fatalf("policy decision evidence should bypass local write gate: %v", err)
	}
	if _, err := module.UsageLedger().AppendUsageEvent(ctx, model.UsageEvent{SpaceID: "space-1", Operation: model.OperationChat, Status: model.UsageStatusSucceeded}); err != nil {
		t.Fatalf("usage evidence should bypass local write gate: %v", err)
	}
}

func TestInferenceDerivedSyncBypassesLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	hostWithGate := &rejectingWriteHost{Host: host}
	module := NewModule()
	if result := module.Init(ctx, hostWithGate); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	if _, err := module.UpsertDerivedEndpoint(ctx, model.Endpoint{Key: "openai", ConnectorType: model.ConnectorOpenAICompatible, PrivacyClass: model.PrivacyClassThirdParty, Enabled: true}); err != nil {
		t.Fatalf("derived endpoint sync failed: %v", err)
	}
	items, err := module.GlobalManager().ListEndpoints(ctx)
	if err != nil {
		t.Fatalf("list endpoints: %v", err)
	}
	if len(items) != 1 || items[0].Key != "openai" {
		t.Fatalf("unexpected derived endpoints: %#v", items)
	}
}

type rejectingWriteHost struct{ *runtimetest.Host }

func (h *rejectingWriteHost) RequireLocalWriteAllowed() error { return errors.New("rejected") }

func TestInferenceWALRecoveryReplaysMissingFiles(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	host := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	host.WAL = wm
	host.WALRegistry = wal.NewRegistry()
	host.WALWaiter = wal.NewApplyWaiter()
	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	if _, err := module.GlobalManager().UpsertModel(ctx, model.Model{Key: "gpt", Kind: model.ModelKindGenerative, ProviderModelName: "gpt", Enabled: true}); err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := mustInferenceSpace(t, module, "space-1").UpsertPolicy(ctx, model.Policy{SpaceID: "space-1", Action: model.PolicyActionAllow, State: model.PolicyStateActive}); err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
	if _, err := module.UsageLedger().AppendUsageEvent(ctx, model.UsageEvent{SpaceID: "space-1", Operation: model.OperationChat, Status: model.UsageStatusSucceeded}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	now := time.Now().UTC()
	budget, err := module.CreateBudgetScope(ctx, model.BudgetScope{ID: "replay-budget", Route: model.BudgetRoute{EndpointID: uuid.New(), ModelID: uuid.New(), CapabilityID: uuid.New(), Operation: model.OperationChat}, Limits: model.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, MaxConcurrentProviderCalls: 1, BatchingMode: model.BatchingModeSingle, MaxBatchItems: 1, MaxProviderMillisPerRequest: 1000, MaxProviderMillis: 1000, Deadline: now.Add(time.Hour)}, ExpiresAt: now.Add(time.Hour), RetentionUntil: now.Add(2 * time.Hour)})
	if err != nil {
		t.Fatalf("create replay budget: %v", err)
	}
	if _, _, err := module.ReserveProviderAttempt(ctx, model.BudgetReservationRequest{ScopeID: budget.ID, AttemptID: "replay-attempt", RequestID: "replay-request", Route: budget.Route, BatchItems: 1, ProviderMillis: 1000, Now: now}); err != nil {
		t.Fatalf("reserve replay budget: %v", err)
	}
	last := wm.LastCommittedLSN()
	if err := wm.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "meta")); err != nil {
		t.Fatalf("remove meta: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "graphs")); err != nil {
		t.Fatalf("remove graphs: %v", err)
	}

	wm2, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer wm2.Close()
	host2 := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	host2.WAL = wm2
	host2.WALRegistry = wal.NewRegistry()
	host2.WALProgress = wal.NewFileProgressStore(filepath.Join(dataDir, "wal-progress-replay", "applied.json"))
	module2 := NewModule()
	if result := module2.Init(ctx, host2); !result.OK {
		t.Fatalf("init replay module: %#v", result)
	}
	recovery := wal.NewRecovery(wm2, host2.WALRegistry, host2.WALProgress)
	applied, err := recovery.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if applied != last {
		t.Fatalf("applied LSN %s, want %s", applied, last)
	}
	models, err := module2.GlobalManager().ListModels(ctx)
	if err != nil {
		t.Fatalf("list models: %v", err)
	}
	if len(models) != 1 || models[0].Key != "gpt" {
		t.Fatalf("unexpected models after replay: %#v", models)
	}
	policies, err := mustInferenceSpace(t, module2, "space-1").ListPolicies(ctx)
	if err != nil {
		t.Fatalf("list policies: %v", err)
	}
	if len(policies) != 1 || policies[0].Action != model.PolicyActionAllow {
		t.Fatalf("unexpected policies after replay: %#v", policies)
	}
	events, err := module2.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		t.Fatalf("list usage: %v", err)
	}
	if len(events) != 1 || events[0].Operation != model.OperationChat {
		t.Fatalf("unexpected events after replay: %#v", events)
	}
	budgetAfterReplay, err := module2.GetBudgetScope(ctx, budget.ID)
	if err != nil || budgetAfterReplay.Usage.Attempts != 1 || budgetAfterReplay.Attempts[0].ID != "replay-attempt" {
		t.Fatalf("unexpected budget after replay: %#v err=%v", budgetAfterReplay, err)
	}
}

func TestInferenceBudgetWALFailsClosedForLegacyRecovery(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wm.Close()
	payload := rawInference(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(model.BudgetScope{ID: "scope-1"})})
	lsn, err := wm.Append(ctx, wal.PendingRecord{Type: recordTypeInferenceBudget, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		t.Fatalf("append budget record: %v", err)
	}
	if err := wm.Sync(ctx, lsn); err != nil {
		t.Fatalf("sync budget record: %v", err)
	}
	legacyRegistry := wal.NewRegistry()
	if err := legacyRegistry.Register(recordTypeInferenceUsage, wal.ApplierFunc(func(context.Context, wal.Record) error { return nil })); err != nil {
		t.Fatalf("register legacy usage applier: %v", err)
	}
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "progress", "applied.json"))
	if _, err := wal.NewRecovery(wm, legacyRegistry, progress).Recover(ctx); err == nil || !strings.Contains(err.Error(), string(recordTypeInferenceBudget)) {
		t.Fatalf("legacy recovery error=%v, want unsupported budget record", err)
	}
	applied, err := progress.AppliedLSN(ctx)
	if err != nil || applied != 0 {
		t.Fatalf("legacy recovery advanced progress to %s, err=%v", applied, err)
	}
}

func TestInferenceBudgetWALRejectsUnsupportedSchemaAndMutationKinds(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	budgetPayload := rawInference(inferenceMutationRecord{Kind: "budget_scope.upsert", Payload: rawInference(model.BudgetScope{ID: "scope-1"})})
	if err := module.applyInferenceBudget(ctx, wal.Record{Type: recordTypeInferenceBudget, SchemaVersion: 2, Encoding: wal.PayloadEncodingJSON, Payload: budgetPayload}); err == nil || !strings.Contains(err.Error(), "schema version") {
		t.Fatalf("unsupported budget schema error=%v", err)
	}
	unknownPayload := rawInference(inferenceMutationRecord{Kind: "future.mutation"})
	if err := module.applyInferenceBudget(ctx, wal.Record{Type: recordTypeInferenceBudget, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: unknownPayload}); err == nil || !strings.Contains(err.Error(), "unsupported inference budget mutation") {
		t.Fatalf("unknown budget mutation error=%v", err)
	}
	if err := module.applyInferenceUsage(ctx, wal.Record{Type: recordTypeInferenceUsage, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: unknownPayload}); err == nil || !strings.Contains(err.Error(), "unsupported inference usage mutation") {
		t.Fatalf("unknown usage mutation error=%v", err)
	}
}

func TestInferenceUsageWALReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	host := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	host.WAL = wm
	host.WALRegistry = wal.NewRegistry()
	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	if _, err := module.UsageLedger().AppendUsageEvent(ctx, model.UsageEvent{SpaceID: "space-1", Operation: model.OperationChat, Status: model.UsageStatusSucceeded}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	last := wm.LastCommittedLSN()
	if err := wm.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	wm2, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal")})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer wm2.Close()
	host2 := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	host2.WAL = wm2
	host2.WALRegistry = wal.NewRegistry()
	host2.WALProgress = wal.NewFileProgressStore(filepath.Join(dataDir, "wal-progress-idempotent", "applied.json"))
	module2 := NewModule()
	if result := module2.Init(ctx, host2); !result.OK {
		t.Fatalf("init replay module: %#v", result)
	}
	recovery := wal.NewRecovery(wm2, host2.WALRegistry, host2.WALProgress)
	applied, err := recovery.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if applied != last {
		t.Fatalf("applied LSN %s, want %s", applied, last)
	}
	events, err := module2.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		t.Fatalf("list usage: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("usage replay duplicated event: %#v", events)
	}
}

func TestInferenceReloadAfterSnapshotRefreshesCachedStores(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	host := runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init: %#v", result)
	}
	oldSpace := mustInferenceSpace(t, module, "space-1")
	if _, err := oldSpace.UpsertProfile(ctx, model.Profile{Key: "old", Operation: model.OperationChat, Enabled: true}); err != nil {
		t.Fatalf("upsert old profile: %v", err)
	}

	replacement := NewModule()
	if result := replacement.Init(ctx, runtimetest.New(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init replacement: %#v", result)
	}
	profiles, err := mustInferenceSpace(t, replacement, "space-1").ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list replacement profiles: %v", err)
	}
	for _, profile := range profiles {
		if err := mustInferenceSpace(t, replacement, "space-1").DeleteProfile(ctx, profile.ID); err != nil {
			t.Fatalf("delete replacement profile: %v", err)
		}
	}
	if _, err := mustInferenceSpace(t, replacement, "space-1").UpsertProfile(ctx, model.Profile{Key: "new", Operation: model.OperationChat, Enabled: true}); err != nil {
		t.Fatalf("upsert replacement profile: %v", err)
	}

	if err := module.ReloadAfterSnapshot(ctx); err != nil {
		t.Fatalf("reload after snapshot: %v", err)
	}
	fresh, err := mustInferenceSpace(t, module, "space-1").ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list fresh profiles: %v", err)
	}
	if len(fresh) != 1 || fresh[0].Key != "new" {
		t.Fatalf("unexpected profiles after reload: %#v", fresh)
	}
}

func mustInferenceSpace(t *testing.T, module *Module, spaceID string) interface {
	ListProfiles(context.Context) ([]model.Profile, error)
	ListPolicies(context.Context) ([]model.Policy, error)
	UpsertProfile(context.Context, model.Profile) (model.Profile, error)
	UpsertPolicy(context.Context, model.Policy) (model.Policy, error)
	DeleteProfile(context.Context, model.ProfileID) error
} {
	t.Helper()
	mgr, err := module.SpaceManager(context.Background(), spaceID)
	if err != nil {
		t.Fatalf("space manager %s: %v", spaceID, err)
	}
	return mgr
}
