package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

func TestUsageLedgerMigratesLegacyBudgetScopeSequencesForSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	location := t.TempDir()
	early := domaininference.BudgetScope{ID: "scope-early", CreatedAt: time.Unix(10, 0).UTC()}
	late := domaininference.BudgetScope{ID: "scope-late", CreatedAt: time.Unix(20, 0).UTC()}
	raw, err := json.Marshal(budgetScopesState{Scopes: []domaininference.BudgetScope{late, early}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(location, budgetScopesFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	ledger := NewUsageLedger()
	if err := ledger.Init(ctx, location); err != nil {
		t.Fatalf("init legacy ledger: %v", err)
	}
	scopes, err := ledger.ListBudgetScopes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[domaininference.BudgetScopeID]uint64{}
	for _, scope := range scopes {
		byID[scope.ID] = scope.ScopeSequence
	}
	if byID[early.ID] != 1 || byID[late.ID] != 2 {
		t.Fatalf("migrated sequences=%v", byID)
	}
	highWater, err := ledger.BudgetScopeHighWater(ctx)
	if err != nil || highWater != 2 {
		t.Fatalf("high-water=%d error=%v", highWater, err)
	}
	restored := NewUsageLedger()
	if err := restored.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := restored.ReplaceBudgetScopes(ctx, scopes, highWater); err != nil {
		t.Fatalf("restore migrated snapshot: %v", err)
	}
	legacyRestore := NewUsageLedger()
	if err := legacyRestore.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := legacyRestore.ReplaceBudgetScopes(ctx, []domaininference.BudgetScope{late, early}, 0); err != nil {
		t.Fatalf("restore legacy zero-sequence snapshot: %v", err)
	}
	if got, err := legacyRestore.BudgetScopeHighWater(ctx); err != nil || got != 2 {
		t.Fatalf("legacy restore high-water=%d error=%v", got, err)
	}
}

func TestInferenceSnapshotRestoreJournalRecoversEveryPersistenceBoundary(t *testing.T) {
	for _, failureFile := range []string{budgetScopesFileName, usageFileName} {
		t.Run(failureFile, func(t *testing.T) {
			ctx := context.Background()
			location := t.TempDir()
			ledger := NewUsageLedger().(*usageLedger)
			if err := ledger.Init(ctx, location); err != nil {
				t.Fatal(err)
			}
			scope := domaininference.BudgetScope{ID: "snapshot-scope", ScopeSequence: 1, State: domaininference.BudgetScopeCompleted}
			event := domaininference.UsageEvent{ID: uuid.New()}
			ledger.writeOverride = func(ctx context.Context, path string, value any) error {
				if filepath.Base(path) == failureFile {
					return errors.New("injected snapshot persistence failure")
				}
				return writeJSONEncrypted(ctx, path, value, ledger.encryption)
			}
			if err := ledger.ReplaceInferenceSnapshot(ctx, []domaininference.BudgetScope{scope}, 1, []domaininference.UsageEvent{event}); err == nil {
				t.Fatal("snapshot restore unexpectedly succeeded")
			}
			if scopes, err := ledger.ListBudgetScopes(ctx); err != nil || len(scopes) != 0 {
				t.Fatalf("failed restore mutated in-memory scopes=%+v error=%v", scopes, err)
			}
			if events, err := ledger.ListUsageEvents(ctx); err != nil || len(events) != 0 {
				t.Fatalf("failed restore mutated in-memory usage=%+v error=%v", events, err)
			}
			recovered := NewUsageLedger()
			if err := recovered.Init(ctx, location); err != nil {
				t.Fatalf("recover snapshot journal: %v", err)
			}
			scopes, err := recovered.ListBudgetScopes(ctx)
			if err != nil || len(scopes) != 1 || scopes[0].ID != scope.ID {
				t.Fatalf("recovered scopes=%+v error=%v", scopes, err)
			}
			events, err := recovered.ListUsageEvents(ctx)
			if err != nil || len(events) != 1 || events[0].ID != event.ID {
				t.Fatalf("recovered usage=%+v error=%v", events, err)
			}
			if _, err := os.Stat(filepath.Join(location, inferenceSnapshotJournalFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("snapshot journal was not cleared: %v", err)
			}
		})
	}
}

func TestBudgetScopePersistenceFailureDoesNotMutateMemoryAndReplayPersists(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			location := t.TempDir()
			ledger := NewUsageLedger().(*usageLedger)
			if err := ledger.Init(ctx, location); err != nil {
				t.Fatal(err)
			}
			scope := domaininference.BudgetScope{ID: "scope-a", ScopeSequence: 1, State: domaininference.BudgetScopeActive}
			if operation != "create" {
				if _, err := ledger.UpsertBudgetScope(ctx, scope); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(location, budgetScopesFileName)
			backup := path + ".saved"
			if _, err := os.Stat(path); err == nil {
				if err := os.Rename(path, backup); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			restorePath := func() {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(backup); err == nil {
					if err := os.Rename(backup, path); err != nil {
						t.Fatal(err)
					}
				}
			}
			updated := scope
			updated.State = domaininference.BudgetScopeCompleted
			var mutationErr error
			switch operation {
			case "create":
				_, mutationErr = ledger.UpsertBudgetScope(ctx, scope)
			case "update":
				_, mutationErr = ledger.UpsertBudgetScope(ctx, updated)
			case "delete":
				mutationErr = ledger.DeleteBudgetScope(ctx, scope.ID)
			}
			if mutationErr == nil {
				t.Fatal("mutation unexpectedly succeeded with blocked budget file")
			}
			persisted, getErr := ledger.GetBudgetScope(ctx, scope.ID)
			if operation == "create" {
				if !errors.Is(getErr, ErrBudgetScopeNotFound) {
					t.Fatalf("failed create mutated memory: scope=%+v error=%v", persisted, getErr)
				}
			} else if getErr != nil || persisted.State != domaininference.BudgetScopeActive {
				t.Fatalf("failed %s mutated memory: scope=%+v error=%v", operation, persisted, getErr)
			}
			restorePath()
			switch operation {
			case "create":
				_, mutationErr = ledger.UpsertBudgetScope(ctx, scope)
			case "update":
				_, mutationErr = ledger.UpsertBudgetScope(ctx, updated)
			case "delete":
				mutationErr = ledger.DeleteBudgetScope(ctx, scope.ID)
			}
			if mutationErr != nil {
				t.Fatalf("replay mutation: %v", mutationErr)
			}
			reopened := NewUsageLedger()
			if err := reopened.Init(ctx, location); err != nil {
				t.Fatal(err)
			}
			persisted, getErr = reopened.GetBudgetScope(ctx, scope.ID)
			switch operation {
			case "create":
				if getErr != nil || persisted.State != domaininference.BudgetScopeActive {
					t.Fatalf("reopened create scope=%+v error=%v", persisted, getErr)
				}
			case "update":
				if getErr != nil || persisted.State != domaininference.BudgetScopeCompleted {
					t.Fatalf("reopened update scope=%+v error=%v", persisted, getErr)
				}
			case "delete":
				if !errors.Is(getErr, ErrBudgetScopeNotFound) {
					t.Fatalf("reopened delete scope=%+v error=%v", persisted, getErr)
				}
			}
		})
	}
}

func TestGlobalManagerRoundTrip(t *testing.T) {
	ctx := context.Background()
	metaDir := t.TempDir()
	mgr := NewGlobalManager()
	if err := mgr.Init(ctx, metaDir); err != nil {
		t.Fatalf("init global manager: %v", err)
	}
	endpoint, err := mgr.UpsertEndpoint(ctx, domaininference.Endpoint{Key: "openai", DisplayName: "OpenAI", ConnectorType: domaininference.ConnectorOpenAICompatible, BaseURL: "https://api.openai.com/v1", NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, AuthTypes: []domaininference.CredentialAuthType{domaininference.CredentialAuthBearer}, Operations: []domaininference.Operation{domaininference.OperationChat}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	if endpoint.ID == uuid.Nil {
		t.Fatalf("expected endpoint id")
	}
	model, err := mgr.UpsertModel(ctx, domaininference.Model{Key: "openai/gpt-4o-mini", Kind: domaininference.ModelKindGenerative, ProviderModelName: "gpt-4o-mini", ConnectorTypes: []domaininference.ConnectorType{domaininference.ConnectorOpenAICompatible}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := mgr.UpsertCapability(ctx, domaininference.Capability{EndpointID: endpoint.ID, ModelID: model.ID, Operation: domaininference.OperationChat, Key: "openai-gpt-4o-mini-chat", SupportsJSONMode: true, Enabled: true}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	secret, err := mgr.UpsertSecret(ctx, domaininference.Secret{OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "daemon", Kind: "inline_encrypted", Ciphertext: &domaininference.EncryptedSecretPayload{Algorithm: "MYCEL-ENVELOPE-V1", CipherB64: "cipher"}, SecretSuffix: "test"})
	if err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	credential, err := mgr.UpsertCredential(ctx, domaininference.Credential{Key: "openai-prod", EndpointID: endpoint.ID, OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "daemon", AuthType: domaininference.CredentialAuthBearer, SecretID: secret.ID, Status: domaininference.CredentialStatusActive})
	if err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	if err := mgr.DeleteSecret(ctx, secret.ID); err == nil {
		t.Fatal("DeleteSecret() error=nil while credential still references secret")
	}
	if err := mgr.DeleteCredential(ctx, credential.ID); err != nil {
		t.Fatalf("delete credential: %v", err)
	}
	if err := mgr.DeleteSecret(ctx, secret.ID); err != nil {
		t.Fatalf("delete unreferenced secret: %v", err)
	}
	if _, err := mgr.UpsertSecret(ctx, secret); err != nil {
		t.Fatalf("restore secret for round trip: %v", err)
	}
	if _, err := mgr.UpsertCredential(ctx, credential); err != nil {
		t.Fatalf("restore credential for round trip: %v", err)
	}

	reloaded := NewGlobalManager()
	if err := reloaded.Init(ctx, metaDir); err != nil {
		t.Fatalf("reload global manager: %v", err)
	}
	endpoints, err := reloaded.ListEndpoints(ctx)
	if err != nil || len(endpoints) != 1 || endpoints[0].Key != "openai" {
		t.Fatalf("unexpected endpoints after reload: %#v err=%v", endpoints, err)
	}
	models, err := reloaded.ListModels(ctx)
	if err != nil || len(models) != 1 || models[0].Key != "openai/gpt-4o-mini" {
		t.Fatalf("unexpected models after reload: %#v err=%v", models, err)
	}
	credentials, err := reloaded.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 || credentials[0].Key != "openai-prod" {
		t.Fatalf("unexpected credentials after reload: %#v err=%v", credentials, err)
	}
}

func TestSpaceManagerAndUsageLedgerRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	spaceDir := filepath.Join(root, "graphs", "space-1", "inference")
	mgr := NewSpaceManager()
	if err := mgr.Init(ctx, spaceDir, "space-1"); err != nil {
		t.Fatalf("init space manager: %v", err)
	}
	profile, err := mgr.UpsertProfile(ctx, domaininference.Profile{Key: "summarize", Operation: domaininference.OperationChat, Purpose: "automation", Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if profile.SpaceID != "space-1" || profile.ID == uuid.Nil {
		t.Fatalf("unexpected profile defaults: %#v", profile)
	}
	if _, err := mgr.UpsertPolicy(ctx, domaininference.Policy{Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive, Operations: []domaininference.Operation{domaininference.OperationChat}}); err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
	decision, err := mgr.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, Action: domaininference.PolicyDecisionAllowed})
	if err != nil {
		t.Fatalf("upsert decision: %v", err)
	}
	if decision.SpaceID != "space-1" || decision.DecidedAt.IsZero() {
		t.Fatalf("unexpected decision defaults: %#v", decision)
	}

	reloaded := NewSpaceManager()
	if err := reloaded.Init(ctx, spaceDir, "space-1"); err != nil {
		t.Fatalf("reload space manager: %v", err)
	}
	profiles, err := reloaded.ListProfiles(ctx)
	if err != nil || len(profiles) != 1 || profiles[0].Key != "summarize" {
		t.Fatalf("unexpected profiles after reload: %#v err=%v", profiles, err)
	}
	decisions, err := reloaded.ListPolicyDecisions(ctx)
	if err != nil || len(decisions) != 1 || decisions[0].Action != domaininference.PolicyDecisionAllowed {
		t.Fatalf("unexpected decisions after reload: %#v err=%v", decisions, err)
	}

	ledger := NewUsageLedger()
	if err := ledger.Init(ctx, filepath.Join(root, "meta", "accounting")); err != nil {
		t.Fatalf("init usage ledger: %v", err)
	}
	if _, err := ledger.AppendUsageEvent(ctx, domaininference.UsageEvent{Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, Status: domaininference.UsageStatusSucceeded, SpaceID: "space-1"}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	reloadedLedger := NewUsageLedger()
	if err := reloadedLedger.Init(ctx, filepath.Join(root, "meta", "accounting")); err != nil {
		t.Fatalf("reload usage ledger: %v", err)
	}
	events, err := reloadedLedger.ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusSucceeded {
		t.Fatalf("unexpected usage events: %#v err=%v", events, err)
	}
}

func TestUsageLedgerBudgetScopeRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	location := t.TempDir()
	ledger := NewUsageLedger()
	if err := ledger.Init(ctx, location); err != nil {
		t.Fatalf("Init() error=%v", err)
	}
	now := time.Now().UTC()
	scope := domaininference.BudgetScope{ID: "scope-1", State: domaininference.BudgetScopeActive, Limits: domaininference.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, BatchingMode: domaininference.BatchingModeSingle, MaxBatchItems: 1, Deadline: now.Add(time.Hour)}, Attempts: []domaininference.BudgetAttempt{{ID: "attempt-1", RequestID: "request-1", State: domaininference.BudgetAttemptReserved}}, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour), RetentionUntil: now.Add(2 * time.Hour)}
	if _, err := ledger.UpsertBudgetScope(ctx, scope); err != nil {
		t.Fatalf("UpsertBudgetScope() error=%v", err)
	}
	scope.Attempts[0].RequestID = "mutated"
	stored, err := ledger.GetBudgetScope(ctx, "scope-1")
	if err != nil || stored.Attempts[0].RequestID != "request-1" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	stored.Attempts[0].RequestID = "mutated-again"
	stored, _ = ledger.GetBudgetScope(ctx, "scope-1")
	if stored.Attempts[0].RequestID != "request-1" {
		t.Fatalf("GetBudgetScope() leaked mutable state: %+v", stored)
	}
	reloaded := NewUsageLedger()
	if err := reloaded.Init(ctx, location); err != nil {
		t.Fatalf("reload Init() error=%v", err)
	}
	stored, err = reloaded.GetBudgetScope(ctx, "scope-1")
	if err != nil || stored.ID != "scope-1" || len(stored.Attempts) != 1 {
		t.Fatalf("reloaded=%+v err=%v", stored, err)
	}
	if err := reloaded.DeleteBudgetScope(ctx, "scope-1"); err != nil {
		t.Fatalf("DeleteBudgetScope() error=%v", err)
	}
	if _, err := reloaded.GetBudgetScope(ctx, "scope-1"); !errors.Is(err, ErrBudgetScopeNotFound) {
		t.Fatalf("GetBudgetScope() error=%v", err)
	}
}
