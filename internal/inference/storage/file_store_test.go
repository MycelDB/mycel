package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

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
	model, err := mgr.UpsertModel(ctx, domaininference.Model{Key: "openai/gpt-4o-mini", Operation: domaininference.OperationChat, ProviderModelName: "gpt-4o-mini", ConnectorTypes: []domaininference.ConnectorType{domaininference.ConnectorOpenAICompatible}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := mgr.UpsertCapability(ctx, domaininference.Capability{EndpointID: endpoint.ID, ModelID: model.ID, Operation: domaininference.OperationChat, Key: "openai-gpt-4o-mini-chat", SupportsJSONMode: true, Enabled: true}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	secret, err := mgr.UpsertSecret(ctx, domaininference.Secret{OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "daemon", Kind: "external_ref", ExternalRef: "env://OPENAI_API_KEY"})
	if err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	if _, err := mgr.UpsertCredential(ctx, domaininference.Credential{Key: "openai-prod", EndpointID: endpoint.ID, OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "daemon", AuthType: domaininference.CredentialAuthBearer, SecretID: secret.ID, Status: domaininference.CredentialStatusActive}); err != nil {
		t.Fatalf("upsert credential: %v", err)
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
