package service

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestResolveAllowsMatchingProfileGrantAndPolicy(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)

	result, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !result.Allowed || result.Decision.Action != domaininference.PolicyDecisionAllowed {
		t.Fatalf("expected allowed result, got %#v", result.Decision)
	}
	if result.Profile.ID != fixture.profile.ID || result.Capability.ID != fixture.capability.ID || result.Credential.ID != fixture.credential.ID || result.CredentialGrant.ID != fixture.grant.ID {
		t.Fatalf("unexpected resolved resources: %#v", result)
	}
	decisions, err := fixture.spaceMgr.ListPolicyDecisions(ctx)
	if err != nil || len(decisions) != 1 || decisions[0].Action != domaininference.PolicyDecisionAllowed {
		t.Fatalf("decision was not persisted: %#v err=%v", decisions, err)
	}
}

func TestResolveDeniesWhenGrantMissing(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	if err := fixture.spaceMgr.DeleteCredentialGrant(ctx, fixture.grant.ID); err != nil {
		t.Fatalf("delete grant: %v", err)
	}

	result, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Allowed || result.Decision.Action != domaininference.PolicyDecisionDenied || !strings.Contains(result.Decision.Reason, "credential grant") {
		t.Fatalf("expected denied missing grant decision, got %#v", result.Decision)
	}
}

func TestResolveDenyPolicyWins(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	if _, err := fixture.spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: fixture.spaceID, Scope: domaininference.Scope{SpaceID: fixture.spaceID}, Operations: []domaininference.Operation{domaininference.OperationChat}, Action: domaininference.PolicyActionDeny, State: domaininference.PolicyStateActive, Reason: "operator disabled automation inference"}); err != nil {
		t.Fatalf("upsert deny policy: %v", err)
	}

	result, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Allowed || result.Decision.Action != domaininference.PolicyDecisionDenied || result.Decision.Reason != "operator disabled automation inference" {
		t.Fatalf("expected deny policy decision, got %#v", result.Decision)
	}
}

func TestResolveRestrictPolicyEnforcesTokenCeiling(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	if _, err := fixture.spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: fixture.spaceID, Scope: domaininference.Scope{SpaceID: fixture.spaceID}, Operations: []domaininference.Operation{domaininference.OperationChat}, Action: domaininference.PolicyActionRestrict, MaxOutputTokens: 32, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert restrict policy: %v", err)
	}

	result, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a", Parameters: domaininference.Parameters{MaxOutputTokens: 64}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if result.Allowed || result.Decision.Action != domaininference.PolicyDecisionDenied || !strings.Contains(result.Decision.Reason, "max output") {
		t.Fatalf("expected token ceiling denial, got %#v", result.Decision)
	}
}

func TestResolvePrincipalOwnedCredentialRequiresMatchingPrincipal(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fixture.credential.OwnerType = domaininference.CredentialOwnerPrincipal
	fixture.credential.OwnerID = "owner-a"
	if _, err := module.GlobalManager().UpsertCredential(ctx, fixture.credential); err != nil {
		t.Fatalf("update credential owner: %v", err)
	}

	denied, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if denied.Allowed {
		t.Fatalf("expected principal owner mismatch denial")
	}

	allowed, err := module.Resolve(ctx, ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "owner-a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !allowed.Allowed {
		t.Fatalf("expected matching principal to be allowed, got %#v", allowed.Decision)
	}
}

type resolverFixture struct {
	spaceID  string
	spaceMgr interface {
		UpsertProfile(context.Context, domaininference.Profile) (domaininference.Profile, error)
		UpsertPolicy(context.Context, domaininference.Policy) (domaininference.Policy, error)
		UpsertCredentialGrant(context.Context, domaininference.CredentialGrant) (domaininference.CredentialGrant, error)
		DeleteCredentialGrant(context.Context, domaininference.CredentialGrantID) error
		ListPolicyDecisions(context.Context) ([]domaininference.PolicyDecision, error)
	}
	profile    domaininference.Profile
	endpoint   domaininference.Endpoint
	model      domaininference.Model
	capability domaininference.Capability
	secret     domaininference.Secret
	credential domaininference.Credential
	grant      domaininference.CredentialGrant
	policy     domaininference.Policy
}

func newResolverFixture(t *testing.T, ctx context.Context) (*Module, resolverFixture) {
	t.Helper()
	module := NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init module: %#v", result)
	}
	spaceID := uuid.NewString()
	spaceMgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	endpoint, err := module.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{Key: "openai", ConnectorType: domaininference.ConnectorOpenAICompatible, NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, Operations: []domaininference.Operation{domaininference.OperationChat}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	model, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{Key: "gpt-test", Operation: domaininference.OperationChat, ProviderModelName: "gpt-test", ConnectorTypes: []domaininference.ConnectorType{domaininference.ConnectorOpenAICompatible}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	capability, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{EndpointID: endpoint.ID, ModelID: model.ID, Operation: domaininference.OperationChat, Key: "openai:gpt-test:chat", SupportsJSONMode: true, MaxOutputTokens: 128, Enabled: true})
	if err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	secret, err := module.GlobalManager().UpsertSecret(ctx, domaininference.Secret{OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", Kind: "external_ref", ExternalRef: "env://OPENAI_API_KEY"})
	if err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	credential, err := module.GlobalManager().UpsertCredential(ctx, domaininference.Credential{Key: "openai-key", EndpointID: endpoint.ID, OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", AuthType: domaininference.CredentialAuthAPIKey, SecretID: secret.ID, Status: domaininference.CredentialStatusActive})
	if err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	profile, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: spaceID, Key: "automation-chat", Operation: domaininference.OperationChat, Purpose: "automation", DomainIDs: []string{"domain-a"}, CapabilityRefs: []string{capability.ID.String()}, PrivacyRequirement: domaininference.PrivacyRequirement{AllowedPrivacyClasses: []domaininference.PrivacyClass{domaininference.PrivacyClassThirdParty}}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	grant, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{SpaceID: spaceID, CredentialID: credential.ID, Scope: domaininference.Scope{SpaceID: spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{profile.ID.String()}, EndpointRefs: []string{endpoint.ID.String()}, ModelRefs: []string{model.ID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeAutomation}, State: domaininference.GrantStateActive, CreatedBy: "admin"})
	if err != nil {
		t.Fatalf("upsert grant: %v", err)
	}
	policy, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: spaceID, Scope: domaininference.Scope{SpaceID: spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{profile.ID.String()}, Action: domaininference.PolicyActionAllow, AllowedPrivacyClasses: []domaininference.PrivacyClass{domaininference.PrivacyClassThirdParty}, State: domaininference.PolicyStateActive, CreatedBy: "admin"})
	if err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
	return module, resolverFixture{spaceID: spaceID, spaceMgr: spaceMgr, profile: profile, endpoint: endpoint, model: model, capability: capability, secret: secret, credential: credential, grant: grant, policy: policy}
}
