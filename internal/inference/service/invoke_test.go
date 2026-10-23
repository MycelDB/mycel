package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
)

func TestInvokeFakeChatRecordsSuccessUsage(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fake := &connectors.FakeConnector{Text: "hello from fake"}
	resolver := &countingSecretResolver{value: "super-secret"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(resolver)

	authorized := false
	started := false
	resp, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"}, Input: "hello", RequestID: "req-1", AfterAuthorization: func(_ context.Context, resolved inferenceruntime.ResolveResult) error {
		if !resolved.Allowed || resolver.calls != 0 {
			t.Fatalf("authorization hook ran after credential resolution: resolved=%+v secret_calls=%d", resolved, resolver.calls)
		}
		authorized = true
		return nil
	}, BeforeProviderAttempt: func(_ context.Context, _ inferenceruntime.ResolveResult) error {
		if !authorized || resolver.calls != 1 {
			t.Fatalf("provider-start hook order: authorized=%v secret_calls=%d", authorized, resolver.calls)
		}
		started = true
		return nil
	}})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if !resp.Allowed || resp.Text != "hello from fake" || resp.Decision.Action != domaininference.PolicyDecisionAllowed || !authorized || !started {
		t.Fatalf("unexpected response: %#v", resp)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 1 || resolver.calls != 1 {
		t.Fatalf("expected connector and resolver calls, calls=%d secret=%d", chatCalls, resolver.calls)
	}
	events, err := module.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("usage events: %#v err=%v", events, err)
	}
	event := events[0]
	if event.Status != domaininference.UsageStatusSucceeded || event.PolicyDecisionID != resp.Decision.ID || event.ProviderRequestID != "fake" || event.TotalTokens == 0 {
		t.Fatalf("unexpected usage event: %#v", event)
	}
	if strings.Contains(event.ErrorMessage, "super-secret") || strings.Contains(strings.TrimSpace(event.ProviderRequestID), "super-secret") {
		t.Fatalf("usage event leaked secret: %#v", event)
	}
}

func TestInvokeDeniedDoesNotResolveSecretOrCallConnector(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	if err := fixture.spaceMgr.DeleteCredentialGrant(ctx, fixture.grant.ID); err != nil {
		t.Fatalf("delete grant: %v", err)
	}
	fake := &connectors.FakeConnector{Text: "should not run"}
	resolver := &countingSecretResolver{value: "super-secret"}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(resolver)

	resp, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"}, Input: "hello", RequestID: "req-denied"})
	if !errors.Is(err, inferenceruntime.ErrDenied) {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
	if resp.Allowed || resp.Decision.Action != domaininference.PolicyDecisionDenied {
		t.Fatalf("unexpected denied response: %#v", resp)
	}
	_, chatCalls := fake.Calls()
	if chatCalls != 0 || resolver.calls != 0 {
		t.Fatalf("denied request should not call connector or resolver, calls=%d secret=%d", chatCalls, resolver.calls)
	}
	events, err := module.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusDenied {
		t.Fatalf("expected one denied usage event: %#v err=%v", events, err)
	}
}

func TestInvokeConnectorFailureRecordsFailedUsage(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	module.SetConnector(domaininference.ConnectorOpenAICompatible, &connectors.FakeConnector{Err: connectors.NewFakeError("fake_failure", false, "fake connector failed")})
	module.SetSecretResolver(&countingSecretResolver{value: "super-secret"})

	_, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileRef: fixture.profile.Key, ActorPrincipalID: "actor-a"}, Input: "hello"})
	if err == nil || !strings.Contains(err.Error(), "fake connector failed") {
		t.Fatalf("expected connector failure, got %v", err)
	}
	events, err := module.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("usage events: %#v err=%v", events, err)
	}
	if events[0].Status != domaininference.UsageStatusFailed || events[0].ErrorCode != "fake_failure" {
		t.Fatalf("unexpected failure usage event: %#v", events[0])
	}
}

func TestInvokeTokenBasisFixtureThroughFullPath(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fixture.endpoint.ConnectorType = domaininference.ConnectorFake
	fixture.endpoint.Operations = []domaininference.Operation{domaininference.OperationEmbeddings}
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, fixture.endpoint); err != nil {
		t.Fatalf("update fake endpoint: %v", err)
	}
	profile, model, capability := setupEmbeddingInvokePath(t, ctx, module, fixture, domaininference.Model{
		Key:               "token-basis-model",
		Kind:              domaininference.ModelKindEmbedding,
		ProviderModelName: "mycel-token-basis-v1",
		EmbeddingDims:     4,
		VectorSpace:       "fixture/token-basis-v1/4",
		Enabled:           true,
		Metadata: map[string]any{
			connectors.FakeFixtureMetadataKey: map[string]any{"strategy": connectors.FakeFixtureTokenBasisV1},
		},
	})
	fake := &connectors.FakeConnector{}
	module.SetConnector(domaininference.ConnectorFake, fake)
	resolver := &countingSecretResolver{value: "must-not-be-read-by-fake"}
	module.SetSecretResolver(resolver)

	resp, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: profile.Key, ActorPrincipalID: "actor-a", ModelID: model.ID, CapabilityID: capability.ID}, Input: "axis-2", RequestID: "token-basis-integration"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if !resp.Allowed || len(resp.Embedding) != 4 || resp.Embedding[0] != 0 || resp.Embedding[1] != 0 || resp.Embedding[2] != 1 || resp.Embedding[3] != 0 {
		t.Fatalf("response = %#v", resp)
	}
	if !strings.HasPrefix(resp.ProviderRequestID, "fake-tbv1-") || resp.Usage.TotalTokens != 1 {
		t.Fatalf("request accounting = %#v", resp)
	}
	if resolver.calls != 0 {
		t.Fatalf("fake fixture resolved credentials %d time(s)", resolver.calls)
	}
}

func TestInvokeRejectsWrongEmbeddingDimensions(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fixture.endpoint.Operations = []domaininference.Operation{domaininference.OperationEmbeddings}
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, fixture.endpoint); err != nil {
		t.Fatalf("update endpoint: %v", err)
	}
	profile, model, capability := setupEmbeddingInvokePath(t, ctx, module, fixture, domaininference.Model{Key: "dimension-model", Kind: domaininference.ModelKindEmbedding, ProviderModelName: "dimension-model", EmbeddingDims: 3, VectorSpace: "fixture/dimension-3", Enabled: true})
	module.SetConnector(domaininference.ConnectorOpenAICompatible, &connectors.FakeConnector{Vector: []float64{1, 0}})
	module.SetSecretResolver(&countingSecretResolver{value: "unused-by-test"})

	resp, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: profile.Key, ActorPrincipalID: "actor-a", ModelID: model.ID, CapabilityID: capability.ID}, Input: "wrong dimensions"})
	if err == nil || connectors.ErrorCode(err) != "invalid_embedding_dimensions" {
		t.Fatalf("Invoke() response=%#v error=%v", resp, err)
	}
	events, listErr := module.UsageLedger().ListUsageEvents(ctx)
	if listErr != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusFailed || events[0].ErrorCode != "invalid_embedding_dimensions" {
		t.Fatalf("usage events=%#v error=%v", events, listErr)
	}
}

func TestInvokeFakeEmbeddingsThroughFullPath(t *testing.T) {
	ctx := context.Background()
	module, fixture := newResolverFixture(t, ctx)
	fixture.endpoint.Operations = []domaininference.Operation{domaininference.OperationChat, domaininference.OperationEmbeddings}
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, fixture.endpoint); err != nil {
		t.Fatalf("update endpoint operations: %v", err)
	}
	embeddingProfile, err := fixture.spaceMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: fixture.spaceID, Key: "semantic-embeddings", Operation: domaininference.OperationEmbeddings, DomainIDs: []string{"domain-a"}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert embedding profile: %v", err)
	}
	embeddingModel, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{Key: "embed-model", Kind: domaininference.ModelKindEmbedding, ProviderModelName: "embed-model", Enabled: true})
	if err != nil {
		t.Fatalf("upsert embedding model: %v", err)
	}
	embeddingCapability, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{EndpointID: fixture.endpoint.ID, ModelID: embeddingModel.ID, Operation: domaininference.OperationEmbeddings, Enabled: true})
	if err != nil {
		t.Fatalf("upsert embedding capability: %v", err)
	}
	if _, err := fixture.spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{SpaceID: fixture.spaceID, CredentialID: fixture.credential.ID, Scope: domaininference.Scope{SpaceID: fixture.spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{embeddingProfile.ID.String()}, EndpointRefs: []string{fixture.endpoint.ID.String()}, ModelRefs: []string{embeddingModel.ID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeSemantic}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert embedding grant: %v", err)
	}
	if _, err := fixture.spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: fixture.spaceID, Scope: domaininference.Scope{SpaceID: fixture.spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{embeddingProfile.ID.String()}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert embedding policy: %v", err)
	}
	fake := &connectors.FakeConnector{Vector: []float64{0.25, 0.75}}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	module.SetSecretResolver(&countingSecretResolver{value: "super-secret"})

	resp, err := module.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: fixture.spaceID, DomainID: "domain-a", Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: embeddingProfile.Key, ActorPrincipalID: "actor-a", ModelID: embeddingModel.ID, CapabilityID: embeddingCapability.ID}, Input: "embed this"})
	if err != nil {
		t.Fatalf("Invoke() embeddings error = %v", err)
	}
	if !resp.Allowed || len(resp.Embedding) != 2 || resp.Embedding[0] != 0.25 {
		t.Fatalf("unexpected embedding response: %#v", resp)
	}
	embedCalls, _ := fake.Calls()
	if embedCalls != 1 {
		t.Fatalf("expected one embedding call, got %d", embedCalls)
	}
}

func setupEmbeddingInvokePath(t *testing.T, ctx context.Context, module *Module, fixture resolverFixture, modelInput domaininference.Model) (domaininference.Profile, domaininference.Model, domaininference.Capability) {
	t.Helper()
	profile, err := fixture.spaceMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: fixture.spaceID, Key: modelInput.Key + "-profile", Operation: domaininference.OperationEmbeddings, DomainIDs: []string{"domain-a"}, Enabled: true})
	if err != nil {
		t.Fatalf("upsert embedding profile: %v", err)
	}
	model, err := module.GlobalManager().UpsertModel(ctx, modelInput)
	if err != nil {
		t.Fatalf("upsert embedding model: %v", err)
	}
	capability, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{EndpointID: fixture.endpoint.ID, ModelID: model.ID, Operation: domaininference.OperationEmbeddings, Enabled: true})
	if err != nil {
		t.Fatalf("upsert embedding capability: %v", err)
	}
	if _, err := fixture.spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{SpaceID: fixture.spaceID, CredentialID: fixture.credential.ID, Scope: domaininference.Scope{SpaceID: fixture.spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{profile.ID.String()}, EndpointRefs: []string{fixture.endpoint.ID.String()}, ModelRefs: []string{model.ID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeSemantic}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert embedding grant: %v", err)
	}
	if _, err := fixture.spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: fixture.spaceID, Scope: domaininference.Scope{SpaceID: fixture.spaceID, DomainID: "domain-a"}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{profile.ID.String()}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert embedding policy: %v", err)
	}
	return profile, model, capability
}

type countingSecretResolver struct {
	value string
	calls int
}

func (r *countingSecretResolver) ResolveSecret(context.Context, domaininference.Secret) (string, error) {
	r.calls++
	return r.value, nil
}
