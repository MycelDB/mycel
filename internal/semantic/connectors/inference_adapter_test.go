package connectors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInferenceAdapterEmbedsThroughStandaloneInference(t *testing.T) {
	ctx := context.Background()
	module := inferenceservice.NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	fake := &inferenceconnectors.FakeConnector{Vector: []float64{0.4, 0.6}}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	ids := semanticInferenceAdapterIDs{spaceID: domainspace.SpaceID(uuid.New()), domainID: graph.DomainID(uuid.New()), indexID: domainsemantic.SemanticIndexID(uuid.New()), nodeID: graph.NodeID(uuid.New()), endpointID: domainsemantic.ModelEndpointID(uuid.New()), modelID: domainsemantic.InferenceModelID(uuid.New()), capID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), credentialID: domainsemantic.InferenceCredentialID(uuid.New()), secretID: domainsemantic.SecretID(uuid.New()), grantID: domainsemantic.CredentialGrantID(uuid.New()), profileID: uuid.New()}
	seedStandaloneInferenceForSemanticAdapter(t, ctx, module, ids)
	now := time.Now().UTC()
	budgetRequest := inferenceruntime.SemanticBudgetScopeRequest{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticRuleID: ids.indexID.String(), EmbeddingBindingKey: "search", CorrelationID: "sgt9-test", ProfileID: domaininference.ProfileID(ids.profileID), ActorPrincipalID: "actor-a", TotalTimeout: time.Minute, Limits: domaininference.BudgetLimits{MaxRequests: 10, MaxAttempts: 12, MaxRetryAttempts: 2, MaxRetriesPerRequest: 1, MaxConcurrentProviderCalls: 1, BatchingMode: domaininference.BatchingModeSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxInputTokens: 1000, MaxProviderMillisPerRequest: 1000, MaxProviderMillis: 10_000, Currency: "USD", PriceDefinitionVersion: "test-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1000, Deadline: now.Add(time.Minute)}}
	invalidRequest := budgetRequest
	invalidRequest.Limits.MaxRequests = 10_001
	invalidRequest.Limits.MaxAttempts = 10_001
	invalidRequest.Limits.MaxRetryAttempts = 0
	invalidRequest.Limits.MaxRetriesPerRequest = 0
	if err := module.ValidateSemanticBudgetScope(ctx, invalidRequest); err == nil {
		t.Fatal("preflight accepted scope beyond daemon attempt capacity")
	}
	if scopes, err := module.ListBudgetScopes(ctx); err != nil || len(scopes) != 0 {
		t.Fatalf("preflight mutated scopes=%+v error=%v", scopes, err)
	}
	scope, err := module.EnsureSemanticBudgetScope(ctx, budgetRequest)
	if err != nil {
		t.Fatalf("EnsureSemanticBudgetScope() error=%v", err)
	}

	resp, err := (InferenceAdapter{Invoker: module}).Embed(ctx, EmbedInput{ModelEndpointID: ids.endpointID, ModelID: ids.modelID, ModelEndpointCapabilityID: ids.capID, CredentialID: ids.credentialID, CredentialGrantID: ids.grantID, SpaceID: ids.spaceID, DomainID: ids.domainID, SemanticRuleID: domainsemantic.SemanticRuleID(ids.indexID), EmbeddingBindingKey: "search", SemanticIndexID: ids.indexID, TargetNodeID: ids.nodeID, ActorPrincipalID: identity.PrincipalID("actor-a"), InferenceProfileID: ids.profileID, Input: "semantic text", Reason: "semantic_backfill", ProviderBudgetRequired: true})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if len(resp.Vector) != 2 || resp.PolicyDecisionID == uuid.Nil || resp.ProviderRequestID != "fake" || resp.CredentialGrantID != ids.grantID {
		t.Fatalf("unexpected response: %#v", resp)
	}
	embedCalls, _ := fake.Calls()
	if embedCalls != 1 {
		t.Fatalf("expected standalone fake connector to be called once, got %d", embedCalls)
	}
	persistedScope, err := module.GetBudgetScope(ctx, scope.ID)
	if err != nil || persistedScope.Usage.Requests != 1 || persistedScope.Usage.Attempts != 1 || len(persistedScope.AttemptEvents) != 2 {
		t.Fatalf("unexpected semantic budget scope: %#v err=%v", persistedScope, err)
	}
	events, err := module.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 || events[0].Status != domaininference.UsageStatusSucceeded || events[0].SemanticIndexID != ids.indexID.String() {
		t.Fatalf("unexpected usage events: %#v err=%v", events, err)
	}
}

func TestSemanticBudgetPreflightRejectsCapabilityConnectorBatchingMismatch(t *testing.T) {
	ctx := context.Background()
	module := inferenceservice.NewModule()
	if result := module.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	module.SetConnector(domaininference.ConnectorOpenAICompatible, &inferenceconnectors.FakeConnector{})
	ids := semanticInferenceAdapterIDs{spaceID: domainspace.SpaceID(uuid.New()), domainID: graph.DomainID(uuid.New()), indexID: domainsemantic.SemanticIndexID(uuid.New()), nodeID: graph.NodeID(uuid.New()), endpointID: domainsemantic.ModelEndpointID(uuid.New()), modelID: domainsemantic.InferenceModelID(uuid.New()), capID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), credentialID: domainsemantic.InferenceCredentialID(uuid.New()), secretID: domainsemantic.SecretID(uuid.New()), grantID: domainsemantic.CredentialGrantID(uuid.New()), profileID: uuid.New()}
	seedStandaloneInferenceForSemanticAdapter(t, ctx, module, ids)
	if _, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{ID: domaininference.CapabilityID(ids.capID), EndpointID: domaininference.EndpointID(ids.endpointID), ModelID: domaininference.ModelID(ids.modelID), Operation: domaininference.OperationEmbeddings, BatchingMode: domaininference.BatchingModeBatch, MaxBatchItems: 2, Enabled: true}); err != nil {
		t.Fatalf("update capability: %v", err)
	}
	now := time.Now().UTC()
	err := module.ValidateSemanticBudgetScope(ctx, inferenceruntime.SemanticBudgetScopeRequest{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticRuleID: ids.indexID.String(), EmbeddingBindingKey: "search", CorrelationID: "sgt9-test", ProfileID: domaininference.ProfileID(ids.profileID), ActorPrincipalID: "actor-a", TotalTimeout: time.Minute, Limits: domaininference.BudgetLimits{MaxRequests: 1, MaxAttempts: 1, MaxConcurrentProviderCalls: 1, BatchingMode: domaininference.BatchingModeSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 100, MaxInputTokens: 100, MaxProviderMillisPerRequest: 1000, MaxProviderMillis: 1000, Currency: "USD", PriceDefinitionVersion: "test-v1", InputMicrosPerMillionTokens: 1, MaxEstimatedCostMicros: 1000, Deadline: now.Add(time.Minute)}})
	var budgetErr inferenceservice.BudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Reason != "connector_batching_unsupported" {
		t.Fatalf("ValidateSemanticBudgetScope() error=%v", err)
	}
	if scopes, listErr := module.ListBudgetScopes(ctx); listErr != nil || len(scopes) != 0 {
		t.Fatalf("preflight mutated scopes=%+v error=%v", scopes, listErr)
	}
}

func TestInferenceAdapterForwardsStableBudgetRetryIdentity(t *testing.T) {
	invoker := &capturingInferenceInvoker{}
	requestID := uuid.NewString()
	adapter := InferenceAdapter{Invoker: invoker}
	if _, err := adapter.Embed(context.Background(), EmbedInput{SpaceID: domainspace.SpaceID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", InferenceProfileID: uuid.New(), Input: "semantic text", ProviderBudgetRequired: true, ProviderBudgetRequestID: requestID, ProviderBudgetRetry: true, ProviderBudgetRetryIndex: 2}); err != nil {
		t.Fatalf("Embed() error=%v", err)
	}
	if invoker.request.RequestID != requestID || !invoker.request.BudgetRetry || invoker.request.BudgetRetryIndex != 2 {
		t.Fatalf("retry metadata=%+v", invoker.request)
	}
}

func TestInferenceAdapterClassifiesReplicatedBudgetContentionAsRetryable(t *testing.T) {
	adapter := InferenceAdapter{Invoker: &capturingInferenceInvoker{err: status.Error(codes.FailedPrecondition, "scope changed")}}
	_, err := adapter.Embed(context.Background(), EmbedInput{SpaceID: domainspace.SpaceID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", InferenceProfileID: uuid.New(), Input: "semantic text", ProviderBudgetRequired: true})
	var failure EmbeddingFailure
	if !errors.As(err, &failure) || failure.Category != "budget_contention" || !failure.Retryable {
		t.Fatalf("Embed() error=%T %v failure=%+v", err, err, failure)
	}
}

func TestInferenceAdapterPreservesSanitizedRetryMetadata(t *testing.T) {
	adapter := InferenceAdapter{Invoker: failingInferenceInvoker{err: inferenceconnectors.ConnectorError{Code: "emulator_429", Retryable: true, RetryAfter: 250 * time.Millisecond, Err: fmt.Errorf("provider body must not escape")}}}
	_, err := adapter.Embed(context.Background(), EmbedInput{InferenceProfileID: uuid.New(), Input: "semantic text"})
	var failure EmbeddingFailure
	if !errors.As(err, &failure) {
		t.Fatalf("Embed() error=%T %v, want EmbeddingFailure", err, err)
	}
	if failure.Category != "rate_limited" || !failure.Retryable || failure.RetryAfter != 250*time.Millisecond {
		t.Fatalf("unexpected failure metadata: %#v", failure)
	}
	if strings.Contains(err.Error(), "provider body") {
		t.Fatalf("embedding failure leaked provider body: %v", err)
	}
}

func TestInferenceAdapterPreservesCancellationIdentity(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := InferenceAdapter{Invoker: failingInferenceInvoker{err: inferenceconnectors.ConnectorError{Code: "network_error", Retryable: true, Err: tt.err}}}
			_, err := adapter.Embed(context.Background(), EmbedInput{InferenceProfileID: uuid.New(), Input: "semantic text"})
			if !errors.Is(err, tt.err) {
				t.Fatalf("Embed() error=%T %v, want %v identity", err, err, tt.err)
			}
			var failure EmbeddingFailure
			if errors.As(err, &failure) {
				t.Fatalf("cancellation was reclassified as provider failure: %#v", failure)
			}
		})
	}
}

func TestInferenceAdapterClassifiesRateLimitFromStatusNotSubstring(t *testing.T) {
	tests := []struct {
		name     string
		failure  inferenceconnectors.ConnectorError
		category string
	}{
		{
			name:     "http 429 with non-rate provider code",
			failure:  inferenceconnectors.ConnectorError{Code: "insufficient_quota", StatusCode: http.StatusTooManyRequests, Retryable: true},
			category: "rate_limited",
		},
		{
			name:     "unrelated code containing rate",
			failure:  inferenceconnectors.ConnectorError{Code: "generate_failed", Retryable: false},
			category: "provider_rejected",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := InferenceAdapter{Invoker: failingInferenceInvoker{err: tt.failure}}
			_, err := adapter.Embed(context.Background(), EmbedInput{InferenceProfileID: uuid.New(), Input: "semantic text"})
			var failure EmbeddingFailure
			if !errors.As(err, &failure) {
				t.Fatalf("Embed() error=%T %v, want EmbeddingFailure", err, err)
			}
			if failure.Category != tt.category || failure.Retryable != tt.failure.Retryable {
				t.Fatalf("failure=%#v, want category=%q retryable=%t", failure, tt.category, tt.failure.Retryable)
			}
		})
	}
}

func TestInferenceAdapterFailsClosedWhenBudgetReaderUnavailable(t *testing.T) {
	adapter := InferenceAdapter{Invoker: failingInferenceInvoker{}}
	_, err := adapter.Embed(context.Background(), EmbedInput{SpaceID: domainspace.SpaceID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", InferenceProfileID: uuid.New(), Input: "semantic text", ProviderBudgetRequired: true})
	if err == nil || !strings.Contains(err.Error(), "budget enforcement is unavailable") {
		t.Fatalf("expected unavailable budget reader error, got %v", err)
	}
}

func TestInferenceAdapterRequiresProfile(t *testing.T) {
	ctx := context.Background()
	_, err := (InferenceAdapter{Invoker: inferenceservice.NewModule()}).Embed(ctx, EmbedInput{Input: "semantic text"})
	if err == nil || !strings.Contains(err.Error(), "does not declare an inference profile") {
		t.Fatalf("expected missing profile error, got %v", err)
	}
}

type failingInferenceInvoker struct{ err error }

type capturingInferenceInvoker struct {
	request inferenceruntime.InvokeRequest
	err     error
}

func (f failingInferenceInvoker) Invoke(context.Context, inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	return inferenceruntime.InvokeResponse{}, f.err
}

func (c *capturingInferenceInvoker) Invoke(_ context.Context, request inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error) {
	c.request = request
	return inferenceruntime.InvokeResponse{}, c.err
}

func (c *capturingInferenceInvoker) CurrentSemanticBudgetScope(context.Context, string, string, string) (domaininference.BudgetScope, error) {
	return domaininference.BudgetScope{ID: domaininference.BudgetScopeID(uuid.NewString())}, nil
}

type semanticInferenceAdapterIDs struct {
	spaceID      domainspace.SpaceID
	domainID     graph.DomainID
	indexID      domainsemantic.SemanticIndexID
	nodeID       graph.NodeID
	endpointID   domainsemantic.ModelEndpointID
	modelID      domainsemantic.InferenceModelID
	capID        domainsemantic.ModelEndpointCapabilityID
	credentialID domainsemantic.InferenceCredentialID
	secretID     domainsemantic.SecretID
	grantID      domainsemantic.CredentialGrantID
	profileID    uuid.UUID
}

func seedStandaloneInferenceForSemanticAdapter(t *testing.T, ctx context.Context, module *inferenceservice.Module, ids semanticInferenceAdapterIDs) {
	t.Helper()
	if _, err := module.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{ID: domaininference.EndpointID(ids.endpointID), Key: "endpoint", ConnectorType: domaininference.ConnectorOpenAICompatible, NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, Enabled: true}); err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	if _, err := module.GlobalManager().UpsertModel(ctx, domaininference.Model{ID: domaininference.ModelID(ids.modelID), Key: "model", Kind: domaininference.ModelKindEmbedding, ProviderModelName: "text-embedding-3-small", Enabled: true}); err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCapability(ctx, domaininference.Capability{ID: domaininference.CapabilityID(ids.capID), EndpointID: domaininference.EndpointID(ids.endpointID), ModelID: domaininference.ModelID(ids.modelID), Operation: domaininference.OperationEmbeddings, Enabled: true}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	if _, err := module.GlobalManager().UpsertSecret(ctx, domaininference.Secret{ID: domaininference.SecretID(ids.secretID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", Kind: "none"}); err != nil {
		t.Fatalf("upsert secret: %v", err)
	}
	if _, err := module.GlobalManager().UpsertCredential(ctx, domaininference.Credential{ID: domaininference.CredentialID(ids.credentialID), Key: "credential", EndpointID: domaininference.EndpointID(ids.endpointID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "system", AuthType: domaininference.CredentialAuthNone, SecretID: domaininference.SecretID(ids.secretID), Status: domaininference.CredentialStatusActive}); err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	spaceMgr, err := module.SpaceManager(ctx, ids.spaceID.String())
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{ID: domaininference.ProfileID(ids.profileID), SpaceID: ids.spaceID.String(), Key: "semantic", Operation: domaininference.OperationEmbeddings, DomainIDs: []string{ids.domainID.String()}, EndpointRefs: []string{ids.endpointID.String()}, ModelRefs: []string{ids.modelID.String()}, CapabilityRefs: []string{ids.capID.String()}, Enabled: true}); err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(ids.grantID), SpaceID: ids.spaceID.String(), CredentialID: domaininference.CredentialID(ids.credentialID), Scope: domaininference.Scope{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticIndexID: ids.indexID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{ids.profileID.String()}, EndpointRefs: []string{ids.endpointID.String()}, ModelRefs: []string{ids.modelID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeSemantic}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert grant: %v", err)
	}
	if _, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: ids.spaceID.String(), Scope: domaininference.Scope{SpaceID: ids.spaceID.String(), DomainID: ids.domainID.String(), SemanticIndexID: ids.indexID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{ids.profileID.String()}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert policy: %v", err)
	}
}
