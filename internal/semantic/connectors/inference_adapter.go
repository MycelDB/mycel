package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InferenceAdapter routes semantic embedding requests through the standalone
// inference subsystem. Semantic indexes must declare an inference profile; INF11
// removes the legacy semantic connector fallback.
type InferenceInvoker interface {
	Invoke(ctx context.Context, req inferenceruntime.InvokeRequest) (inferenceruntime.InvokeResponse, error)
}

type semanticBudgetScopeReader interface {
	CurrentSemanticBudgetScope(ctx context.Context, spaceID, semanticRuleID, embeddingBindingKey string) (domaininference.BudgetScope, error)
}

type InferenceAdapter struct {
	Invoker InferenceInvoker
}

type Embedder interface {
	Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error)
}

func (a InferenceAdapter) Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error) {
	if a.Invoker == nil {
		return EmbeddingResponse{}, fmt.Errorf("inference manager is not configured")
	}
	if strings.TrimSpace(in.InferenceProfile) == "" && in.InferenceProfileID == nilUUID {
		return EmbeddingResponse{}, fmt.Errorf("semantic index does not declare an inference profile")
	}
	req := inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: in.SpaceID.String(), DomainID: in.DomainID.String(), SemanticRuleID: in.SemanticRuleID.String(), EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: in.SemanticIndexID.String(), NodeID: in.TargetNodeID.String(), Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: in.InferenceProfile, ProfileID: domaininference.ProfileID(in.InferenceProfileID), EndpointID: domaininference.EndpointID(in.ModelEndpointID), ModelID: domaininference.ModelID(in.ModelID), CapabilityID: domaininference.CapabilityID(in.ModelEndpointCapabilityID), ActorPrincipalID: inferencePrincipalString(in.ActorPrincipalID), OnBehalfOfPrincipalID: inferencePrincipalString(in.OnBehalfOfPrincipalID), Metadata: map[string]any{"semantic_reason": in.Reason, "embedding_binding_key": in.EmbeddingBindingKey}}, Input: in.Input, SemanticRuleID: in.SemanticRuleID.String(), EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: in.SemanticIndexID.String(), Metadata: map[string]any{"semantic_connector": "inference"}}
	if in.ProviderBudgetRequired {
		reader, ok := a.Invoker.(semanticBudgetScopeReader)
		if !ok {
			return EmbeddingResponse{}, fmt.Errorf("semantic provider budget enforcement is unavailable")
		}
		scope, err := reader.CurrentSemanticBudgetScope(ctx, in.SpaceID.String(), in.SemanticRuleID.String(), in.EmbeddingBindingKey)
		if err != nil {
			return EmbeddingResponse{}, fmt.Errorf("semantic provider budget scope is unavailable: %w", err)
		}
		requestID := strings.TrimSpace(in.ProviderBudgetRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
		}
		req.RequestID = requestID
		req.BudgetScopeID = scope.ID
		req.BudgetAttemptID = domaininference.BudgetAttemptID(uuid.NewString())
		req.BudgetRetry = in.ProviderBudgetRetry
		req.BudgetRetryIndex = in.ProviderBudgetRetryIndex
		req.BatchItems = 1
	}
	resp, err := a.Invoker.Invoke(ctx, req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, ctxErr
		}
		if errors.Is(err, context.Canceled) {
			return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, context.DeadlineExceeded
		}
		if in.ProviderBudgetRequired && status.Code(err) == codes.FailedPrecondition {
			return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, EmbeddingFailure{Category: "budget_contention", Retryable: true}
		}
		var connectorErr inferenceconnectors.ConnectorError
		if errors.As(err, &connectorErr) {
			return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, EmbeddingFailure{Category: inferenceFailureCategory(connectorErr), Retryable: connectorErr.Retryable, RetryAfter: connectorErr.RetryAfter}
		}
		return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, err
	}
	return EmbeddingResponse{Vector: append([]float64(nil), resp.Embedding...), InputTokens: int(resp.Usage.InputTokens), OutputTokens: int(resp.Usage.OutputTokens), TotalTokens: int(resp.Usage.TotalTokens), TokenCountSource: tokenCountSource(resp), ProviderRequestID: resp.ProviderRequestID, PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID), CredentialID: domainsemantic.InferenceCredentialID(resp.Decision.CredentialID), CredentialGrantID: domainsemantic.CredentialGrantID(resp.Decision.CredentialGrantID), EndpointID: domainsemantic.ModelEndpointID(resp.Decision.EndpointID), ModelID: domainsemantic.InferenceModelID(resp.Decision.ModelID), CapabilityID: domainsemantic.ModelEndpointCapabilityID(resp.Decision.CapabilityID)}, nil
}

func inferenceFailureCategory(err inferenceconnectors.ConnectorError) string {
	code := strings.ToLower(strings.TrimSpace(err.Code))
	if err.StatusCode == http.StatusTooManyRequests || isRateLimitConnectorCode(code) {
		return "rate_limited"
	}
	if err.Retryable {
		return "provider_error"
	}
	return "provider_rejected"
}

func isRateLimitConnectorCode(code string) bool {
	switch code {
	case "429", "provider_http_429", "emulator_429", "rate_limit", "rate_limited", "rate_limit_exceeded", "too_many_requests":
		return true
	default:
		return false
	}
}

func tokenCountSource(resp inferenceruntime.InvokeResponse) string {
	if resp.Usage.TokenCountSource != "" {
		return resp.Usage.TokenCountSource
	}
	if resp.Usage.TotalTokens > 0 {
		return "provider_reported"
	}
	return "unavailable"
}

var nilUUID domaininference.ProfileID

func inferencePrincipalString(id interface{ String() string }) string {
	value := strings.TrimSpace(id.String())
	if value == "" || value == "00000000-0000-0000-0000-000000000000" {
		return ""
	}
	return value
}

func IsInferenceDenied(err error) bool {
	return errors.Is(err, inferenceruntime.ErrDenied)
}
