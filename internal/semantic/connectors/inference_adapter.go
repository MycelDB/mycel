package connectors

import (
	"context"
	"errors"
	"fmt"
	"strings"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

// InferenceAdapter routes semantic embedding requests through the standalone
// inference subsystem. A fallback connector can be supplied so legacy semantic
// indexes without an inference profile continue to function during the INF6
// conversion tranche.
type InferenceAdapter struct {
	Manager  inferenceservice.Manager
	Fallback Embedder
}

type Embedder interface {
	Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error)
}

func (a InferenceAdapter) Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error) {
	if a.Manager == nil {
		return a.fallback(ctx, in, fmt.Errorf("inference manager is not configured"))
	}
	if strings.TrimSpace(in.InferenceProfile) == "" && in.InferenceProfileID == nilUUID {
		return a.fallback(ctx, in, fmt.Errorf("semantic index does not declare an inference profile"))
	}
	resp, err := a.Manager.Invoke(ctx, inferenceservice.InvokeRequest{Resolve: inferenceservice.ResolveRequest{SpaceID: in.SpaceID.String(), DomainID: in.DomainID.String(), SemanticIndexID: in.SemanticIndexID.String(), NodeID: in.TargetNodeID.String(), Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: in.InferenceProfile, ProfileID: domaininference.ProfileID(in.InferenceProfileID), EndpointID: domaininference.EndpointID(in.ModelEndpointID), ModelID: domaininference.ModelID(in.ModelID), CapabilityID: domaininference.CapabilityID(in.ModelEndpointCapabilityID), ActorPrincipalID: in.ActorPrincipalID.String(), OnBehalfOfPrincipalID: in.OnBehalfOfPrincipalID.String(), Metadata: map[string]any{"semantic_reason": in.Reason}}, Input: in.Input, SemanticIndexID: in.SemanticIndexID.String(), Metadata: map[string]any{"semantic_connector": "inference"}})
	if err != nil {
		return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, err
	}
	return EmbeddingResponse{Vector: append([]float64(nil), resp.Embedding...), InputTokens: int(resp.Usage.InputTokens), OutputTokens: int(resp.Usage.OutputTokens), TotalTokens: int(resp.Usage.TotalTokens), TokenCountSource: tokenCountSource(resp), ProviderRequestID: resp.ProviderRequestID, PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, nil
}

func (a InferenceAdapter) fallback(ctx context.Context, in EmbedInput, cause error) (EmbeddingResponse, error) {
	if a.Fallback == nil {
		return EmbeddingResponse{}, cause
	}
	resp, err := a.Fallback.Embed(ctx, in)
	if err != nil {
		if cause != nil {
			return resp, fmt.Errorf("%w; fallback failed: %v", cause, err)
		}
		return resp, err
	}
	return resp, nil
}

func tokenCountSource(resp inferenceservice.InvokeResponse) string {
	if resp.Usage.TokenCountSource != "" {
		return resp.Usage.TokenCountSource
	}
	if resp.Usage.TotalTokens > 0 {
		return "provider_reported"
	}
	return "unavailable"
}

var nilUUID domaininference.ProfileID

func IsInferenceDenied(err error) bool {
	return errors.Is(err, inferenceservice.ErrDenied)
}
