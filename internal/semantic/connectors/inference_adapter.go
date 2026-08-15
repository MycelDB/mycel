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
// inference subsystem. Semantic indexes must declare an inference profile; INF11
// removes the legacy semantic connector fallback.
type InferenceAdapter struct {
	Manager inferenceservice.Manager
}

type Embedder interface {
	Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error)
}

func (a InferenceAdapter) Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error) {
	if a.Manager == nil {
		return EmbeddingResponse{}, fmt.Errorf("inference manager is not configured")
	}
	if strings.TrimSpace(in.InferenceProfile) == "" && in.InferenceProfileID == nilUUID {
		return EmbeddingResponse{}, fmt.Errorf("semantic index does not declare an inference profile")
	}
	resp, err := a.Manager.Invoke(ctx, inferenceservice.InvokeRequest{Resolve: inferenceservice.ResolveRequest{SpaceID: in.SpaceID.String(), DomainID: in.DomainID.String(), SemanticIndexID: in.SemanticIndexID.String(), NodeID: in.TargetNodeID.String(), Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, ProfileRef: in.InferenceProfile, ProfileID: domaininference.ProfileID(in.InferenceProfileID), EndpointID: domaininference.EndpointID(in.ModelEndpointID), ModelID: domaininference.ModelID(in.ModelID), CapabilityID: domaininference.CapabilityID(in.ModelEndpointCapabilityID), ActorPrincipalID: inferencePrincipalString(in.ActorPrincipalID), OnBehalfOfPrincipalID: inferencePrincipalString(in.OnBehalfOfPrincipalID), Metadata: map[string]any{"semantic_reason": in.Reason}}, Input: in.Input, SemanticIndexID: in.SemanticIndexID.String(), Metadata: map[string]any{"semantic_connector": "inference"}})
	if err != nil {
		return EmbeddingResponse{PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID)}, err
	}
	return EmbeddingResponse{Vector: append([]float64(nil), resp.Embedding...), InputTokens: int(resp.Usage.InputTokens), OutputTokens: int(resp.Usage.OutputTokens), TotalTokens: int(resp.Usage.TotalTokens), TokenCountSource: tokenCountSource(resp), ProviderRequestID: resp.ProviderRequestID, PolicyDecisionID: domainsemantic.PolicyDecisionID(resp.Decision.ID), CredentialID: domainsemantic.InferenceCredentialID(resp.Decision.CredentialID), CredentialGrantID: domainsemantic.CredentialGrantID(resp.Decision.CredentialGrantID), EndpointID: domainsemantic.ModelEndpointID(resp.Decision.EndpointID), ModelID: domainsemantic.InferenceModelID(resp.Decision.ModelID), CapabilityID: domainsemantic.ModelEndpointCapabilityID(resp.Decision.CapabilityID)}, nil
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

func inferencePrincipalString(id interface{ String() string }) string {
	value := strings.TrimSpace(id.String())
	if value == "" || value == "00000000-0000-0000-0000-000000000000" {
		return ""
	}
	return value
}

func IsInferenceDenied(err error) bool {
	return errors.Is(err, inferenceservice.ErrDenied)
}
