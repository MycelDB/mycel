package connectors

import (
	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type EmbedInput struct {
	ModelEndpointID           domainsemantic.ModelEndpointID
	ModelID                   domainsemantic.InferenceModelID
	ModelEndpointCapabilityID domainsemantic.ModelEndpointCapabilityID
	CredentialID              domainsemantic.InferenceCredentialID
	CredentialGrantID         domainsemantic.CredentialGrantID
	PolicyDecisionID          domainsemantic.PolicyDecisionID
	InferenceProfile          string
	InferenceProfileID        uuid.UUID
	SpaceID                   domainspace.SpaceID
	DomainID                  graph.DomainID
	SemanticIndexID           domainsemantic.SemanticIndexID
	TargetNodeID              graph.NodeID
	ActorPrincipalID          identity.PrincipalID
	EffectivePrincipalID      identity.PrincipalID
	OnBehalfOfPrincipalID     identity.PrincipalID
	Input                     string
	Reason                    string
}

type EmbeddingResponse struct {
	Vector            []float64
	InputTokens       int
	OutputTokens      int
	TotalTokens       int
	TokenCountSource  string
	ProviderRequestID string
	PolicyDecisionID  domainsemantic.PolicyDecisionID
	CredentialID      domainsemantic.InferenceCredentialID
	CredentialGrantID domainsemantic.CredentialGrantID
	EndpointID        domainsemantic.ModelEndpointID
	ModelID           domainsemantic.InferenceModelID
	CapabilityID      domainsemantic.ModelEndpointCapabilityID
}
