package search

import (
	"context"

	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

type Planner struct {
	GlobalManager storesemantic.GlobalManager
	SpaceManager  storesemantic.SpaceManager
	Connector     ConnectorService
	VectorBackend vectorstore.Backend
}

type ConnectorService interface {
	Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error)
}

type Input struct {
	SpaceID          domainspace.SpaceID
	DomainID         graph.DomainID
	SemanticIndexIDs []domainsemantic.SemanticIndexID
	Purpose          domainsemantic.SemanticIndexPurpose
	Text             string
	Limit            int
	MinScore         float64
	ActorPrincipalID identity.UserID
}

type Result struct {
	Results  []SearchResult `json:"results"`
	Warnings []string       `json:"warnings,omitempty"`
	Groups   []GroupSummary `json:"groups,omitempty"`
}

type SearchResult struct {
	SemanticIndexID   domainsemantic.SemanticIndexID           `json:"semantic_index_id"`
	NodeID            graph.NodeID                             `json:"node_id"`
	RecordID          domainsemantic.AdvancedEmbeddingRecordID `json:"record_id"`
	Score             float64                                  `json:"score"`
	ModelEndpointID   domainsemantic.ModelEndpointID           `json:"model_endpoint_id"`
	ModelID           domainsemantic.InferenceModelID          `json:"model_id"`
	VectorStoreID     domainsemantic.VectorStoreID             `json:"vector_store_id"`
	CredentialGrantID domainsemantic.CredentialGrantID         `json:"credential_grant_id,omitempty"`
	VectorSpaceKey    string                                   `json:"vector_space_key,omitempty"`
	SourceHash        string                                   `json:"source_hash,omitempty"`
	SourceMode        string                                   `json:"source_mode,omitempty"`
}

type GroupSummary struct {
	VectorSpaceKey    string                           `json:"vector_space_key"`
	ModelEndpointID   domainsemantic.ModelEndpointID   `json:"model_endpoint_id"`
	ModelID           domainsemantic.InferenceModelID  `json:"model_id"`
	CredentialGrantID domainsemantic.CredentialGrantID `json:"credential_grant_id,omitempty"`
	SemanticIndexIDs  []domainsemantic.SemanticIndexID `json:"semantic_index_ids"`
	ResultCount       int                              `json:"result_count"`
}
