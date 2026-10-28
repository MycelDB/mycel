package search

import (
	"context"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type Planner struct {
	GlobalManager storesemantic.GlobalManager
	SpaceManager  storesemantic.SpaceManager
	Connector     ConnectorService
	VectorBackend vectorstore.Backend
	ObserveStage  func(string)
}

type ConnectorService interface {
	Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error)
}

type Input struct {
	SpaceID             domainspace.SpaceID
	DomainID            graph.DomainID
	SemanticRuleIDs     []domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexIDs    []domainsemantic.SemanticIndexID // transitional adapter filter; treated as rule IDs when rule-native search is active
	Purpose             domainsemantic.SemanticIndexPurpose
	Text                string
	Limit               int
	MinScore            float64
	ActorPrincipalID    identity.PrincipalID
}

type Result struct {
	Results                  []SearchResult            `json:"results"`
	CandidateScanTruncated   bool                      `json:"candidate_scan_truncated,omitempty"`
	Warnings                 []string                  `json:"warnings,omitempty"`
	WarningDetails           []Warning                 `json:"warning_details,omitempty"`
	Groups                   []GroupSummary            `json:"groups,omitempty"`
	ProjectionSearchBindings []ProjectionSearchBinding `json:"-"`
}

type ProjectionSearchBinding struct {
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
}

type Warning struct {
	Code                string                        `json:"code"`
	Message             string                        `json:"message"`
	SemanticRuleID      domainsemantic.SemanticRuleID `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string                        `json:"embedding_binding_key,omitempty"`
	Retryable           bool                          `json:"retryable,omitempty"`
}

type SearchResult struct {
	SemanticRuleID            domainsemantic.SemanticRuleID              `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey       string                                     `json:"embedding_binding_key,omitempty"`
	SemanticIndexID           domainsemantic.SemanticIndexID             `json:"semantic_index_id,omitempty"` // transitional response field
	NodeID                    graph.NodeID                               `json:"node_id"`
	TargetNodeID              graph.NodeID                               `json:"target_node_id,omitempty"`
	RecordID                  domainsemantic.AdvancedEmbeddingRecordID   `json:"record_id"`
	MatchedRecordIDs          []domainsemantic.AdvancedEmbeddingRecordID `json:"matched_record_ids,omitempty"`
	MatchedBindings           []MatchedBinding                           `json:"matched_bindings,omitempty"`
	Score                     float64                                    `json:"score"`
	ModelEndpointID           domainsemantic.ModelEndpointID             `json:"model_endpoint_id"`
	ModelID                   domainsemantic.InferenceModelID            `json:"model_id"`
	VectorStoreID             domainsemantic.VectorStoreID               `json:"vector_store_id"`
	CredentialGrantID         domainsemantic.CredentialGrantID           `json:"credential_grant_id,omitempty"`
	VectorSpaceKey            string                                     `json:"vector_space_key,omitempty"`
	SourceHash                string                                     `json:"source_hash,omitempty"`
	SourceGraphRevision       uint64                                     `json:"source_graph_revision,omitempty"`
	SemanticGeneration        uint64                                     `json:"semantic_generation,omitempty"`
	SemanticConfigurationHash string                                     `json:"semantic_configuration_hash,omitempty"`
	RebuildEpoch              string                                     `json:"rebuild_epoch,omitempty"`
	IntelligenceProfileID     domainsemantic.IntelligenceProfileID       `json:"intelligence_profile_id,omitempty"`
	ModelEndpointCapabilityID domainsemantic.ModelEndpointCapabilityID   `json:"model_endpoint_capability_id,omitempty"`
	Dimensions                int                                        `json:"dimensions,omitempty"`
	SourceMode                string                                     `json:"source_mode,omitempty"`
	CreatedAt                 time.Time                                  `json:"created_at,omitempty"`
}

type MatchedBinding struct {
	SemanticRuleID      domainsemantic.SemanticRuleID            `json:"semantic_rule_id"`
	EmbeddingBindingKey string                                   `json:"embedding_binding_key"`
	RecordID            domainsemantic.AdvancedEmbeddingRecordID `json:"record_id"`
	Score               float64                                  `json:"score"`
}

type GroupSummary struct {
	SemanticRuleID      domainsemantic.SemanticRuleID    `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string                           `json:"embedding_binding_key,omitempty"`
	VectorSpaceKey      string                           `json:"vector_space_key"`
	ModelEndpointID     domainsemantic.ModelEndpointID   `json:"model_endpoint_id"`
	ModelID             domainsemantic.InferenceModelID  `json:"model_id"`
	CredentialGrantID   domainsemantic.CredentialGrantID `json:"credential_grant_id,omitempty"`
	SemanticIndexIDs    []domainsemantic.SemanticIndexID `json:"semantic_index_ids,omitempty"` // transitional response field
	SemanticRuleIDs     []domainsemantic.SemanticRuleID  `json:"semantic_rule_ids,omitempty"`
	ResultCount         int                              `json:"result_count"`
}
