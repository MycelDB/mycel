package vectorstore

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type SearchInput struct {
	SpaceID             domainspace.SpaceID
	DomainID            graph.DomainID
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexID     domainsemantic.SemanticIndexID // transitional legacy index search
	VectorStoreID       domainsemantic.VectorStoreID
	VectorSpaceKey      string
	Query               []float64
	Limit               int
	MinScore            float64
}

type SearchResult struct {
	Record              domainsemantic.AdvancedEmbeddingRecord
	Score               float64
	NodeID              graph.NodeID
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexID     domainsemantic.SemanticIndexID
}

type DeleteInput struct {
	SpaceID             domainspace.SpaceID
	DomainID            graph.DomainID
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexID     domainsemantic.SemanticIndexID
	NodeID              graph.NodeID
	VectorStoreID       domainsemantic.VectorStoreID
	TargetRecordID      domainsemantic.AdvancedEmbeddingRecordID
	SourceMode          string
	Reason              string
	ModelEndpointID     domainsemantic.ModelEndpointID
	ModelID             domainsemantic.InferenceModelID
	CredentialID        domainsemantic.InferenceCredentialID
	CredentialGrantID   domainsemantic.CredentialGrantID
	PolicyDecisionID    domainsemantic.PolicyDecisionID
	ModelEndpointCapID  domainsemantic.ModelEndpointCapabilityID
	CreatedAt           time.Time
}

type SearchIndexKey struct {
	SpaceID             domainspace.SpaceID           `json:"space_id"`
	DomainID            graph.DomainID                `json:"domain_id,omitempty"`
	SemanticRuleID      domainsemantic.SemanticRuleID `json:"semantic_rule_id"`
	EmbeddingBindingKey string                        `json:"embedding_binding_key"`
	VectorStoreID       domainsemantic.VectorStoreID  `json:"vector_store_id,omitempty"`
	VectorSpaceKey      string                        `json:"vector_space_key,omitempty"`
}

type RebuildLimit struct {
	MaxRecords int
	MaxBytes   int64
}

type VerifyDeletedInput struct {
	SpaceID         domainspace.SpaceID
	DomainID        graph.DomainID
	SemanticIndexID domainsemantic.SemanticIndexID
	NodeID          graph.NodeID
	SourceMode      string
	TargetRecordID  domainsemantic.AdvancedEmbeddingRecordID
}

type Backend interface {
	Upsert(ctx context.Context, rec domainsemantic.AdvancedEmbeddingRecord) (domainsemantic.AdvancedEmbeddingRecord, error)
	Search(ctx context.Context, in SearchInput) ([]SearchResult, error)
	Delete(ctx context.Context, in DeleteInput) (domainsemantic.AdvancedEmbeddingRecord, error)
	VerifyDeleted(ctx context.Context, in VerifyDeletedInput) (bool, error)
}

type SearchIndexRebuilder interface {
	RebuildSearchIndex(ctx context.Context, key SearchIndexKey, limit RebuildLimit) (domainsemantic.SemanticSearchIndexState, error)
}

type RecordLister interface {
	ListRecords(ctx context.Context, spaceID domainspace.SpaceID, semanticIndexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error)
}

func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
