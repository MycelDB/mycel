package vectorstore

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
)

type SearchInput struct {
	SpaceID         domainspace.SpaceID
	DomainID        graph.DomainID
	SemanticIndexID domainsemantic.SemanticIndexID
	Query           []float64
	Limit           int
	MinScore        float64
}

type SearchResult struct {
	Record          domainsemantic.AdvancedEmbeddingRecord
	Score           float64
	NodeID          graph.NodeID
	SemanticIndexID domainsemantic.SemanticIndexID
}

type DeleteInput struct {
	SpaceID            domainspace.SpaceID
	DomainID           graph.DomainID
	SemanticIndexID    domainsemantic.SemanticIndexID
	NodeID             graph.NodeID
	VectorStoreID      domainsemantic.VectorStoreID
	TargetRecordID     domainsemantic.AdvancedEmbeddingRecordID
	SourceMode         string
	Reason             string
	ModelEndpointID    domainsemantic.ModelEndpointID
	ModelID            domainsemantic.InferenceModelID
	CredentialID       domainsemantic.InferenceCredentialID
	CredentialGrantID  domainsemantic.CredentialGrantID
	PolicyDecisionID   domainsemantic.PolicyDecisionID
	ModelEndpointCapID domainsemantic.ModelEndpointCapabilityID
	CreatedAt          time.Time
}

type VerifyDeletedInput struct {
	SpaceID         domainspace.SpaceID
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

func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}
