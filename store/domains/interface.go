package domains

import (
	"context"

	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	SpaceID     domainspace.SpaceID
	Key         string
	Name        string
	Description string
	Default     bool
}

// Manager manages graph domains inside spaces.
type Manager interface {
	Init(ctx context.Context, location string) error
	Create(ctx context.Context, in CreateInput) (graph.Domain, error)
	EnsureDefault(ctx context.Context, spaceID domainspace.SpaceID) (graph.Domain, error)
	GetByID(ctx context.Context, id graph.DomainID) (graph.Domain, error)
	FindBySpaceAndKey(ctx context.Context, spaceID domainspace.SpaceID, key string) (graph.Domain, error)
	GetDefault(ctx context.Context, spaceID domainspace.SpaceID) (graph.Domain, error)
	ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Domain, error)
	SetEmbeddingPolicy(ctx context.Context, policy domainembedding.DomainEmbeddingPolicy) (domainembedding.DomainEmbeddingPolicy, error)
	GetEmbeddingPolicy(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) (domainembedding.DomainEmbeddingPolicy, error)
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
}
