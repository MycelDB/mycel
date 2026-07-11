package domains

import (
	"context"

	"github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	SpaceID       domainspace.SpaceID
	Key           string
	Name          string
	Description   string
	DiscoveryMode graph.DomainDiscoveryMode
	Default       bool
}

type UpdateInput struct {
	DomainID      graph.DomainID
	Name          *string
	Description   *string
	DiscoveryMode *graph.DomainDiscoveryMode
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
	Update(ctx context.Context, in UpdateInput) (graph.Domain, error)
	DeleteByID(ctx context.Context, id graph.DomainID) error
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
}
