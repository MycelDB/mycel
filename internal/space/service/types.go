package service

import (
	"context"

	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const ModuleName = "space"

type Manager interface {
	ListVisibleSpaces(ctx context.Context, userID string, includeArchived bool) ([]domainspace.Space, error)
	GetVisibleSpace(ctx context.Context, userID string, spaceID string) (domainspace.Space, error)
	ListSpaces(ctx context.Context, includeArchived bool) ([]domainspace.Space, error)
	GetSpace(ctx context.Context, spaceID string) (domainspace.Space, error)
	CreateSpace(ctx context.Context, input CreateSpaceInput) (domainspace.Space, graph.Domain, error)
	DeleteSpace(ctx context.Context, spaceID string) error
	GrantSpaceUser(ctx context.Context, spaceID string, userID string, role string) (SpaceGrant, error)
	EffectiveAccess(ctx context.Context, userID string, sp domainspace.Space) (EffectiveAccess, error)
	DomainEffectiveAccess(ctx context.Context, userID string, spaceID string) (EffectiveAccess, error)
	ListDomains(ctx context.Context, spaceID string, includeSystem bool) ([]graph.Domain, error)
	GetDomainByRef(ctx context.Context, spaceID string, domainRef string) (graph.Domain, error)
	ListVisibleDomains(ctx context.Context, userID string, spaceID string, includeSystem bool) ([]graph.Domain, error)
	GetVisibleDomain(ctx context.Context, userID string, spaceID string, domainID string, key string) (graph.Domain, error)
	CreateDomain(ctx context.Context, userID string, input CreateDomainInput) (graph.Domain, error)
	UpdateDomain(ctx context.Context, userID string, input UpdateDomainInput) (graph.Domain, error)
	DeleteDomain(ctx context.Context, userID string, spaceID string, domainID string) error
}

type CreateSpaceInput struct {
	Name              string
	OwnerUserID       identity.UserID
	DefaultDomainKey  string
	DefaultDomainName string
	CommandID         string
}

type CreateSpaceResult struct {
	Space     domainspace.Space
	Domain    graph.Domain
	CommitLSN wal.LSN
}

type CreateDomainInput struct {
	SpaceID       string
	Key           string
	Name          string
	Description   string
	DiscoveryMode graph.DomainDiscoveryMode
	SearchMode    graph.DomainSearchMode
	SemanticMode  graph.DomainSemanticMode
	ReadOnly      bool
}

type UpdateDomainInput struct {
	SpaceID       string
	DomainID      string
	Name          *string
	Description   *string
	DiscoveryMode *graph.DomainDiscoveryMode
	SearchMode    *graph.DomainSearchMode
	SemanticMode  *graph.DomainSemanticMode
	ReadOnly      *bool
}

type SpaceGrant struct {
	ID           string
	SpaceID      string
	UserID       string
	Role         string
	Capabilities []string
}

type EffectiveAccess struct {
	Roles        []string
	Capabilities []string
}
