package spaces

import (
	"context"

	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	OwnerID  identity.PrincipalID
	Name     string
	Status   string
	Settings domainspace.SpaceSettings
}

// Manager manages spaces for principals.
type Manager interface {
	Init(ctx context.Context, location string) error
	ExistsByID(ctx context.Context, id domainspace.SpaceID) (bool, error)
	GetByID(ctx context.Context, id domainspace.SpaceID) (domainspace.Space, error)
	List(ctx context.Context) ([]domainspace.Space, error)
	ListByOwner(ctx context.Context, ownerID identity.PrincipalID) ([]domainspace.Space, error)
	FindByOwnerAndName(ctx context.Context, ownerID identity.PrincipalID, name string) (domainspace.Space, error)
	Create(ctx context.Context, in CreateInput) (domainspace.Space, error)
	ApplyCreate(ctx context.Context, space domainspace.Space) (domainspace.Space, error)
	DeleteByID(ctx context.Context, id domainspace.SpaceID) error
	ApplyDelete(ctx context.Context, id domainspace.SpaceID) error
}
