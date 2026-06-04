package space

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	OwnerID  identity.UserID
	Name     string
	Status   string
	Settings domainspace.SpaceSettings
}

// Manager manages spaces for users.
type Manager interface {
	Init(ctx context.Context, location string) error
	ExistsByID(ctx context.Context, id domainspace.SpaceID) (bool, error)
	GetByID(ctx context.Context, id domainspace.SpaceID) (domainspace.Space, error)
	List(ctx context.Context) ([]domainspace.Space, error)
	ListByOwner(ctx context.Context, ownerID identity.UserID) ([]domainspace.Space, error)
	FindByOwnerAndName(ctx context.Context, ownerID identity.UserID, name string) (domainspace.Space, error)
	Create(ctx context.Context, in CreateInput) (domainspace.Space, error)
	DeleteByID(ctx context.Context, id domainspace.SpaceID) error
}
