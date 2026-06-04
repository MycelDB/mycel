package space

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	OwnerID  identity.UserID
	Name     string
	Status   string
	Settings identity.SpaceSettings
}

// Manager manages spaces for users.
type Manager interface {
	Init(ctx context.Context, location string) error
	ExistsByID(ctx context.Context, id identity.SpaceID) (bool, error)
	GetByID(ctx context.Context, id identity.SpaceID) (identity.Space, error)
	List(ctx context.Context) ([]identity.Space, error)
	ListByOwner(ctx context.Context, ownerID identity.UserID) ([]identity.Space, error)
	FindByOwnerAndName(ctx context.Context, ownerID identity.UserID, name string) (identity.Space, error)
	Create(ctx context.Context, in CreateInput) (identity.Space, error)
	DeleteByID(ctx context.Context, id identity.SpaceID) error
}
