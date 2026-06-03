package space

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	OwnerID  model.UserID
	Name     string
	Status   string
	Settings model.SpaceSettings
}

// Manager manages spaces for users.
type Manager interface {
	Init(ctx context.Context, location string) error
	ExistsByID(ctx context.Context, id model.SpaceID) (bool, error)
	GetByID(ctx context.Context, id model.SpaceID) (model.Space, error)
	ListByOwner(ctx context.Context, ownerID model.UserID) ([]model.Space, error)
	FindByOwnerAndName(ctx context.Context, ownerID model.UserID, name string) (model.Space, error)
	Create(ctx context.Context, in CreateInput) (model.Space, error)
	DeleteByID(ctx context.Context, id model.SpaceID) error
}
