package spacemgmt

import (
	"context"

	"knot_db/model"
)

// CreateSpaceInput is the create payload managed by SpaceManager.
type CreateSpaceInput struct {
	OwnerID  model.UserID
	Name     string
	Status   string
	Settings model.SpaceSettings
}

// SpaceManager manages spaces for users.
type SpaceManager interface {
	Init(ctx context.Context, location string) error
	ExistsByID(ctx context.Context, id model.SpaceID) (bool, error)
	GetByID(ctx context.Context, id model.SpaceID) (model.Space, error)
	FindByOwnerAndName(ctx context.Context, ownerID model.UserID, name string) (model.Space, error)
	Create(ctx context.Context, in CreateSpaceInput) (model.Space, error)
}
