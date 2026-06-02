package access

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// GrantInput is the grant/update payload managed by Manager.
type GrantInput struct {
	SpaceID     model.SpaceID
	UserID      model.UserID
	Permissions []model.SpacePermission
}

// RevokeInput is the revoke payload managed by Manager.
type RevokeInput struct {
	SpaceID model.SpaceID
	UserID  model.UserID
}

// Manager manages per-space user access rules.
type Manager interface {
	Init(ctx context.Context, location string) error
	Grant(ctx context.Context, in GrantInput) (model.SpaceAccessRule, error)
	Revoke(ctx context.Context, in RevokeInput) error
	Can(ctx context.Context, userID model.UserID, spaceID model.SpaceID, permission model.SpacePermission) (bool, error)
	RulesForSpace(ctx context.Context, spaceID model.SpaceID) ([]model.SpaceAccessRule, error)
}
