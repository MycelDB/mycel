package access

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// GrantSystemRoleInput is the grant/update payload for system roles.
type GrantSystemRoleInput struct {
	UserID model.UserID
	Roles  []model.SystemRole
}

// RevokeSystemRoleInput is the revoke payload for system roles.
type RevokeSystemRoleInput struct {
	UserID model.UserID
}

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

// Manager manages system roles and per-space user access rules.
type Manager interface {
	Init(ctx context.Context, location string) error
	GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (model.SystemAccessRule, error)
	RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error
	SystemRolesForUser(ctx context.Context, userID model.UserID) ([]model.SystemRole, error)
	SystemRules(ctx context.Context) ([]model.SystemAccessRule, error)
	CanSystem(ctx context.Context, userID model.UserID, permission model.SystemPermission) (bool, error)
	Grant(ctx context.Context, in GrantInput) (model.SpaceAccessRule, error)
	Revoke(ctx context.Context, in RevokeInput) error
	Can(ctx context.Context, userID model.UserID, spaceID model.SpaceID, permission model.SpacePermission) (bool, error)
	RulesForSpace(ctx context.Context, spaceID model.SpaceID) ([]model.SpaceAccessRule, error)
}
