package acl

import (
	"context"

	domainaccess "martinbeauvais.com/mbgit/knotbase/knotdb/domain/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// GrantSystemRoleInput is the grant/update payload for system roles.
type GrantSystemRoleInput struct {
	UserID identity.UserID
	Roles  []domainaccess.SystemRole
}

// RevokeSystemRoleInput is the revoke payload for system roles.
type RevokeSystemRoleInput struct {
	UserID identity.UserID
}

// GrantInput is the grant/update payload managed by Manager.
type GrantInput struct {
	SpaceID     domainspace.SpaceID
	UserID      identity.UserID
	Permissions []domainaccess.SpacePermission
}

// RevokeInput is the revoke payload managed by Manager.
type RevokeInput struct {
	SpaceID domainspace.SpaceID
	UserID  identity.UserID
}

// Manager manages system roles and per-space user access rules.
type Manager interface {
	Init(ctx context.Context, location string) error
	GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (domainaccess.SystemAccessRule, error)
	RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error
	SystemRolesForUser(ctx context.Context, userID identity.UserID) ([]domainaccess.SystemRole, error)
	SystemRules(ctx context.Context) ([]domainaccess.SystemAccessRule, error)
	CanSystem(ctx context.Context, userID identity.UserID, permission domainaccess.SystemPermission) (bool, error)
	Grant(ctx context.Context, in GrantInput) (domainaccess.SpaceAccessRule, error)
	Revoke(ctx context.Context, in RevokeInput) error
	DeleteForUser(ctx context.Context, userID identity.UserID) error
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
	Can(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID, permission domainaccess.SpacePermission) (bool, error)
	RulesForSpace(ctx context.Context, spaceID domainspace.SpaceID) ([]domainaccess.SpaceAccessRule, error)
}
