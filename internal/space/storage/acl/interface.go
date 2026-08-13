package acl

import (
	"context"

	"github.com/myceldb/mycel/internal/identity/model"
	domainaccess "github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// GrantSystemRoleInput is the grant/update payload for system roles.
type GrantSystemRoleInput struct {
	PrincipalID identity.PrincipalID
	Roles       []domainaccess.SystemRole
}

// RevokeSystemRoleInput is the revoke payload for system roles.
type RevokeSystemRoleInput struct {
	PrincipalID identity.PrincipalID
}

// GrantInput is the grant/update payload managed by Manager.
type GrantInput struct {
	SpaceID     domainspace.SpaceID
	PrincipalID identity.PrincipalID
	Permissions []domainaccess.SpacePermission
}

// RevokeInput is the revoke payload managed by Manager.
type RevokeInput struct {
	SpaceID     domainspace.SpaceID
	PrincipalID identity.PrincipalID
}

// Manager manages system roles and per-space principal access rules.
type Manager interface {
	Init(ctx context.Context, location string) error
	GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (domainaccess.SystemAccessRule, error)
	RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error
	SystemRolesForPrincipal(ctx context.Context, principalID identity.PrincipalID) ([]domainaccess.SystemRole, error)
	SystemRules(ctx context.Context) ([]domainaccess.SystemAccessRule, error)
	CanSystem(ctx context.Context, principalID identity.PrincipalID, permission domainaccess.SystemPermission) (bool, error)
	Grant(ctx context.Context, in GrantInput) (domainaccess.SpaceAccessRule, error)
	ApplyGrant(ctx context.Context, rule domainaccess.SpaceAccessRule) (domainaccess.SpaceAccessRule, error)
	Revoke(ctx context.Context, in RevokeInput) error
	DeleteForPrincipal(ctx context.Context, principalID identity.PrincipalID) error
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
	Can(ctx context.Context, principalID identity.PrincipalID, spaceID domainspace.SpaceID, permission domainaccess.SpacePermission) (bool, error)
	RulesForSpace(ctx context.Context, spaceID domainspace.SpaceID) ([]domainaccess.SpaceAccessRule, error)
}
