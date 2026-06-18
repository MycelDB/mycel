package internal

import (
	"github.com/myceldb/mycel/domain/access"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// GrantSystemRoleInput defines a request to grant or update a user's system roles.
type GrantSystemRoleInput struct {
	AccessToken AccessToken
	UserID      identity.UserID
	Roles       []access.SystemRole
}

// RevokeSystemRoleInput defines a request to revoke a user's system roles.
type RevokeSystemRoleInput struct {
	AccessToken AccessToken
	UserID      identity.UserID
}

// ListSystemAccessInput defines a request to list system access rules.
type ListSystemAccessInput struct {
	AccessToken AccessToken
}

// GrantSpaceAccessInput defines a request to grant or update user access for a space.
type GrantSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	UserID      identity.UserID
	Permissions []access.SpacePermission
}

// RevokeSpaceAccessInput defines a request to revoke user access for a space.
type RevokeSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	UserID      identity.UserID
}

// ListSpaceAccessInput defines a request to list access rules for a space.
type ListSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}
