package model

import "github.com/google/uuid"

// SpacePermission is an action granted to a user for a space.
type SpacePermission string

const (
	SpacePermissionRead  SpacePermission = "read"
	SpacePermissionWrite SpacePermission = "write"
	SpacePermissionAdmin SpacePermission = "admin"
)

// SpaceAccessRuleID uniquely identifies a space access rule.
type SpaceAccessRuleID = uuid.UUID

// SpaceAccessRule grants permissions to a user for a space.
type SpaceAccessRule struct {
	ID          SpaceAccessRuleID `json:"id"`
	SpaceID     SpaceID           `json:"space_id"`
	UserID      UserID            `json:"user_id"`
	Permissions []SpacePermission `json:"permissions"`
}

// PermissionImplies reports whether a granted permission satisfies a requested permission.
func PermissionImplies(granted SpacePermission, requested SpacePermission) bool {
	if granted == requested {
		return true
	}
	switch granted {
	case SpacePermissionAdmin:
		return requested == SpacePermissionWrite || requested == SpacePermissionRead
	case SpacePermissionWrite:
		return requested == SpacePermissionRead
	default:
		return false
	}
}
