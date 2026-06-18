package access

import (
	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// SystemRole is a global role granted to a user for system-wide administration.
type SystemRole string

const (
	SystemRoleSuperuser SystemRole = "superuser"
	SystemRoleUserAdmin SystemRole = "user_admin"
	SystemRoleOperator  SystemRole = "operator"
)

// SystemPermission is a system-level capability implied by one or more system roles.
type SystemPermission string

const (
	SystemPermissionManageUsers   SystemPermission = "users:manage"
	SystemPermissionCreateSpaces  SystemPermission = "spaces:create"
	SystemPermissionManageAccess  SystemPermission = "access:manage"
	SystemPermissionOperateSystem SystemPermission = "system:operate"
)

// SystemAccessRuleID uniquely identifies a system access rule.
type SystemAccessRuleID = uuid.UUID

// SystemAccessRule grants system roles to a user.
type SystemAccessRule struct {
	ID     SystemAccessRuleID `json:"id"`
	UserID identity.UserID    `json:"user_id"`
	Roles  []SystemRole       `json:"roles"`
}

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
	ID          SpaceAccessRuleID   `json:"id"`
	SpaceID     domainspace.SpaceID `json:"space_id"`
	UserID      identity.UserID     `json:"user_id"`
	Permissions []SpacePermission   `json:"permissions"`
}

// RoleAllows reports whether a system role satisfies a system permission.
func RoleAllows(role SystemRole, permission SystemPermission) bool {
	switch role {
	case SystemRoleSuperuser:
		return true
	case SystemRoleUserAdmin:
		return permission == SystemPermissionManageUsers
	case SystemRoleOperator:
		return permission == SystemPermissionOperateSystem
	default:
		return false
	}
}

// PermissionImplies reports whether a granted space permission satisfies a requested permission.
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
