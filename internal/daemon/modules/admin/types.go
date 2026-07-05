package admin

import (
	"context"
	"time"
)

const (
	AdminStateActive   = "active"
	AdminStateDisabled = "disabled"
	AdminStateDeleted  = "deleted"

	OperatorRoleSystemAdmin   = "system_admin"
	OperatorRoleUserAdmin     = "user_admin"
	OperatorRoleSpaceAdmin    = "space_admin"
	OperatorRoleSemanticAdmin = "semantic_admin"
	OperatorRoleStorageAdmin  = "storage_admin"
	OperatorRoleMeshAdmin     = "mesh_admin"
	OperatorRoleAuditReader   = "audit_reader"
)

type AccessScope struct {
	Type     string `json:"type"`
	SpaceID  string `json:"space_id,omitempty"`
	DomainID string `json:"domain_id,omitempty"`
}

type RoleGrant struct {
	ID                  string      `json:"id"`
	OperatorID          string      `json:"operator_id"`
	Role                string      `json:"role"`
	Scope               AccessScope `json:"scope"`
	Reason              string      `json:"reason,omitempty"`
	GrantedByOperatorID string      `json:"granted_by_operator_id,omitempty"`
	CreatedAt           time.Time   `json:"created_at"`
}

type CapabilityGrant struct {
	ID                  string      `json:"id"`
	OperatorID          string      `json:"operator_id"`
	Capability          string      `json:"capability"`
	Scope               AccessScope `json:"scope"`
	Reason              string      `json:"reason,omitempty"`
	GrantedByOperatorID string      `json:"granted_by_operator_id,omitempty"`
	CreatedAt           time.Time   `json:"created_at"`
}

type Admin struct {
	ID               string            `json:"id"`
	Username         string            `json:"username"`
	Email            string            `json:"email,omitempty"`
	State            string            `json:"state"`
	PasswordHash     string            `json:"password_hash"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	RoleGrants       []RoleGrant       `json:"role_grants,omitempty"`
	CapabilityGrants []CapabilityGrant `json:"capability_grants,omitempty"`
}

type AdminSummary struct {
	ID               string            `json:"id"`
	Username         string            `json:"username"`
	Email            string            `json:"email,omitempty"`
	State            string            `json:"state"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	RoleGrants       []RoleGrant       `json:"role_grants,omitempty"`
	CapabilityGrants []CapabilityGrant `json:"capability_grants,omitempty"`
}

func (a Admin) normalized() Admin {
	if a.State == "" {
		a.State = AdminStateActive
	}
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = a.CreatedAt
	}
	if a.RoleGrants == nil {
		a.RoleGrants = []RoleGrant{}
	}
	if a.CapabilityGrants == nil {
		a.CapabilityGrants = []CapabilityGrant{}
	}
	return a
}

func (a Admin) toSummary() AdminSummary {
	a = a.normalized()
	return AdminSummary{ID: a.ID, Username: a.Username, Email: a.Email, State: a.State, CreatedAt: a.CreatedAt, UpdatedAt: a.UpdatedAt, RoleGrants: append([]RoleGrant(nil), a.RoleGrants...), CapabilityGrants: append([]CapabilityGrant(nil), a.CapabilityGrants...)}
}

type AdminLister interface {
	ListAdmins(ctx context.Context) ([]AdminSummary, error)
}

type OperatorAuthenticator interface {
	AuthenticateOperator(ctx context.Context, username string, password string) (AdminSummary, error)
}

type OperatorPasswordManager interface {
	SetOperatorPassword(ctx context.Context, operatorID string, password string) (AdminSummary, error)
}

type OperatorManager interface {
	AdminLister
	OperatorAuthenticator
	OperatorPasswordManager
	GetOperator(ctx context.Context, operatorID string) (AdminSummary, error)
	FindOperator(ctx context.Context, username string, email string) (AdminSummary, error)
	CreateOperator(ctx context.Context, input CreateOperatorInput) (AdminSummary, error)
	UpdateOperator(ctx context.Context, input UpdateOperatorInput) (AdminSummary, error)
	DisableOperator(ctx context.Context, operatorID string) (AdminSummary, error)
	EnableOperator(ctx context.Context, operatorID string) (AdminSummary, error)
	DeleteOperator(ctx context.Context, operatorID string) (AdminSummary, error)
	GrantRole(ctx context.Context, operatorID string, role string, scope AccessScope, reason string, grantedBy string) (RoleGrant, AdminSummary, error)
	RevokeRole(ctx context.Context, operatorID string, grantID string) (AdminSummary, error)
	GrantCapability(ctx context.Context, operatorID string, capability string, scope AccessScope, reason string, grantedBy string) (CapabilityGrant, AdminSummary, error)
	RevokeCapability(ctx context.Context, operatorID string, grantID string) (AdminSummary, error)
	IsSystemAdmin(ctx context.Context, operatorID string) (bool, error)
	HasCapability(ctx context.Context, operatorID string, capability string) (bool, error)
}

type CreateOperatorInput struct {
	Username         string
	Email            string
	Password         string
	Disabled         bool
	Roles            []RoleGrant
	CapabilityGrants []CapabilityGrant
	CreatedBy        string
}

type UpdateOperatorInput struct {
	OperatorID string
	Email      *string
}
