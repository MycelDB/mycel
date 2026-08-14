package principal

import (
	"context"
	"time"

	domainauth "github.com/myceldb/mycel/internal/identity/auth"
)

const (
	ModuleName = "identity"

	PrincipalKindHuman   = "human"
	PrincipalKindService = "service"
	PrincipalKindSystem  = "system"

	PrincipalStateActive   = "active"
	PrincipalStateDisabled = "disabled"
	PrincipalStateDeleted  = "deleted"

	GrantStateActive  = "active"
	GrantStateRevoked = "revoked"

	RoleSystemAdmin         = "system.admin"
	RoleIdentityAdmin       = "identity.admin"
	RoleSpaceAdmin          = "space.admin"
	RoleClusterOperator     = "cluster.operator"
	RoleBackupOperator      = "backup.operator"
	RoleSemanticAdmin       = "semantic.admin"
	RoleInferenceAdmin      = "inference.admin"
	RoleAutomationAdmin     = "automation.admin"
	RoleAuditReader         = "audit.reader"
	RoleSpaceOwner          = "space.owner"
	RoleSpaceEditor         = "space.editor"
	RoleSpaceViewer         = "space.viewer"
	RoleAutomationWorker    = "automation.worker"
	RoleSemanticMaintenance = "semantic.maintenance"
	RoleImportWorker        = "import.worker"
)

type AccessScope struct {
	Type     string `json:"type"`
	SpaceID  string `json:"space_id,omitempty"`
	DomainID string `json:"domain_id,omitempty"`
}

type Principal struct {
	ID           string    `json:"id"`
	Username     string    `json:"username,omitempty"`
	Email        string    `json:"email,omitempty"`
	DisplayName  string    `json:"display_name,omitempty"`
	Kind         string    `json:"kind"`
	State        string    `json:"state"`
	LoginEnabled bool      `json:"login_enabled"`
	PasswordHash string    `json:"password_hash,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	CreatedBy    string    `json:"created_by,omitempty"`
}

type PrincipalSummary struct {
	ID           string    `json:"id"`
	Username     string    `json:"username,omitempty"`
	Email        string    `json:"email,omitempty"`
	DisplayName  string    `json:"display_name,omitempty"`
	Kind         string    `json:"kind"`
	State        string    `json:"state"`
	LoginEnabled bool      `json:"login_enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type RoleBinding struct {
	ID          string      `json:"id"`
	PrincipalID string      `json:"principal_id"`
	Role        string      `json:"role"`
	Scope       AccessScope `json:"scope"`
	State       string      `json:"state"`
	Reason      string      `json:"reason,omitempty"`
	CreatedBy   string      `json:"created_by,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	RevokedBy   string      `json:"revoked_by,omitempty"`
	RevokedAt   time.Time   `json:"revoked_at,omitempty"`
}

type CapabilityGrant struct {
	ID          string      `json:"id"`
	PrincipalID string      `json:"principal_id"`
	Capability  string      `json:"capability"`
	Scope       AccessScope `json:"scope"`
	State       string      `json:"state"`
	Reason      string      `json:"reason,omitempty"`
	CreatedBy   string      `json:"created_by,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	RevokedBy   string      `json:"revoked_by,omitempty"`
	RevokedAt   time.Time   `json:"revoked_at,omitempty"`
}

type CreatePrincipalInput struct {
	Username     string
	Email        string
	DisplayName  string
	Kind         string
	Password     string
	LoginEnabled bool
	Disabled     bool
	Roles        []RoleBinding
	Capabilities []CapabilityGrant
	CreatedBy    string
}

type UpdatePrincipalInput struct {
	PrincipalID  string
	Email        *string
	DisplayName  *string
	Username     *string
	Kind         *string
	LoginEnabled *bool
}

type EffectiveAccess struct {
	Roles        []string
	Capabilities []string
}

type Manager interface {
	ListPrincipals(ctx context.Context) ([]PrincipalSummary, error)
	GetPrincipal(ctx context.Context, principalID string) (PrincipalSummary, error)
	FindPrincipal(ctx context.Context, username string, email string) (PrincipalSummary, error)
	CreatePrincipal(ctx context.Context, input CreatePrincipalInput) (PrincipalSummary, error)
	UpdatePrincipal(ctx context.Context, input UpdatePrincipalInput) (PrincipalSummary, error)
	DisablePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error)
	EnablePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error)
	DeletePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error)
	SetPrincipalPassword(ctx context.Context, principalID string, password string) (PrincipalSummary, error)
	AuthenticatePrincipal(ctx context.Context, username string, password string) (PrincipalSummary, error)
	CreateAuthSession(ctx context.Context, principal PrincipalSummary, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration, absoluteTTL time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error)
	RefreshAuthSession(ctx context.Context, refreshToken domainauth.RefreshToken, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration) (PrincipalSummary, domainauth.RefreshToken, domainauth.RefreshSession, error)
	ListPrincipalSessions(ctx context.Context, principalID string) ([]domainauth.RefreshSession, error)
	RevokePrincipalSession(ctx context.Context, principalID string, sessionID string) error
	RevokePrincipalSessions(ctx context.Context, principalID string) (int, error)
	ListRoleBindings(ctx context.Context, principalID string) ([]RoleBinding, error)
	GrantRole(ctx context.Context, principalID string, role string, scope AccessScope, reason string, grantedBy string) (RoleBinding, PrincipalSummary, error)
	RevokeRole(ctx context.Context, principalID string, bindingID string, revokedBy string) (PrincipalSummary, error)
	ListCapabilityGrants(ctx context.Context, principalID string) ([]CapabilityGrant, error)
	GrantCapability(ctx context.Context, principalID string, capability string, scope AccessScope, reason string, grantedBy string) (CapabilityGrant, PrincipalSummary, error)
	RevokeCapability(ctx context.Context, principalID string, grantID string, revokedBy string) (PrincipalSummary, error)
	Authorize(ctx context.Context, principalID string, capability string, scope AccessScope) error
	HasCapability(ctx context.Context, principalID string, capability string) (bool, error)
	EffectiveAccess(ctx context.Context, principalID string, scope AccessScope) (EffectiveAccess, error)
}

func (p Principal) normalized() Principal {
	if p.Kind == "" {
		p.Kind = PrincipalKindHuman
	}
	if p.State == "" {
		p.State = PrincipalStateActive
	}
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = p.CreatedAt
	}
	return p
}

func (p Principal) toSummary() PrincipalSummary {
	p = p.normalized()
	return PrincipalSummary{ID: p.ID, Username: p.Username, Email: p.Email, DisplayName: p.DisplayName, Kind: p.Kind, State: p.State, LoginEnabled: p.LoginEnabled, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}
}
