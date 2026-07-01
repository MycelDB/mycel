package internal

import (
	"time"

	domainauth "github.com/myceldb/mycel/domain/auth"
	"github.com/myceldb/mycel/domain/identity"
)

// AuthInput is the authentication request payload.
type AuthInput struct {
	UserRef  identity.UserRef
	Password string
}

// AccessToken is the opaque bearer token returned on successful auth.
type AccessToken string

// AuthResult is the authentication result returned to external callers.
type AuthResult struct {
	AccessToken AccessToken `json:"access_token"`
}

// RefreshToken is a plaintext refresh token returned once to callers.
type RefreshToken = domainauth.RefreshToken

// RefreshSessionID uniquely identifies a durable refresh session.
type RefreshSessionID = domainauth.RefreshSessionID

// RefreshSessionStatus defines lifecycle state for a durable refresh session.
type RefreshSessionStatus = domainauth.RefreshSessionStatus

const (
	RefreshSessionStatusActive  = domainauth.RefreshSessionStatusActive
	RefreshSessionStatusRevoked = domainauth.RefreshSessionStatusRevoked
	RefreshSessionStatusExpired = domainauth.RefreshSessionStatusExpired
)

// RefreshSessionMetadata contains coarse caller-provided client metadata.
type RefreshSessionMetadata = domainauth.RefreshSessionMetadata

// RefreshSessionInfo is the public non-sensitive view of a durable refresh session.
type RefreshSessionInfo struct {
	ID                RefreshSessionID       `json:"id"`
	UserID            identity.UserID        `json:"user_id"`
	UserRef           identity.UserRef       `json:"user_ref"`
	Status            RefreshSessionStatus   `json:"status"`
	TokenFamilyID     string                 `json:"token_family_id"`
	RotationCounter   int                    `json:"rotation_counter"`
	CreatedAt         time.Time              `json:"created_at"`
	LastUsedAt        time.Time              `json:"last_used_at"`
	IdleExpiresAt     time.Time              `json:"idle_expires_at"`
	AbsoluteExpiresAt time.Time              `json:"absolute_expires_at"`
	RevokedAt         time.Time              `json:"revoked_at,omitempty"`
	RevokedReason     string                 `json:"revoked_reason,omitempty"`
	Metadata          RefreshSessionMetadata `json:"metadata,omitempty"`
}

// LoginSessionInput authenticates a user and creates a durable refresh session.
type LoginSessionInput struct {
	UserRef  identity.UserRef
	Password string
	Metadata RefreshSessionMetadata
}

// LoginSessionResult contains a short-lived access token plus a one-time plaintext refresh token.
type LoginSessionResult struct {
	AccessToken          AccessToken        `json:"access_token"`
	AccessTokenExpiresAt time.Time          `json:"access_token_expires_at"`
	RefreshToken         RefreshToken       `json:"refresh_token"`
	RefreshSession       RefreshSessionInfo `json:"refresh_session"`
}

// CurrentUserInput identifies the bearer token whose authenticated user should be returned.
type CurrentUserInput struct {
	AccessToken AccessToken
}
