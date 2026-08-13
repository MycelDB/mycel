package auth

import (
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/identity/model"
)

// RefreshSessionID uniquely identifies a durable refresh session.
type RefreshSessionID = uuid.UUID

// AuditEventID uniquely identifies an auth/session audit event.
type AuditEventID = uuid.UUID

// TokenFamilyID groups refresh sessions/tokens that should be revoked together
// after suspected token reuse.
type TokenFamilyID string

// RefreshSessionStatus defines lifecycle state for a durable refresh session.
type RefreshSessionStatus string

const (
	RefreshSessionStatusActive  RefreshSessionStatus = "active"
	RefreshSessionStatusRevoked RefreshSessionStatus = "revoked"
	RefreshSessionStatusExpired RefreshSessionStatus = "expired"
)

// RefreshSessionMetadata contains coarse caller-provided client metadata.
// Values should be hashed or otherwise privacy-preserving before storage when
// they originate from request metadata such as IP address or User-Agent.
type RefreshSessionMetadata struct {
	UserAgentHash string `json:"user_agent_hash,omitempty"`
	IPPrefixHash  string `json:"ip_prefix_hash,omitempty"`
	ClientName    string `json:"client_name,omitempty"`
}

// RefreshSession is the durable store record for a refresh session.
//
// RefreshTokenHash stores only a cryptographic hash of the refresh token. The
// plaintext refresh token must never be persisted.
type RefreshSession struct {
	ID RefreshSessionID `json:"id"`

	// PrincipalID is the authoritative owner for the unified identity model.
	PrincipalID  identity.PrincipalID  `json:"principal_id,omitempty"`
	PrincipalRef identity.PrincipalRef `json:"principal_ref,omitempty"`

	Status                     RefreshSessionStatus   `json:"status"`
	TokenFamilyID              TokenFamilyID          `json:"token_family_id"`
	RefreshTokenHash           string                 `json:"refresh_token_hash,omitempty"`
	ConsumedRefreshTokenHashes []string               `json:"consumed_refresh_token_hashes,omitempty"`
	RotationCounter            int                    `json:"rotation_counter"`
	CreatedAt                  time.Time              `json:"created_at"`
	LastUsedAt                 time.Time              `json:"last_used_at"`
	IdleExpiresAt              time.Time              `json:"idle_expires_at"`
	AbsoluteExpiresAt          time.Time              `json:"absolute_expires_at"`
	RevokedAt                  time.Time              `json:"revoked_at,omitempty"`
	RevokedReason              string                 `json:"revoked_reason,omitempty"`
	RedactedAt                 time.Time              `json:"redacted_at,omitempty"`
	Metadata                   RefreshSessionMetadata `json:"metadata,omitempty"`
}

// AuthAuditEvent is a non-sensitive auth/session lifecycle audit record.
type AuthAuditEvent struct {
	ID   AuditEventID `json:"id"`
	Type string       `json:"type"`

	PrincipalID  *identity.PrincipalID `json:"principal_id,omitempty"`
	PrincipalRef identity.PrincipalRef `json:"principal_ref,omitempty"`
	SessionID    *RefreshSessionID     `json:"session_id,omitempty"`
	Message      string                `json:"message,omitempty"`
	Metadata     map[string]string     `json:"metadata,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
}
