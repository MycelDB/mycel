package session

import (
	"context"
	"time"

	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
)

// Manager manages durable auth refresh-session records.
type Manager interface {
	Init(ctx context.Context, location string) error
	Create(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error)
	GetByID(ctx context.Context, id domainauth.RefreshSessionID) (domainauth.RefreshSession, error)
	FindByTokenHash(ctx context.Context, hash string) (domainauth.RefreshSession, error)
	FindByConsumedTokenHash(ctx context.Context, hash string) (domainauth.RefreshSession, error)
	ListByUser(ctx context.Context, userID identity.UserID) ([]domainauth.RefreshSession, error)
	Update(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error)
	RevokeByID(ctx context.Context, id domainauth.RefreshSessionID, revokedAt time.Time, reason string) (domainauth.RefreshSession, error)
	RevokeFamily(ctx context.Context, familyID domainauth.TokenFamilyID, revokedAt time.Time, reason string) (int, error)
	DeleteExpiredRedacted(ctx context.Context, cutoff time.Time) (int, error)
	RecordAuditEvent(ctx context.Context, event domainauth.AuthAuditEvent) (domainauth.AuthAuditEvent, error)
	ListAuditEvents(ctx context.Context, userID *identity.UserID) ([]domainauth.AuthAuditEvent, error)
}
