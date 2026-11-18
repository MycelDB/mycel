package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateAuthSessionClassifiesInactiveSessions(t *testing.T) {
	ctx := context.Background()
	sessionID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	principal := daemonauth.Principal{PrincipalID: "principal-1", AuthSessionID: sessionID.String(), Username: "alice"}
	now := time.Now().UTC()
	active := domainauth.RefreshSession{ID: sessionID, PrincipalID: identity.PrincipalID("principal-1"), Status: domainauth.RefreshSessionStatusActive, CreatedAt: now.Add(-time.Minute), LastUsedAt: now.Add(-time.Minute), IdleExpiresAt: now.Add(time.Hour), AbsoluteExpiresAt: now.Add(24 * time.Hour)}

	if err := validateAuthSession(ctx, authSessionPrincipalManager{session: active}, principal); err != nil {
		t.Fatalf("active auth session rejected: %v", err)
	}

	tests := []struct {
		name    string
		session domainauth.RefreshSession
		reason  string
	}{
		{name: "revoked", session: withSessionStatus(active, domainauth.RefreshSessionStatusRevoked), reason: daemonauth.ReasonAuthSessionRevoked},
		{name: "expired_status", session: withSessionStatus(active, domainauth.RefreshSessionStatusExpired), reason: daemonauth.ReasonAuthSessionExpired},
		{name: "idle_expired", session: withIdleExpiry(active, now.Add(-time.Second)), reason: daemonauth.ReasonAuthSessionExpired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAuthSession(ctx, authSessionPrincipalManager{session: test.session}, principal)
			requireAuthReason(t, err, test.reason)
		})
	}

	err := validateAuthSession(ctx, authSessionPrincipalManager{}, principal)
	requireAuthReason(t, err, daemonauth.ReasonAuthSessionInvalid)
}

type authSessionPrincipalManager struct {
	fakePrincipalManager
	session domainauth.RefreshSession
}

func (m authSessionPrincipalManager) ListPrincipalSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	if m.session.ID == uuid.Nil {
		return nil, nil
	}
	return []domainauth.RefreshSession{m.session}, nil
}

func withSessionStatus(session domainauth.RefreshSession, status domainauth.RefreshSessionStatus) domainauth.RefreshSession {
	session.Status = status
	return session
}

func withIdleExpiry(session domainauth.RefreshSession, expiresAt time.Time) domainauth.RefreshSession {
	session.IdleExpiresAt = expiresAt
	return session
}

func requireAuthReason(t *testing.T, err error, want string) {
	t.Helper()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
	}
	if reason, ok := daemonauth.ErrorInfoReason(err); !ok || reason != want {
		t.Fatalf("auth reason = %q, %v; want %q, true", reason, ok, want)
	}
}
