package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAuthenticator struct {
	admin           daemonadmin.AdminSummary
	err             error
	refreshToken    domainauth.RefreshToken
	revokedOperator string
	revokedSession  string
}

func (f *fakeAuthenticator) AuthenticateOperator(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	if f.err != nil {
		return daemonadmin.AdminSummary{}, f.err
	}
	return f.admin, nil
}

func (f *fakeAuthenticator) CreateOperatorAuthSession(context.Context, daemonadmin.AdminSummary, domainauth.RefreshSessionMetadata, int, time.Duration, time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return "", domainauth.RefreshSession{}, f.err
	}
	f.refreshToken = "refresh-1"
	return f.refreshToken, domainauth.RefreshSession{ID: uuid.New(), CreatedAt: time.Now().UTC()}, nil
}

func (f *fakeAuthenticator) RefreshOperatorAuthSession(context.Context, domainauth.RefreshToken, domainauth.RefreshSessionMetadata, int, time.Duration) (daemonadmin.AdminSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return daemonadmin.AdminSummary{}, "", domainauth.RefreshSession{}, f.err
	}
	f.refreshToken = "refresh-2"
	return f.admin, f.refreshToken, domainauth.RefreshSession{ID: uuid.New(), CreatedAt: time.Now().UTC()}, nil
}

func (f *fakeAuthenticator) ListOperatorSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	return nil, f.err
}

func (f *fakeAuthenticator) RevokeOperatorSession(_ context.Context, operatorID string, sessionID string) error {
	f.revokedOperator = operatorID
	f.revokedSession = sessionID
	return f.err
}

func (f *fakeAuthenticator) RevokeOperatorSessions(context.Context, string) (int, error) {
	return 0, f.err
}

func TestLoginOperatorIssuesAccessToken(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(&fakeAuthenticator{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: createdAt}}, tokens)

	res, err := svc.LoginOperator(context.Background(), &adminv1.LoginOperatorRequest{Username: "admin", Password: "pass"})
	if err != nil {
		t.Fatalf("LoginOperator() error = %v", err)
	}
	if res.GetAccessToken() == "" || res.GetAccessTokenExpireTime() == nil || res.GetRefreshToken() == "" {
		t.Fatalf("expected access token and expiry, got %#v", res)
	}
	principal, err := tokens.Verify(res.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if principal.OperatorID != "op-1" || principal.Username != "admin" || principal.AuthSessionID == "" {
		t.Fatalf("unexpected principal: %#v", principal)
	}
	if res.GetOperator().GetUsername() != "admin" {
		t.Fatalf("unexpected operator: %#v", res.GetOperator())
	}
}

func TestLoginOperatorRejectsInvalidCredentials(t *testing.T) {
	svc := NewAuthService(&fakeAuthenticator{err: daemonadmin.ErrInvalidCredentials}, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	_, err := svc.LoginOperator(context.Background(), &adminv1.LoginOperatorRequest{Username: "admin", Password: "wrong"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestRefreshOperatorRotatesRefreshToken(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(&fakeAuthenticator{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: createdAt}}, tokens)
	refreshToken := "refresh-1"
	res, err := svc.RefreshOperator(context.Background(), &adminv1.RefreshOperatorRequest{RefreshToken: &refreshToken})
	if err != nil {
		t.Fatalf("RefreshOperator() error = %v", err)
	}
	if res.GetAccessToken() == "" || res.GetRefreshToken() != "refresh-2" {
		t.Fatalf("unexpected refresh response: %#v", res)
	}
	principal, err := tokens.Verify(res.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if principal.OperatorID != "op-1" || principal.AuthSessionID == "" {
		t.Fatalf("unexpected principal: %#v", principal)
	}
}

func TestLogoutOperatorRevokesCurrentSession(t *testing.T) {
	auth := &fakeAuthenticator{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: time.Now().UTC()}}
	svc := NewAuthService(auth, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindOperator, OperatorID: "op-1", AuthSessionID: "session-1", Username: "admin"})
	if _, err := svc.LogoutOperator(ctx, &adminv1.LogoutOperatorRequest{}); err != nil {
		t.Fatalf("LogoutOperator() error = %v", err)
	}
	if auth.revokedOperator != "op-1" || auth.revokedSession != "session-1" {
		t.Fatalf("unexpected revoked session operator=%q session=%q", auth.revokedOperator, auth.revokedSession)
	}
}

func TestLogoutOperatorReturnsRevokeError(t *testing.T) {
	auth := &fakeAuthenticator{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: time.Now().UTC()}, err: daemonadmin.ErrAdminNotFound}
	svc := NewAuthService(auth, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindOperator, OperatorID: "op-1", AuthSessionID: "missing", Username: "admin"})
	_, err := svc.LogoutOperator(ctx, &adminv1.LogoutOperatorRequest{})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("LogoutOperator() code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestWhoAmIRequiresAuthenticatedContext(t *testing.T) {
	svc := NewAuthService(nil, nil)
	_, err := svc.WhoAmI(context.Background(), &adminv1.WhoAmIRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestWhoAmIReturnsPrincipal(t *testing.T) {
	svc := NewAuthService(nil, nil)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{OperatorID: "op-1", Username: "admin", CreatedAt: time.Unix(100, 0)})
	res, err := svc.WhoAmI(ctx, &adminv1.WhoAmIRequest{})
	if err != nil {
		t.Fatalf("WhoAmI() error = %v", err)
	}
	if res.GetOperator().GetOperatorId() != "op-1" || res.GetOperator().GetUsername() != "admin" {
		t.Fatalf("unexpected whoami response: %#v", res)
	}
}
