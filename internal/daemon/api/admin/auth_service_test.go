package admin

import (
	"context"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAuthenticator struct {
	admin daemonadmin.AdminSummary
	err   error
}

func (f fakeAuthenticator) AuthenticateOperator(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	if f.err != nil {
		return daemonadmin.AdminSummary{}, f.err
	}
	return f.admin, nil
}

func TestLoginOperatorIssuesAccessToken(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(fakeAuthenticator{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: createdAt}}, tokens)

	res, err := svc.LoginOperator(context.Background(), &adminv1.LoginOperatorRequest{Username: "admin", Password: "pass"})
	if err != nil {
		t.Fatalf("LoginOperator() error = %v", err)
	}
	if res.GetAccessToken() == "" || res.GetAccessTokenExpireTime() == nil {
		t.Fatalf("expected access token and expiry, got %#v", res)
	}
	principal, err := tokens.Verify(res.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if principal.OperatorID != "op-1" || principal.Username != "admin" {
		t.Fatalf("unexpected principal: %#v", principal)
	}
	if res.GetOperator().GetUsername() != "admin" {
		t.Fatalf("unexpected operator: %#v", res.GetOperator())
	}
}

func TestLoginOperatorRejectsInvalidCredentials(t *testing.T) {
	svc := NewAuthService(fakeAuthenticator{err: daemonadmin.ErrInvalidCredentials}, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	_, err := svc.LoginOperator(context.Background(), &adminv1.LoginOperatorRequest{Username: "admin", Password: "wrong"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
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
