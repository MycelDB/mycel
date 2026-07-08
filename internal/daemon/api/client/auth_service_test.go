package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthServiceLoginWhoAmIRefreshAndLogout(t *testing.T) {
	module := initAuthTestUserModule(t)
	user, err := module.CreateUser(context.Background(), daemonuser.CreateUserInput{Username: "alice", Password: "alice-pass"})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(module, tokens)

	login, err := svc.Login(context.Background(), &clientv1.LoginRequest{Username: "alice", Password: "alice-pass", Client: &clientv1.ClientInfo{Name: "test"}})
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if login.GetAccessToken() == "" || login.GetRefreshToken() == "" || login.GetPrincipal().GetUserId() != user.ID || login.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected login response: %#v", login)
	}
	principal, err := tokens.Verify(login.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify login token: %v", err)
	}
	if principal.Kind != daemonauth.PrincipalKindUser || principal.UserID != user.ID || principal.AuthSessionID == "" {
		t.Fatalf("unexpected principal: %+v", principal)
	}
	authCtx := daemonauth.ContextWithPrincipal(context.Background(), principal)
	who, err := svc.WhoAmI(authCtx, &clientv1.WhoAmIRequest{})
	if err != nil || who.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("WhoAmI() = %#v, %v", who, err)
	}
	list, err := svc.ListAuthSessions(authCtx, &clientv1.ListAuthSessionsRequest{})
	if err != nil || len(list.GetSessions()) != 1 || !list.GetSessions()[0].GetCurrent() || list.GetSessions()[0].GetClient().GetName() != "test" {
		t.Fatalf("ListAuthSessions() = %#v, %v", list, err)
	}

	refresh, err := svc.Refresh(context.Background(), &clientv1.RefreshRequest{RefreshToken: login.RefreshToken, Client: &clientv1.ClientInfo{Name: "refresh-test"}})
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if refresh.GetRefreshToken() == "" || refresh.GetRefreshToken() == login.GetRefreshToken() || refresh.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected refresh response: %#v", refresh)
	}
	if _, err := svc.Refresh(context.Background(), &clientv1.RefreshRequest{RefreshToken: login.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected old refresh token reuse to fail, got %v", err)
	}
	refreshedPrincipal, err := tokens.Verify(refresh.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify refresh access token: %v", err)
	}
	if _, err := svc.Logout(daemonauth.ContextWithPrincipal(context.Background(), refreshedPrincipal), &clientv1.LogoutRequest{}); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	if _, err := svc.Refresh(context.Background(), &clientv1.RefreshRequest{RefreshToken: refresh.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected logged-out refresh token to fail, got %v", err)
	}
}

func TestAuthServiceDisabledUserCannotLogin(t *testing.T) {
	module := initAuthTestUserModule(t)
	user, err := module.CreateUser(context.Background(), daemonuser.CreateUserInput{Username: "disabled", Password: "pass"})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if _, err := module.DisableUser(context.Background(), user.ID); err != nil {
		t.Fatalf("DisableUser() error = %v", err)
	}
	svc := NewAuthService(module, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	if _, err := svc.Login(context.Background(), &clientv1.LoginRequest{Username: "disabled", Password: "pass"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected disabled login to be unauthenticated, got %v", err)
	}
}

func TestAuthServiceRevokeOtherAuthSessions(t *testing.T) {
	module := initAuthTestUserModule(t)
	_, err := module.CreateUser(context.Background(), daemonuser.CreateUserInput{Username: "carol", Password: "carol-pass"})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(module, tokens)
	first, err := svc.Login(context.Background(), &clientv1.LoginRequest{Username: "carol", Password: "carol-pass"})
	if err != nil {
		t.Fatalf("first Login() error = %v", err)
	}
	second, err := svc.Login(context.Background(), &clientv1.LoginRequest{Username: "carol", Password: "carol-pass"})
	if err != nil {
		t.Fatalf("second Login() error = %v", err)
	}
	principal, err := tokens.Verify(second.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify token: %v", err)
	}
	res, err := svc.RevokeOtherAuthSessions(daemonauth.ContextWithPrincipal(context.Background(), principal), &clientv1.RevokeOtherAuthSessionsRequest{})
	if err != nil {
		t.Fatalf("RevokeOtherAuthSessions() error = %v", err)
	}
	if res.GetRevokedCount() != 1 {
		t.Fatalf("expected one revoked session, got %d", res.GetRevokedCount())
	}
	if _, err := svc.Refresh(context.Background(), &clientv1.RefreshRequest{RefreshToken: first.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected first session refresh to fail, got %v", err)
	}
	if _, err := svc.Refresh(context.Background(), &clientv1.RefreshRequest{RefreshToken: second.RefreshToken}); err != nil {
		t.Fatalf("expected current session refresh to remain valid: %v", err)
	}
}

func initAuthTestUserModule(t *testing.T) *daemonuser.Module {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: logger}
	module := daemonuser.NewModule()
	if result := module.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init user module: %v", result.Error)
	}
	return module
}
