package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthServiceLoginWhoAmIRefreshAndLogout(t *testing.T) {
	module := initAuthTestUserModule(t)
	user, err := module.CreatePrincipal(context.Background(), principalservice.CreatePrincipalInput{Username: "alice", Password: "alice-pass", LoginEnabled: true})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(module, tokens)

	login, err := svc.Login(context.Background(), &commonv1.LoginRequest{Username: "alice", Password: "alice-pass", Client: &commonv1.ClientInfo{Name: "test"}})
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if login.GetAccessToken() == "" || login.GetRefreshToken() == "" || login.GetPrincipal().GetPrincipalId() != user.ID || login.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected login response: %#v", login)
	}
	principal, err := tokens.Verify(login.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify login token: %v", err)
	}
	if principal.Kind != daemonauth.PrincipalKindHuman || principal.PrincipalID != user.ID || principal.AuthSessionID == "" {
		t.Fatalf("unexpected principal: %+v", principal)
	}
	authCtx := daemonauth.ContextWithPrincipal(context.Background(), principal)
	who, err := svc.WhoAmI(authCtx, &commonv1.WhoAmIRequest{})
	if err != nil || who.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("WhoAmI() = %#v, %v", who, err)
	}
	list, err := svc.ListAuthSessions(authCtx, &commonv1.ListAuthSessionsRequest{})
	if err != nil || len(list.GetSessions()) != 1 || !list.GetSessions()[0].GetCurrent() || list.GetSessions()[0].GetClient().GetName() != "test" {
		t.Fatalf("ListAuthSessions() = %#v, %v", list, err)
	}

	refresh, err := svc.Refresh(context.Background(), &commonv1.RefreshRequest{RefreshToken: login.RefreshToken, Client: &commonv1.ClientInfo{Name: "refresh-test"}})
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if refresh.GetRefreshToken() == "" || refresh.GetRefreshToken() == login.GetRefreshToken() || refresh.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected refresh response: %#v", refresh)
	}
	if _, err := svc.Refresh(context.Background(), &commonv1.RefreshRequest{RefreshToken: login.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected old refresh token reuse to fail, got %v", err)
	}
	refreshedPrincipal, err := tokens.Verify(refresh.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify refresh access token: %v", err)
	}
	if _, err := svc.Logout(daemonauth.ContextWithPrincipal(context.Background(), refreshedPrincipal), &commonv1.LogoutRequest{}); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	if _, err := svc.Refresh(context.Background(), &commonv1.RefreshRequest{RefreshToken: refresh.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected logged-out refresh token to fail, got %v", err)
	}
}

func TestAuthServiceDisabledUserCannotLogin(t *testing.T) {
	module := initAuthTestUserModule(t)
	user, err := module.CreatePrincipal(context.Background(), principalservice.CreatePrincipalInput{Username: "disabled", Password: "pass", LoginEnabled: true})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if _, err := module.DisablePrincipal(context.Background(), user.ID); err != nil {
		t.Fatalf("DisablePrincipal() error = %v", err)
	}
	svc := NewAuthService(module, daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute))
	if _, err := svc.Login(context.Background(), &commonv1.LoginRequest{Username: "disabled", Password: "pass"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected disabled login to be unauthenticated, got %v", err)
	}
}

func TestAuthServiceRevokeOtherAuthSessions(t *testing.T) {
	module := initAuthTestUserModule(t)
	_, err := module.CreatePrincipal(context.Background(), principalservice.CreatePrincipalInput{Username: "carol", Password: "carol-pass", LoginEnabled: true})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	svc := NewAuthService(module, tokens)
	first, err := svc.Login(context.Background(), &commonv1.LoginRequest{Username: "carol", Password: "carol-pass"})
	if err != nil {
		t.Fatalf("first Login() error = %v", err)
	}
	second, err := svc.Login(context.Background(), &commonv1.LoginRequest{Username: "carol", Password: "carol-pass"})
	if err != nil {
		t.Fatalf("second Login() error = %v", err)
	}
	principal, err := tokens.Verify(second.GetAccessToken())
	if err != nil {
		t.Fatalf("Verify token: %v", err)
	}
	res, err := svc.RevokeOtherAuthSessions(daemonauth.ContextWithPrincipal(context.Background(), principal), &commonv1.RevokeOtherAuthSessionsRequest{})
	if err != nil {
		t.Fatalf("RevokeOtherAuthSessions() error = %v", err)
	}
	if res.GetRevokedCount() != 1 {
		t.Fatalf("expected one revoked session, got %d", res.GetRevokedCount())
	}
	if _, err := svc.Refresh(context.Background(), &commonv1.RefreshRequest{RefreshToken: first.RefreshToken}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected first session refresh to fail, got %v", err)
	}
	if _, err := svc.Refresh(context.Background(), &commonv1.RefreshRequest{RefreshToken: second.RefreshToken}); err != nil {
		t.Fatalf("expected current session refresh to remain valid: %v", err)
	}
}

func initAuthTestUserModule(t *testing.T) *principalservice.Module {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: logger}
	module := principalservice.NewModule()
	if result := module.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init principal module: %v", result.Error)
	}
	return module
}
