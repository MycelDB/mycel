package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTokenManagerIssueVerify(t *testing.T) {
	mgr := NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	mgr.now = func() time.Time { return time.Unix(100, 0) }
	principal := Principal{PrincipalID: "op-1", Username: "admin", CreatedAt: time.Unix(50, 0)}
	token, expireAt, err := mgr.Issue(principal)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if !expireAt.Equal(time.Unix(160, 0)) {
		t.Fatalf("unexpected expire time %s", expireAt)
	}
	got, err := mgr.Verify(token)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if got.PrincipalID != principal.PrincipalID || got.Username != principal.Username {
		t.Fatalf("unexpected principal %#v", got)
	}
}

func TestTokenManagerRejectsTamperedAndExpiredTokens(t *testing.T) {
	mgr := NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	mgr.now = func() time.Time { return time.Unix(100, 0) }
	token, _, err := mgr.Issue(Principal{PrincipalID: "op-1", Username: "admin"})
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if _, err := mgr.Verify(token + "tampered"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected invalid token, got %v", err)
	}
	mgr.now = func() time.Time { return time.Unix(161, 0) }
	if _, err := mgr.Verify(token); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expected expired token, got %v", err)
	}
}

func TestUnaryInterceptorRequiresBearerToken(t *testing.T) {
	mgr := NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	interceptor := mgr.UnaryInterceptor(map[string]bool{})
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/protected"}, func(ctx context.Context, req any) (any, error) { return nil, nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestUnaryInterceptorAddsPrincipal(t *testing.T) {
	mgr := NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	token, _, err := mgr.Issue(Principal{PrincipalID: "op-1", Username: "admin"})
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	interceptor := mgr.UnaryInterceptor(map[string]bool{})
	_, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/protected"}, func(ctx context.Context, req any) (any, error) {
		principal, ok := PrincipalFromContext(ctx)
		if !ok || principal.Username != "admin" {
			t.Fatalf("missing principal in context: %#v", principal)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("interceptor error = %v", err)
	}
}
