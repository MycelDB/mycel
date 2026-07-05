package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const DefaultAccessTokenTTL = 15 * time.Minute

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("expired token")
)

type PrincipalKind string

const (
	PrincipalKindOperator PrincipalKind = "operator"
	PrincipalKindUser     PrincipalKind = "user"
)

type Principal struct {
	Kind          PrincipalKind
	OperatorID    string
	UserID        string
	AuthSessionID string
	Username      string
	CreatedAt     time.Time
}

type contextKey struct{}

func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(contextKey{}).(Principal)
	return principal, ok
}

type TokenManager struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

type tokenPayload struct {
	Kind          PrincipalKind `json:"kind,omitempty"`
	OperatorID    string        `json:"operator_id,omitempty"`
	UserID        string        `json:"user_id,omitempty"`
	AuthSessionID string        `json:"auth_session_id,omitempty"`
	Username      string        `json:"username"`
	CreatedAt     int64         `json:"created_at"`
	IssuedAt      int64         `json:"iat"`
	ExpiresAt     int64         `json:"exp"`
}

func NewRandomTokenManager(ttl time.Duration) (*TokenManager, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate token signing secret: %w", err)
	}
	return NewTokenManager(secret, ttl), nil
}

func NewTokenManager(secret []byte, ttl time.Duration) *TokenManager {
	if ttl <= 0 {
		ttl = DefaultAccessTokenTTL
	}
	return &TokenManager{secret: append([]byte(nil), secret...), ttl: ttl, now: time.Now}
}

func (m *TokenManager) Issue(principal Principal) (string, time.Time, error) {
	if m == nil || len(m.secret) == 0 {
		return "", time.Time{}, fmt.Errorf("token manager is not configured")
	}
	now := m.now().UTC()
	expireAt := now.Add(m.ttl)
	kind := principal.Kind
	if kind == "" {
		if principal.OperatorID != "" {
			kind = PrincipalKindOperator
		} else if principal.UserID != "" {
			kind = PrincipalKindUser
		}
	}
	payload := tokenPayload{Kind: kind, OperatorID: principal.OperatorID, UserID: principal.UserID, AuthSessionID: principal.AuthSessionID, Username: principal.Username, CreatedAt: principal.CreatedAt.UTC().Unix(), IssuedAt: now.Unix(), ExpiresAt: expireAt.Unix()}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", time.Time{}, err
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signature := m.sign(payloadPart)
	return payloadPart + "." + signature, expireAt, nil
}

func (m *TokenManager) Verify(token string) (Principal, error) {
	if m == nil || len(m.secret) == 0 {
		return Principal{}, fmt.Errorf("token manager is not configured")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Principal{}, ErrInvalidToken
	}
	expected := m.sign(parts[0])
	if !hmac.Equal([]byte(expected), []byte(parts[1])) {
		return Principal{}, ErrInvalidToken
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Principal{}, ErrInvalidToken
	}
	var payload tokenPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return Principal{}, ErrInvalidToken
	}
	if payload.Kind == "" && payload.OperatorID != "" {
		payload.Kind = PrincipalKindOperator
	}
	if payload.Kind == "" && payload.UserID != "" {
		payload.Kind = PrincipalKindUser
	}
	if payload.Username == "" || payload.ExpiresAt <= 0 || (payload.OperatorID == "" && payload.UserID == "") {
		return Principal{}, ErrInvalidToken
	}
	if !m.now().UTC().Before(time.Unix(payload.ExpiresAt, 0)) {
		return Principal{}, ErrExpiredToken
	}
	return Principal{Kind: payload.Kind, OperatorID: payload.OperatorID, UserID: payload.UserID, AuthSessionID: payload.AuthSessionID, Username: payload.Username, CreatedAt: time.Unix(payload.CreatedAt, 0).UTC()}, nil
}

func (m *TokenManager) UnaryInterceptor(publicMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}
		principal, err := m.PrincipalFromIncomingContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ContextWithPrincipal(ctx, principal), req)
	}
}

func (m *TokenManager) StreamInterceptor(publicMethods map[string]bool) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if publicMethods[info.FullMethod] {
			return handler(srv, stream)
		}
		principal, err := m.PrincipalFromIncomingContext(stream.Context())
		if err != nil {
			return err
		}
		return handler(srv, &principalServerStream{ServerStream: stream, ctx: ContextWithPrincipal(stream.Context(), principal)})
	}
}

type principalServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *principalServerStream) Context() context.Context { return s.ctx }

func (m *TokenManager) PrincipalFromIncomingContext(ctx context.Context) (Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Principal{}, status.Error(codes.Unauthenticated, "authorization metadata is required")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return Principal{}, status.Error(codes.Unauthenticated, "authorization metadata is required")
	}
	token, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok || strings.TrimSpace(token) == "" {
		return Principal{}, status.Error(codes.Unauthenticated, "bearer authorization token is required")
	}
	principal, err := m.Verify(strings.TrimSpace(token))
	if err != nil {
		if errors.Is(err, ErrExpiredToken) {
			return Principal{}, status.Error(codes.Unauthenticated, "authorization token is expired")
		}
		return Principal{}, status.Error(codes.Unauthenticated, "authorization token is invalid")
	}
	return principal, nil
}

func (m *TokenManager) sign(payload string) string {
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
