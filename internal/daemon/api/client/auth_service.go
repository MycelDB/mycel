package client

import (
	"context"
	"errors"
	"strconv"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListPageSize       = 100
	maxListPageSize           = 1000
	defaultRefreshTokenBytes  = 32
	defaultRefreshIdleTTL     = 30 * 24 * time.Hour
	defaultRefreshAbsoluteTTL = 90 * 24 * time.Hour
)

type AuthService struct {
	commonv1.UnimplementedAuthServiceServer
	principals principalservice.Manager
	tokens     *daemonauth.TokenManager
}

func NewAuthService(principals principalservice.Manager, tokens *daemonauth.TokenManager) *AuthService {
	return &AuthService{principals: principals, tokens: tokens}
}

func (s *AuthService) Login(ctx context.Context, req *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	principal, err := s.principals.AuthenticatePrincipal(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, mapAuthError(err, "login")
	}
	refreshToken, rec, err := s.principals.CreateAuthSession(ctx, principal, clientMetadata(req.GetClient()), defaultRefreshTokenBytes, defaultRefreshIdleTTL, defaultRefreshAbsoluteTTL)
	if err != nil {
		return nil, mapAuthError(err, "create auth session")
	}
	accessToken, expireAt, err := s.tokens.Issue(authPrincipal(principal, rec.ID.String()))
	if err != nil {
		return nil, err
	}
	refreshTokenText := string(refreshToken)
	return &commonv1.LoginResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(expireAt), Principal: mapPrincipal(principal), RefreshToken: &refreshTokenText, AuthSessionId: rec.ID.String()}, nil
}

func (s *AuthService) Refresh(ctx context.Context, req *commonv1.RefreshRequest) (*commonv1.RefreshResponse, error) {
	if req.GetRefreshToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh_token is required")
	}
	principal, refreshToken, rec, err := s.principals.RefreshAuthSession(ctx, domainauth.RefreshToken(req.GetRefreshToken()), clientMetadata(req.GetClient()), defaultRefreshTokenBytes, defaultRefreshIdleTTL)
	if err != nil {
		return nil, mapAuthError(err, "refresh auth session")
	}
	accessToken, expireAt, err := s.tokens.Issue(authPrincipal(principal, rec.ID.String()))
	if err != nil {
		return nil, err
	}
	refreshTokenText := string(refreshToken)
	return &commonv1.RefreshResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(expireAt), Principal: mapPrincipal(principal), RefreshToken: &refreshTokenText, AuthSessionId: rec.ID.String()}, nil
}

func (s *AuthService) Logout(ctx context.Context, req *commonv1.LogoutRequest) (*commonv1.LogoutResponse, error) {
	principal, err := authenticatedPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sessionID := req.GetAuthSessionId()
	if sessionID == "" {
		sessionID = principal.AuthSessionID
	}
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "auth_session_id is required")
	}
	if err := s.principals.RevokePrincipalSession(ctx, principal.PrincipalID, sessionID); err != nil {
		return nil, mapAuthError(err, "logout")
	}
	return &commonv1.LogoutResponse{}, nil
}

func (s *AuthService) WhoAmI(ctx context.Context, req *commonv1.WhoAmIRequest) (*commonv1.WhoAmIResponse, error) {
	principal, err := authenticatedPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &commonv1.WhoAmIResponse{Principal: &commonv1.AuthPrincipal{PrincipalId: principal.PrincipalID, Username: principal.Username}}, nil
}

func (s *AuthService) ListAuthSessions(ctx context.Context, req *commonv1.ListAuthSessionsRequest) (*commonv1.ListAuthSessionsResponse, error) {
	principal, err := authenticatedPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	sessions, err := s.principals.ListPrincipalSessions(ctx, principal.PrincipalID)
	if err != nil {
		return nil, mapAuthError(err, "list auth sessions")
	}
	filtered := make([]domainauth.RefreshSession, 0, len(sessions))
	for _, session := range sessions {
		if !req.GetIncludeInactive() && sessionState(session) != commonv1.AuthSessionState_AUTH_SESSION_STATE_ACTIVE {
			continue
		}
		filtered = append(filtered, session)
	}
	if offset > len(filtered) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the session list")
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	out := make([]*commonv1.AuthSessionSummary, 0, end-offset)
	for _, session := range filtered[offset:end] {
		out = append(out, mapSession(session, session.ID.String() == principal.AuthSessionID))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &commonv1.ListAuthSessionsResponse{Sessions: out, NextPageToken: next}, nil
}

func (s *AuthService) RevokeAuthSession(ctx context.Context, req *commonv1.RevokeAuthSessionRequest) (*commonv1.RevokeAuthSessionResponse, error) {
	principal, err := authenticatedPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.principals.RevokePrincipalSession(ctx, principal.PrincipalID, req.GetAuthSessionId()); err != nil {
		return nil, mapAuthError(err, "revoke auth session")
	}
	return &commonv1.RevokeAuthSessionResponse{}, nil
}

func (s *AuthService) RevokeOtherAuthSessions(ctx context.Context, req *commonv1.RevokeOtherAuthSessionsRequest) (*commonv1.RevokeOtherAuthSessionsResponse, error) {
	principal, err := authenticatedPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if principal.AuthSessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "current auth session is unknown")
	}
	sessions, err := s.principals.ListPrincipalSessions(ctx, principal.PrincipalID)
	if err != nil {
		return nil, mapAuthError(err, "list auth sessions")
	}
	count := 0
	for _, session := range sessions {
		if session.ID.String() == principal.AuthSessionID || session.Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		if err := s.principals.RevokePrincipalSession(ctx, principal.PrincipalID, session.ID.String()); err != nil {
			return nil, mapAuthError(err, "revoke auth session")
		}
		count++
	}
	return &commonv1.RevokeOtherAuthSessionsResponse{RevokedCount: int32(count)}, nil
}

func authPrincipal(principal principalservice.PrincipalSummary, sessionID string) daemonauth.Principal {
	kind := daemonauth.PrincipalKindHuman
	switch principal.Kind {
	case principalservice.PrincipalKindService:
		kind = daemonauth.PrincipalKindService
	case principalservice.PrincipalKindSystem:
		kind = daemonauth.PrincipalKindSystem
	}
	return daemonauth.Principal{Kind: kind, PrincipalID: principal.ID, AuthSessionID: sessionID, Username: principal.Username, CreatedAt: principal.CreatedAt}
}

func authenticatedPrincipalFromContext(ctx context.Context) (daemonauth.Principal, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok || principal.PrincipalID == "" {
		return daemonauth.Principal{}, status.Error(codes.Unauthenticated, "principal authentication is required")
	}
	return principal, nil
}

func mapPrincipal(principal principalservice.PrincipalSummary) *commonv1.AuthPrincipal {
	return &commonv1.AuthPrincipal{PrincipalId: principal.ID, Username: principal.Username}
}

func clientMetadata(client *commonv1.ClientInfo) domainauth.RefreshSessionMetadata {
	if client == nil {
		return domainauth.RefreshSessionMetadata{}
	}
	return domainauth.RefreshSessionMetadata{ClientName: client.GetName()}
}

func mapSession(session domainauth.RefreshSession, current bool) *commonv1.AuthSessionSummary {
	return &commonv1.AuthSessionSummary{AuthSessionId: session.ID.String(), CreateTime: timestamppb.New(session.CreatedAt), LastSeenTime: timestamppb.New(session.LastUsedAt), ExpireTime: timestamppb.New(session.AbsoluteExpiresAt), Current: current, Client: &commonv1.ClientInfo{Name: session.Metadata.ClientName}, State: sessionState(session)}
}

func sessionState(session domainauth.RefreshSession) commonv1.AuthSessionState {
	now := time.Now().UTC()
	if session.Status == domainauth.RefreshSessionStatusRevoked {
		return commonv1.AuthSessionState_AUTH_SESSION_STATE_REVOKED
	}
	if session.Status == domainauth.RefreshSessionStatusExpired || (!session.AbsoluteExpiresAt.IsZero() && !session.AbsoluteExpiresAt.After(now)) || (!session.IdleExpiresAt.IsZero() && !session.IdleExpiresAt.After(now)) {
		return commonv1.AuthSessionState_AUTH_SESSION_STATE_EXPIRED
	}
	return commonv1.AuthSessionState_AUTH_SESSION_STATE_ACTIVE
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, errors.New("page_token must be a non-negative integer offset")
	}
	return offset, nil
}

func normalizePageSize(size int32) int {
	if size <= 0 {
		return defaultListPageSize
	}
	if size > maxListPageSize {
		return maxListPageSize
	}
	return int(size)
}

func mapAuthError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, principalservice.ErrInvalidCredentials) {
		return status.Error(codes.Unauthenticated, "invalid credentials")
	}
	if errors.Is(err, principalservice.ErrPrincipalNotFound) || errors.Is(err, storesession.ErrSessionNotFound) {
		return status.Error(codes.NotFound, "principal or session not found")
	}
	if errors.Is(err, storesession.ErrInvalidInput) || errors.Is(err, principalservice.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
