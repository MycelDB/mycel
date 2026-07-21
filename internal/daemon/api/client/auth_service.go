package client

import (
	"context"
	"errors"
	"strconv"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	daemonuser "github.com/myceldb/mycel/internal/identity/service/user"
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
	clientv1.UnimplementedAuthServiceServer
	users  daemonuser.Manager
	tokens *daemonauth.TokenManager
}

func NewAuthService(users daemonuser.Manager, tokens *daemonauth.TokenManager) *AuthService {
	return &AuthService{users: users, tokens: tokens}
}

func (s *AuthService) Login(ctx context.Context, req *clientv1.LoginRequest) (*clientv1.LoginResponse, error) {
	user, err := s.users.AuthenticateUser(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, mapAuthError(err, "login")
	}
	refreshToken, rec, err := s.users.CreateAuthSession(ctx, user, clientMetadata(req.GetClient()), defaultRefreshTokenBytes, defaultRefreshIdleTTL, defaultRefreshAbsoluteTTL)
	if err != nil {
		return nil, mapAuthError(err, "create auth session")
	}
	accessToken, expireAt, err := s.tokens.Issue(userPrincipal(user, rec.ID.String()))
	if err != nil {
		return nil, err
	}
	refreshTokenText := string(refreshToken)
	return &clientv1.LoginResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(expireAt), Principal: mapPrincipal(user), RefreshToken: &refreshTokenText}, nil
}

func (s *AuthService) Refresh(ctx context.Context, req *clientv1.RefreshRequest) (*clientv1.RefreshResponse, error) {
	if req.GetRefreshToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh_token is required")
	}
	user, refreshToken, rec, err := s.users.RefreshAuthSession(ctx, domainauth.RefreshToken(req.GetRefreshToken()), clientMetadata(req.GetClient()), defaultRefreshTokenBytes, defaultRefreshIdleTTL)
	if err != nil {
		return nil, mapAuthError(err, "refresh auth session")
	}
	accessToken, expireAt, err := s.tokens.Issue(userPrincipal(user, rec.ID.String()))
	if err != nil {
		return nil, err
	}
	refreshTokenText := string(refreshToken)
	return &clientv1.RefreshResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(expireAt), Principal: mapPrincipal(user), RefreshToken: &refreshTokenText}, nil
}

func (s *AuthService) Logout(ctx context.Context, req *clientv1.LogoutRequest) (*clientv1.LogoutResponse, error) {
	principal, err := userPrincipalFromContext(ctx)
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
	if err := s.users.RevokeUserSession(ctx, principal.UserID, sessionID); err != nil {
		return nil, mapAuthError(err, "logout")
	}
	return &clientv1.LogoutResponse{}, nil
}

func (s *AuthService) WhoAmI(ctx context.Context, req *clientv1.WhoAmIRequest) (*clientv1.WhoAmIResponse, error) {
	principal, err := userPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &clientv1.WhoAmIResponse{Principal: &clientv1.AuthPrincipal{UserId: principal.UserID, Username: principal.Username}}, nil
}

func (s *AuthService) ListAuthSessions(ctx context.Context, req *clientv1.ListAuthSessionsRequest) (*clientv1.ListAuthSessionsResponse, error) {
	principal, err := userPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	sessions, err := s.users.ListUserSessions(ctx, principal.UserID)
	if err != nil {
		return nil, mapAuthError(err, "list auth sessions")
	}
	filtered := make([]domainauth.RefreshSession, 0, len(sessions))
	for _, session := range sessions {
		if !req.GetIncludeInactive() && sessionState(session) != clientv1.AuthSessionState_AUTH_SESSION_STATE_ACTIVE {
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
	out := make([]*clientv1.AuthSessionSummary, 0, end-offset)
	for _, session := range filtered[offset:end] {
		out = append(out, mapSession(session, session.ID.String() == principal.AuthSessionID))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &clientv1.ListAuthSessionsResponse{Sessions: out, NextPageToken: next}, nil
}

func (s *AuthService) RevokeAuthSession(ctx context.Context, req *clientv1.RevokeAuthSessionRequest) (*clientv1.RevokeAuthSessionResponse, error) {
	principal, err := userPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.users.RevokeUserSession(ctx, principal.UserID, req.GetAuthSessionId()); err != nil {
		return nil, mapAuthError(err, "revoke auth session")
	}
	return &clientv1.RevokeAuthSessionResponse{}, nil
}

func (s *AuthService) RevokeOtherAuthSessions(ctx context.Context, req *clientv1.RevokeOtherAuthSessionsRequest) (*clientv1.RevokeOtherAuthSessionsResponse, error) {
	principal, err := userPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if principal.AuthSessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "current auth session is unknown")
	}
	sessions, err := s.users.ListUserSessions(ctx, principal.UserID)
	if err != nil {
		return nil, mapAuthError(err, "list auth sessions")
	}
	count := 0
	for _, session := range sessions {
		if session.ID.String() == principal.AuthSessionID || session.Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		if err := s.users.RevokeUserSession(ctx, principal.UserID, session.ID.String()); err != nil {
			return nil, mapAuthError(err, "revoke auth session")
		}
		count++
	}
	return &clientv1.RevokeOtherAuthSessionsResponse{RevokedCount: int32(count)}, nil
}

func userPrincipal(user daemonuser.UserSummary, sessionID string) daemonauth.Principal {
	return daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: user.ID, AuthSessionID: sessionID, Username: user.Username, CreatedAt: user.CreatedAt}
}

func userPrincipalFromContext(ctx context.Context) (daemonauth.Principal, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok || principal.Kind != daemonauth.PrincipalKindUser || principal.UserID == "" {
		return daemonauth.Principal{}, status.Error(codes.Unauthenticated, "user authentication is required")
	}
	return principal, nil
}

func mapPrincipal(user daemonuser.UserSummary) *clientv1.AuthPrincipal {
	return &clientv1.AuthPrincipal{UserId: user.ID, Username: user.Username}
}

func clientMetadata(client *clientv1.ClientInfo) domainauth.RefreshSessionMetadata {
	if client == nil {
		return domainauth.RefreshSessionMetadata{}
	}
	return domainauth.RefreshSessionMetadata{ClientName: client.GetName()}
}

func mapSession(session domainauth.RefreshSession, current bool) *clientv1.AuthSessionSummary {
	return &clientv1.AuthSessionSummary{AuthSessionId: session.ID.String(), CreateTime: timestamppb.New(session.CreatedAt), LastSeenTime: timestamppb.New(session.LastUsedAt), ExpireTime: timestamppb.New(session.AbsoluteExpiresAt), Current: current, Client: &clientv1.ClientInfo{Name: session.Metadata.ClientName}, State: sessionState(session)}
}

func sessionState(session domainauth.RefreshSession) clientv1.AuthSessionState {
	now := time.Now().UTC()
	if session.Status == domainauth.RefreshSessionStatusRevoked {
		return clientv1.AuthSessionState_AUTH_SESSION_STATE_REVOKED
	}
	if session.Status == domainauth.RefreshSessionStatusExpired || (!session.AbsoluteExpiresAt.IsZero() && !session.AbsoluteExpiresAt.After(now)) || (!session.IdleExpiresAt.IsZero() && !session.IdleExpiresAt.After(now)) {
		return clientv1.AuthSessionState_AUTH_SESSION_STATE_EXPIRED
	}
	return clientv1.AuthSessionState_AUTH_SESSION_STATE_ACTIVE
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
	if errors.Is(err, daemonuser.ErrInvalidCredentials) || errors.Is(err, daemonuser.ErrInvalidRefreshToken) {
		return status.Error(codes.Unauthenticated, "invalid credentials")
	}
	if errors.Is(err, daemonuser.ErrUserNotFound) || errors.Is(err, storesession.ErrSessionNotFound) {
		return status.Error(codes.NotFound, "user or session not found")
	}
	if errors.Is(err, storesession.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
