package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultOperatorRefreshTokenBytes  = 32
	defaultOperatorRefreshIdleTTL     = 30 * 24 * time.Hour
	defaultOperatorRefreshAbsoluteTTL = 90 * 24 * time.Hour
)

type AuthService struct {
	adminv1.UnimplementedAdminAuthServiceServer
	authenticator daemonadmin.OperatorAuthManager
	tokens        *daemonauth.TokenManager
}

func NewAuthService(authenticator daemonadmin.OperatorAuthManager, tokens *daemonauth.TokenManager) *AuthService {
	return &AuthService{authenticator: authenticator, tokens: tokens}
}

func (s *AuthService) LoginOperator(ctx context.Context, req *adminv1.LoginOperatorRequest) (*adminv1.LoginOperatorResponse, error) {
	if s.authenticator == nil || s.tokens == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin auth service is not configured")
	}
	admin, err := s.authenticator.AuthenticateOperator(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		if errors.Is(err, daemonadmin.ErrInvalidCredentials) {
			return nil, status.Error(codes.Unauthenticated, "invalid operator credentials")
		}
		return nil, status.Errorf(codes.Internal, "authenticate operator: %v", err)
	}
	refreshToken, rec, err := s.authenticator.CreateOperatorAuthSession(ctx, admin, operatorClientMetadata(req.GetClient()), defaultOperatorRefreshTokenBytes, defaultOperatorRefreshIdleTTL, defaultOperatorRefreshAbsoluteTTL)
	if err != nil {
		return nil, mapOperatorAuthError(err, "create operator auth session")
	}
	token, expireAt, err := s.tokens.Issue(operatorPrincipal(admin, rec.ID.String()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue access token: %v", err)
	}
	refreshTokenText := string(refreshToken)
	return &adminv1.LoginOperatorResponse{AccessToken: token, AccessTokenExpireTime: timestamppb.New(expireAt), Operator: mapAdminSummary(admin), RefreshToken: &refreshTokenText}, nil
}

func (s *AuthService) RefreshOperator(ctx context.Context, req *adminv1.RefreshOperatorRequest) (*adminv1.RefreshOperatorResponse, error) {
	if s.authenticator == nil || s.tokens == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin auth service is not configured")
	}
	if req.GetRefreshToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh_token is required")
	}
	admin, refreshToken, rec, err := s.authenticator.RefreshOperatorAuthSession(ctx, domainauth.RefreshToken(req.GetRefreshToken()), operatorClientMetadata(req.GetClient()), defaultOperatorRefreshTokenBytes, defaultOperatorRefreshIdleTTL)
	if err != nil {
		return nil, mapOperatorAuthError(err, "refresh operator auth session")
	}
	token, expireAt, err := s.tokens.Issue(operatorPrincipal(admin, rec.ID.String()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue access token: %v", err)
	}
	refreshTokenText := string(refreshToken)
	return &adminv1.RefreshOperatorResponse{AccessToken: token, AccessTokenExpireTime: timestamppb.New(expireAt), Operator: mapAdminSummary(admin), RefreshToken: &refreshTokenText}, nil
}

func (s *AuthService) LogoutOperator(ctx context.Context, req *adminv1.LogoutOperatorRequest) (*adminv1.LogoutOperatorResponse, error) {
	if s.authenticator == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin auth service is not configured")
	}
	principal, err := principalFromContext(ctx)
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
	if err := s.authenticator.RevokeOperatorSession(ctx, principal.OperatorID, sessionID); err != nil {
		return nil, mapOperatorAuthError(err, "logout operator")
	}
	return &adminv1.LogoutOperatorResponse{}, nil
}

func (s *AuthService) WhoAmI(ctx context.Context, req *adminv1.WhoAmIRequest) (*adminv1.WhoAmIResponse, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "operator authentication is required")
	}
	return &adminv1.WhoAmIResponse{Operator: mapPrincipal(principal)}, nil
}

func operatorPrincipal(admin daemonadmin.AdminSummary, sessionID string) daemonauth.Principal {
	return daemonauth.Principal{Kind: daemonauth.PrincipalKindOperator, OperatorID: admin.ID, AuthSessionID: sessionID, Username: admin.Username, CreatedAt: admin.CreatedAt}
}

func operatorClientMetadata(client *adminv1.OperatorClientInfo) domainauth.RefreshSessionMetadata {
	if client == nil {
		return domainauth.RefreshSessionMetadata{}
	}
	return domainauth.RefreshSessionMetadata{ClientName: strings.TrimSpace(client.GetName())}
}

func mapOperatorAuthError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonadmin.ErrInvalidCredentials) || errors.Is(err, daemonadmin.ErrInvalidRefreshToken) {
		return status.Error(codes.Unauthenticated, "invalid operator credentials")
	}
	if errors.Is(err, daemonadmin.ErrAdminNotFound) || errors.Is(err, storesession.ErrSessionNotFound) {
		return status.Error(codes.NotFound, "operator or session not found")
	}
	if errors.Is(err, storesession.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

func mapPrincipal(principal daemonauth.Principal) *adminv1.Operator {
	return &adminv1.Operator{
		OperatorId: principal.OperatorID,
		Username:   principal.Username,
		State:      adminv1.OperatorState_OPERATOR_STATE_ACTIVE,
		CreateTime: timestamppb.New(principal.CreatedAt),
	}
}
