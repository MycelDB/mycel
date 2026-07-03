package admin

import (
	"context"
	"errors"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuthService struct {
	adminv1.UnimplementedAdminAuthServiceServer
	authenticator daemonadmin.OperatorAuthenticator
	tokens        *daemonauth.TokenManager
}

func NewAuthService(authenticator daemonadmin.OperatorAuthenticator, tokens *daemonauth.TokenManager) *AuthService {
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
	principal := daemonauth.Principal{OperatorID: admin.ID, Username: admin.Username, CreatedAt: admin.CreatedAt}
	token, expireAt, err := s.tokens.Issue(principal)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue access token: %v", err)
	}
	return &adminv1.LoginOperatorResponse{AccessToken: token, AccessTokenExpireTime: timestamppb.New(expireAt), Operator: mapAdminSummary(admin)}, nil
}

func (s *AuthService) WhoAmI(ctx context.Context, req *adminv1.WhoAmIRequest) (*adminv1.WhoAmIResponse, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "operator authentication is required")
	}
	return &adminv1.WhoAmIResponse{Operator: mapPrincipal(principal)}, nil
}

func mapPrincipal(principal daemonauth.Principal) *adminv1.Operator {
	return &adminv1.Operator{
		OperatorId: principal.OperatorID,
		Username:   principal.Username,
		State:      adminv1.OperatorState_OPERATOR_STATE_ACTIVE,
		CreateTime: timestamppb.New(principal.CreatedAt),
	}
}
