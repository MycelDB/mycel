package admin

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OperatorAuthorizer interface {
	HasCapability(ctx context.Context, operatorID string, capability string) (bool, error)
}

type UserService struct {
	adminv1.UnimplementedAdminUserServiceServer
	users      daemonuser.Manager
	authorizer OperatorAuthorizer
}

func NewUserService(users daemonuser.Manager, authorizer OperatorAuthorizer) *UserService {
	return &UserService{users: users, authorizer: authorizer}
}

func (s *UserService) ListUsers(ctx context.Context, req *adminv1.ListUsersRequest) (*adminv1.ListUsersResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	users, err := s.users.ListUsers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list users: %v", err)
	}
	filtered := make([]daemonuser.UserSummary, 0, len(users))
	for _, user := range users {
		if user.State == daemonuser.UserStateDeleted && !req.GetIncludeDeleted() {
			continue
		}
		if user.State == daemonuser.UserStateDisabled && !req.GetIncludeDisabled() {
			continue
		}
		filtered = append(filtered, user)
	}
	if offset > len(filtered) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the user list")
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	out := make([]*adminv1.User, 0, end-offset)
	for _, user := range filtered[offset:end] {
		out = append(out, mapUserSummary(user))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListUsersResponse{Users: out, NextPageToken: next}, nil
}

func (s *UserService) GetUser(ctx context.Context, req *adminv1.GetUserRequest) (*adminv1.GetUserResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	user, err := s.users.GetUser(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "get user")
	}
	return &adminv1.GetUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) FindUser(ctx context.Context, req *adminv1.FindUserRequest) (*adminv1.FindUserResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	user, err := s.users.FindUser(ctx, req.GetUsername())
	if err != nil {
		return nil, mapUserError(err, "find user")
	}
	return &adminv1.FindUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) CreateUser(ctx context.Context, req *adminv1.CreateUserRequest) (*adminv1.CreateUserResponse, error) {
	if _, err := s.requireUserCreate(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetUsername()) == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and password are required")
	}
	user, err := s.users.CreateUser(ctx, daemonuser.CreateUserInput{Username: req.GetUsername(), Password: req.GetPassword(), Disabled: req.GetDisabled()})
	if err != nil {
		return nil, mapUserError(err, "create user")
	}
	return &adminv1.CreateUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) DisableUser(ctx context.Context, req *adminv1.DisableUserRequest) (*adminv1.DisableUserResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	user, err := s.users.DisableUser(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "disable user")
	}
	if req.GetRevokeSessions() {
		if _, err := s.users.RevokeUserSessions(ctx, req.GetUserId()); err != nil {
			return nil, mapUserError(err, "revoke user sessions")
		}
	}
	return &adminv1.DisableUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) EnableUser(ctx context.Context, req *adminv1.EnableUserRequest) (*adminv1.EnableUserResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	user, err := s.users.EnableUser(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "enable user")
	}
	return &adminv1.EnableUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) DeleteUser(ctx context.Context, req *adminv1.DeleteUserRequest) (*adminv1.DeleteUserResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	user, err := s.users.DeleteUser(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "delete user")
	}
	if req.GetRevokeSessions() {
		if _, err := s.users.RevokeUserSessions(ctx, req.GetUserId()); err != nil {
			return nil, mapUserError(err, "revoke user sessions")
		}
	}
	return &adminv1.DeleteUserResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) SetUserPassword(ctx context.Context, req *adminv1.SetUserPasswordRequest) (*adminv1.SetUserPasswordResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password must not be empty")
	}
	user, err := s.users.SetUserPassword(ctx, req.GetUserId(), req.GetPassword())
	if err != nil {
		return nil, mapUserError(err, "set user password")
	}
	if req.GetRevokeSessions() {
		if _, err := s.users.RevokeUserSessions(ctx, req.GetUserId()); err != nil {
			return nil, mapUserError(err, "revoke user sessions")
		}
	}
	return &adminv1.SetUserPasswordResponse{User: mapUserSummary(user)}, nil
}

func (s *UserService) ListUserSessions(ctx context.Context, req *adminv1.ListUserSessionsRequest) (*adminv1.ListUserSessionsResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	sessions, err := s.users.ListUserSessions(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "list user sessions")
	}
	filtered := make([]domainauth.RefreshSession, 0, len(sessions))
	for _, session := range sessions {
		if !req.GetIncludeInactive() && session.Status != domainauth.RefreshSessionStatusActive {
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
	out := make([]*adminv1.AdminAuthSessionSummary, 0, end-offset)
	for _, session := range filtered[offset:end] {
		out = append(out, mapUserSession(session))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListUserSessionsResponse{Sessions: out, NextPageToken: next}, nil
}

func (s *UserService) RevokeUserSession(ctx context.Context, req *adminv1.RevokeUserSessionRequest) (*adminv1.RevokeUserSessionResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	if err := s.users.RevokeUserSession(ctx, req.GetUserId(), req.GetAuthSessionId()); err != nil {
		return nil, mapUserError(err, "revoke user session")
	}
	return &adminv1.RevokeUserSessionResponse{}, nil
}

func (s *UserService) RevokeUserSessions(ctx context.Context, req *adminv1.RevokeUserSessionsRequest) (*adminv1.RevokeUserSessionsResponse, error) {
	if _, err := s.requireUserManage(ctx); err != nil {
		return nil, err
	}
	count, err := s.users.RevokeUserSessions(ctx, req.GetUserId())
	if err != nil {
		return nil, mapUserError(err, "revoke user sessions")
	}
	return &adminv1.RevokeUserSessionsResponse{RevokedCount: int32(count)}, nil
}

func (s *UserService) requireUserCreate(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireCapability(ctx, commonv1.Capability_CAPABILITY_USER_CREATE)
}

func (s *UserService) requireUserManage(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireCapability(ctx, commonv1.Capability_CAPABILITY_USER_MANAGE)
}

func (s *UserService) requireCapability(ctx context.Context, capability commonv1.Capability) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, capability.String())
	if err != nil {
		return daemonauth.Principal{}, mapAdminError(err, "authorize user operation")
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "user management capability is required")
	}
	return principal, nil
}

func mapUserSummary(user daemonuser.UserSummary) *adminv1.User {
	return &adminv1.User{UserId: user.ID, Username: user.Username, State: userStateFromInternal(user.State), CreateTime: timestamppb.New(user.CreatedAt), UpdateTime: timestamppb.New(user.UpdatedAt)}
}

func userStateFromInternal(state string) adminv1.UserState {
	switch state {
	case daemonuser.UserStateDisabled:
		return adminv1.UserState_USER_STATE_DISABLED
	case daemonuser.UserStateDeleted:
		return adminv1.UserState_USER_STATE_DELETED
	default:
		return adminv1.UserState_USER_STATE_ACTIVE
	}
}

func mapUserSession(session domainauth.RefreshSession) *adminv1.AdminAuthSessionSummary {
	state := adminv1.AdminAuthSessionState_ADMIN_AUTH_SESSION_STATE_ACTIVE
	now := time.Now().UTC()
	if session.Status == domainauth.RefreshSessionStatusRevoked {
		state = adminv1.AdminAuthSessionState_ADMIN_AUTH_SESSION_STATE_REVOKED
	} else if session.Status == domainauth.RefreshSessionStatusExpired || (!session.AbsoluteExpiresAt.IsZero() && session.AbsoluteExpiresAt.Before(now)) || (!session.IdleExpiresAt.IsZero() && session.IdleExpiresAt.Before(now)) {
		state = adminv1.AdminAuthSessionState_ADMIN_AUTH_SESSION_STATE_EXPIRED
	}
	return &adminv1.AdminAuthSessionSummary{AuthSessionId: session.ID.String(), CreateTime: timestamppb.New(session.CreatedAt), LastSeenTime: timestamppb.New(session.LastUsedAt), ExpireTime: timestamppb.New(session.AbsoluteExpiresAt), State: state, Client: &adminv1.AdminClientInfo{Name: session.Metadata.ClientName}}
}

func mapUserError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonuser.ErrUserNotFound) || errors.Is(err, storesession.ErrSessionNotFound) {
		return status.Error(codes.NotFound, "user or session not found")
	}
	if errors.Is(err, daemonuser.ErrDuplicateUser) {
		return status.Error(codes.AlreadyExists, "user already exists")
	}
	if errors.Is(err, storesession.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
