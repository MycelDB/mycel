package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeUserManager struct {
	users            []daemonuser.UserSummary
	user             daemonuser.UserSummary
	sessions         []domainauth.RefreshSession
	created          daemonuser.CreateUserInput
	passwordUserID   string
	password         string
	revokedSessionID string
	revokedAllUserID string
	revokedCount     int
	err              error
}

func (f *fakeUserManager) ListUsers(context.Context) ([]daemonuser.UserSummary, error) {
	return f.users, f.err
}
func (f *fakeUserManager) GetUser(context.Context, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) FindUser(context.Context, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) CreateUser(ctx context.Context, input daemonuser.CreateUserInput) (daemonuser.UserSummary, error) {
	f.created = input
	return f.user, f.err
}
func (f *fakeUserManager) DisableUser(context.Context, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) EnableUser(context.Context, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) DeleteUser(context.Context, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) SetUserPassword(ctx context.Context, userID string, password string) (daemonuser.UserSummary, error) {
	f.passwordUserID = userID
	f.password = password
	return f.user, f.err
}
func (f *fakeUserManager) AuthenticateUser(context.Context, string, string) (daemonuser.UserSummary, error) {
	return f.user, f.err
}
func (f *fakeUserManager) CreateAuthSession(context.Context, daemonuser.UserSummary, domainauth.RefreshSessionMetadata, int, time.Duration, time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	return "refresh", domainauth.RefreshSession{}, f.err
}
func (f *fakeUserManager) RefreshAuthSession(context.Context, domainauth.RefreshToken, domainauth.RefreshSessionMetadata, int, time.Duration) (daemonuser.UserSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	return f.user, "refresh", domainauth.RefreshSession{}, f.err
}
func (f *fakeUserManager) ListUserSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	return f.sessions, f.err
}
func (f *fakeUserManager) RevokeUserSession(ctx context.Context, userID string, sessionID string) error {
	f.revokedSessionID = sessionID
	return f.err
}
func (f *fakeUserManager) RevokeUserSessions(ctx context.Context, userID string) (int, error) {
	f.revokedAllUserID = userID
	return f.revokedCount, f.err
}

type fakeAuthorizer struct{ allowed bool }

func (f fakeAuthorizer) HasCapability(context.Context, string, string) (bool, error) {
	return f.allowed, nil
}

func TestUserServiceRequiresAuthenticationAndCapability(t *testing.T) {
	svc := NewUserService(&fakeUserManager{}, fakeAuthorizer{allowed: true})
	if _, err := svc.ListUsers(context.Background(), &adminv1.ListUsersRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	svc = NewUserService(&fakeUserManager{}, fakeAuthorizer{allowed: false})
	if _, err := svc.ListUsers(userAuthenticatedContext(), &adminv1.ListUsersRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestUserServiceListFiltersAndMapsUsers(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	manager := &fakeUserManager{users: []daemonuser.UserSummary{{ID: "u1", Username: "active", State: daemonuser.UserStateActive, CreatedAt: createdAt, UpdatedAt: createdAt}, {ID: "u2", Username: "disabled", State: daemonuser.UserStateDisabled, CreatedAt: createdAt, UpdatedAt: createdAt}, {ID: "u3", Username: "deleted", State: daemonuser.UserStateDeleted, CreatedAt: createdAt, UpdatedAt: createdAt}}}
	svc := NewUserService(manager, fakeAuthorizer{allowed: true})
	res, err := svc.ListUsers(userAuthenticatedContext(), &adminv1.ListUsersRequest{})
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if len(res.GetUsers()) != 1 || res.GetUsers()[0].GetUsername() != "active" {
		t.Fatalf("unexpected filtered users: %#v", res)
	}
	res, err = svc.ListUsers(userAuthenticatedContext(), &adminv1.ListUsersRequest{IncludeDisabled: true, IncludeDeleted: true})
	if err != nil || len(res.GetUsers()) != 3 {
		t.Fatalf("expected all users, got %#v err=%v", res, err)
	}
}

func TestUserServiceCreate(t *testing.T) {
	manager := &fakeUserManager{user: daemonuser.UserSummary{ID: "u1", Username: "alice", State: daemonuser.UserStateActive, CreatedAt: time.Now(), UpdatedAt: time.Now()}}
	svc := NewUserService(manager, fakeAuthorizer{allowed: true})
	password := "pass"
	res, err := svc.CreateUser(userAuthenticatedContext(), &adminv1.CreateUserRequest{Username: "alice", Password: &password})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if manager.created.Username != "alice" || manager.created.Password != "pass" || res.GetUser().GetUserId() != "u1" {
		t.Fatalf("unexpected create call/result: input=%+v res=%#v", manager.created, res)
	}
}

func TestUserServicePasswordAndSessions(t *testing.T) {
	sessionID := uuid.New()
	manager := &fakeUserManager{user: daemonuser.UserSummary{ID: "u1", Username: "alice", State: daemonuser.UserStateActive, CreatedAt: time.Now(), UpdatedAt: time.Now()}, revokedCount: 2, sessions: []domainauth.RefreshSession{{ID: sessionID, Status: domainauth.RefreshSessionStatusActive, CreatedAt: time.Now(), LastUsedAt: time.Now(), AbsoluteExpiresAt: time.Now().Add(time.Hour), Metadata: domainauth.RefreshSessionMetadata{ClientName: "test"}}}}
	svc := NewUserService(manager, fakeAuthorizer{allowed: true})
	_, err := svc.SetUserPassword(userAuthenticatedContext(), &adminv1.SetUserPasswordRequest{UserId: "u1", Password: "new-pass", RevokeSessions: true})
	if err != nil {
		t.Fatalf("SetUserPassword() error = %v", err)
	}
	if manager.passwordUserID != "u1" || manager.password != "new-pass" || manager.revokedAllUserID != "u1" {
		t.Fatalf("unexpected password/session calls: %+v", manager)
	}
	list, err := svc.ListUserSessions(userAuthenticatedContext(), &adminv1.ListUserSessionsRequest{UserId: "u1"})
	if err != nil || len(list.GetSessions()) != 1 || list.GetSessions()[0].GetClient().GetName() != "test" {
		t.Fatalf("unexpected sessions: %#v err=%v", list, err)
	}
	_, err = svc.RevokeUserSession(userAuthenticatedContext(), &adminv1.RevokeUserSessionRequest{UserId: "u1", AuthSessionId: sessionID.String()})
	if err != nil || manager.revokedSessionID != sessionID.String() {
		t.Fatalf("revoke session failed err=%v manager=%+v", err, manager)
	}
	res, err := svc.RevokeUserSessions(userAuthenticatedContext(), &adminv1.RevokeUserSessionsRequest{UserId: "u1"})
	if err != nil || res.GetRevokedCount() != 2 {
		t.Fatalf("revoke sessions failed res=%#v err=%v", res, err)
	}
}

func TestUserServiceRequiresExpectedCapabilities(t *testing.T) {
	ctx := userAuthenticatedContext()
	manager := &fakeUserManager{user: daemonuser.UserSummary{ID: "u1", Username: "alice", State: daemonuser.UserStateActive, CreatedAt: time.Now(), UpdatedAt: time.Now()}}
	authz := &recordingAuthorizer{allowed: true}
	svc := NewUserService(manager, authz)
	password := "pass"
	_, _ = svc.CreateUser(ctx, &adminv1.CreateUserRequest{Username: "alice", Password: &password})
	if authz.lastCapability != commonv1.Capability_CAPABILITY_USER_CREATE.String() {
		t.Fatalf("expected USER_CREATE, got %s", authz.lastCapability)
	}
	_, _ = svc.ListUsers(ctx, &adminv1.ListUsersRequest{})
	if authz.lastCapability != commonv1.Capability_CAPABILITY_USER_MANAGE.String() {
		t.Fatalf("expected USER_MANAGE, got %s", authz.lastCapability)
	}
}

type recordingAuthorizer struct {
	allowed        bool
	lastCapability string
}

func (r *recordingAuthorizer) HasCapability(ctx context.Context, operatorID string, capability string) (bool, error) {
	r.lastCapability = capability
	return r.allowed, nil
}

func userAuthenticatedContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{OperatorID: "op-1", Username: "admin"})
}
