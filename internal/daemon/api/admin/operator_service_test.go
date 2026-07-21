package admin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	daemonadmin "github.com/myceldb/mycel/internal/identity/service/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeOperatorManager struct {
	admins      []daemonadmin.AdminSummary
	admin       daemonadmin.AdminSummary
	systemAdmin bool
	operatorID  string
	password    string
	sessions    []domainauth.RefreshSession
	revoked     int
	err         error
}

func (f *fakeOperatorManager) ListAdmins(context.Context) ([]daemonadmin.AdminSummary, error) {
	return f.admins, f.err
}
func (f *fakeOperatorManager) AuthenticateOperator(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) SetOperatorPassword(ctx context.Context, operatorID string, password string) (daemonadmin.AdminSummary, error) {
	f.operatorID = operatorID
	f.password = password
	return f.admin, f.err
}
func (f *fakeOperatorManager) GetOperator(context.Context, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) FindOperator(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) CreateOperator(context.Context, daemonadmin.CreateOperatorInput) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) UpdateOperator(context.Context, daemonadmin.UpdateOperatorInput) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) DisableOperator(context.Context, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) EnableOperator(context.Context, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) DeleteOperator(context.Context, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) GrantRole(context.Context, string, string, daemonadmin.AccessScope, string, string) (daemonadmin.RoleGrant, daemonadmin.AdminSummary, error) {
	return daemonadmin.RoleGrant{ID: "grant-1", Role: daemonadmin.OperatorRoleSystemAdmin}, f.admin, f.err
}
func (f *fakeOperatorManager) RevokeRole(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) GrantCapability(context.Context, string, string, daemonadmin.AccessScope, string, string) (daemonadmin.CapabilityGrant, daemonadmin.AdminSummary, error) {
	return daemonadmin.CapabilityGrant{ID: "grant-1", Capability: "CAPABILITY_OPERATOR_MANAGE"}, f.admin, f.err
}
func (f *fakeOperatorManager) RevokeCapability(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	return f.admin, f.err
}
func (f *fakeOperatorManager) IsSystemAdmin(context.Context, string) (bool, error) {
	return f.systemAdmin, f.err
}
func (f *fakeOperatorManager) HasCapability(context.Context, string, string) (bool, error) {
	return f.systemAdmin, f.err
}
func (f *fakeOperatorManager) CreateOperatorAuthSession(context.Context, daemonadmin.AdminSummary, domainauth.RefreshSessionMetadata, int, time.Duration, time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return "", domainauth.RefreshSession{}, f.err
	}
	return "refresh", domainauth.RefreshSession{ID: uuid.New(), CreatedAt: time.Now().UTC()}, nil
}

func (f *fakeOperatorManager) RefreshOperatorAuthSession(context.Context, domainauth.RefreshToken, domainauth.RefreshSessionMetadata, int, time.Duration) (daemonadmin.AdminSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return daemonadmin.AdminSummary{}, "", domainauth.RefreshSession{}, f.err
	}
	return f.admin, "refresh-2", domainauth.RefreshSession{ID: uuid.New(), CreatedAt: time.Now().UTC()}, nil
}

func (f *fakeOperatorManager) ListOperatorSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	return f.sessions, f.err
}

func (f *fakeOperatorManager) RevokeOperatorSession(context.Context, string, string) error {
	f.revoked++
	return f.err
}

func (f *fakeOperatorManager) RevokeOperatorSessions(context.Context, string) (int, error) {
	f.revoked++
	return 1, f.err
}

func TestListOperatorsMapsAdminSummaries(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	svc := NewOperatorService(&fakeOperatorManager{admins: []daemonadmin.AdminSummary{{ID: "admin-1", Username: "admin", State: daemonadmin.AdminStateActive, CreatedAt: createdAt, UpdatedAt: createdAt}}})
	res, err := svc.ListOperators(authenticatedContext(), &adminv1.ListOperatorsRequest{})
	if err != nil {
		t.Fatalf("ListOperators() error = %v", err)
	}
	if len(res.GetOperators()) != 1 {
		t.Fatalf("expected 1 operator, got %d", len(res.GetOperators()))
	}
	op := res.GetOperators()[0]
	if op.GetOperatorId() != "admin-1" || op.GetUsername() != "admin" {
		t.Fatalf("unexpected operator mapping: %#v", op)
	}
	if op.GetState() != adminv1.OperatorState_OPERATOR_STATE_ACTIVE {
		t.Fatalf("expected active state, got %s", op.GetState())
	}
	if got := op.GetCreateTime().AsTime(); !got.Equal(createdAt) {
		t.Fatalf("expected create time %s, got %s", createdAt, got)
	}
	if strings.Contains(op.String(), "password") || strings.Contains(op.String(), "hash") {
		t.Fatalf("operator response leaked password/hash material: %s", op.String())
	}
}

func TestListOperatorsRequiresAuthentication(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{})
	_, err := svc.ListOperators(context.Background(), &adminv1.ListOperatorsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestListOperatorsPaginates(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{admins: []daemonadmin.AdminSummary{{ID: "1", Username: "a", State: daemonadmin.AdminStateActive, CreatedAt: time.Now()}, {ID: "2", Username: "b", State: daemonadmin.AdminStateActive, CreatedAt: time.Now()}, {ID: "3", Username: "c", State: daemonadmin.AdminStateActive, CreatedAt: time.Now()}}})
	first, err := svc.ListOperators(authenticatedContext(), &adminv1.ListOperatorsRequest{PageSize: 2})
	if err != nil {
		t.Fatalf("first ListOperators() error = %v", err)
	}
	if len(first.GetOperators()) != 2 || first.GetNextPageToken() != "2" {
		t.Fatalf("unexpected first page: %#v", first)
	}
	second, err := svc.ListOperators(authenticatedContext(), &adminv1.ListOperatorsRequest{PageSize: 2, PageToken: first.GetNextPageToken()})
	if err != nil {
		t.Fatalf("second ListOperators() error = %v", err)
	}
	if len(second.GetOperators()) != 1 || second.GetNextPageToken() != "" || second.GetOperators()[0].GetUsername() != "c" {
		t.Fatalf("unexpected second page: %#v", second)
	}
}

func TestListOperatorsRejectsInvalidPageToken(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{})
	_, err := svc.ListOperators(authenticatedContext(), &adminv1.ListOperatorsRequest{PageToken: "bad"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateOperatorRequiresSystemAdmin(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{systemAdmin: false})
	password := "pass"
	_, err := svc.CreateOperator(authenticatedContext(), &adminv1.CreateOperatorRequest{Username: "bob", Password: &password})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestSetOperatorPasswordChangesOwnPassword(t *testing.T) {
	createdAt := time.Now().UTC()
	manager := &fakeOperatorManager{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: createdAt}}
	svc := NewOperatorService(manager)
	res, err := svc.SetOperatorPassword(authenticatedContext(), &adminv1.SetOperatorPasswordRequest{OperatorId: "op-1", Password: "new-pass"})
	if err != nil {
		t.Fatalf("SetOperatorPassword() error = %v", err)
	}
	if manager.operatorID != "op-1" || manager.password != "new-pass" {
		t.Fatalf("password manager called with operatorID=%q password=%q", manager.operatorID, manager.password)
	}
	if res.GetOperator().GetUsername() != "admin" {
		t.Fatalf("unexpected operator response: %#v", res.GetOperator())
	}
}

func TestSetOperatorPasswordUsesPrincipalWhenOperatorIDMissing(t *testing.T) {
	manager := &fakeOperatorManager{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: time.Now()}}
	svc := NewOperatorService(manager)
	_, err := svc.SetOperatorPassword(authenticatedContext(), &adminv1.SetOperatorPasswordRequest{Password: "new-pass"})
	if err != nil {
		t.Fatalf("SetOperatorPassword() error = %v", err)
	}
	if manager.operatorID != "op-1" {
		t.Fatalf("expected principal operator id, got %q", manager.operatorID)
	}
}

func TestSetOperatorPasswordRejectsUnauthenticatedOtherOrEmpty(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{})
	if _, err := svc.SetOperatorPassword(context.Background(), &adminv1.SetOperatorPasswordRequest{OperatorId: "op-1", Password: "new-pass"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if _, err := svc.SetOperatorPassword(authenticatedContext(), &adminv1.SetOperatorPasswordRequest{OperatorId: "op-2", Password: "new-pass"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
	if _, err := svc.SetOperatorPassword(authenticatedContext(), &adminv1.SetOperatorPasswordRequest{OperatorId: "op-1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestGrantRoleRequiresSystemAdmin(t *testing.T) {
	svc := NewOperatorService(&fakeOperatorManager{systemAdmin: true, admin: daemonadmin.AdminSummary{ID: "op-2", Username: "bob"}})
	res, err := svc.GrantOperatorRole(authenticatedContext(), &adminv1.GrantOperatorRoleRequest{OperatorId: "op-2", Role: adminv1.OperatorRole_OPERATOR_ROLE_USER_ADMIN})
	if err != nil {
		t.Fatalf("GrantOperatorRole() error = %v", err)
	}
	if res.GetGrant().GetRole() != adminv1.OperatorRole_OPERATOR_ROLE_SYSTEM_ADMIN {
		t.Fatalf("unexpected grant: %#v", res.GetGrant())
	}
}

func authenticatedContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{OperatorID: "op-1", Username: "admin"})
}
