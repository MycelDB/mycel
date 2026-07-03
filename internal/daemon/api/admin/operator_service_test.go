package admin

import (
	"context"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAdminLister struct {
	admins []daemonadmin.AdminSummary
	err    error
}

func (f fakeAdminLister) ListAdmins(context.Context) ([]daemonadmin.AdminSummary, error) {
	return f.admins, f.err
}

type fakePasswordManager struct {
	admin      daemonadmin.AdminSummary
	operatorID string
	password   string
	err        error
}

func (f *fakePasswordManager) SetOperatorPassword(ctx context.Context, operatorID string, password string) (daemonadmin.AdminSummary, error) {
	f.operatorID = operatorID
	f.password = password
	if f.err != nil {
		return daemonadmin.AdminSummary{}, f.err
	}
	return f.admin, nil
}

func TestListOperatorsMapsAdminSummaries(t *testing.T) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	svc := NewOperatorService(fakeAdminLister{admins: []daemonadmin.AdminSummary{{ID: "admin-1", Username: "admin", CreatedAt: createdAt}}}, nil)

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
	svc := NewOperatorService(fakeAdminLister{}, nil)
	_, err := svc.ListOperators(context.Background(), &adminv1.ListOperatorsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
}

func TestListOperatorsPaginates(t *testing.T) {
	svc := NewOperatorService(fakeAdminLister{admins: []daemonadmin.AdminSummary{
		{ID: "1", Username: "a", CreatedAt: time.Now()},
		{ID: "2", Username: "b", CreatedAt: time.Now()},
		{ID: "3", Username: "c", CreatedAt: time.Now()},
	}}, nil)
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
	svc := NewOperatorService(fakeAdminLister{}, nil)
	_, err := svc.ListOperators(authenticatedContext(), &adminv1.ListOperatorsRequest{PageToken: "bad"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestSetOperatorPasswordChangesOwnPassword(t *testing.T) {
	createdAt := time.Now().UTC()
	manager := &fakePasswordManager{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: createdAt}}
	svc := NewOperatorService(nil, manager)
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
	manager := &fakePasswordManager{admin: daemonadmin.AdminSummary{ID: "op-1", Username: "admin", CreatedAt: time.Now()}}
	svc := NewOperatorService(nil, manager)
	_, err := svc.SetOperatorPassword(authenticatedContext(), &adminv1.SetOperatorPasswordRequest{Password: "new-pass"})
	if err != nil {
		t.Fatalf("SetOperatorPassword() error = %v", err)
	}
	if manager.operatorID != "op-1" {
		t.Fatalf("expected principal operator id, got %q", manager.operatorID)
	}
}

func TestSetOperatorPasswordRejectsUnauthenticatedOtherOrEmpty(t *testing.T) {
	svc := NewOperatorService(nil, &fakePasswordManager{})
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

func authenticatedContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{OperatorID: "op-1", Username: "admin"})
}
