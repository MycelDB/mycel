package access

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

func TestDefaultManager_PermissionHierarchy(t *testing.T) {
	m := newTestManager(t)
	spaceID := model.SpaceID(uuid.New())
	userID := model.UserID(uuid.New())

	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: userID, Permissions: []model.SpacePermission{model.SpacePermissionWrite}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}
	canRead, err := m.Can(context.Background(), userID, spaceID, model.SpacePermissionRead)
	if err != nil || !canRead {
		t.Fatalf("expected write to imply read, can=%v err=%v", canRead, err)
	}
	canAdmin, err := m.Can(context.Background(), userID, spaceID, model.SpacePermissionAdmin)
	if err != nil || canAdmin {
		t.Fatalf("expected write not to imply admin, can=%v err=%v", canAdmin, err)
	}

	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: userID, Permissions: []model.SpacePermission{model.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("admin grant failed: %v", err)
	}
	canWrite, err := m.Can(context.Background(), userID, spaceID, model.SpacePermissionWrite)
	if err != nil || !canWrite {
		t.Fatalf("expected admin to imply write, can=%v err=%v", canWrite, err)
	}
}

func TestDefaultManager_RevokeLastAdminFails(t *testing.T) {
	m := newTestManager(t)
	spaceID := model.SpaceID(uuid.New())
	adminID := model.UserID(uuid.New())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: adminID, Permissions: []model.SpacePermission{model.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}

	err := m.Revoke(context.Background(), RevokeInput{SpaceID: spaceID, UserID: adminID})
	if !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin, got: %v", err)
	}
}

func TestDefaultManager_RevokeAdminSucceedsWhenAnotherAdminRemains(t *testing.T) {
	m := newTestManager(t)
	spaceID := model.SpaceID(uuid.New())
	adminA := model.UserID(uuid.New())
	adminB := model.UserID(uuid.New())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: adminA, Permissions: []model.SpacePermission{model.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant adminA failed: %v", err)
	}
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: adminB, Permissions: []model.SpacePermission{model.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant adminB failed: %v", err)
	}

	if err := m.Revoke(context.Background(), RevokeInput{SpaceID: spaceID, UserID: adminA}); err != nil {
		t.Fatalf("expected revoke success, got: %v", err)
	}
	canAdmin, err := m.Can(context.Background(), adminB, spaceID, model.SpacePermissionAdmin)
	if err != nil || !canAdmin {
		t.Fatalf("expected adminB to remain admin, can=%v err=%v", canAdmin, err)
	}
}

func TestDefaultManager_DowngradeLastAdminFails(t *testing.T) {
	m := newTestManager(t)
	spaceID := model.SpaceID(uuid.New())
	adminID := model.UserID(uuid.New())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: adminID, Permissions: []model.SpacePermission{model.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}

	_, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, UserID: adminID, Permissions: []model.SpacePermission{model.SpacePermissionRead}})
	if !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin, got: %v", err)
	}
}

func newTestManager(t *testing.T) Manager {
	t.Helper()
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	return m
}
