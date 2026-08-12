package acl

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/identity/model"
	domainaccess "github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestDefaultManager_SystemRolePermissions(t *testing.T) {
	m := newTestManager(t)
	principalID := identity.PrincipalID(uuid.NewString())

	if _, err := m.GrantSystemRole(context.Background(), GrantSystemRoleInput{PrincipalID: principalID, Roles: []domainaccess.SystemRole{domainaccess.SystemRoleUserAdmin}}); err != nil {
		t.Fatalf("grant system role failed: %v", err)
	}
	canManageUsers, err := m.CanSystem(context.Background(), principalID, domainaccess.SystemPermissionManageUsers)
	if err != nil || !canManageUsers {
		t.Fatalf("expected user_admin to manage users, can=%v err=%v", canManageUsers, err)
	}
	canCreateSpaces, err := m.CanSystem(context.Background(), principalID, domainaccess.SystemPermissionCreateSpaces)
	if err != nil || canCreateSpaces {
		t.Fatalf("expected user_admin not to create spaces, can=%v err=%v", canCreateSpaces, err)
	}

	if _, err := m.GrantSystemRole(context.Background(), GrantSystemRoleInput{PrincipalID: principalID, Roles: []domainaccess.SystemRole{domainaccess.SystemRoleSuperuser}}); err != nil {
		t.Fatalf("grant superuser failed: %v", err)
	}
	canCreateSpaces, err = m.CanSystem(context.Background(), principalID, domainaccess.SystemPermissionCreateSpaces)
	if err != nil || !canCreateSpaces {
		t.Fatalf("expected superuser to create spaces, can=%v err=%v", canCreateSpaces, err)
	}
}

func TestDefaultManager_RevokeLastSuperuserFails(t *testing.T) {
	m := newTestManager(t)
	principalID := identity.PrincipalID(uuid.NewString())
	if _, err := m.GrantSystemRole(context.Background(), GrantSystemRoleInput{PrincipalID: principalID, Roles: []domainaccess.SystemRole{domainaccess.SystemRoleSuperuser}}); err != nil {
		t.Fatalf("grant superuser failed: %v", err)
	}

	err := m.RevokeSystemRole(context.Background(), RevokeSystemRoleInput{PrincipalID: principalID})
	if !errors.Is(err, ErrLastSuperuser) {
		t.Fatalf("expected ErrLastSuperuser, got: %v", err)
	}
}

func TestDefaultManager_PermissionHierarchy(t *testing.T) {
	m := newTestManager(t)
	spaceID := domainspace.SpaceID(uuid.New())
	principalID := identity.PrincipalID(uuid.NewString())

	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: principalID, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionWrite}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}
	canRead, err := m.Can(context.Background(), principalID, spaceID, domainaccess.SpacePermissionRead)
	if err != nil || !canRead {
		t.Fatalf("expected write to imply read, can=%v err=%v", canRead, err)
	}
	canAdmin, err := m.Can(context.Background(), principalID, spaceID, domainaccess.SpacePermissionAdmin)
	if err != nil || canAdmin {
		t.Fatalf("expected write not to imply admin, can=%v err=%v", canAdmin, err)
	}

	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: principalID, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("admin grant failed: %v", err)
	}
	canWrite, err := m.Can(context.Background(), principalID, spaceID, domainaccess.SpacePermissionWrite)
	if err != nil || !canWrite {
		t.Fatalf("expected admin to imply write, can=%v err=%v", canWrite, err)
	}
}

func TestDefaultManager_RevokeLastAdminFails(t *testing.T) {
	m := newTestManager(t)
	spaceID := domainspace.SpaceID(uuid.New())
	adminID := identity.PrincipalID(uuid.NewString())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: adminID, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}

	err := m.Revoke(context.Background(), RevokeInput{SpaceID: spaceID, PrincipalID: adminID})
	if !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin, got: %v", err)
	}
}

func TestDefaultManager_RevokeAdminSucceedsWhenAnotherAdminRemains(t *testing.T) {
	m := newTestManager(t)
	spaceID := domainspace.SpaceID(uuid.New())
	adminA := identity.PrincipalID(uuid.NewString())
	adminB := identity.PrincipalID(uuid.NewString())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: adminA, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant adminA failed: %v", err)
	}
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: adminB, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant adminB failed: %v", err)
	}

	if err := m.Revoke(context.Background(), RevokeInput{SpaceID: spaceID, PrincipalID: adminA}); err != nil {
		t.Fatalf("expected revoke success, got: %v", err)
	}
	canAdmin, err := m.Can(context.Background(), adminB, spaceID, domainaccess.SpacePermissionAdmin)
	if err != nil || !canAdmin {
		t.Fatalf("expected adminB to remain admin, can=%v err=%v", canAdmin, err)
	}
}

func TestDefaultManager_DowngradeLastAdminFails(t *testing.T) {
	m := newTestManager(t)
	spaceID := domainspace.SpaceID(uuid.New())
	adminID := identity.PrincipalID(uuid.NewString())
	if _, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: adminID, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant failed: %v", err)
	}

	_, err := m.Grant(context.Background(), GrantInput{SpaceID: spaceID, PrincipalID: adminID, Permissions: []domainaccess.SpacePermission{domainaccess.SpacePermissionRead}})
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
