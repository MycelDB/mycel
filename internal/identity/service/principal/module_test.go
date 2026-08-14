package principal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func newTestModule(t *testing.T) (*Module, context.Context) {
	t.Helper()
	ctx := context.Background()
	m := NewModule()
	host := runtimetest.New(runtimetest.Config{DataDir: filepath.Join(t.TempDir(), "data"), Mode: "standalone", BootstrapAdminUsername: "admin", BootstrapAdminPassword: "admin-pass"}, nil)
	if res := m.Init(ctx, host); !res.OK {
		t.Fatalf("Init() failed: %v", res.Error)
	}
	return m, ctx
}

func TestStandaloneBootstrapCreatesSystemAdminPrincipal(t *testing.T) {
	m, ctx := newTestModule(t)
	principals, err := m.ListPrincipals(ctx)
	if err != nil {
		t.Fatalf("ListPrincipals() error = %v", err)
	}
	if len(principals) != 2 {
		t.Fatalf("expected admin and automation bootstrap principals, got %#v", principals)
	}
	admin, err := m.FindPrincipal(ctx, "admin", "")
	if err != nil {
		t.Fatalf("FindPrincipal(admin) error = %v", err)
	}
	automationPrincipal, err := m.GetPrincipal(ctx, ServicePrincipalAutomation)
	if err != nil {
		t.Fatalf("GetPrincipal(automation) error = %v", err)
	}
	if automationPrincipal.Kind != PrincipalKindService || automationPrincipal.State != PrincipalStateActive || automationPrincipal.LoginEnabled {
		t.Fatalf("unexpected automation service principal: %#v", automationPrincipal)
	}
	ok, err := m.HasCapability(ctx, automationPrincipal.ID, "automation.worker")
	if err != nil || !ok {
		t.Fatalf("automation service principal should have worker capability, ok=%v err=%v", ok, err)
	}
	if admin.Username != "admin" || admin.Kind != PrincipalKindHuman || admin.State != PrincipalStateActive || !admin.LoginEnabled {
		t.Fatalf("unexpected bootstrap principal: %#v", admin)
	}
	ok, err = m.HasCapability(ctx, admin.ID, "identity.principal.update")
	if err != nil || !ok {
		t.Fatalf("bootstrap principal should have system-admin capabilities, ok=%v err=%v", ok, err)
	}
	if _, err := m.AuthenticatePrincipal(ctx, "admin", "admin-pass"); err != nil {
		t.Fatalf("AuthenticatePrincipal(bootstrap) error = %v", err)
	}
}

func TestPrincipalCRUDGrantsSessionsAndLastAdminInvariant(t *testing.T) {
	m, ctx := newTestModule(t)
	admin, err := m.FindPrincipal(ctx, "admin", "")
	if err != nil {
		t.Fatalf("FindPrincipal(admin) error = %v", err)
	}
	alice, err := m.CreatePrincipal(ctx, CreatePrincipalInput{Username: "alice", Email: "alice@example.com", DisplayName: "Alice", Kind: PrincipalKindHuman, Password: "alice-pass", LoginEnabled: true, CreatedBy: admin.ID})
	if err != nil {
		t.Fatalf("CreatePrincipal(alice) error = %v", err)
	}
	if _, err := m.CreatePrincipal(ctx, CreatePrincipalInput{Username: "ALICE", Kind: PrincipalKindHuman}); !errors.Is(err, ErrDuplicatePrincipal) {
		t.Fatalf("expected duplicate username error, got %v", err)
	}
	role, _, err := m.GrantRole(ctx, alice.ID, RoleSpaceViewer, AccessScope{Type: "space", SpaceID: "space-1"}, "test", admin.ID)
	if err != nil {
		t.Fatalf("GrantRole() error = %v", err)
	}
	capGrant, _, err := m.GrantCapability(ctx, alice.ID, "CAPABILITY_QUERY_RUN", AccessScope{Type: "space", SpaceID: "space-1"}, "test", admin.ID)
	if err != nil {
		t.Fatalf("GrantCapability() error = %v", err)
	}
	access, err := m.EffectiveAccess(ctx, alice.ID, AccessScope{Type: "space", SpaceID: "space-1"})
	if err != nil {
		t.Fatalf("EffectiveAccess() error = %v", err)
	}
	if !containsString(access.Roles, RoleSpaceViewer) || !containsString(access.Capabilities, "query.run") {
		t.Fatalf("expected role/capability in effective access, got %#v", access)
	}
	if err := m.Authorize(ctx, alice.ID, "query.run", AccessScope{Type: "domain", DomainID: "domain-without-space"}); err == nil {
		t.Fatalf("space-scoped grant authorized domain request without matching space")
	}
	if _, err := m.RevokeCapability(ctx, alice.ID, capGrant.ID, admin.ID); err != nil {
		t.Fatalf("RevokeCapability() error = %v", err)
	}
	if _, err := m.RevokeRole(ctx, alice.ID, role.ID, admin.ID); err != nil {
		t.Fatalf("RevokeRole() error = %v", err)
	}
	refresh, rec, err := m.CreateAuthSession(ctx, alice, domainauth.RefreshSessionMetadata{}, 32, time.Hour, 24*time.Hour)
	if err != nil || refresh == "" || rec.ID.String() == "" {
		t.Fatalf("CreateAuthSession() refresh=%q rec=%#v err=%v", refresh, rec, err)
	}
	sessions, err := m.ListPrincipalSessions(ctx, alice.ID)
	if err != nil || len(sessions) != 1 {
		t.Fatalf("ListPrincipalSessions() sessions=%#v err=%v", sessions, err)
	}
	if err := m.RevokePrincipalSession(ctx, alice.ID, rec.ID.String()); err != nil {
		t.Fatalf("RevokePrincipalSession() error = %v", err)
	}
	if _, err := m.DisablePrincipal(ctx, admin.ID); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("expected last system admin disable to fail, got %v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
