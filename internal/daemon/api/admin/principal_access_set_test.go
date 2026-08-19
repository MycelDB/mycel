package admin

import (
	"context"
	"path/filepath"
	"testing"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestPrincipalServiceSetAccessForExactScope(t *testing.T) {
	ctx := context.Background()
	manager := principalservice.NewModule()
	host := runtimetest.New(runtimetest.Config{DataDir: filepath.Join(t.TempDir(), "data"), Mode: "standalone", BootstrapAdminUsername: "admin", BootstrapAdminPassword: "admin-pass"}, nil)
	if res := manager.Init(ctx, host); !res.OK {
		t.Fatalf("principal Init() failed: %v", res.Error)
	}
	admin, err := manager.FindPrincipal(ctx, "admin", "")
	if err != nil {
		t.Fatalf("FindPrincipal(admin) error = %v", err)
	}
	alice, err := manager.CreatePrincipal(ctx, principalservice.CreatePrincipalInput{Username: "alice", Kind: principalservice.PrincipalKindHuman, CreatedBy: admin.ID})
	if err != nil {
		t.Fatalf("CreatePrincipal(alice) error = %v", err)
	}
	service := NewPrincipalService(manager, nil)
	authCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{PrincipalID: admin.ID, Username: admin.Username})
	scope := &commonv1.AccessScope{Type: commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SPACE, SpaceId: strPtr("space-1")}

	roles, err := service.SetPrincipalRolesForScope(authCtx, &adminv1.SetPrincipalRolesForScopeRequest{PrincipalId: alice.ID, Scope: scope, Roles: []string{"space.viewer", "space.editor"}, Reason: "test"})
	if err != nil {
		t.Fatalf("SetPrincipalRolesForScope() error = %v", err)
	}
	if len(roles.GetGrants()) != 2 || !hasRole(roles.GetEffectiveRoles(), "space.viewer") || !hasRole(roles.GetEffectiveRoles(), "space.editor") {
		t.Fatalf("unexpected role set response: %#v", roles)
	}
	roles, err = service.SetPrincipalRolesForScope(authCtx, &adminv1.SetPrincipalRolesForScopeRequest{PrincipalId: alice.ID, Scope: scope, Roles: []string{"space.viewer"}, Reason: "remove editor"})
	if err != nil {
		t.Fatalf("SetPrincipalRolesForScope(remove) error = %v", err)
	}
	if len(roles.GetGrants()) != 1 || roles.GetGrants()[0].GetRole() != "space.viewer" {
		t.Fatalf("expected exactly space.viewer direct grant, got %#v", roles.GetGrants())
	}

	caps, err := service.SetPrincipalCapabilitiesForScope(authCtx, &adminv1.SetPrincipalCapabilitiesForScopeRequest{PrincipalId: alice.ID, Scope: scope, Capabilities: []commonv1.Capability{commonv1.Capability_CAPABILITY_AUTOMATION_MANAGE, commonv1.Capability_CAPABILITY_INFERENCE_PROFILE_READ}, Reason: "test"})
	if err != nil {
		t.Fatalf("SetPrincipalCapabilitiesForScope() error = %v", err)
	}
	if len(caps.GetGrants()) != 2 || !hasCapability(caps.GetEffectiveCapabilities(), commonv1.Capability_CAPABILITY_AUTOMATION_MANAGE) || !hasCapability(caps.GetEffectiveCapabilities(), commonv1.Capability_CAPABILITY_GRAPH_READ) {
		t.Fatalf("expected direct and role-inherited capabilities, got %#v", caps)
	}
	caps, err = service.SetPrincipalCapabilitiesForScope(authCtx, &adminv1.SetPrincipalCapabilitiesForScopeRequest{PrincipalId: alice.ID, Scope: scope, Capabilities: []commonv1.Capability{commonv1.Capability_CAPABILITY_AUTOMATION_MANAGE}, Reason: "remove profile"})
	if err != nil {
		t.Fatalf("SetPrincipalCapabilitiesForScope(remove) error = %v", err)
	}
	if len(caps.GetGrants()) != 1 || caps.GetGrants()[0].GetCapability() != commonv1.Capability_CAPABILITY_AUTOMATION_MANAGE {
		t.Fatalf("expected one direct capability grant, got %#v", caps.GetGrants())
	}
}

func strPtr(value string) *string { return &value }

func hasRole(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasCapability(values []commonv1.Capability, want commonv1.Capability) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
