package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type scopedSemanticAuthorizer struct {
	capability string
	scope      principalservice.AccessScope
}

func (a *scopedSemanticAuthorizer) HasCapability(context.Context, string, string) (bool, error) {
	return false, nil
}

func (a *scopedSemanticAuthorizer) Authorize(_ context.Context, _ string, capability string, scope principalservice.AccessScope) error {
	a.capability = capability
	a.scope = scope
	return nil
}

func TestAdminSemanticManageAuthorizationUsesRequestedScope(t *testing.T) {
	authz := &scopedSemanticAuthorizer{}
	service := NewAdminSemanticService(nil, nil, authz)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{PrincipalID: "alice"})

	spaceID := domainspace.SpaceID(uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	if _, err := service.requireSemanticManage(ctx, semanticScope(spaceID, graph.DomainID(uuid.Nil))); err != nil {
		t.Fatalf("requireSemanticManage() error = %v", err)
	}
	if authz.capability != commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String() {
		t.Fatalf("capability = %q, want %q", authz.capability, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String())
	}
	if authz.scope.Type != "space" || authz.scope.SpaceID != spaceID.String() {
		t.Fatalf("scope = %+v, want space %s", authz.scope, spaceID)
	}
}

func TestAdminSemanticMaintenanceAuthorizationUsesRequestedScope(t *testing.T) {
	authz := &scopedSemanticAuthorizer{}
	service := NewAdminSemanticMaintenanceService(nil, authz)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{PrincipalID: "alice"})

	spaceID := domainspace.SpaceID(uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	if err := service.requireMaintenance(ctx, semanticScope(spaceID, graph.DomainID(uuid.Nil))); err != nil {
		t.Fatalf("requireMaintenance() error = %v", err)
	}
	if authz.capability != commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String() {
		t.Fatalf("capability = %q, want %q", authz.capability, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String())
	}
	if authz.scope.Type != "space" || authz.scope.SpaceID != spaceID.String() {
		t.Fatalf("scope = %+v, want space %s", authz.scope, spaceID)
	}
}
