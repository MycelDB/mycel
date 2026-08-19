package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	graph "github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
)

func TestAdminAutomationAuthorizationResolvesDomainSpace(t *testing.T) {
	spaceID := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	domainID := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	authz := &capturingAutomationAuthorizer{}
	svc := NewAdminAutomationService(nil, authz).WithDomainResolver(staticAutomationDomainResolver{
		domain: graph.Domain{ID: graph.DomainID(domainID), SpaceID: spaceID},
	})
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{PrincipalID: "alice"})

	if err := svc.requireAutomationCapability(ctx, graph.DomainID(domainID), capAutomationRead); err != nil {
		t.Fatalf("requireAutomationCapability() error = %v", err)
	}

	want := principalservice.AccessScope{Type: "domain", SpaceID: spaceID.String(), DomainID: domainID.String()}
	if authz.principalID != "alice" || authz.capability != capAutomationRead || authz.scope != want {
		t.Fatalf("Authorize() = principal %q capability %q scope %+v, want principal alice capability %q scope %+v", authz.principalID, authz.capability, authz.scope, capAutomationRead, want)
	}
}

type staticAutomationDomainResolver struct {
	domain graph.Domain
	err    error
}

func (r staticAutomationDomainResolver) GetDomain(context.Context, string) (graph.Domain, error) {
	return r.domain, r.err
}

type capturingAutomationAuthorizer struct {
	principalID string
	capability  string
	scope       principalservice.AccessScope
}

func (a *capturingAutomationAuthorizer) HasCapability(context.Context, string, string) (bool, error) {
	return false, nil
}

func (a *capturingAutomationAuthorizer) Authorize(_ context.Context, principalID string, capability string, scope principalservice.AccessScope) error {
	a.principalID = principalID
	a.capability = capability
	a.scope = scope
	return nil
}
