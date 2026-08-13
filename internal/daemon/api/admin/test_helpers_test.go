package admin

import (
	"context"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	identity "github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
)

type fakeAuthorizer struct{ allowed bool }

func (f fakeAuthorizer) HasCapability(context.Context, string, string) (bool, error) {
	return f.allowed, nil
}

type fakeOperatorManager struct{ systemAdmin bool }

func (f *fakeOperatorManager) HasCapability(context.Context, string, string) (bool, error) {
	return f.systemAdmin, nil
}

func authenticatedContext() context.Context {
	return daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: "admin-1", Username: "admin", CreatedAt: time.Now().UTC()})
}

type fakePrincipalAuthManager struct {
	principal        principalservice.PrincipalSummary
	err              error
	refreshToken     domainauth.RefreshToken
	revokedPrincipal string
	revokedSession   string
}

func (f *fakePrincipalAuthManager) ListPrincipals(context.Context) ([]principalservice.PrincipalSummary, error) {
	return []principalservice.PrincipalSummary{f.principal}, f.err
}
func (f *fakePrincipalAuthManager) GetPrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) FindPrincipal(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) CreatePrincipal(context.Context, principalservice.CreatePrincipalInput) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) UpdatePrincipal(context.Context, principalservice.UpdatePrincipalInput) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) DisablePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) EnablePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) DeletePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) SetPrincipalPassword(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) AuthenticatePrincipal(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) CreateAuthSession(context.Context, principalservice.PrincipalSummary, domainauth.RefreshSessionMetadata, int, time.Duration, time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return "", domainauth.RefreshSession{}, f.err
	}
	f.refreshToken = "refresh-1"
	return f.refreshToken, domainauth.RefreshSession{ID: uuid.New(), PrincipalID: identity.PrincipalID(f.principal.ID), CreatedAt: time.Now().UTC()}, nil
}
func (f *fakePrincipalAuthManager) RefreshAuthSession(context.Context, domainauth.RefreshToken, domainauth.RefreshSessionMetadata, int, time.Duration) (principalservice.PrincipalSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if f.err != nil {
		return principalservice.PrincipalSummary{}, "", domainauth.RefreshSession{}, f.err
	}
	f.refreshToken = "refresh-2"
	return f.principal, f.refreshToken, domainauth.RefreshSession{ID: uuid.New(), PrincipalID: identity.PrincipalID(f.principal.ID), CreatedAt: time.Now().UTC()}, nil
}
func (f *fakePrincipalAuthManager) ListPrincipalSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	return nil, f.err
}
func (f *fakePrincipalAuthManager) RevokePrincipalSession(_ context.Context, principalID string, sessionID string) error {
	f.revokedPrincipal = principalID
	f.revokedSession = sessionID
	return f.err
}
func (f *fakePrincipalAuthManager) RevokePrincipalSessions(context.Context, string) (int, error) {
	return 0, f.err
}
func (f *fakePrincipalAuthManager) ListRoleBindings(context.Context, string) ([]principalservice.RoleBinding, error) {
	return nil, f.err
}
func (f *fakePrincipalAuthManager) GrantRole(context.Context, string, string, principalservice.AccessScope, string, string) (principalservice.RoleBinding, principalservice.PrincipalSummary, error) {
	return principalservice.RoleBinding{}, f.principal, f.err
}
func (f *fakePrincipalAuthManager) RevokeRole(context.Context, string, string, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) ListCapabilityGrants(context.Context, string) ([]principalservice.CapabilityGrant, error) {
	return nil, f.err
}
func (f *fakePrincipalAuthManager) GrantCapability(context.Context, string, string, principalservice.AccessScope, string, string) (principalservice.CapabilityGrant, principalservice.PrincipalSummary, error) {
	return principalservice.CapabilityGrant{}, f.principal, f.err
}
func (f *fakePrincipalAuthManager) RevokeCapability(context.Context, string, string, string) (principalservice.PrincipalSummary, error) {
	return f.principal, f.err
}
func (f *fakePrincipalAuthManager) Authorize(context.Context, string, string, principalservice.AccessScope) error {
	return f.err
}
func (f *fakePrincipalAuthManager) HasCapability(context.Context, string, string) (bool, error) {
	return f.err == nil, f.err
}
func (f *fakePrincipalAuthManager) EffectiveAccess(context.Context, string, principalservice.AccessScope) (principalservice.EffectiveAccess, error) {
	return principalservice.EffectiveAccess{}, f.err
}
