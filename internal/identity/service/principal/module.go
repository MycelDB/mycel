package principal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Module struct {
	mu                  sync.Mutex
	store               Store
	sessions            storesession.Manager
	dataDir             string
	gate                *quiesce.Gate
	wal                 *wal.Manager
	walProgress         wal.AppliedLSNStore
	walWaiter           *wal.ApplyWaiter
	writeAllowed        func() error
	raftGroups          *consensus.MultiGroup
	raftAppliedCommands map[string]struct{}
}

func NewModule() *Module { return &Module{gate: quiesce.NewGate(ModuleName)} }

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	identityDir := filepath.Join(host.DataDir(), "identity")
	if err := ensureDir(identityDir, 0o700); err != nil {
		return runtime.Abort(ModuleName, "filesystem", "failed to create identity directory", err)
	}
	store, storeCreated, err := OpenStore(identityDir)
	if err != nil {
		return runtime.Abort(ModuleName, "store", "failed to open identity store", err)
	}
	m.store = store
	m.dataDir = host.DataDir()
	sessionsDir := filepath.Join(identityDir, "sessions")
	if err := ensureDir(sessionsDir, 0o700); err != nil {
		return runtime.Abort(ModuleName, "filesystem", "failed to create identity sessions directory", err)
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(ctx, sessionsDir); err != nil {
		return runtime.Abort(ModuleName, "store", "failed to open identity session store", err)
	}
	m.sessions = sessions
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypePrincipalPut, wal.ApplierFunc(m.applyPrincipalPut)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register principal put WAL applier", err)
			}
			if err := registry.Register(recordTypeRoleBindingPut, wal.ApplierFunc(m.applyRoleBindingPut)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register role binding WAL applier", err)
			}
			if err := registry.Register(recordTypeCapabilityGrantPut, wal.ApplierFunc(m.applyCapabilityGrantPut)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register capability grant WAL applier", err)
			}
		}
	}
	m.writeAllowed = func() error { return nil }
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.loadRaftAppliedCommands()
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if registrar, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register identity quiesce participant", err)
		}
	}
	if host.Log() != nil {
		host.Log().Info("identity store ready", "path", filepath.Join(identityDir, StoreFilename), "created", storeCreated)
	}
	if hostMode(host) == "standalone" {
		if err := m.ensureStandaloneSystemAdmin(ctx, host.Log(), hostBootstrapAdminUsername(host), hostBootstrapAdminPassword(host)); err != nil {
			return runtime.Abort(ModuleName, "store", "failed to ensure standalone system admin principal", err)
		}
	}
	return runtime.OK(ModuleName)
}

func ensureDir(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (m *Module) SetWriteAllowed(fn func() error) { m.writeAllowed = fn }

func (m *Module) requireLocalWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	return m.gate.Enter(ctx)
}

func (m *Module) ListPrincipals(ctx context.Context) ([]PrincipalSummary, error) {
	principals, err := m.store.ListPrincipals(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PrincipalSummary, 0, len(principals))
	for _, p := range principals {
		out = append(out, p.toSummary())
	}
	return out, nil
}

func (m *Module) GetPrincipal(ctx context.Context, principalID string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	return p.toSummary(), nil
}

func (m *Module) FindPrincipal(ctx context.Context, username string, email string) (PrincipalSummary, error) {
	p, err := m.store.FindPrincipal(ctx, username, email)
	if err != nil {
		return PrincipalSummary{}, err
	}
	return p.toSummary(), nil
}

func (m *Module) CreatePrincipal(ctx context.Context, input CreatePrincipalInput) (PrincipalSummary, error) {
	if m.raftGroups == nil {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return PrincipalSummary{}, err
		}
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return PrincipalSummary{}, err
	}
	defer release()
	username := strings.TrimSpace(input.Username)
	kind := normalizePrincipalKind(input.Kind)
	if kind != PrincipalKindSystem && username == "" {
		return PrincipalSummary{}, fmt.Errorf("%w: username is required", ErrInvalidInput)
	}
	if input.LoginEnabled && input.Password == "" {
		return PrincipalSummary{}, fmt.Errorf("%w: password is required", ErrInvalidInput)
	}
	var hash string
	if input.Password != "" {
		hash, err = HashPassword(input.Password)
		if err != nil {
			return PrincipalSummary{}, err
		}
	}
	now := time.Now().UTC()
	state := PrincipalStateActive
	if input.Disabled {
		state = PrincipalStateDisabled
	}
	p := Principal{ID: uuid.NewString(), Username: username, Email: strings.TrimSpace(input.Email), DisplayName: strings.TrimSpace(input.DisplayName), Kind: kind, State: state, LoginEnabled: input.LoginEnabled, PasswordHash: hash, CreatedAt: now, UpdatedAt: now, CreatedBy: strings.TrimSpace(input.CreatedBy)}
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	for _, role := range input.Roles {
		if _, _, err := m.GrantRole(ctx, applied.ID, role.Role, role.Scope, role.Reason, input.CreatedBy); err != nil {
			return PrincipalSummary{}, err
		}
	}
	for _, grant := range input.Capabilities {
		if _, _, err := m.GrantCapability(ctx, applied.ID, grant.Capability, grant.Scope, grant.Reason, input.CreatedBy); err != nil {
			return PrincipalSummary{}, err
		}
	}
	return applied.toSummary(), nil
}

func (m *Module) UpdatePrincipal(ctx context.Context, input UpdatePrincipalInput) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, input.PrincipalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	if input.Email != nil {
		p.Email = *input.Email
	}
	if input.DisplayName != nil {
		p.DisplayName = *input.DisplayName
	}
	if input.Username != nil {
		p.Username = *input.Username
	}
	if input.Kind != nil {
		p.Kind = *input.Kind
	}
	if input.LoginEnabled != nil {
		if !*input.LoginEnabled && m.isLastSystemAdminPrincipal(ctx, p.ID) {
			return PrincipalSummary{}, ErrLastSystemAdmin
		}
		p.LoginEnabled = *input.LoginEnabled
	}
	p.UpdatedAt = time.Now().UTC()
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) DisablePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	if p.State == PrincipalStateActive && m.isLastSystemAdminPrincipal(ctx, p.ID) {
		return PrincipalSummary{}, ErrLastSystemAdmin
	}
	p.State = PrincipalStateDisabled
	p.UpdatedAt = time.Now().UTC()
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) EnablePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	p.State = PrincipalStateActive
	p.UpdatedAt = time.Now().UTC()
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) DeletePrincipal(ctx context.Context, principalID string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	if p.State != PrincipalStateDeleted && m.isLastSystemAdminPrincipal(ctx, p.ID) {
		return PrincipalSummary{}, ErrLastSystemAdmin
	}
	p.State = PrincipalStateDeleted
	p.UpdatedAt = time.Now().UTC()
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) SetPrincipalPassword(ctx context.Context, principalID string, password string) (PrincipalSummary, error) {
	if password == "" {
		return PrincipalSummary{}, fmt.Errorf("%w: password is required", ErrInvalidInput)
	}
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	hash, err := HashPassword(password)
	if err != nil {
		return PrincipalSummary{}, err
	}
	p.PasswordHash = hash
	p.LoginEnabled = true
	p.UpdatedAt = time.Now().UTC()
	applied, err := m.commitPrincipalPut(ctx, p, "identity-principal-put")
	if err != nil {
		return PrincipalSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) AuthenticatePrincipal(ctx context.Context, username string, password string) (PrincipalSummary, error) {
	p, err := m.store.FindPrincipal(ctx, username, "")
	if err != nil {
		return PrincipalSummary{}, ErrInvalidCredentials
	}
	p = p.normalized()
	if p.State != PrincipalStateActive || !p.LoginEnabled || !VerifyPassword(p.PasswordHash, password) {
		return PrincipalSummary{}, ErrInvalidCredentials
	}
	return p.toSummary(), nil
}

func (m *Module) CreateAuthSession(ctx context.Context, principal PrincipalSummary, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration, absoluteTTL time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	if principal.ID == "" {
		return "", domainauth.RefreshSession{}, ErrPrincipalNotFound
	}
	refreshToken, err := domainauth.NewRefreshToken(tokenBytes)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	refreshTokenHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	now := time.Now().UTC()
	rec := domainauth.RefreshSession{ID: domainauth.RefreshSessionID(uuid.New()), PrincipalID: identity.PrincipalID(principal.ID), PrincipalRef: identity.PrincipalRef(principal.Username), Status: domainauth.RefreshSessionStatusActive, TokenFamilyID: domainauth.TokenFamilyID(uuid.NewString()), RefreshTokenHash: refreshTokenHash, CreatedAt: now, LastUsedAt: now, IdleExpiresAt: now.Add(idleTTL), AbsoluteExpiresAt: now.Add(absoluteTTL), Metadata: metadata}
	if m.raftGroups != nil {
		applied, err := m.commitSessionPutRaft(ctx, rec, "identity-principal-session-put")
		return refreshToken, applied, err
	}
	stored, err := m.sessions.Create(ctx, rec)
	return refreshToken, stored, err
}

func (m *Module) RefreshAuthSession(ctx context.Context, refreshToken domainauth.RefreshToken, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration) (PrincipalSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	presentedHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, ErrInvalidCredentials
	}
	rec, err := m.sessions.FindByTokenHash(ctx, presentedHash)
	if err != nil {
		if reused, reusedErr := m.sessions.FindByConsumedTokenHash(ctx, presentedHash); reusedErr == nil {
			_, _ = m.sessions.RevokeFamily(ctx, reused.TokenFamilyID, time.Now().UTC(), "refresh token reuse detected")
		}
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, ErrInvalidCredentials
	}
	now := time.Now().UTC()
	if rec.Status != domainauth.RefreshSessionStatusActive || (!rec.AbsoluteExpiresAt.IsZero() && !rec.AbsoluteExpiresAt.After(now)) || (!rec.IdleExpiresAt.IsZero() && !rec.IdleExpiresAt.After(now)) {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, ErrInvalidCredentials
	}
	p, err := m.store.GetPrincipal(ctx, string(rec.PrincipalID))
	if err != nil || p.State != PrincipalStateActive || !p.LoginEnabled {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, ErrInvalidCredentials
	}
	rotated, err := domainauth.NewRefreshToken(tokenBytes)
	if err != nil {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, err
	}
	rotatedHash, err := domainauth.HashRefreshToken(rotated)
	if err != nil {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, err
	}
	rec.ConsumedRefreshTokenHashes = append(rec.ConsumedRefreshTokenHashes, rec.RefreshTokenHash)
	rec.RefreshTokenHash = rotatedHash
	rec.RotationCounter++
	rec.LastUsedAt = now
	rec.IdleExpiresAt = now.Add(idleTTL)
	rec.Metadata = metadata
	if m.raftGroups != nil {
		rec, err = m.commitSessionPutRaft(ctx, rec, "identity-principal-session-put")
	} else {
		rec, err = m.sessions.Update(ctx, rec)
	}
	if err != nil {
		return PrincipalSummary{}, "", domainauth.RefreshSession{}, err
	}
	return p.toSummary(), rotated, rec, nil
}

func (m *Module) ListPrincipalSessions(ctx context.Context, principalID string) ([]domainauth.RefreshSession, error) {
	return m.sessions.ListByPrincipal(ctx, identity.PrincipalID(strings.TrimSpace(principalID)))
}

func (m *Module) RevokePrincipalSession(ctx context.Context, principalID string, sessionID string) error {
	id, err := uuid.Parse(strings.TrimSpace(sessionID))
	if err != nil {
		return storesession.ErrSessionNotFound
	}
	rec, err := m.sessions.GetByID(ctx, domainauth.RefreshSessionID(id))
	if err != nil {
		return err
	}
	if string(rec.PrincipalID) != strings.TrimSpace(principalID) {
		return storesession.ErrSessionNotFound
	}
	_, err = m.sessions.RevokeByID(ctx, rec.ID, time.Now().UTC(), "revoked")
	return err
}

func (m *Module) RevokePrincipalSessions(ctx context.Context, principalID string) (int, error) {
	sessions, err := m.ListPrincipalSessions(ctx, principalID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, rec := range sessions {
		if rec.Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		if _, err := m.sessions.RevokeByID(ctx, rec.ID, time.Now().UTC(), "revoked"); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (m *Module) ListRoleBindings(ctx context.Context, principalID string) ([]RoleBinding, error) {
	return m.store.ListRoleBindings(ctx, principalID)
}

func (m *Module) GrantRole(ctx context.Context, principalID string, role string, scope AccessScope, reason string, grantedBy string) (RoleBinding, PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return RoleBinding{}, PrincipalSummary{}, err
	}
	binding := RoleBinding{ID: uuid.NewString(), PrincipalID: p.ID, Role: role, Scope: scope, State: GrantStateActive, Reason: reason, CreatedBy: grantedBy, CreatedAt: time.Now().UTC()}
	applied, err := m.commitRoleBindingPut(ctx, binding, "identity-role-binding-put")
	if err != nil {
		return RoleBinding{}, PrincipalSummary{}, err
	}
	return applied, p.toSummary(), nil
}

func (m *Module) RevokeRole(ctx context.Context, principalID string, bindingID string, revokedBy string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	bindings, err := m.store.ListRoleBindings(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	for _, binding := range bindings {
		if binding.ID != bindingID {
			continue
		}
		if binding.State == GrantStateActive && canonicalRole(binding.Role) == RoleSystemAdmin && m.isLastSystemAdminRole(ctx, binding.PrincipalID, binding.ID) {
			return PrincipalSummary{}, ErrLastSystemAdmin
		}
		binding.State = GrantStateRevoked
		binding.RevokedBy = strings.TrimSpace(revokedBy)
		binding.RevokedAt = time.Now().UTC()
		if _, err := m.commitRoleBindingPut(ctx, binding, "identity-role-binding-put"); err != nil {
			return PrincipalSummary{}, err
		}
		return p.toSummary(), nil
	}
	return PrincipalSummary{}, ErrGrantNotFound
}

func (m *Module) ListCapabilityGrants(ctx context.Context, principalID string) ([]CapabilityGrant, error) {
	return m.store.ListCapabilityGrants(ctx, principalID)
}

func (m *Module) GrantCapability(ctx context.Context, principalID string, capability string, scope AccessScope, reason string, grantedBy string) (CapabilityGrant, PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return CapabilityGrant{}, PrincipalSummary{}, err
	}
	grant := CapabilityGrant{ID: uuid.NewString(), PrincipalID: p.ID, Capability: capability, Scope: scope, State: GrantStateActive, Reason: reason, CreatedBy: grantedBy, CreatedAt: time.Now().UTC()}
	applied, err := m.commitCapabilityGrantPut(ctx, grant, "identity-capability-grant-put")
	if err != nil {
		return CapabilityGrant{}, PrincipalSummary{}, err
	}
	return applied, p.toSummary(), nil
}

func (m *Module) RevokeCapability(ctx context.Context, principalID string, grantID string, revokedBy string) (PrincipalSummary, error) {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	grants, err := m.store.ListCapabilityGrants(ctx, principalID)
	if err != nil {
		return PrincipalSummary{}, err
	}
	for _, grant := range grants {
		if grant.ID != grantID {
			continue
		}
		grant.State = GrantStateRevoked
		grant.RevokedBy = strings.TrimSpace(revokedBy)
		grant.RevokedAt = time.Now().UTC()
		if _, err := m.commitCapabilityGrantPut(ctx, grant, "identity-capability-grant-put"); err != nil {
			return PrincipalSummary{}, err
		}
		return p.toSummary(), nil
	}
	return PrincipalSummary{}, ErrGrantNotFound
}

func (m *Module) Authorize(ctx context.Context, principalID string, capability string, scope AccessScope) error {
	ok, err := m.hasCapabilityInScope(ctx, principalID, capability, scope)
	if err != nil {
		return err
	}
	if !ok {
		return status.Error(codes.PermissionDenied, permissionDenied(capability).Error())
	}
	return nil
}

func (m *Module) HasCapability(ctx context.Context, principalID string, capability string) (bool, error) {
	return m.hasCapabilityInScope(ctx, principalID, capability, AccessScope{Type: "system"})
}

func (m *Module) EffectiveAccess(ctx context.Context, principalID string, scope AccessScope) (EffectiveAccess, error) {
	caps := map[string]struct{}{}
	roles := map[string]struct{}{}
	bindings, err := m.store.ListRoleBindings(ctx, principalID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	for _, binding := range bindings {
		if binding.State != GrantStateActive || !scopeApplies(binding.Scope, scope) {
			continue
		}
		roles[binding.Role] = struct{}{}
		for _, cap := range roleCapabilities(binding.Role) {
			caps[cap] = struct{}{}
		}
	}
	grants, err := m.store.ListCapabilityGrants(ctx, principalID)
	if err != nil {
		return EffectiveAccess{}, err
	}
	for _, grant := range grants {
		if grant.State == GrantStateActive && scopeApplies(grant.Scope, scope) {
			caps[grant.Capability] = struct{}{}
		}
	}
	out := EffectiveAccess{Roles: make([]string, 0, len(roles)), Capabilities: make([]string, 0, len(caps))}
	for role := range roles {
		out.Roles = append(out.Roles, role)
	}
	for cap := range caps {
		out.Capabilities = append(out.Capabilities, cap)
	}
	return out, nil
}

func (m *Module) hasCapabilityInScope(ctx context.Context, principalID string, capability string, scope AccessScope) (bool, error) {
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		return false, nil
	}
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil {
		return false, err
	}
	if p.State != PrincipalStateActive {
		return false, nil
	}
	requested := canonicalCapability(capability)
	bindings, err := m.store.ListRoleBindings(ctx, principalID)
	if err != nil {
		return false, err
	}
	for _, binding := range bindings {
		if binding.State != GrantStateActive || !scopeApplies(binding.Scope, scope) {
			continue
		}
		for _, cap := range roleCapabilities(binding.Role) {
			if capabilityMatches(cap, requested) {
				return true, nil
			}
		}
	}
	grants, err := m.store.ListCapabilityGrants(ctx, principalID)
	if err != nil {
		return false, err
	}
	for _, grant := range grants {
		if grant.State == GrantStateActive && scopeApplies(grant.Scope, scope) && capabilityMatches(grant.Capability, requested) {
			return true, nil
		}
	}
	return false, nil
}

func (m *Module) ensureStandaloneSystemAdmin(ctx context.Context, logger *slog.Logger, username string, password string) error {
	if m.hasActiveSystemAdmin(ctx) {
		return nil
	}
	username = strings.TrimSpace(username)
	if username == "" {
		username = "admin"
	}
	generated := false
	if password == "" {
		var err error
		password, err = GeneratePassword(18)
		if err != nil {
			return err
		}
		generated = true
	}
	summary, err := m.CreatePrincipal(ctx, CreatePrincipalInput{Username: username, Password: password, Kind: PrincipalKindHuman, LoginEnabled: true})
	if err != nil {
		if !errors.Is(err, ErrDuplicatePrincipal) {
			return err
		}
		summary, err = m.FindPrincipal(ctx, username, "")
		if err != nil {
			return err
		}
	}
	if _, _, err := m.GrantRole(ctx, summary.ID, RoleSystemAdmin, AccessScope{Type: "system"}, "bootstrap system admin", summary.ID); err != nil {
		return err
	}
	if logger != nil {
		if generated {
			logger.Warn("default standalone principal created; change this password immediately", "username", username, "password", password, "change_password_required", true)
		} else {
			logger.Warn("default standalone principal created from configured bootstrap credentials", "username", username, "password_configured", true, "change_password_required", true)
		}
	}
	return nil
}

func (m *Module) hasActiveSystemAdmin(ctx context.Context) bool {
	principals, err := m.store.ListPrincipals(ctx)
	if err != nil {
		return false
	}
	for _, p := range principals {
		if p.State == PrincipalStateActive && p.LoginEnabled {
			bindings, _ := m.store.ListRoleBindings(ctx, p.ID)
			for _, binding := range bindings {
				if binding.State == GrantStateActive && canonicalRole(binding.Role) == RoleSystemAdmin && normalizeScope(binding.Scope).Type == "system" {
					return true
				}
			}
		}
	}
	return false
}

func (m *Module) isLastSystemAdminPrincipal(ctx context.Context, principalID string) bool {
	if !m.isSystemAdminPrincipal(ctx, principalID) {
		return false
	}
	principals, err := m.store.ListPrincipals(ctx)
	if err != nil {
		return false
	}
	count := 0
	for _, p := range principals {
		if p.ID != principalID && m.isSystemAdminPrincipal(ctx, p.ID) {
			count++
		}
	}
	return count == 0
}

func (m *Module) isSystemAdminPrincipal(ctx context.Context, principalID string) bool {
	p, err := m.store.GetPrincipal(ctx, principalID)
	if err != nil || p.State != PrincipalStateActive || !p.LoginEnabled {
		return false
	}
	bindings, err := m.store.ListRoleBindings(ctx, principalID)
	if err != nil {
		return false
	}
	for _, binding := range bindings {
		if binding.State == GrantStateActive && canonicalRole(binding.Role) == RoleSystemAdmin && normalizeScope(binding.Scope).Type == "system" {
			return true
		}
	}
	return false
}

func (m *Module) isLastSystemAdminRole(ctx context.Context, principalID string, excludingBindingID string) bool {
	principals, err := m.store.ListPrincipals(ctx)
	if err != nil {
		return false
	}
	count := 0
	for _, p := range principals {
		if p.State != PrincipalStateActive || !p.LoginEnabled {
			continue
		}
		bindings, _ := m.store.ListRoleBindings(ctx, p.ID)
		for _, binding := range bindings {
			if binding.ID == excludingBindingID || binding.State != GrantStateActive {
				continue
			}
			if canonicalRole(binding.Role) == RoleSystemAdmin && normalizeScope(binding.Scope).Type == "system" {
				count++
			}
		}
	}
	return count == 0 && principalID != ""
}

func hostMode(host runtime.Host) string { return stringHostConfigField(host, "Mode") }
func hostBootstrapAdminUsername(host runtime.Host) string {
	return stringHostConfigField(host, "BootstrapAdminUsername")
}
func hostBootstrapAdminPassword(host runtime.Host) string {
	return stringHostConfigField(host, "BootstrapAdminPassword")
}

func stringHostConfigField(host runtime.Host, name string) string {
	value := reflect.Indirect(reflect.ValueOf(host))
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return ""
	}
	configField := value.FieldByName("Config")
	if !configField.IsValid() {
		return ""
	}
	field := configField.FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}
	return field.String()
}
