package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

const ModuleName = "admin"

var ErrInvalidCredentials = errors.New("invalid operator credentials")

type Module struct {
	store    Store
	sessions storesession.Manager
	gate     *quiesce.Gate
}

func NewModule() *Module { return &Module{gate: quiesce.NewGate(ModuleName)} }

func OpenLister(dataDir string) (*Module, error) {
	store, err := OpenExistingStore(filepath.Join(dataDir, "admins"))
	if err != nil {
		return nil, err
	}
	mgr := storesession.NewManager()
	if err := mgr.Init(context.Background(), filepath.Join(dataDir, "admins", "sessions")); err != nil {
		return nil, err
	}
	return &Module{store: store, sessions: mgr}, nil
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	adminDir := filepath.Join(rt.Config.DataDir, "admins")
	adminDirCreated, err := ensureDir(adminDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create admins directory", err)
	}
	rt.Logger.Info("admin directory ready", "path", adminDir, "created", adminDirCreated)

	store, storeCreated, err := OpenStore(adminDir)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open admin store", err)
	}
	m.store = store
	sessionsDir := filepath.Join(adminDir, "sessions")
	sessionsDirCreated, err := ensureDir(sessionsDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create admin sessions directory", err)
	}
	rt.Logger.Info("admin sessions directory ready", "path", sessionsDir, "created", sessionsDirCreated)
	sessions := storesession.NewManager()
	if err := sessions.Init(ctx, sessionsDir); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open admin session store", err)
	}
	m.sessions = sessions
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register admin quiesce participant", err)
		}
	}
	rt.Logger.Info("admin store ready", "path", filepath.Join(adminDir, StoreFilename), "created", storeCreated)

	admins, err := store.List(ctx)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to list admins", err)
	}
	if rt.Config.Mode == "standalone" && len(admins) == 0 {
		username, password, generated, err := bootstrapAdminCredentials(rt.Config.BootstrapAdminUsername, rt.Config.BootstrapAdminPassword)
		if err != nil {
			return daemonruntime.Abort(ModuleName, "security", "failed to resolve default admin credentials", err)
		}
		hash, err := HashPassword(password)
		if err != nil {
			return daemonruntime.Abort(ModuleName, "security", "failed to hash default admin password", err)
		}
		now := time.Now().UTC()
		admin := Admin{ID: uuid.NewString(), Username: username, State: AdminStateActive, PasswordHash: hash, CreatedAt: now, UpdatedAt: now}
		admin.RoleGrants = []RoleGrant{newRoleGrant(admin.ID, OperatorRoleSystemAdmin, systemScope(), "bootstrap system admin", admin.ID)}
		if err := store.Create(ctx, admin); err != nil {
			if errors.Is(err, ErrDuplicateAdmin) {
				return daemonruntime.Continue(ModuleName, "store", "default admin already exists", err)
			}
			return daemonruntime.Abort(ModuleName, "store", "failed to create default admin", err)
		}
		if generated {
			rt.Logger.Warn("default standalone admin created; change this password immediately", "username", admin.Username, "password", password, "change_password_required", true)
		} else {
			rt.Logger.Warn("default standalone admin created from configured bootstrap credentials", "username", admin.Username, "password_configured", true, "change_password_required", true)
		}
	}
	if rt.Config.Mode == "standalone" {
		if err := m.ensureStandaloneSystemAdmin(ctx, rt.Logger); err != nil {
			return daemonruntime.Abort(ModuleName, "store", "failed to ensure standalone system admin", err)
		}
	}
	return daemonruntime.OK(ModuleName)
}

func bootstrapAdminCredentials(username string, password string) (string, string, bool, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		username = "admin"
	}
	if password != "" {
		return username, password, false, nil
	}
	generated, err := GeneratePassword()
	if err != nil {
		return "", "", false, fmt.Errorf("generate password: %w", err)
	}
	return username, generated, true, nil
}

func (m *Module) ensureStandaloneSystemAdmin(ctx context.Context, logger *slog.Logger) error {
	admins, err := m.store.List(ctx)
	if err != nil {
		return err
	}
	for _, admin := range admins {
		if admin.State == AdminStateActive && hasSystemAdminRole(admin) {
			return nil
		}
	}
	for _, admin := range admins {
		if admin.State != AdminStateActive {
			continue
		}
		_, err := m.store.Update(ctx, admin.ID, func(admin *Admin) error {
			admin.RoleGrants = append(admin.RoleGrants, newRoleGrant(admin.ID, OperatorRoleSystemAdmin, systemScope(), "standalone system admin migration", admin.ID))
			return nil
		})
		if err != nil {
			return err
		}
		logger.Warn("existing standalone admin promoted to system admin", "username", admin.Username, "change_password_required", false)
		return nil
	}
	return nil
}

func (m *Module) AuthenticateOperator(ctx context.Context, username string, password string) (AdminSummary, error) {
	if m.store == nil {
		return AdminSummary{}, fmt.Errorf("admin module is not initialized")
	}
	admin, err := m.store.Find(ctx, username, "")
	if err != nil {
		if errors.Is(err, ErrAdminNotFound) {
			return AdminSummary{}, ErrInvalidCredentials
		}
		return AdminSummary{}, err
	}
	if admin.State != AdminStateActive {
		return AdminSummary{}, ErrInvalidCredentials
	}
	if err := VerifyPassword(admin.PasswordHash, password); err != nil {
		return AdminSummary{}, ErrInvalidCredentials
	}
	return admin.toSummary(), nil
}

func (m *Module) CreateOperatorAuthSession(ctx context.Context, operator AdminSummary, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration, absoluteTTL time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	defer release()
	if m.sessions == nil {
		return "", domainauth.RefreshSession{}, fmt.Errorf("admin session store is not initialized")
	}
	operatorID, err := parseOperatorID(operator.ID)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
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
	rec, err := m.sessions.Create(ctx, domainauth.RefreshSession{UserID: operatorID, UserRef: identity.UserRef(operator.Username), Status: domainauth.RefreshSessionStatusActive, RefreshTokenHash: refreshTokenHash, CreatedAt: now, LastUsedAt: now, IdleExpiresAt: now.Add(idleTTL), AbsoluteExpiresAt: now.Add(absoluteTTL), Metadata: metadata})
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	return refreshToken, rec, nil
}

func (m *Module) RefreshOperatorAuthSession(ctx context.Context, refreshToken domainauth.RefreshToken, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration) (AdminSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	defer release()
	if m.store == nil || m.sessions == nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, fmt.Errorf("admin module is not initialized")
	}
	refreshTokenHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	rec, err := m.sessions.FindByTokenHash(ctx, refreshTokenHash)
	if err != nil {
		if errors.Is(err, storesession.ErrSessionNotFound) {
			if consumed, reuseErr := m.sessions.FindByConsumedTokenHash(ctx, refreshTokenHash); reuseErr == nil {
				_, _ = m.sessions.RevokeFamily(ctx, consumed.TokenFamilyID, time.Now().UTC(), "refresh token reuse detected")
			}
			return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
		}
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	if !domainauth.VerifyRefreshTokenHash(refreshToken, rec.RefreshTokenHash) {
		return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	now := time.Now().UTC()
	if !operatorRefreshSessionRefreshable(rec, now) {
		if rec.Status == domainauth.RefreshSessionStatusActive && operatorRefreshSessionExpired(rec, now) {
			rec.Status = domainauth.RefreshSessionStatusExpired
			_, _ = m.sessions.Update(ctx, rec)
		}
		return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	admin, err := m.adminForOperatorSession(ctx, rec)
	if err != nil {
		if errors.Is(err, ErrAdminNotFound) {
			return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
		}
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	if admin.State != AdminStateActive {
		return AdminSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	newRefreshToken, err := domainauth.NewRefreshToken(tokenBytes)
	if err != nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	newRefreshTokenHash, err := domainauth.HashRefreshToken(newRefreshToken)
	if err != nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	oldRefreshTokenHash := rec.RefreshTokenHash
	rec.RefreshTokenHash = newRefreshTokenHash
	rec.ConsumedRefreshTokenHashes = append(rec.ConsumedRefreshTokenHashes, oldRefreshTokenHash)
	rec.RotationCounter++
	rec.LastUsedAt = now
	rec.IdleExpiresAt = now.Add(idleTTL)
	if rec.IdleExpiresAt.After(rec.AbsoluteExpiresAt) {
		rec.IdleExpiresAt = rec.AbsoluteExpiresAt
	}
	rec.Metadata = metadata
	updated, err := m.sessions.Update(ctx, rec)
	if err != nil {
		return AdminSummary{}, "", domainauth.RefreshSession{}, err
	}
	return admin.toSummary(), newRefreshToken, updated, nil
}

func (m *Module) adminForOperatorSession(ctx context.Context, session domainauth.RefreshSession) (Admin, error) {
	admin, err := m.store.GetByID(ctx, session.UserID.String())
	if err == nil {
		return admin, nil
	}
	if !errors.Is(err, ErrAdminNotFound) {
		return Admin{}, err
	}
	if username := strings.TrimSpace(string(session.UserRef)); username != "" {
		return m.store.Find(ctx, username, "")
	}
	return Admin{}, ErrAdminNotFound
}

func (m *Module) ListOperatorSessions(ctx context.Context, operatorID string) ([]domainauth.RefreshSession, error) {
	id, err := parseOperatorID(operatorID)
	if err != nil {
		return nil, err
	}
	return m.sessions.ListByUser(ctx, id)
}

func (m *Module) RevokeOperatorSession(ctx context.Context, operatorID string, sessionID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	operatorUUID, err := parseOperatorID(operatorID)
	if err != nil {
		return err
	}
	sessionUUID, err := uuid.Parse(strings.TrimSpace(sessionID))
	if err != nil {
		return ErrAdminNotFound
	}
	rec, err := m.sessions.GetByID(ctx, domainauth.RefreshSessionID(sessionUUID))
	if err != nil {
		return err
	}
	if rec.UserID != operatorUUID {
		return ErrAdminNotFound
	}
	_, err = m.sessions.RevokeByID(ctx, rec.ID, time.Now().UTC(), "operator session revoked")
	return err
}

func (m *Module) RevokeOperatorSessions(ctx context.Context, operatorID string) (int, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	id, err := parseOperatorID(operatorID)
	if err != nil {
		return 0, err
	}
	sessions, err := m.sessions.ListByUser(ctx, id)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, session := range sessions {
		if session.Status != domainauth.RefreshSessionStatusActive {
			continue
		}
		if _, err := m.sessions.RevokeByID(ctx, session.ID, time.Now().UTC(), "operator sessions revoked"); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (m *Module) SetOperatorPassword(ctx context.Context, operatorID string, password string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	if m.store == nil {
		return AdminSummary{}, fmt.Errorf("admin module is not initialized")
	}
	if strings.TrimSpace(operatorID) == "" {
		return AdminSummary{}, ErrAdminNotFound
	}
	if password == "" {
		return AdminSummary{}, fmt.Errorf("password must not be empty")
	}
	hash, err := HashPassword(password)
	if err != nil {
		return AdminSummary{}, err
	}
	admin, err := m.store.UpdatePasswordHash(ctx, operatorID, hash)
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) ListAdmins(ctx context.Context) ([]AdminSummary, error) {
	if m.store == nil {
		return nil, fmt.Errorf("admin module is not initialized")
	}
	admins, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make([]AdminSummary, 0, len(admins))
	for _, admin := range admins {
		summaries = append(summaries, admin.toSummary())
	}
	return summaries, nil
}

func (m *Module) GetOperator(ctx context.Context, operatorID string) (AdminSummary, error) {
	admin, err := m.store.GetByID(ctx, operatorID)
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) FindOperator(ctx context.Context, username string, email string) (AdminSummary, error) {
	admin, err := m.store.Find(ctx, username, email)
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) CreateOperator(ctx context.Context, input CreateOperatorInput) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	if strings.TrimSpace(input.Username) == "" {
		return AdminSummary{}, fmt.Errorf("username must not be empty")
	}
	if input.Password == "" {
		return AdminSummary{}, fmt.Errorf("password must not be empty")
	}
	hash, err := HashPassword(input.Password)
	if err != nil {
		return AdminSummary{}, err
	}
	now := time.Now().UTC()
	state := AdminStateActive
	if input.Disabled {
		state = AdminStateDisabled
	}
	operatorID := uuid.NewString()
	admin := Admin{ID: operatorID, Username: strings.TrimSpace(input.Username), Email: strings.TrimSpace(input.Email), State: state, PasswordHash: hash, CreatedAt: now, UpdatedAt: now}
	for _, grant := range input.Roles {
		if grant.ID == "" {
			grant.ID = uuid.NewString()
		}
		grant.OperatorID = operatorID
		if grant.CreatedAt.IsZero() {
			grant.CreatedAt = now
		}
		admin.RoleGrants = append(admin.RoleGrants, grant)
	}
	for _, grant := range input.CapabilityGrants {
		if grant.ID == "" {
			grant.ID = uuid.NewString()
		}
		grant.OperatorID = operatorID
		if grant.CreatedAt.IsZero() {
			grant.CreatedAt = now
		}
		admin.CapabilityGrants = append(admin.CapabilityGrants, grant)
	}
	if err := m.store.Create(ctx, admin); err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) UpdateOperator(ctx context.Context, input UpdateOperatorInput) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	admin, err := m.store.Update(ctx, input.OperatorID, func(admin *Admin) error {
		if input.Email != nil {
			admin.Email = strings.TrimSpace(*input.Email)
		}
		return nil
	})
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) DisableOperator(ctx context.Context, operatorID string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	if err := m.ensureCanRemoveSystemAdmin(ctx, operatorID, ""); err != nil {
		return AdminSummary{}, err
	}
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error { admin.State = AdminStateDisabled; return nil })
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) EnableOperator(ctx context.Context, operatorID string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error { admin.State = AdminStateActive; return nil })
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) DeleteOperator(ctx context.Context, operatorID string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	if err := m.ensureCanRemoveSystemAdmin(ctx, operatorID, ""); err != nil {
		return AdminSummary{}, err
	}
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error { admin.State = AdminStateDeleted; return nil })
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) GrantRole(ctx context.Context, operatorID string, role string, scope AccessScope, reason string, grantedBy string) (RoleGrant, AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return RoleGrant{}, AdminSummary{}, err
	}
	defer release()
	grant := newRoleGrant(operatorID, role, normalizeScope(scope), reason, grantedBy)
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error { admin.RoleGrants = append(admin.RoleGrants, grant); return nil })
	if err != nil {
		return RoleGrant{}, AdminSummary{}, err
	}
	return grant, admin.toSummary(), nil
}

func (m *Module) RevokeRole(ctx context.Context, operatorID string, grantID string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	if err := m.ensureCanRemoveSystemAdmin(ctx, operatorID, grantID); err != nil {
		return AdminSummary{}, err
	}
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error {
		for i, grant := range admin.RoleGrants {
			if grant.ID == grantID {
				admin.RoleGrants = append(admin.RoleGrants[:i], admin.RoleGrants[i+1:]...)
				return nil
			}
		}
		return ErrGrantNotFound
	})
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) GrantCapability(ctx context.Context, operatorID string, capability string, scope AccessScope, reason string, grantedBy string) (CapabilityGrant, AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return CapabilityGrant{}, AdminSummary{}, err
	}
	defer release()
	grant := CapabilityGrant{ID: uuid.NewString(), OperatorID: operatorID, Capability: capability, Scope: normalizeScope(scope), Reason: reason, GrantedByOperatorID: grantedBy, CreatedAt: time.Now().UTC()}
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error { admin.CapabilityGrants = append(admin.CapabilityGrants, grant); return nil })
	if err != nil {
		return CapabilityGrant{}, AdminSummary{}, err
	}
	return grant, admin.toSummary(), nil
}

func (m *Module) RevokeCapability(ctx context.Context, operatorID string, grantID string) (AdminSummary, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return AdminSummary{}, err
	}
	defer release()
	admin, err := m.store.Update(ctx, operatorID, func(admin *Admin) error {
		for i, grant := range admin.CapabilityGrants {
			if grant.ID == grantID {
				admin.CapabilityGrants = append(admin.CapabilityGrants[:i], admin.CapabilityGrants[i+1:]...)
				return nil
			}
		}
		return ErrGrantNotFound
	})
	if err != nil {
		return AdminSummary{}, err
	}
	return admin.toSummary(), nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) ensureCanRemoveSystemAdmin(ctx context.Context, operatorID string, grantID string) error {
	admins, err := m.store.List(ctx)
	if err != nil {
		return err
	}
	activeSystemAdmins := 0
	removesActiveSystemAdmin := false
	for _, admin := range admins {
		isActiveSystemAdmin := admin.State == AdminStateActive && hasSystemAdminRole(admin)
		if isActiveSystemAdmin {
			activeSystemAdmins++
		}
		if admin.ID != operatorID {
			continue
		}
		if grantID == "" {
			removesActiveSystemAdmin = isActiveSystemAdmin
			continue
		}
		if admin.State != AdminStateActive {
			continue
		}
		for _, grant := range admin.RoleGrants {
			if grant.ID == grantID && grant.Role == OperatorRoleSystemAdmin {
				removesActiveSystemAdmin = true
				break
			}
		}
	}
	if removesActiveSystemAdmin && activeSystemAdmins <= 1 {
		return ErrLastSystemAdmin
	}
	return nil
}

func hasSystemAdminRole(admin Admin) bool {
	for _, grant := range admin.RoleGrants {
		if grant.Role == OperatorRoleSystemAdmin {
			return true
		}
	}
	return false
}

func (m *Module) IsSystemAdmin(ctx context.Context, operatorID string) (bool, error) {
	admin, err := m.store.GetByID(ctx, operatorID)
	if err != nil {
		return false, err
	}
	if admin.State != AdminStateActive {
		return false, nil
	}
	for _, grant := range admin.RoleGrants {
		if grant.Role == OperatorRoleSystemAdmin {
			return true, nil
		}
	}
	return false, nil
}

func (m *Module) HasCapability(ctx context.Context, operatorID string, capability string) (bool, error) {
	admin, err := m.store.GetByID(ctx, operatorID)
	if err != nil {
		return false, err
	}
	if admin.State != AdminStateActive {
		return false, nil
	}
	capability = strings.TrimSpace(capability)
	for _, grant := range admin.CapabilityGrants {
		if grant.Capability == capability {
			return true, nil
		}
	}
	for _, grant := range admin.RoleGrants {
		for _, roleCapability := range capabilitiesForRole(grant.Role) {
			if roleCapability == capability {
				return true, nil
			}
		}
	}
	return false, nil
}

func capabilitiesForRole(role string) []string {
	switch role {
	case OperatorRoleSystemAdmin:
		return []string{"CAPABILITY_OPERATOR_CREATE", "CAPABILITY_OPERATOR_MANAGE", "CAPABILITY_USER_CREATE", "CAPABILITY_USER_MANAGE", "CAPABILITY_SPACE_CREATE", "CAPABILITY_SPACE_UPDATE", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_ARCHIVE", "CAPABILITY_SPACE_DELETE", "CAPABILITY_SEMANTIC_SEARCH", "CAPABILITY_DAEMON_CONFIGURE", "CAPABILITY_MESH_MANAGE", "CAPABILITY_SYSTEM_COMPACT_SPACE", "CAPABILITY_SYSTEM_MAINTAIN_SPACE", "CAPABILITY_SYSTEM_BACKUP_SPACE"}
	case OperatorRoleUserAdmin:
		return []string{"CAPABILITY_USER_CREATE", "CAPABILITY_USER_MANAGE"}
	case OperatorRoleSpaceAdmin:
		return []string{"CAPABILITY_SPACE_CREATE", "CAPABILITY_SPACE_ARCHIVE", "CAPABILITY_SPACE_DELETE", "CAPABILITY_SPACE_MANAGE_ACCESS"}
	case OperatorRoleSemanticAdmin:
		return []string{"CAPABILITY_SEMANTIC_SEARCH"}
	case OperatorRoleStorageAdmin:
		return []string{"CAPABILITY_SYSTEM_COMPACT_SPACE", "CAPABILITY_SYSTEM_MAINTAIN_SPACE", "CAPABILITY_SYSTEM_BACKUP_SPACE"}
	case OperatorRoleMeshAdmin:
		return []string{"CAPABILITY_MESH_MANAGE"}
	case OperatorRoleAuditReader:
		return nil
	default:
		return nil
	}
}

func newRoleGrant(operatorID string, role string, scope AccessScope, reason string, grantedBy string) RoleGrant {
	return RoleGrant{ID: uuid.NewString(), OperatorID: operatorID, Role: role, Scope: normalizeScope(scope), Reason: reason, GrantedByOperatorID: grantedBy, CreatedAt: time.Now().UTC()}
}

func systemScope() AccessScope { return AccessScope{Type: "system"} }

func normalizeScope(scope AccessScope) AccessScope {
	if strings.TrimSpace(scope.Type) == "" {
		scope.Type = "system"
	}
	return scope
}

func operatorRefreshSessionRefreshable(rec domainauth.RefreshSession, now time.Time) bool {
	return rec.Status == domainauth.RefreshSessionStatusActive && !operatorRefreshSessionExpired(rec, now)
}

func operatorRefreshSessionExpired(rec domainauth.RefreshSession, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	return (!rec.AbsoluteExpiresAt.IsZero() && !rec.AbsoluteExpiresAt.After(now)) || (!rec.IdleExpiresAt.IsZero() && !rec.IdleExpiresAt.After(now))
}

func parseOperatorID(operatorID string) (identity.UserID, error) {
	operatorID = strings.TrimSpace(operatorID)
	if operatorID == "" {
		return uuid.Nil, ErrAdminNotFound
	}
	if id, err := uuid.Parse(operatorID); err == nil {
		return identity.UserID(id), nil
	}
	return identity.UserID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("mycel-operator:"+operatorID))), nil
}

func ensureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
