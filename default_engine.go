package knotdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/space"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/user"
	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/graphstore"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

var (
	ErrInvalidConfig      = errors.New("invalid engine config")
	ErrNotReady           = errors.New("engine not ready")
	ErrClosed             = errors.New("engine closed")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrNotFound           = errors.New("not found")
	ErrConflict           = errors.New("conflict")
)

type defaultEngine struct {
	state           EngineState
	dataDir         string
	userManager     user.Manager
	spaceManager    space.Manager
	templateManager coretemplate.Manager
	accessManager   access.Manager
	authMu          sync.RWMutex
	authCache       map[AccessToken]authClaims
}

// authClaims is the expanded authorization context cached by access token.
// It is intentionally unexported; external callers only receive AccessToken.
type authClaims struct {
	Iss      string
	Sub      string
	Aud      string
	JTI      string
	IAT      int64
	EXP      int64
	UserID   model.UserID
	UserRef  model.UserRef
	Roles    []model.SystemRole
	OwnerIDs []model.UserID
	SpaceIDs []model.SpaceID
	Scopes   []string
}

// NewEngine opens (or creates) a local embedded KnotDB runtime.
//
// If userManager, spaceManager, templateManager, or accessManager is nil, default file-backed managers are used.
func NewEngine(cfg EngineConfig, userManager user.Manager, spaceManager space.Manager, templateManager coretemplate.Manager, accessManager access.Manager) (Engine, error) {
	e := &defaultEngine{state: EngineStateClose, userManager: userManager, spaceManager: spaceManager, templateManager: templateManager, accessManager: accessManager, authCache: map[AccessToken]authClaims{}}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
}

// DefaultEngine opens (or creates) a local embedded KnotDB runtime.
// Deprecated: use NewEngine(cfg, userManager, spaceManager, templateManager, accessManager) instead.
func DefaultEngine(cfg EngineConfig) (Engine, error) {
	return NewEngine(cfg, nil, nil, nil, nil)
}

func (e *defaultEngine) Open(cfg EngineConfig) error {
	e.state = EngineStateOpen

	if cfg.DataDir == "" {
		e.state = EngineStateClose
		return fmt.Errorf("%w: data_dir is required", ErrInvalidConfig)
	}
	if cfg.Mode != EngineModeStandalone {
		e.state = EngineStateClose
		return fmt.Errorf("%w: unsupported mode %q", ErrInvalidConfig, cfg.Mode)
	}

	created := false
	if _, err := os.Stat(cfg.DataDir); err != nil {
		if os.IsNotExist(err) {
			if !cfg.CreateIfMissing {
				e.state = EngineStateClose
				return fmt.Errorf("%w: data_dir does not exist", ErrInvalidConfig)
			}
			created = true
		} else {
			e.state = EngineStateClose
			return err
		}
	} else if cfg.CreateIfMissing {
		if _, err := os.Stat(filepath.Join(metaDir(cfg.DataDir), "users.json")); err != nil {
			if os.IsNotExist(err) {
				created = true
			} else {
				e.state = EngineStateClose
				return err
			}
		}
	}
	if created {
		if cfg.AdminUsername == "" {
			e.state = EngineStateClose
			return fmt.Errorf("%w: admin_username is required when creating a standalone store", ErrInvalidConfig)
		}
		if cfg.AdminPassword == "" {
			e.state = EngineStateClose
			return fmt.Errorf("%w: admin_password is required when creating a standalone store", ErrInvalidConfig)
		}
	}
	if err := ensureStorageLayout(cfg.DataDir); err != nil {
		e.state = EngineStateClose
		return err
	}

	if e.userManager == nil {
		e.userManager = user.NewManager()
	}
	if err := e.userManager.Init(context.Background(), metaDir(cfg.DataDir), cfg.UserStoreEncryptionKeyB64); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.spaceManager == nil {
		e.spaceManager = space.NewManager()
	}
	if err := e.spaceManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.templateManager == nil {
		e.templateManager = coretemplate.NewManager()
	}
	if err := e.templateManager.Init(context.Background(), templatesDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.accessManager == nil {
		e.accessManager = access.NewManager()
	}
	if err := e.accessManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}

	um := e.userManager

	if created {
		exists, err := um.ExistsByRef(context.Background(), model.UserRef(cfg.AdminUsername))
		if err != nil {
			e.state = EngineStateClose
			return err
		}
		var admin model.User
		if !exists {
			status := model.UserStatusActive
			username := cfg.AdminUsername
			admin, err = um.Create(context.Background(), user.CreateInput{
				User: model.UserInput{
					Ref:      model.UserRef(cfg.AdminUsername),
					Username: &username,
					Status:   status,
				},
				Password: cfg.AdminPassword,
			})
			if err != nil {
				e.state = EngineStateClose
				return err
			}
		} else {
			admin, err = um.GetByRef(context.Background(), model.UserRef(cfg.AdminUsername))
			if err != nil {
				e.state = EngineStateClose
				return err
			}
		}
		if _, err := e.accessManager.GrantSystemRole(context.Background(), access.GrantSystemRoleInput{
			UserID: admin.ID,
			Roles:  []model.SystemRole{model.SystemRoleSuperuser},
		}); err != nil {
			e.state = EngineStateClose
			return err
		}
	}

	e.authMu.Lock()
	e.authCache = map[AccessToken]authClaims{}
	e.authMu.Unlock()

	e.dataDir = cfg.DataDir
	e.state = EngineStateReady
	return nil
}

func (e *defaultEngine) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.state == EngineStateClose {
		return ErrClosed
	}
	if e.state != EngineStateReady {
		return ErrNotReady
	}
	return nil
}

func (e *defaultEngine) Authenticate(ctx context.Context, in AuthInput) (AuthResult, error) {
	if err := e.Ready(ctx); err != nil {
		return AuthResult{}, err
	}
	if in.UserRef == "" || in.Password == "" {
		return AuthResult{}, fmt.Errorf("%w: user_ref and password are required", ErrInvalidCredentials)
	}

	account, err := e.userManager.Authenticate(ctx, in.UserRef, in.Password)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) || errors.Is(err, user.ErrInvalidInput) {
			return AuthResult{}, ErrInvalidCredentials
		}
		return AuthResult{}, err
	}
	if account.Status != model.UserStatusActive {
		return AuthResult{}, ErrInvalidCredentials
	}

	now := time.Now().Unix()
	exp := now + 3600
	uid := account.ID.String()
	accessToken := AccessToken(uuid.NewString())
	roles, err := e.accessManager.SystemRolesForUser(ctx, account.ID)
	if err != nil {
		return AuthResult{}, err
	}
	claims := authClaims{
		Iss:      "knotdb",
		Sub:      "user:" + uid,
		Aud:      "knotdb",
		JTI:      uuid.NewString(),
		IAT:      now,
		EXP:      exp,
		UserID:   account.ID,
		UserRef:  account.Ref,
		Roles:    roles,
		OwnerIDs: []model.UserID{account.ID},
		SpaceIDs: []model.SpaceID{},
		Scopes:   scopesForSystemRoles(roles),
	}

	e.authMu.Lock()
	if e.authCache == nil {
		e.authCache = map[AccessToken]authClaims{}
	}
	e.authCache[accessToken] = claims
	e.authMu.Unlock()

	return AuthResult{AccessToken: accessToken}, nil
}

func (e *defaultEngine) CreateUser(ctx context.Context, in CreateUserInput) (model.User, error) {
	if err := e.Ready(ctx); err != nil {
		return model.User{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return model.User{}, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageUsers)
	if err != nil {
		return model.User{}, err
	}
	if !canManageUsers {
		return model.User{}, ErrUnauthorized
	}
	created, err := e.userManager.Create(ctx, user.CreateInput{User: in.User, Password: in.Password})
	if err != nil {
		if errors.Is(err, user.ErrInvalidInput) {
			return model.User{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		return model.User{}, err
	}
	return created, nil
}

func (e *defaultEngine) ListUsers(ctx context.Context, in ListUsersInput) ([]model.User, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageUsers)
	if err != nil {
		return nil, err
	}
	if !canManageUsers {
		return nil, ErrUnauthorized
	}
	return e.userManager.List(ctx)
}

func (e *defaultEngine) DeleteUser(ctx context.Context, in DeleteUserInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageUsers)
	if err != nil {
		return err
	}
	if !canManageUsers {
		return ErrUnauthorized
	}
	if in.UserID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidConfig)
	}
	if _, err := e.userManager.GetByID(ctx, in.UserID); err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return ErrNotFound
		}
		return err
	}
	if err := e.ensureNotLastSuperuser(ctx, in.UserID); err != nil {
		return err
	}
	ownedSpaces, err := e.spaceManager.ListByOwner(ctx, in.UserID)
	if err != nil {
		return err
	}
	for _, sp := range ownedSpaces {
		if err := e.deleteSpaceByID(ctx, sp.SpaceID); err != nil {
			return err
		}
	}
	if err := e.accessManager.DeleteForUser(ctx, in.UserID); err != nil {
		return err
	}
	if err := e.userManager.DeleteByID(ctx, in.UserID); err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return ErrNotFound
		}
		return err
	}
	e.purgeCachedClaimsForUser(in.UserID)
	return nil
}

func (e *defaultEngine) CreateSpace(ctx context.Context, in CreateSpaceInput) (SpaceInfo, error) {
	if err := e.Ready(ctx); err != nil {
		return SpaceInfo{}, err
	}
	if in.Name == "" {
		return SpaceInfo{}, fmt.Errorf("%w: space name is required", ErrInvalidConfig)
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return SpaceInfo{}, err
	}
	if auth.UserID == uuid.Nil {
		return SpaceInfo{}, ErrUnauthorized
	}
	canCreateSpace, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionCreateSpaces)
	if err != nil {
		return SpaceInfo{}, err
	}
	if !canCreateSpace {
		return SpaceInfo{}, ErrUnauthorized
	}

	sp, err := e.spaceManager.Create(ctx, space.CreateInput{OwnerID: auth.UserID, Name: in.Name})
	if err != nil {
		return SpaceInfo{}, err
	}
	if _, err := e.accessManager.Grant(ctx, access.GrantInput{
		SpaceID:     sp.SpaceID,
		UserID:      auth.UserID,
		Permissions: []model.SpacePermission{model.SpacePermissionAdmin},
	}); err != nil {
		return SpaceInfo{}, err
	}

	e.grantSpaceToCachedClaims(in.AccessToken, sp.SpaceID)
	return SpaceInfo{OwnerID: sp.OwnerID, SpaceID: sp.SpaceID, Name: sp.Name}, nil
}

func (e *defaultEngine) ListSpaces(ctx context.Context, in ListSpacesInput) ([]model.Space, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManageAccess, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageAccess)
	if err != nil {
		return nil, err
	}
	if !canManageAccess {
		return nil, ErrUnauthorized
	}
	return e.spaceManager.List(ctx)
}

func (e *defaultEngine) DeleteSpace(ctx context.Context, in DeleteSpaceInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return err
	}
	return e.deleteSpaceByID(ctx, in.SpaceID)
}

func (e *defaultEngine) ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]graph.Template, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if in.SpaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	if _, err := e.spaceManager.GetByID(ctx, in.SpaceID); err != nil {
		if errors.Is(err, space.ErrSpaceNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	canAdmin, err := e.canAdminSpace(ctx, auth.UserID, in.SpaceID)
	if err != nil {
		return nil, err
	}
	if !canAdmin {
		return nil, ErrUnauthorized
	}

	return e.templateManager.Import(ctx, in.SpaceID, in.Document)
}

func (e *defaultEngine) GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (model.SystemAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return model.SystemAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return model.SystemAccessRule{}, err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageAccess)
	if err != nil {
		return model.SystemAccessRule{}, err
	}
	if !canManage {
		return model.SystemAccessRule{}, ErrUnauthorized
	}
	return e.accessManager.GrantSystemRole(ctx, access.GrantSystemRoleInput{UserID: in.UserID, Roles: in.Roles})
}

func (e *defaultEngine) RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, model.SystemPermissionManageAccess)
	if err != nil {
		return err
	}
	if !canManage {
		return ErrUnauthorized
	}
	return e.accessManager.RevokeSystemRole(ctx, access.RevokeSystemRoleInput{UserID: in.UserID})
}

func (e *defaultEngine) GrantSpaceAccess(ctx context.Context, in GrantSpaceAccessInput) (model.SpaceAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return model.SpaceAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return model.SpaceAccessRule{}, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return model.SpaceAccessRule{}, err
	}
	return e.accessManager.Grant(ctx, access.GrantInput{SpaceID: in.SpaceID, UserID: in.UserID, Permissions: in.Permissions})
}

func (e *defaultEngine) RevokeSpaceAccess(ctx context.Context, in RevokeSpaceAccessInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return err
	}
	return e.accessManager.Revoke(ctx, access.RevokeInput{SpaceID: in.SpaceID, UserID: in.UserID})
}

func (e *defaultEngine) ListSpaceAccess(ctx context.Context, in ListSpaceAccessInput) ([]model.SpaceAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return nil, err
	}
	return e.accessManager.RulesForSpace(ctx, in.SpaceID)
}

func (e *defaultEngine) ensureSpaceAdmin(ctx context.Context, userID model.UserID, spaceID model.SpaceID) error {
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, space.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	canAdmin, err := e.canAdminSpace(ctx, userID, spaceID)
	if err != nil {
		return err
	}
	if !canAdmin {
		return ErrUnauthorized
	}
	return nil
}

func (e *defaultEngine) OpenSession(ctx context.Context, in OpenSessionInput) (graph.Session, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if auth.UserID == uuid.Nil {
		return nil, ErrUnauthorized
	}

	spaceID := in.SpaceID
	if spaceID == uuid.Nil && len(auth.SpaceIDs) > 0 {
		spaceID = auth.SpaceIDs[0]
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}

	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, space.ErrSpaceNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	canRead, err := e.canReadSpace(ctx, auth.UserID, spaceID)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrUnauthorized
	}
	canWrite, err := e.canWriteSpace(ctx, auth.UserID, spaceID)
	if err != nil {
		return nil, err
	}

	if err := ensureGraphSpaceDir(e.dataDir, spaceID); err != nil {
		return nil, err
	}
	return graphstore.NewSession(
		graphsDir(e.dataDir),
		spaceID,
		e.templateManager,
		graphstore.Permissions{Read: canRead, Write: canWrite},
		graphstore.Errors{Closed: ErrClosed, NotFound: ErrNotFound, Unauthorized: ErrUnauthorized, Conflict: ErrConflict},
	), nil
}

func (e *defaultEngine) Close() error {
	e.state = EngineStateClose
	e.authMu.Lock()
	e.authCache = map[AccessToken]authClaims{}
	e.authMu.Unlock()
	return nil
}

func (e *defaultEngine) authClaimsForAccessToken(ctx context.Context, accessToken AccessToken) (authClaims, error) {
	if err := ctx.Err(); err != nil {
		return authClaims{}, err
	}
	if accessToken == "" {
		return authClaims{}, ErrUnauthorized
	}

	e.authMu.Lock()
	defer e.authMu.Unlock()
	claims, ok := e.authCache[accessToken]
	if !ok {
		return authClaims{}, ErrUnauthorized
	}
	if claims.EXP <= time.Now().Unix() {
		delete(e.authCache, accessToken)
		return authClaims{}, ErrUnauthorized
	}
	return claims, nil
}

func (e *defaultEngine) grantSpaceToCachedClaims(accessToken AccessToken, spaceID model.SpaceID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	claims, ok := e.authCache[accessToken]
	if !ok || containsSpaceID(claims.SpaceIDs, spaceID) {
		return
	}
	claims.SpaceIDs = append(claims.SpaceIDs, spaceID)
	e.authCache[accessToken] = claims
}

func (e *defaultEngine) deleteSpaceByID(ctx context.Context, spaceID model.SpaceID) error {
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, space.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	if err := e.templateManager.DeleteForSpace(ctx, spaceID); err != nil {
		return err
	}
	if err := e.accessManager.DeleteForSpace(ctx, spaceID); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(graphsDir(e.dataDir), spaceID.String())); err != nil {
		return err
	}
	if err := e.spaceManager.DeleteByID(ctx, spaceID); err != nil {
		if errors.Is(err, space.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	e.purgeCachedSpace(spaceID)
	return nil
}

func (e *defaultEngine) ensureNotLastSuperuser(ctx context.Context, userID model.UserID) error {
	roles, err := e.accessManager.SystemRolesForUser(ctx, userID)
	if err != nil {
		return err
	}
	if !containsSystemRole(roles, model.SystemRoleSuperuser) {
		return nil
	}
	rules, err := e.accessManager.SystemRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.UserID != userID && containsSystemRole(rule.Roles, model.SystemRoleSuperuser) {
			return nil
		}
	}
	return access.ErrLastSuperuser
}

func (e *defaultEngine) purgeCachedClaimsForUser(userID model.UserID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	for token, claims := range e.authCache {
		if claims.UserID == userID {
			delete(e.authCache, token)
		}
	}
}

func (e *defaultEngine) purgeCachedSpace(spaceID model.SpaceID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	for token, claims := range e.authCache {
		filtered := claims.SpaceIDs[:0]
		for _, existing := range claims.SpaceIDs {
			if existing != spaceID {
				filtered = append(filtered, existing)
			}
		}
		claims.SpaceIDs = filtered
		e.authCache[token] = claims
	}
}

func (e *defaultEngine) canReadSpace(ctx context.Context, userID model.UserID, spaceID model.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, model.SpacePermissionRead)
}

func (e *defaultEngine) canWriteSpace(ctx context.Context, userID model.UserID, spaceID model.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, model.SpacePermissionWrite)
}

func (e *defaultEngine) canAdminSpace(ctx context.Context, userID model.UserID, spaceID model.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, model.SpacePermissionAdmin)
}

func (e *defaultEngine) canAdminSystem(ctx context.Context, userID model.UserID) (bool, error) {
	return e.accessManager.CanSystem(ctx, userID, model.SystemPermissionManageAccess)
}

func scopesForSystemRoles(roles []model.SystemRole) []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(scope string) {
		if _, ok := seen[scope]; ok {
			return
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	for _, role := range roles {
		if model.RoleAllows(role, model.SystemPermissionManageUsers) {
			add("admin:users")
		}
		if model.RoleAllows(role, model.SystemPermissionCreateSpaces) {
			add("space:create")
		}
		if model.RoleAllows(role, model.SystemPermissionManageAccess) {
			add("access:manage")
			add("template:write")
			add("graph:read")
			add("graph:write")
		}
		if model.RoleAllows(role, model.SystemPermissionOperateSystem) {
			add("system:operate")
		}
	}
	return out
}

func ensureStorageLayout(dataDir string) error {
	for _, dir := range []string{dataDir, metaDir(dataDir), graphsDir(dataDir), templatesDir(dataDir)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func metaDir(dataDir string) string {
	return filepath.Join(dataDir, "meta")
}

func graphsDir(dataDir string) string {
	return filepath.Join(dataDir, "graphs")
}

func templatesDir(dataDir string) string {
	return filepath.Join(metaDir(dataDir), "templates")
}

func ensureGraphSpaceDir(dataDir string, spaceID model.SpaceID) error {
	path := filepath.Join(graphsDir(dataDir), spaceID.String())
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, ".space"), []byte("active\n"), 0o600)
}

func contains(items []string, wanted string) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsUserID(items []model.UserID, wanted model.UserID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSpaceID(items []model.SpaceID, wanted model.SpaceID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSystemRole(items []model.SystemRole, wanted model.SystemRole) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}
