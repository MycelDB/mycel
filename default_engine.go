package knotdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	coreaccess "martinbeauvais.com/mbgit/knotbase/knotdb/core/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/space"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/user"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/graphstore"
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
	accessManager   coreaccess.Manager
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
	UserID   identity.UserID
	UserRef  identity.UserRef
	Roles    []access.SystemRole
	OwnerIDs []identity.UserID
	SpaceIDs []identity.SpaceID
	Scopes   []string
}

// NewEngine opens (or creates) a local embedded KnotDB runtime.
//
// If userManager, spaceManager, templateManager, or accessManager is nil, default file-backed managers are used.
func NewEngine(cfg EngineConfig, userManager user.Manager, spaceManager space.Manager, templateManager coretemplate.Manager, accessManager coreaccess.Manager) (Engine, error) {
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
	} else {
		storeState, err := inspectMetadataStore(cfg.DataDir)
		if err != nil {
			e.state = EngineStateClose
			return err
		}
		switch {
		case storeState.complete:
			created = false
		case storeState.empty:
			if !cfg.CreateIfMissing {
				e.state = EngineStateClose
				return fmt.Errorf("%w: data_dir is not initialized", ErrInvalidConfig)
			}
			created = true
		default:
			e.state = EngineStateClose
			return fmt.Errorf("%w: incomplete metadata store; missing %s", ErrInvalidConfig, strings.Join(storeState.missing, ", "))
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
		e.accessManager = coreaccess.NewManager()
	}
	if err := e.accessManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}

	um := e.userManager

	if created {
		exists, err := um.ExistsByRef(context.Background(), identity.UserRef(cfg.AdminUsername))
		if err != nil {
			e.state = EngineStateClose
			return err
		}
		var admin identity.User
		if !exists {
			status := identity.UserStatusActive
			username := cfg.AdminUsername
			admin, err = um.Create(context.Background(), user.CreateInput{
				User: identity.UserInput{
					Ref:      identity.UserRef(cfg.AdminUsername),
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
			admin, err = um.GetByRef(context.Background(), identity.UserRef(cfg.AdminUsername))
			if err != nil {
				e.state = EngineStateClose
				return err
			}
		}
		if _, err := e.accessManager.GrantSystemRole(context.Background(), coreaccess.GrantSystemRoleInput{
			UserID: admin.ID,
			Roles:  []access.SystemRole{access.SystemRoleSuperuser},
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
	if account.Status != identity.UserStatusActive {
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
		OwnerIDs: []identity.UserID{account.ID},
		SpaceIDs: []identity.SpaceID{},
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

func (e *defaultEngine) CreateUser(ctx context.Context, in CreateUserInput) (identity.User, error) {
	if err := e.Ready(ctx); err != nil {
		return identity.User{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return identity.User{}, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
	if err != nil {
		return identity.User{}, err
	}
	if !canManageUsers {
		return identity.User{}, ErrUnauthorized
	}
	created, err := e.userManager.Create(ctx, user.CreateInput{User: in.User, Password: in.Password})
	if err != nil {
		if errors.Is(err, user.ErrInvalidInput) {
			return identity.User{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		return identity.User{}, err
	}
	return created, nil
}

func (e *defaultEngine) ListUsers(ctx context.Context, in ListUsersInput) ([]identity.User, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
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
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
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
	canCreateSpace, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionCreateSpaces)
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
	if _, err := e.accessManager.Grant(ctx, coreaccess.GrantInput{
		SpaceID:     sp.SpaceID,
		UserID:      auth.UserID,
		Permissions: []access.SpacePermission{access.SpacePermissionAdmin},
	}); err != nil {
		return SpaceInfo{}, err
	}

	e.grantSpaceToCachedClaims(in.AccessToken, sp.SpaceID)
	return SpaceInfo{OwnerID: sp.OwnerID, SpaceID: sp.SpaceID, Name: sp.Name}, nil
}

func (e *defaultEngine) ListSpaces(ctx context.Context, in ListSpacesInput) ([]identity.Space, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	spaces, err := e.spaceManager.List(ctx)
	if err != nil {
		return nil, err
	}
	canManageAccess, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return nil, err
	}
	if canManageAccess {
		return spaces, nil
	}
	accessible := []identity.Space{}
	for _, sp := range spaces {
		canRead, err := e.canReadSpace(ctx, auth.UserID, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		if canRead {
			accessible = append(accessible, sp)
		}
	}
	return accessible, nil
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

func (e *defaultEngine) ListTemplates(ctx context.Context, in ListTemplatesInput) ([]graph.Template, error) {
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
	canRead, err := e.canReadSpace(ctx, auth.UserID, in.SpaceID)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrUnauthorized
	}
	return e.templateManager.ListBySpace(ctx, in.SpaceID)
}

func (e *defaultEngine) GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (access.SystemAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return access.SystemAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return access.SystemAccessRule{}, err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return access.SystemAccessRule{}, err
	}
	if !canManage {
		return access.SystemAccessRule{}, ErrUnauthorized
	}
	return e.accessManager.GrantSystemRole(ctx, coreaccess.GrantSystemRoleInput{UserID: in.UserID, Roles: in.Roles})
}

func (e *defaultEngine) RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return err
	}
	if !canManage {
		return ErrUnauthorized
	}
	return e.accessManager.RevokeSystemRole(ctx, coreaccess.RevokeSystemRoleInput{UserID: in.UserID})
}

func (e *defaultEngine) ListSystemAccess(ctx context.Context, in ListSystemAccessInput) ([]access.SystemAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return nil, err
	}
	if !canManage {
		return nil, ErrUnauthorized
	}
	return e.accessManager.SystemRules(ctx)
}

func (e *defaultEngine) GrantSpaceAccess(ctx context.Context, in GrantSpaceAccessInput) (access.SpaceAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return access.SpaceAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return access.SpaceAccessRule{}, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return access.SpaceAccessRule{}, err
	}
	return e.accessManager.Grant(ctx, coreaccess.GrantInput{SpaceID: in.SpaceID, UserID: in.UserID, Permissions: in.Permissions})
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
	return e.accessManager.Revoke(ctx, coreaccess.RevokeInput{SpaceID: in.SpaceID, UserID: in.UserID})
}

func (e *defaultEngine) ListSpaceAccess(ctx context.Context, in ListSpaceAccessInput) ([]access.SpaceAccessRule, error) {
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

func (e *defaultEngine) ensureSpaceAdmin(ctx context.Context, userID identity.UserID, spaceID identity.SpaceID) error {
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

func (e *defaultEngine) grantSpaceToCachedClaims(accessToken AccessToken, spaceID identity.SpaceID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	claims, ok := e.authCache[accessToken]
	if !ok || containsSpaceID(claims.SpaceIDs, spaceID) {
		return
	}
	claims.SpaceIDs = append(claims.SpaceIDs, spaceID)
	e.authCache[accessToken] = claims
}

func (e *defaultEngine) deleteSpaceByID(ctx context.Context, spaceID identity.SpaceID) error {
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

func (e *defaultEngine) ensureNotLastSuperuser(ctx context.Context, userID identity.UserID) error {
	roles, err := e.accessManager.SystemRolesForUser(ctx, userID)
	if err != nil {
		return err
	}
	if !containsSystemRole(roles, access.SystemRoleSuperuser) {
		return nil
	}
	rules, err := e.accessManager.SystemRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.UserID != userID && containsSystemRole(rule.Roles, access.SystemRoleSuperuser) {
			return nil
		}
	}
	return coreaccess.ErrLastSuperuser
}

func (e *defaultEngine) purgeCachedClaimsForUser(userID identity.UserID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	for token, claims := range e.authCache {
		if claims.UserID == userID {
			delete(e.authCache, token)
		}
	}
}

func (e *defaultEngine) purgeCachedSpace(spaceID identity.SpaceID) {
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

func (e *defaultEngine) canReadSpace(ctx context.Context, userID identity.UserID, spaceID identity.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionRead)
}

func (e *defaultEngine) canWriteSpace(ctx context.Context, userID identity.UserID, spaceID identity.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionWrite)
}

func (e *defaultEngine) canAdminSpace(ctx context.Context, userID identity.UserID, spaceID identity.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionAdmin)
}

func (e *defaultEngine) canAdminSystem(ctx context.Context, userID identity.UserID) (bool, error) {
	return e.accessManager.CanSystem(ctx, userID, access.SystemPermissionManageAccess)
}

func scopesForSystemRoles(roles []access.SystemRole) []string {
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
		if access.RoleAllows(role, access.SystemPermissionManageUsers) {
			add("admin:users")
		}
		if access.RoleAllows(role, access.SystemPermissionCreateSpaces) {
			add("space:create")
		}
		if access.RoleAllows(role, access.SystemPermissionManageAccess) {
			add("access:manage")
			add("template:write")
			add("graph:read")
			add("graph:write")
		}
		if access.RoleAllows(role, access.SystemPermissionOperateSystem) {
			add("system:operate")
		}
	}
	return out
}

type metadataStoreState struct {
	complete bool
	empty    bool
	missing  []string
}

func inspectMetadataStore(dataDir string) (metadataStoreState, error) {
	required := []string{
		filepath.Join(metaDir(dataDir), "users.json"),
		filepath.Join(metaDir(dataDir), "spaces.json"),
		filepath.Join(metaDir(dataDir), "access.json"),
	}
	existing := 0
	missing := []string{}
	for _, path := range required {
		exists, err := regularFileExists(path)
		if err != nil {
			return metadataStoreState{}, err
		}
		if exists {
			existing++
		} else {
			missing = append(missing, path)
		}
	}
	return metadataStoreState{complete: existing == len(required), empty: existing == 0, missing: missing}, nil
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("%w: metadata path is a directory: %s", ErrInvalidConfig, path)
	}
	return true, nil
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

func ensureGraphSpaceDir(dataDir string, spaceID identity.SpaceID) error {
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

func containsUserID(items []identity.UserID, wanted identity.UserID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSpaceID(items []identity.SpaceID, wanted identity.SpaceID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSystemRole(items []access.SystemRole, wanted access.SystemRole) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}
