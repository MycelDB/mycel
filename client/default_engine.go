package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"knot_db/api"
	"knot_db/core/spacemgmt"
	"knot_db/core/usermgmt"
	"knot_db/model"
)

var (
	ErrInvalidConfig      = errors.New("invalid engine config")
	ErrNotReady           = errors.New("engine not ready")
	ErrClosed             = errors.New("engine closed")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrNotFound           = errors.New("not found")
)

type defaultEngine struct {
	state        EngineState
	dataDir      string
	userManager  usermgmt.UserManager
	spaceManager spacemgmt.SpaceManager
	authMu       sync.RWMutex
	authCache    map[AccessToken]authClaims
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
	Roles    []string
	OwnerIDs []model.UserID
	SpaceIDs []model.SpaceID
	Scopes   []string
}

// NewEngine opens (or creates) a local embedded KnotDB runtime.
//
// If userManager or spaceManager is nil, default file-backed managers are used.
func NewEngine(cfg EngineConfig, userManager usermgmt.UserManager, spaceManager spacemgmt.SpaceManager) (Engine, error) {
	e := &defaultEngine{state: EngineStateClose, userManager: userManager, spaceManager: spaceManager, authCache: map[AccessToken]authClaims{}}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
}

// DefaultEngine opens (or creates) a local embedded KnotDB runtime.
// Deprecated: use NewEngine(cfg, userManager, spaceManager) instead.
func DefaultEngine(cfg EngineConfig) (Engine, error) {
	return NewEngine(cfg, nil, nil)
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
			if cfg.AdminUsername == "" {
				e.state = EngineStateClose
				return fmt.Errorf("%w: admin_username is required when creating a standalone store", ErrInvalidConfig)
			}
			if cfg.AdminPassword == "" {
				e.state = EngineStateClose
				return fmt.Errorf("%w: admin_password is required when creating a standalone store", ErrInvalidConfig)
			}
			if mkErr := os.MkdirAll(cfg.DataDir, 0o755); mkErr != nil {
				e.state = EngineStateClose
				return mkErr
			}
			created = true
		} else {
			e.state = EngineStateClose
			return err
		}
	}

	if e.userManager == nil {
		e.userManager = usermgmt.NewUserManager()
	}
	if err := e.userManager.Init(context.Background(), cfg.DataDir, cfg.UserStoreEncryptionKeyB64); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.spaceManager == nil {
		e.spaceManager = spacemgmt.NewSpaceManager()
	}
	if err := e.spaceManager.Init(context.Background(), cfg.DataDir); err != nil {
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
		if !exists {
			status := model.UserStatusActive
			username := cfg.AdminUsername
			_, err := um.Create(context.Background(), usermgmt.CreateUserInput{
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

	user, err := e.userManager.Authenticate(ctx, in.UserRef, in.Password)
	if err != nil {
		if errors.Is(err, usermgmt.ErrUserNotFound) || errors.Is(err, usermgmt.ErrInvalidInput) {
			return AuthResult{}, ErrInvalidCredentials
		}
		return AuthResult{}, err
	}
	if user.Status != model.UserStatusActive {
		return AuthResult{}, ErrInvalidCredentials
	}

	now := time.Now().Unix()
	exp := now + 3600
	uid := user.ID.String()
	accessToken := AccessToken(uuid.NewString())
	claims := authClaims{
		Iss:      "knotdb",
		Sub:      "user:" + uid,
		Aud:      "knotdb",
		JTI:      uuid.NewString(),
		IAT:      now,
		EXP:      exp,
		UserID:   user.ID,
		UserRef:  user.Ref,
		Roles:    []string{"admin"},
		OwnerIDs: []model.UserID{user.ID},
		SpaceIDs: []model.SpaceID{},
		Scopes:   []string{"graph:read", "graph:write", "admin:users", "space:create"},
	}

	e.authMu.Lock()
	if e.authCache == nil {
		e.authCache = map[AccessToken]authClaims{}
	}
	e.authCache[accessToken] = claims
	e.authMu.Unlock()

	return AuthResult{AccessToken: accessToken}, nil
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
	if !contains(auth.Scopes, "space:create") && !contains(auth.Roles, "admin") {
		return SpaceInfo{}, ErrUnauthorized
	}

	space, err := e.spaceManager.Create(ctx, spacemgmt.CreateSpaceInput{OwnerID: auth.UserID, Name: in.Name})
	if err != nil {
		return SpaceInfo{}, err
	}

	e.grantSpaceToCachedClaims(in.AccessToken, space.SpaceID)
	return SpaceInfo{OwnerID: space.OwnerID, SpaceID: space.SpaceID, Name: space.Name}, nil
}

func (e *defaultEngine) OpenSession(ctx context.Context, in OpenSessionInput) (api.GraphSession, error) {
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
	if !contains(auth.Scopes, "graph:read") && !contains(auth.Scopes, "graph:write") && !contains(auth.Roles, "admin") {
		return nil, ErrUnauthorized
	}

	spaceID := in.SpaceID
	if spaceID == uuid.Nil && len(auth.SpaceIDs) > 0 {
		spaceID = auth.SpaceIDs[0]
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}

	space, err := e.spaceManager.GetByID(ctx, spaceID)
	if err != nil {
		if errors.Is(err, spacemgmt.ErrSpaceNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if !contains(auth.Roles, "admin") {
		if !containsSpaceID(auth.SpaceIDs, spaceID) && !containsUserID(auth.OwnerIDs, space.OwnerID) {
			return nil, ErrUnauthorized
		}
	}

	return &defaultGraphSession{dataDir: e.dataDir, spaceID: spaceID}, nil
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
