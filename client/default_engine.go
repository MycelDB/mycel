package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"knot_db/api"
	"knot_db/core/model"
	"knot_db/core/usermgmt"
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
	state       EngineState
	dataDir     string
	userManager usermgmt.UserManager
	authMu      sync.RWMutex
	authCache   map[AccessToken]authClaims
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
	OwnerIDs []string
	SpaceIDs []string
	Scopes   []string
}

// NewEngine opens (or creates) a local embedded KnotDB runtime.
//
// If userManager is nil, a default file-backed user manager is used.
func NewEngine(cfg EngineConfig, userManager usermgmt.UserManager) (Engine, error) {
	e := &defaultEngine{state: EngineStateClose, userManager: userManager, authCache: map[AccessToken]authClaims{}}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
}

// DefaultEngine opens (or creates) a local embedded KnotDB runtime.
// Deprecated: use NewEngine(cfg, userManager) instead.
func DefaultEngine(cfg EngineConfig) (Engine, error) {
	return NewEngine(cfg, nil)
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
				return fmt.Errorf("%w: admin_username is required when creating a standalone database", ErrInvalidConfig)
			}
			if cfg.AdminPassword == "" {
				e.state = EngineStateClose
				return fmt.Errorf("%w: admin_password is required when creating a standalone database", ErrInvalidConfig)
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
		OwnerIDs: []string{"owner:" + uid},
		SpaceIDs: []string{"space:" + uid + ":default"},
		Scopes:   []string{"graph:read", "graph:write", "admin:users", "db:create"},
	}

	e.authMu.Lock()
	if e.authCache == nil {
		e.authCache = map[AccessToken]authClaims{}
	}
	e.authCache[accessToken] = claims
	e.authMu.Unlock()

	return AuthResult{AccessToken: accessToken}, nil
}

func (e *defaultEngine) CreateDatabase(ctx context.Context, in CreateDatabaseInput) (DatabaseInfo, error) {
	if err := e.Ready(ctx); err != nil {
		return DatabaseInfo{}, err
	}
	if in.Name == "" {
		return DatabaseInfo{}, fmt.Errorf("%w: database name is required", ErrInvalidConfig)
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return DatabaseInfo{}, err
	}
	if auth.UserID == uuid.Nil {
		return DatabaseInfo{}, ErrUnauthorized
	}
	if !contains(auth.Scopes, "db:create") && !contains(auth.Roles, "admin") {
		return DatabaseInfo{}, ErrUnauthorized
	}

	ownerID := "owner:" + auth.UserID.String()
	if len(auth.OwnerIDs) > 0 && auth.OwnerIDs[0] != "" {
		ownerID = auth.OwnerIDs[0]
	}

	owners, err := readOwnersFile(e.dataDir)
	if err != nil {
		return DatabaseInfo{}, err
	}
	if !ownerExists(owners, ownerID) {
		owners = append(owners, ownerRecord{OwnerID: ownerID, UserID: auth.UserID.String(), Status: "active"})
		if err := writeOwnersFile(e.dataDir, owners); err != nil {
			return DatabaseInfo{}, err
		}
	}

	spaces, err := readSpacesFile(e.dataDir)
	if err != nil {
		return DatabaseInfo{}, err
	}
	for _, s := range spaces {
		if s.OwnerID == ownerID && s.Name == in.Name {
			return DatabaseInfo{OwnerID: s.OwnerID, SpaceID: s.SpaceID, Name: s.Name}, nil
		}
	}

	space := spaceRecord{SpaceID: "space:" + uuid.NewString(), OwnerID: ownerID, Name: in.Name, Status: "active"}
	spaces = append(spaces, space)
	if err := writeSpacesFile(e.dataDir, spaces); err != nil {
		return DatabaseInfo{}, err
	}

	return DatabaseInfo{OwnerID: ownerID, SpaceID: space.SpaceID, Name: space.Name}, nil
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
	if spaceID == "" && len(auth.SpaceIDs) > 0 {
		spaceID = auth.SpaceIDs[0]
	}
	if spaceID == "" {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}

	spaces, err := readSpacesFile(e.dataDir)
	if err != nil {
		return nil, err
	}
	var space *spaceRecord
	for i := range spaces {
		if spaces[i].SpaceID == spaceID {
			space = &spaces[i]
			break
		}
	}
	if space == nil {
		return nil, ErrNotFound
	}

	if !contains(auth.Roles, "admin") {
		if !contains(auth.SpaceIDs, spaceID) && !contains(auth.OwnerIDs, space.OwnerID) {
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

type ownerRecord struct {
	OwnerID string `json:"owner_id"`
	UserID  string `json:"user_id"`
	Status  string `json:"status"`
}

type spaceRecord struct {
	SpaceID string `json:"space_id"`
	OwnerID string `json:"owner_id"`
	Name    string `json:"name"`
	Status  string `json:"status"`
}

func readOwnersFile(dataDir string) ([]ownerRecord, error) {
	path := filepath.Join(dataDir, "owners.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []ownerRecord{}, nil
		}
		return nil, err
	}
	var out []ownerRecord
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeOwnersFile(dataDir string, owners []ownerRecord) error {
	path := filepath.Join(dataDir, "owners.json")
	b, err := json.MarshalIndent(owners, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o600)
}

func readSpacesFile(dataDir string) ([]spaceRecord, error) {
	path := filepath.Join(dataDir, "spaces.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []spaceRecord{}, nil
		}
		return nil, err
	}
	var out []spaceRecord
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeSpacesFile(dataDir string, spaces []spaceRecord) error {
	path := filepath.Join(dataDir, "spaces.json")
	b, err := json.MarshalIndent(spaces, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o600)
}

func ownerExists(owners []ownerRecord, ownerID string) bool {
	for _, o := range owners {
		if o.OwnerID == ownerID {
			return true
		}
	}
	return false
}

func contains(items []string, wanted string) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}
