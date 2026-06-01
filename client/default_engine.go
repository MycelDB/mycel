package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	userManager *usermgmt.DefaultUserManager
}

// DefaultEngine opens (or creates) a local embedded KnotDB runtime.
func DefaultEngine(cfg EngineConfig) (Engine, error) {
	e := &defaultEngine{state: EngineStateClose}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
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

	um := usermgmt.NewDefaultUserManager()
	if err := um.Init(context.Background(), cfg.DataDir, cfg.UserStoreEncryptionKeyB64); err != nil {
		e.state = EngineStateClose
		return err
	}

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

	e.dataDir = cfg.DataDir
	e.userManager = um
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

func (e *defaultEngine) Authenticate(ctx context.Context, in AuthInput) (AuthToken, error) {
	if err := e.Ready(ctx); err != nil {
		return AuthToken{}, err
	}
	if in.UserRef == "" || in.Password == "" {
		return AuthToken{}, fmt.Errorf("%w: user_ref and password are required", ErrInvalidCredentials)
	}

	user, err := e.userManager.Authenticate(ctx, in.UserRef, in.Password)
	if err != nil {
		if errors.Is(err, usermgmt.ErrUserNotFound) || errors.Is(err, usermgmt.ErrInvalidInput) {
			return AuthToken{}, ErrInvalidCredentials
		}
		return AuthToken{}, err
	}
	if user.Status != model.UserStatusActive {
		return AuthToken{}, ErrInvalidCredentials
	}

	now := time.Now().Unix()
	exp := now + 3600
	uid := user.ID.String()
	return AuthToken{
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
	}, nil
}

func (e *defaultEngine) CreateDatabase(ctx context.Context, in CreateDatabaseInput) (DatabaseInfo, error) {
	if err := e.Ready(ctx); err != nil {
		return DatabaseInfo{}, err
	}
	if in.Name == "" {
		return DatabaseInfo{}, fmt.Errorf("%w: database name is required", ErrInvalidConfig)
	}
	if in.Auth.UserID == uuid.Nil {
		return DatabaseInfo{}, ErrUnauthorized
	}
	if in.Auth.EXP <= time.Now().Unix() {
		return DatabaseInfo{}, ErrUnauthorized
	}
	if !contains(in.Auth.Scopes, "db:create") && !contains(in.Auth.Roles, "admin") {
		return DatabaseInfo{}, ErrUnauthorized
	}

	ownerID := "owner:" + in.Auth.UserID.String()
	if len(in.Auth.OwnerIDs) > 0 && in.Auth.OwnerIDs[0] != "" {
		ownerID = in.Auth.OwnerIDs[0]
	}

	owners, err := readOwnersFile(e.dataDir)
	if err != nil {
		return DatabaseInfo{}, err
	}
	if !ownerExists(owners, ownerID) {
		owners = append(owners, ownerRecord{OwnerID: ownerID, UserID: in.Auth.UserID.String(), Status: "active"})
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
	if in.Auth.UserID == uuid.Nil || in.Auth.EXP <= time.Now().Unix() {
		return nil, ErrUnauthorized
	}
	if !contains(in.Auth.Scopes, "graph:read") && !contains(in.Auth.Scopes, "graph:write") && !contains(in.Auth.Roles, "admin") {
		return nil, ErrUnauthorized
	}

	spaceID := in.SpaceID
	if spaceID == "" && len(in.Auth.SpaceIDs) > 0 {
		spaceID = in.Auth.SpaceIDs[0]
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

	if !contains(in.Auth.Roles, "admin") {
		if !contains(in.Auth.SpaceIDs, spaceID) && !contains(in.Auth.OwnerIDs, space.OwnerID) {
			return nil, ErrUnauthorized
		}
	}

	return &defaultGraphSession{dataDir: e.dataDir, spaceID: spaceID}, nil
}

func (e *defaultEngine) Close() error {
	e.state = EngineStateClose
	return nil
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
