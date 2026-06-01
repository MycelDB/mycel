package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"knot_db/core/identity"
)

var (
	ErrInvalidConfig      = errors.New("invalid engine config")
	ErrNotReady           = errors.New("engine not ready")
	ErrClosed             = errors.New("engine closed")
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type defaultEngine struct {
	state   EngineState
	dataDir string
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
			if writeErr := writeInitialUsersFile(cfg.DataDir, cfg.AdminUsername); writeErr != nil {
				e.state = EngineStateClose
				return writeErr
			}
			if writeErr := writeInitialPasswordsFile(cfg.DataDir, cfg.AdminUsername, cfg.AdminPassword); writeErr != nil {
				e.state = EngineStateClose
				return writeErr
			}
		} else {
			e.state = EngineStateClose
			return err
		}
	}

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

func (e *defaultEngine) Authenticate(ctx context.Context, in AuthInput) (AuthToken, error) {
	if err := e.Ready(ctx); err != nil {
		return AuthToken{}, err
	}
	if in.UserRef == "" || in.Password == "" {
		return AuthToken{}, fmt.Errorf("%w: user_ref and password are required", ErrInvalidCredentials)
	}

	users, err := readUsersFile(e.dataDir)
	if err != nil {
		return AuthToken{}, err
	}
	passwords, err := readPasswordsFile(e.dataDir)
	if err != nil {
		return AuthToken{}, err
	}

	var found bool
	for _, u := range users {
		if u.Ref == in.UserRef && u.Status == identity.UserStatusActive {
			found = true
			break
		}
	}
	if !found {
		return AuthToken{}, ErrInvalidCredentials
	}

	storedPwd, ok := passwords[string(in.UserRef)]
	if !ok || storedPwd != in.Password {
		return AuthToken{}, ErrInvalidCredentials
	}

	return AuthToken{AccessToken: uuid.NewString()}, nil
}

func (e *defaultEngine) Close() error {
	e.state = EngineStateClose
	return nil
}

func writeInitialUsersFile(dataDir, adminUsername string) error {
	path := filepath.Join(dataDir, "users.json")
	username := adminUsername
	content := []identity.User{{
		ID:       uuid.New(),
		Ref:      identity.UserRef(adminUsername),
		Username: &username,
		Status:   identity.UserStatusActive,
	}}
	b, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o600)
}

type passwordFile struct {
	Passwords map[string]string `json:"passwords"`
}

func writeInitialPasswordsFile(dataDir, adminUsername, adminPassword string) error {
	path := filepath.Join(dataDir, "users_passwords.json")
	content := passwordFile{Passwords: map[string]string{adminUsername: adminPassword}}
	b, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o600)
}

func readUsersFile(dataDir string) ([]identity.User, error) {
	path := filepath.Join(dataDir, "users.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var users []identity.User
	if err := json.Unmarshal(raw, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func readPasswordsFile(dataDir string) (map[string]string, error) {
	path := filepath.Join(dataDir, "users_passwords.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pf passwordFile
	if err := json.Unmarshal(raw, &pf); err != nil {
		return nil, err
	}
	if pf.Passwords == nil {
		pf.Passwords = map[string]string{}
	}
	return pf.Passwords, nil
}
