package client

import (
	"context"
	"errors"
	"fmt"
	"os"
)

var (
	ErrInvalidConfig = errors.New("invalid engine config")
	ErrNotReady      = errors.New("engine not ready")
	ErrClosed        = errors.New("engine closed")
)

type runtimeEngine struct {
	state EngineState
}

// DefaultEngine opens (or creates) a local embedded KnotDB runtime.
func DefaultEngine(cfg EngineConfig) (Engine, error) {
	e := &runtimeEngine{state: EngineStateClose}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *runtimeEngine) Open(cfg EngineConfig) error {
	e.state = EngineStateOpen

	if cfg.DataDir == "" {
		e.state = EngineStateClose
		return fmt.Errorf("%w: data_dir is required", ErrInvalidConfig)
	}
	if cfg.Mode == "" {
		cfg.Mode = EngineModeStandalone
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
		} else {
			e.state = EngineStateClose
			return err
		}
	}

	e.state = EngineStateReady
	return nil
}

func (e *runtimeEngine) Ready(ctx context.Context) error {
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

func (e *runtimeEngine) Close() error {
	e.state = EngineStateClose
	return nil
}
