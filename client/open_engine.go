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
	ready  bool
	closed bool
}

// OpenEngine opens (or creates) a local embedded KnotDB runtime.
func OpenEngine(cfg EngineConfig) (Engine, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("%w: data_dir is required", ErrInvalidConfig)
	}
	if cfg.Mode == "" {
		cfg.Mode = EngineModeStandalone
	}
	if cfg.Mode != EngineModeStandalone {
		return nil, fmt.Errorf("%w: unsupported mode %q", ErrInvalidConfig, cfg.Mode)
	}

	if _, err := os.Stat(cfg.DataDir); err != nil {
		if os.IsNotExist(err) {
			if !cfg.CreateIfMissing {
				return nil, fmt.Errorf("%w: data_dir does not exist", ErrInvalidConfig)
			}
			if mkErr := os.MkdirAll(cfg.DataDir, 0o755); mkErr != nil {
				return nil, mkErr
			}
		} else {
			return nil, err
		}
	}

	return &runtimeEngine{ready: true}, nil
}

func (e *runtimeEngine) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.closed {
		return ErrClosed
	}
	if !e.ready {
		return ErrNotReady
	}
	return nil
}

func (e *runtimeEngine) Close() error {
	e.closed = true
	e.ready = false
	return nil
}
