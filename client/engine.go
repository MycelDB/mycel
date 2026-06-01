package client

import "context"

// EngineMode defines runtime mode for KnotDB engine startup.
type EngineMode string

const (
	// EngineModeStandalone starts KnotDB as a standalone embedded engine.
	EngineModeStandalone EngineMode = "standalone"
)

// EngineConfig defines startup configuration for OpenEngine.
type EngineConfig struct {
	DataDir         string
	Mode            EngineMode
	CreateIfMissing bool
}

// Engine represents a running KnotDB engine runtime in-process.
type Engine interface {
	// Ready reports whether the engine is ready to serve operations.
	Ready(ctx context.Context) error
	// Close releases engine resources.
	Close() error
}
