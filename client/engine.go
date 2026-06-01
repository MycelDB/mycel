package client

import (
	"context"

	"knot_db/api"
)

// EngineMode defines runtime mode for KnotDB engine startup.
type EngineMode string

const (
	// EngineModeStandalone starts KnotDB as a standalone embedded engine.
	EngineModeStandalone EngineMode = "standalone"
)

// EngineState defines the lifecycle state of the engine runtime.
type EngineState string

const (
	EngineStateOpen  EngineState = "open"
	EngineStateReady EngineState = "ready"
	EngineStateClose EngineState = "close"
)

// EngineConfig defines startup configuration for DefaultEngine.
type EngineConfig struct {
	DataDir                   string
	Mode                      EngineMode
	CreateIfMissing           bool
	AdminUsername             string
	AdminPassword             string
	UserStoreEncryptionKeyB64 string
}

// Engine represents a running KnotDB engine runtime in-process.
type Engine interface {
	// Open initializes the engine runtime with the given configuration.
	Open(cfg EngineConfig) error
	// Ready reports whether the engine is ready to serve operations.
	Ready(ctx context.Context) error
	// Authenticate validates credentials and returns token claims.
	Authenticate(ctx context.Context, in AuthInput) (AuthToken, error)
	// CreateDatabase creates or returns a database (space) for an authorized user.
	CreateDatabase(ctx context.Context, in CreateDatabaseInput) (DatabaseInfo, error)
	// OpenSession opens a graph session for an authorized token/space scope.
	OpenSession(ctx context.Context, in OpenSessionInput) (api.GraphSession, error)
	// Close releases engine resources.
	Close() error
}
