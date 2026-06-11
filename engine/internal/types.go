package internal

import (
	"time"

	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
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

// EngineConfig defines startup configuration for defaultEngine.
type EngineConfig struct {
	DataDir                   string
	Mode                      EngineMode
	CreateIfMissing           bool
	AdminUsername             string
	AdminPassword             string
	UserStoreEncryptionKeyB64 string
	AccessTokenTTL            time.Duration
	BlobLimits                sessionapi.BlobLimits
	BlobStaleTmpAge           time.Duration
}

type BlobLimits = sessionapi.BlobLimits
