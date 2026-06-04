package knotdb

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
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
	// Authenticate validates credentials and returns an external access token.
	Authenticate(ctx context.Context, in AuthInput) (AuthResult, error)
	// CreateUser creates a user for an authorized system user administrator.
	CreateUser(ctx context.Context, in CreateUserInput) (identity.User, error)
	// ListUsers lists users for an authorized system user administrator.
	ListUsers(ctx context.Context, in ListUsersInput) ([]identity.User, error)
	// DeleteUser hard-deletes a user and spaces owned by that user.
	DeleteUser(ctx context.Context, in DeleteUserInput) error
	// CreateSpace creates or returns a space for an authorized user.
	CreateSpace(ctx context.Context, in CreateSpaceInput) (SpaceInfo, error)
	// ListSpaces lists spaces for an authorized system access administrator.
	ListSpaces(ctx context.Context, in ListSpacesInput) ([]domainspace.Space, error)
	// DeleteSpace hard-deletes a space and all associated persisted constructs.
	DeleteSpace(ctx context.Context, in DeleteSpaceInput) error
	// GrantSystemRole grants or updates a user's system roles.
	GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (access.SystemAccessRule, error)
	// RevokeSystemRole revokes a user's system roles.
	RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error
	// ListSystemAccess lists system access rules.
	ListSystemAccess(ctx context.Context, in ListSystemAccessInput) ([]access.SystemAccessRule, error)
	// GrantSpaceAccess grants or updates a user's access to a space.
	GrantSpaceAccess(ctx context.Context, in GrantSpaceAccessInput) (access.SpaceAccessRule, error)
	// RevokeSpaceAccess revokes a user's access to a space.
	RevokeSpaceAccess(ctx context.Context, in RevokeSpaceAccessInput) error
	// ListSpaceAccess lists access rules for a space.
	ListSpaceAccess(ctx context.Context, in ListSpaceAccessInput) ([]access.SpaceAccessRule, error)
	// OpenSession opens a graph session for an authorized token/space scope.
	OpenSession(ctx context.Context, in OpenSessionInput) (graph.Session, error)
	// Close releases engine resources.
	Close() error
}
