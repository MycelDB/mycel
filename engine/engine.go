// Package engine exposes the public KnotDB engine API and constructor.
package engine

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/core/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/space"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/core/user"
	domainaccess "martinbeauvais.com/mbgit/knotbase/knotdb/domain/access"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	engineinternal "martinbeauvais.com/mbgit/knotbase/knotdb/engine/internal"
	"martinbeauvais.com/mbgit/knotbase/knotdb/session"
)

type EngineMode = engineinternal.EngineMode

const EngineModeStandalone = engineinternal.EngineModeStandalone

type EngineState = engineinternal.EngineState

const (
	EngineStateOpen  = engineinternal.EngineStateOpen
	EngineStateReady = engineinternal.EngineStateReady
	EngineStateClose = engineinternal.EngineStateClose
)

type EngineConfig = engineinternal.EngineConfig
type AccessToken = engineinternal.AccessToken
type AuthInput = engineinternal.AuthInput
type AuthResult = engineinternal.AuthResult
type CreateUserInput = engineinternal.CreateUserInput
type ListUsersInput = engineinternal.ListUsersInput
type DeleteUserInput = engineinternal.DeleteUserInput
type CreateSpaceInput = engineinternal.CreateSpaceInput
type ListSpacesInput = engineinternal.ListSpacesInput
type DeleteSpaceInput = engineinternal.DeleteSpaceInput
type SpaceInfo = engineinternal.SpaceInfo
type GrantSystemRoleInput = engineinternal.GrantSystemRoleInput
type RevokeSystemRoleInput = engineinternal.RevokeSystemRoleInput
type ListSystemAccessInput = engineinternal.ListSystemAccessInput
type GrantSpaceAccessInput = engineinternal.GrantSpaceAccessInput
type RevokeSpaceAccessInput = engineinternal.RevokeSpaceAccessInput
type ListSpaceAccessInput = engineinternal.ListSpaceAccessInput
type OpenSessionInput = engineinternal.OpenSessionInput

const EnvDataDir = engineinternal.EnvDataDir

var (
	ErrInvalidConfig      = engineinternal.ErrInvalidConfig
	ErrNotReady           = engineinternal.ErrNotReady
	ErrClosed             = engineinternal.ErrClosed
	ErrInvalidCredentials = engineinternal.ErrInvalidCredentials
	ErrUnauthorized       = engineinternal.ErrUnauthorized
	ErrNotFound           = engineinternal.ErrNotFound
	ErrConflict           = engineinternal.ErrConflict
)

// Engine represents a running KnotDB engine runtime in-process.
type Engine interface {
	Open(cfg EngineConfig) error
	Ready(ctx context.Context) error
	Authenticate(ctx context.Context, in AuthInput) (AuthResult, error)
	CreateUser(ctx context.Context, in CreateUserInput) (identity.User, error)
	ListUsers(ctx context.Context, in ListUsersInput) ([]identity.User, error)
	DeleteUser(ctx context.Context, in DeleteUserInput) error
	CreateSpace(ctx context.Context, in CreateSpaceInput) (SpaceInfo, error)
	ListSpaces(ctx context.Context, in ListSpacesInput) ([]domainspace.Space, error)
	DeleteSpace(ctx context.Context, in DeleteSpaceInput) error
	GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (domainaccess.SystemAccessRule, error)
	RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error
	ListSystemAccess(ctx context.Context, in ListSystemAccessInput) ([]domainaccess.SystemAccessRule, error)
	GrantSpaceAccess(ctx context.Context, in GrantSpaceAccessInput) (domainaccess.SpaceAccessRule, error)
	RevokeSpaceAccess(ctx context.Context, in RevokeSpaceAccessInput) error
	ListSpaceAccess(ctx context.Context, in ListSpaceAccessInput) ([]domainaccess.SpaceAccessRule, error)
	OpenSession(ctx context.Context, in OpenSessionInput) (session.Session, error)
	Close() error
}

// NewEngine opens (or creates) a local embedded KnotDB runtime.
func NewEngine(cfg EngineConfig, userManager user.Manager, spaceManager space.Manager, templateManager coretemplate.Manager, accessManager access.Manager) (Engine, error) {
	return engineinternal.NewEngine(cfg, userManager, spaceManager, templateManager, accessManager)
}

// ResolveDataDir returns the explicit data directory when non-empty, otherwise
// the value of KNOTDB_DATA_DIR. A leading ~/ is expanded for convenience.
func ResolveDataDir(explicit string) string { return engineinternal.ResolveDataDir(explicit) }
