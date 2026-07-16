package user

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"github.com/myceldb/mycel/internal/wal"
)

type Module struct {
	store          Store
	sessions       storesession.Manager
	gate           *quiesce.Gate
	wal            *wal.Manager
	walProgress    wal.AppliedLSNStore
	walWaiter      *wal.ApplyWaiter
	writeAuthority func() error
}

func NewModule() *Module { return &Module{gate: quiesce.NewGate(ModuleName)} }

func Open(dataDir string) (*Module, error) {
	store, err := OpenExistingStore(filepath.Join(dataDir, "users"))
	if err != nil {
		return nil, err
	}
	mgr := storesession.NewManager()
	if err := mgr.Init(context.Background(), filepath.Join(dataDir, "users", "sessions")); err != nil {
		return nil, err
	}
	return &Module{store: store, sessions: mgr}, nil
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	userDir := filepath.Join(rt.Config.DataDir, "users")
	userDirCreated, err := ensureDir(userDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create users directory", err)
	}
	rt.Logger.Info("user directory ready", "path", userDir, "created", userDirCreated)

	store, storeCreated, err := OpenStore(userDir)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open user store", err)
	}
	m.store = store
	rt.Logger.Info("user store ready", "path", filepath.Join(userDir, StoreFilename), "created", storeCreated)

	sessionsDir := filepath.Join(userDir, "sessions")
	sessionsDirCreated, err := ensureDir(sessionsDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create user sessions directory", err)
	}
	rt.Logger.Info("user sessions directory ready", "path", sessionsDir, "created", sessionsDirCreated)
	sessions := storesession.NewManager()
	if err := sessions.Init(ctx, sessionsDir); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open user session store", err)
	}
	m.sessions = sessions
	m.wal = rt.WAL
	m.walProgress = rt.WALProgress
	m.walWaiter = rt.WALWaiter
	m.writeAuthority = rt.RequireWriteAuthority
	if rt.WALRegistry != nil {
		if err := rt.WALRegistry.Register(recordTypeUserPut, wal.ApplierFunc(m.applyUserPut)); err != nil {
			return daemonruntime.Abort(ModuleName, "wal", "register user put WAL applier", err)
		}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register user quiesce participant", err)
		}
	}
	return daemonruntime.OK(ModuleName)
}

func (m *Module) ListUsers(ctx context.Context) ([]UserSummary, error) {
	if m.store == nil {
		return nil, fmt.Errorf("user module is not initialized")
	}
	users, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make([]UserSummary, 0, len(users))
	for _, user := range users {
		summaries = append(summaries, user.toSummary())
	}
	return summaries, nil
}

func (m *Module) GetUser(ctx context.Context, userID string) (UserSummary, error) {
	user, err := m.store.GetByID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return UserSummary{}, err
	}
	return user.toSummary(), nil
}

func (m *Module) FindUser(ctx context.Context, username string) (UserSummary, error) {
	user, err := m.store.Find(ctx, username)
	if err != nil {
		return UserSummary{}, err
	}
	return user.toSummary(), nil
}

func (m *Module) CreateUser(ctx context.Context, input CreateUserInput) (UserSummary, error) {
	if err := m.requireWriteAuthority(); err != nil {
		return UserSummary{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, err
	}
	defer release()
	username := strings.TrimSpace(input.Username)
	if username == "" {
		return UserSummary{}, fmt.Errorf("username must not be empty")
	}
	if input.Password == "" {
		return UserSummary{}, fmt.Errorf("password must not be empty")
	}
	hash, err := HashPassword(input.Password)
	if err != nil {
		return UserSummary{}, err
	}
	now := time.Now().UTC()
	state := UserStateActive
	if input.Disabled {
		state = UserStateDisabled
	}
	user := User{ID: uuid.NewString(), Username: username, State: state, PasswordHash: hash, CreatedAt: now, UpdatedAt: now}
	if m.wal == nil {
		if err := m.store.Create(ctx, user); err != nil {
			return UserSummary{}, err
		}
		return user.toSummary(), nil
	}
	applied, err := m.commitUserPut(ctx, user)
	if err != nil {
		return UserSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) DisableUser(ctx context.Context, userID string) (UserSummary, error) {
	if err := m.requireWriteAuthority(); err != nil {
		return UserSummary{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, err
	}
	defer release()
	if m.wal == nil {
		user, err := m.store.Update(ctx, strings.TrimSpace(userID), func(user *User) error { user.State = UserStateDisabled; return nil })
		if err != nil {
			return UserSummary{}, err
		}
		return user.toSummary(), nil
	}
	user, err := m.store.GetByID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return UserSummary{}, err
	}
	user.State = UserStateDisabled
	user.UpdatedAt = time.Now().UTC()
	applied, err := m.commitUserPut(ctx, user)
	if err != nil {
		return UserSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) EnableUser(ctx context.Context, userID string) (UserSummary, error) {
	if err := m.requireWriteAuthority(); err != nil {
		return UserSummary{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, err
	}
	defer release()
	if m.wal == nil {
		user, err := m.store.Update(ctx, strings.TrimSpace(userID), func(user *User) error { user.State = UserStateActive; return nil })
		if err != nil {
			return UserSummary{}, err
		}
		return user.toSummary(), nil
	}
	user, err := m.store.GetByID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return UserSummary{}, err
	}
	user.State = UserStateActive
	user.UpdatedAt = time.Now().UTC()
	applied, err := m.commitUserPut(ctx, user)
	if err != nil {
		return UserSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) DeleteUser(ctx context.Context, userID string) (UserSummary, error) {
	if err := m.requireWriteAuthority(); err != nil {
		return UserSummary{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, err
	}
	defer release()
	if m.wal == nil {
		user, err := m.store.Update(ctx, strings.TrimSpace(userID), func(user *User) error { user.State = UserStateDeleted; return nil })
		if err != nil {
			return UserSummary{}, err
		}
		return user.toSummary(), nil
	}
	user, err := m.store.GetByID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return UserSummary{}, err
	}
	user.State = UserStateDeleted
	user.UpdatedAt = time.Now().UTC()
	applied, err := m.commitUserPut(ctx, user)
	if err != nil {
		return UserSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) SetUserPassword(ctx context.Context, userID string, password string) (UserSummary, error) {
	if err := m.requireWriteAuthority(); err != nil {
		return UserSummary{}, err
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, err
	}
	defer release()
	if strings.TrimSpace(userID) == "" {
		return UserSummary{}, ErrUserNotFound
	}
	if password == "" {
		return UserSummary{}, fmt.Errorf("password must not be empty")
	}
	hash, err := HashPassword(password)
	if err != nil {
		return UserSummary{}, err
	}
	if m.wal == nil {
		user, err := m.store.UpdatePasswordHash(ctx, strings.TrimSpace(userID), hash)
		if err != nil {
			return UserSummary{}, err
		}
		return user.toSummary(), nil
	}
	user, err := m.store.GetByID(ctx, strings.TrimSpace(userID))
	if err != nil {
		return UserSummary{}, err
	}
	user.PasswordHash = hash
	user.UpdatedAt = time.Now().UTC()
	applied, err := m.commitUserPut(ctx, user)
	if err != nil {
		return UserSummary{}, err
	}
	return applied.toSummary(), nil
}

func (m *Module) AuthenticateUser(ctx context.Context, username string, password string) (UserSummary, error) {
	if strings.TrimSpace(username) == "" || password == "" {
		return UserSummary{}, ErrInvalidCredentials
	}
	user, err := m.store.Find(ctx, username)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return UserSummary{}, ErrInvalidCredentials
		}
		return UserSummary{}, err
	}
	if user.State != UserStateActive || user.PasswordHash == "" {
		return UserSummary{}, ErrInvalidCredentials
	}
	if err := VerifyPassword(user.PasswordHash, password); err != nil {
		return UserSummary{}, ErrInvalidCredentials
	}
	return user.toSummary(), nil
}

func (m *Module) CreateAuthSession(ctx context.Context, user UserSummary, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration, absoluteTTL time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	defer release()
	userID, err := parseUserID(user.ID)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	refreshToken, err := domainauth.NewRefreshToken(tokenBytes)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	refreshTokenHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	now := time.Now().UTC()
	rec, err := m.sessions.Create(ctx, domainauth.RefreshSession{UserID: userID, UserRef: identity.UserRef(user.Username), Status: domainauth.RefreshSessionStatusActive, RefreshTokenHash: refreshTokenHash, CreatedAt: now, LastUsedAt: now, IdleExpiresAt: now.Add(idleTTL), AbsoluteExpiresAt: now.Add(absoluteTTL), Metadata: metadata})
	if err != nil {
		return "", domainauth.RefreshSession{}, err
	}
	return refreshToken, rec, nil
}

func (m *Module) RefreshAuthSession(ctx context.Context, refreshToken domainauth.RefreshToken, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration) (UserSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	defer release()
	refreshTokenHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	rec, err := m.sessions.FindByTokenHash(ctx, refreshTokenHash)
	if err != nil {
		if errors.Is(err, storesession.ErrSessionNotFound) {
			if consumed, reuseErr := m.sessions.FindByConsumedTokenHash(ctx, refreshTokenHash); reuseErr == nil {
				_, _ = m.sessions.RevokeFamily(ctx, consumed.TokenFamilyID, time.Now().UTC(), "refresh token reuse detected")
			}
			return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
		}
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	if !domainauth.VerifyRefreshTokenHash(refreshToken, rec.RefreshTokenHash) {
		return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	now := time.Now().UTC()
	if !refreshSessionRefreshable(rec, now) {
		if rec.Status == domainauth.RefreshSessionStatusActive && refreshSessionExpired(rec, now) {
			rec.Status = domainauth.RefreshSessionStatusExpired
			_, _ = m.sessions.Update(ctx, rec)
		}
		return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	user, err := m.store.GetByID(ctx, rec.UserID.String())
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
		}
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	if user.State != UserStateActive {
		return UserSummary{}, "", domainauth.RefreshSession{}, ErrInvalidRefreshToken
	}
	newRefreshToken, err := domainauth.NewRefreshToken(tokenBytes)
	if err != nil {
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	newRefreshTokenHash, err := domainauth.HashRefreshToken(newRefreshToken)
	if err != nil {
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	oldRefreshTokenHash := rec.RefreshTokenHash
	rec.RefreshTokenHash = newRefreshTokenHash
	rec.ConsumedRefreshTokenHashes = append(rec.ConsumedRefreshTokenHashes, oldRefreshTokenHash)
	rec.RotationCounter++
	rec.LastUsedAt = now
	rec.IdleExpiresAt = now.Add(idleTTL)
	if rec.IdleExpiresAt.After(rec.AbsoluteExpiresAt) {
		rec.IdleExpiresAt = rec.AbsoluteExpiresAt
	}
	rec.Metadata = metadata
	updated, err := m.sessions.Update(ctx, rec)
	if err != nil {
		return UserSummary{}, "", domainauth.RefreshSession{}, err
	}
	return user.toSummary(), newRefreshToken, updated, nil
}

func (m *Module) ListUserSessions(ctx context.Context, userID string) ([]domainauth.RefreshSession, error) {
	id, err := parseUserID(userID)
	if err != nil {
		return nil, err
	}
	return m.sessions.ListByUser(ctx, id)
}

func (m *Module) RevokeUserSession(ctx context.Context, userID string, sessionID string) error {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	userUUID, err := parseUserID(userID)
	if err != nil {
		return err
	}
	sessionUUID, err := uuid.Parse(strings.TrimSpace(sessionID))
	if err != nil {
		return ErrUserNotFound
	}
	rec, err := m.sessions.GetByID(ctx, domainauth.RefreshSessionID(sessionUUID))
	if err != nil {
		return err
	}
	if rec.UserID != userUUID {
		return ErrUserNotFound
	}
	_, err = m.sessions.RevokeByID(ctx, rec.ID, time.Now().UTC(), "admin user session revoked")
	return err
}

func (m *Module) RevokeUserSessions(ctx context.Context, userID string) (int, error) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	id, err := parseUserID(userID)
	if err != nil {
		return 0, err
	}
	sessions, err := m.sessions.ListByUser(ctx, id)
	if err != nil {
		return 0, err
	}
	count := 0
	now := time.Now().UTC()
	for _, rec := range sessions {
		if rec.Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		if _, err := m.sessions.RevokeByID(ctx, rec.ID, now, "admin user sessions revoked"); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) requireWriteAuthority() error {
	if m.writeAuthority == nil {
		return nil
	}
	return m.writeAuthority()
}

func refreshSessionRefreshable(rec domainauth.RefreshSession, now time.Time) bool {
	return rec.Status == domainauth.RefreshSessionStatusActive && !refreshSessionExpired(rec, now)
}

func refreshSessionExpired(rec domainauth.RefreshSession, now time.Time) bool {
	if !rec.AbsoluteExpiresAt.IsZero() && !now.Before(rec.AbsoluteExpiresAt) {
		return true
	}
	if !rec.IdleExpiresAt.IsZero() && !now.Before(rec.IdleExpiresAt) {
		return true
	}
	return false
}

func parseUserID(userID string) (identity.UserID, error) {
	id, err := uuid.Parse(strings.TrimSpace(userID))
	if err != nil {
		return uuid.Nil, ErrUserNotFound
	}
	return identity.UserID(id), nil
}

func ensureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
