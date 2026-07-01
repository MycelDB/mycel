package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/access"
	domainauth "github.com/myceldb/mycel/domain/auth"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/graphstorage"
	domainsession "github.com/myceldb/mycel/session"
	"github.com/myceldb/mycel/store/acl"
	storedomains "github.com/myceldb/mycel/store/domains"
	storeembedding "github.com/myceldb/mycel/store/embedding"
	storesession "github.com/myceldb/mycel/store/session"
	"github.com/myceldb/mycel/store/spaces"
	storetemplate "github.com/myceldb/mycel/store/template"
	"github.com/myceldb/mycel/store/user"
)

var (
	ErrInvalidConfig      = errors.New("invalid engine config")
	ErrNotReady           = errors.New("engine not ready")
	ErrClosed             = errors.New("engine closed")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrNotFound           = errors.New("not found")
	ErrConflict           = errors.New("conflict")
)

type defaultEngine struct {
	state                    EngineState
	dataDir                  string
	accessTokenTTL           time.Duration
	refreshIdleTTL           time.Duration
	refreshAbsoluteTTL       time.Duration
	refreshAuditRetentionTTL time.Duration
	refreshTokenBytes        int
	blobLimits               domainsession.BlobLimits
	blobStaleTmpAge          time.Duration
	userManager              user.Manager
	spaceManager             spaces.Manager
	domainManager            storedomains.Manager
	templateManager          storetemplate.Manager
	embeddingManager         storeembedding.Manager
	refreshSessionManager    storesession.Manager
	accessManager            acl.Manager
	authMu                   sync.RWMutex
	authCache                map[AccessToken]authClaims
	storeMu                  sync.Mutex
	storeCache               map[domainspace.SpaceID]*graphstorage.LocalStore
}

// authClaims is the expanded authorization context cached by access token.
// It is intentionally unexported; external callers only receive AccessToken.
type authClaims struct {
	Iss      string
	Sub      string
	Aud      string
	JTI      string
	IAT      int64
	EXP      int64
	UserID   identity.UserID
	UserRef  identity.UserRef
	Roles    []access.SystemRole
	OwnerIDs []identity.UserID
	SpaceIDs []domainspace.SpaceID
	Scopes   []string
}

// NewEngine opens (or creates) a local embedded MycelDB runtime.
//
// If userManager, spaceManager, templateManager, or accessManager is nil, default file-backed managers are used.
func NewEngine(cfg EngineConfig, userManager user.Manager, spaceManager spaces.Manager, templateManager storetemplate.Manager, accessManager acl.Manager) (*defaultEngine, error) {
	e := &defaultEngine{state: EngineStateClose, userManager: userManager, spaceManager: spaceManager, templateManager: templateManager, accessManager: accessManager, authCache: map[AccessToken]authClaims{}, storeCache: map[domainspace.SpaceID]*graphstorage.LocalStore{}}
	if err := e.Open(cfg); err != nil {
		return nil, err
	}
	return e, nil
}

// DefaultEngine opens (or creates) a local embedded MycelDB runtime.
// Deprecated: use NewEngine(cfg, userManager, spaceManager, templateManager, accessManager) instead.
func DefaultEngine(cfg EngineConfig) (*defaultEngine, error) {
	return NewEngine(cfg, nil, nil, nil, nil)
}

func (e *defaultEngine) Open(cfg EngineConfig) error {
	e.state = EngineStateOpen
	cfg.DataDir = ResolveDataDir(cfg.DataDir)

	if cfg.DataDir == "" {
		e.state = EngineStateClose
		return fmt.Errorf("%w: data_dir is required or %s must be set", ErrInvalidConfig, EnvDataDir)
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
			created = true
		} else {
			e.state = EngineStateClose
			return err
		}
	} else {
		storeState, err := inspectMetadataStore(cfg.DataDir)
		if err != nil {
			e.state = EngineStateClose
			return err
		}
		switch {
		case storeState.complete:
			created = false
		case storeState.empty:
			if !cfg.CreateIfMissing {
				e.state = EngineStateClose
				return fmt.Errorf("%w: data_dir is not initialized", ErrInvalidConfig)
			}
			created = true
		default:
			e.state = EngineStateClose
			return fmt.Errorf("%w: incomplete metadata store; missing %s", ErrInvalidConfig, strings.Join(storeState.missing, ", "))
		}
	}
	if created {
		if cfg.AdminUsername == "" {
			e.state = EngineStateClose
			return fmt.Errorf("%w: admin_username is required when creating a standalone store", ErrInvalidConfig)
		}
		if cfg.AdminPassword == "" {
			e.state = EngineStateClose
			return fmt.Errorf("%w: admin_password is required when creating a standalone store", ErrInvalidConfig)
		}
	}
	if err := ensureStorageLayout(cfg.DataDir); err != nil {
		e.state = EngineStateClose
		return err
	}
	e.accessTokenTTL = cfg.AccessTokenTTL
	if e.accessTokenTTL <= 0 {
		e.accessTokenTTL = time.Hour
	}
	e.refreshIdleTTL = cfg.RefreshIdleTTL
	if e.refreshIdleTTL <= 0 {
		e.refreshIdleTTL = 30 * 24 * time.Hour
	}
	e.refreshAbsoluteTTL = cfg.RefreshAbsoluteTTL
	if e.refreshAbsoluteTTL <= 0 {
		e.refreshAbsoluteTTL = 90 * 24 * time.Hour
	}
	if e.refreshAbsoluteTTL < e.refreshIdleTTL {
		e.state = EngineStateClose
		return fmt.Errorf("%w: refresh_absolute_ttl must be greater than or equal to refresh_idle_ttl", ErrInvalidConfig)
	}
	e.refreshAuditRetentionTTL = cfg.RefreshAuditRetentionTTL
	if e.refreshAuditRetentionTTL <= 0 {
		e.refreshAuditRetentionTTL = 30 * 24 * time.Hour
	}
	e.refreshTokenBytes = cfg.RefreshTokenBytes
	if e.refreshTokenBytes == 0 {
		e.refreshTokenBytes = 32
	}
	if e.refreshTokenBytes < 32 {
		e.state = EngineStateClose
		return fmt.Errorf("%w: refresh_token_bytes must be at least 32", ErrInvalidConfig)
	}
	e.blobLimits = cfg.BlobLimits
	e.blobStaleTmpAge = cfg.BlobStaleTmpAge

	if e.userManager == nil {
		e.userManager = user.NewManager()
	}
	if err := e.userManager.Init(context.Background(), metaDir(cfg.DataDir), cfg.UserStoreEncryptionKeyB64); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.spaceManager == nil {
		e.spaceManager = spaces.NewManager()
	}
	if err := e.spaceManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.domainManager == nil {
		e.domainManager = storedomains.NewManager()
	}
	if err := e.domainManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.templateManager == nil {
		e.templateManager = storetemplate.NewManager()
	}
	if err := e.templateManager.Init(context.Background(), templatesDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.accessManager == nil {
		e.accessManager = acl.NewManager()
	}
	if err := e.accessManager.Init(context.Background(), metaDir(cfg.DataDir)); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.embeddingManager == nil {
		e.embeddingManager = storeembedding.NewManager()
	}
	if err := e.embeddingManager.Init(context.Background(), filepath.Join(metaDir(cfg.DataDir), "embedding"), cfg.UserStoreEncryptionKeyB64); err != nil {
		e.state = EngineStateClose
		return err
	}
	if e.refreshSessionManager == nil {
		e.refreshSessionManager = storesession.NewManager()
	}
	if err := e.refreshSessionManager.Init(context.Background(), filepath.Join(metaDir(cfg.DataDir), "sessions")); err != nil {
		e.state = EngineStateClose
		return err
	}

	um := e.userManager

	if created {
		exists, err := um.ExistsByRef(context.Background(), identity.UserRef(cfg.AdminUsername))
		if err != nil {
			e.state = EngineStateClose
			return err
		}
		var admin identity.User
		if !exists {
			status := identity.UserStatusActive
			username := cfg.AdminUsername
			admin, err = um.Create(context.Background(), user.CreateInput{
				User: identity.UserInput{
					Ref:      identity.UserRef(cfg.AdminUsername),
					Username: &username,
					Status:   status,
				},
				Password: cfg.AdminPassword,
			})
			if err != nil {
				e.state = EngineStateClose
				return err
			}
		} else {
			admin, err = um.GetByRef(context.Background(), identity.UserRef(cfg.AdminUsername))
			if err != nil {
				e.state = EngineStateClose
				return err
			}
		}
		if _, err := e.accessManager.GrantSystemRole(context.Background(), acl.GrantSystemRoleInput{
			UserID: admin.ID,
			Roles:  []access.SystemRole{access.SystemRoleSuperuser},
		}); err != nil {
			e.state = EngineStateClose
			return err
		}
	}

	if err := e.ensureDefaultDomains(context.Background()); err != nil {
		e.state = EngineStateClose
		return err
	}

	e.authMu.Lock()
	e.authCache = map[AccessToken]authClaims{}
	e.authMu.Unlock()

	e.dataDir = cfg.DataDir
	e.state = EngineStateReady
	return nil
}

func (e *defaultEngine) ensureDefaultDomains(ctx context.Context) error {
	spaces, err := e.spaceManager.List(ctx)
	if err != nil {
		return err
	}
	for _, sp := range spaces {
		if _, err := e.domainManager.EnsureDefault(ctx, sp.SpaceID); err != nil {
			return err
		}
	}
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

func (e *defaultEngine) Authenticate(ctx context.Context, in AuthInput) (AuthResult, error) {
	if err := e.Ready(ctx); err != nil {
		return AuthResult{}, err
	}
	account, err := e.authenticateAccount(ctx, in.UserRef, in.Password)
	if err != nil {
		return AuthResult{}, err
	}
	accessToken, _, err := e.mintAccessToken(ctx, account)
	if err != nil {
		return AuthResult{}, err
	}
	return AuthResult{AccessToken: accessToken}, nil
}

func (e *defaultEngine) LoginSession(ctx context.Context, in LoginSessionInput) (LoginSessionResult, error) {
	if err := e.Ready(ctx); err != nil {
		return LoginSessionResult{}, err
	}
	account, err := e.authenticateAccount(ctx, in.UserRef, in.Password)
	if err != nil {
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.login_failure", UserRef: in.UserRef, Message: "invalid credentials"})
		return LoginSessionResult{}, err
	}

	refreshToken, err := domainauth.NewRefreshToken(e.refreshTokenBytes)
	if err != nil {
		return LoginSessionResult{}, err
	}
	refreshTokenHash, err := domainauth.HashRefreshToken(refreshToken)
	if err != nil {
		return LoginSessionResult{}, err
	}
	now := time.Now().UTC()
	rec, err := e.refreshSessionManager.Create(ctx, domainauth.RefreshSession{
		UserID:            account.ID,
		UserRef:           account.Ref,
		Status:            domainauth.RefreshSessionStatusActive,
		RefreshTokenHash:  refreshTokenHash,
		CreatedAt:         now,
		LastUsedAt:        now,
		IdleExpiresAt:     now.Add(e.refreshIdleTTL),
		AbsoluteExpiresAt: now.Add(e.refreshAbsoluteTTL),
		Metadata:          in.Metadata,
	})
	if err != nil {
		return LoginSessionResult{}, err
	}
	accessToken, accessTokenExpiresAt, err := e.mintAccessToken(ctx, account)
	if err != nil {
		return LoginSessionResult{}, err
	}
	_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.login_success", UserID: &account.ID, UserRef: account.Ref, SessionID: &rec.ID, Message: "login session created"})
	return LoginSessionResult{
		AccessToken:          accessToken,
		AccessTokenExpiresAt: accessTokenExpiresAt,
		RefreshToken:         refreshToken,
		RefreshSession:       refreshSessionInfo(rec),
	}, nil
}

func (e *defaultEngine) RefreshSession(ctx context.Context, in RefreshSessionInput) (RefreshSessionResult, error) {
	if err := e.Ready(ctx); err != nil {
		return RefreshSessionResult{}, err
	}
	refreshTokenHash, err := domainauth.HashRefreshToken(in.RefreshToken)
	if err != nil {
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", Message: "invalid refresh token"})
		return RefreshSessionResult{}, ErrInvalidCredentials
	}
	rec, err := e.refreshSessionManager.FindByTokenHash(ctx, refreshTokenHash)
	if err != nil {
		if errors.Is(err, storesession.ErrSessionNotFound) {
			if consumed, reuseErr := e.refreshSessionManager.FindByConsumedTokenHash(ctx, refreshTokenHash); reuseErr == nil {
				if err := e.handleRefreshTokenReuse(ctx, consumed); err != nil {
					return RefreshSessionResult{}, err
				}
				return RefreshSessionResult{}, ErrInvalidCredentials
			}
			_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", Message: "invalid refresh token"})
			return RefreshSessionResult{}, ErrInvalidCredentials
		}
		return RefreshSessionResult{}, err
	}
	if !domainauth.VerifyRefreshTokenHash(in.RefreshToken, rec.RefreshTokenHash) {
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "invalid refresh token"})
		return RefreshSessionResult{}, ErrInvalidCredentials
	}
	now := time.Now().UTC()
	if !refreshSessionRefreshable(rec, now) {
		if rec.Status == domainauth.RefreshSessionStatusActive && refreshSessionExpired(rec, now) {
			rec.Status = domainauth.RefreshSessionStatusExpired
			if updated, updateErr := e.refreshSessionManager.Update(ctx, rec); updateErr == nil {
				rec = updated
			}
		}
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "refresh session expired or revoked"})
		return RefreshSessionResult{}, ErrInvalidCredentials
	}
	account, err := e.userManager.GetByID(ctx, rec.UserID)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "refresh session user not found"})
			return RefreshSessionResult{}, ErrInvalidCredentials
		}
		return RefreshSessionResult{}, err
	}
	if account.Status != identity.UserStatusActive {
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_failure", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "refresh session user inactive"})
		return RefreshSessionResult{}, ErrInvalidCredentials
	}

	newRefreshToken, err := domainauth.NewRefreshToken(e.refreshTokenBytes)
	if err != nil {
		return RefreshSessionResult{}, err
	}
	newRefreshTokenHash, err := domainauth.HashRefreshToken(newRefreshToken)
	if err != nil {
		return RefreshSessionResult{}, err
	}
	oldRefreshTokenHash := rec.RefreshTokenHash
	rec.RefreshTokenHash = newRefreshTokenHash
	rec.ConsumedRefreshTokenHashes = append(rec.ConsumedRefreshTokenHashes, oldRefreshTokenHash)
	rec.RotationCounter++
	rec.LastUsedAt = now
	rec.IdleExpiresAt = now.Add(e.refreshIdleTTL)
	if rec.IdleExpiresAt.After(rec.AbsoluteExpiresAt) {
		rec.IdleExpiresAt = rec.AbsoluteExpiresAt
	}
	rec.Metadata = in.Metadata
	updated, err := e.refreshSessionManager.Update(ctx, rec)
	if err != nil {
		return RefreshSessionResult{}, err
	}
	accessToken, accessTokenExpiresAt, err := e.mintAccessToken(ctx, account)
	if err != nil {
		return RefreshSessionResult{}, err
	}
	_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_success", UserID: &account.ID, UserRef: account.Ref, SessionID: &updated.ID, Message: "refresh session rotated"})
	return RefreshSessionResult{
		AccessToken:          accessToken,
		AccessTokenExpiresAt: accessTokenExpiresAt,
		RefreshToken:         newRefreshToken,
		RefreshSession:       refreshSessionInfo(updated),
	}, nil
}

func (e *defaultEngine) ListRefreshSessions(ctx context.Context, in ListRefreshSessionsInput) ([]RefreshSessionInfo, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	records, err := e.refreshSessionManager.ListByUser(ctx, auth.UserID)
	if err != nil {
		return nil, err
	}
	out := make([]RefreshSessionInfo, 0, len(records))
	for _, rec := range records {
		out = append(out, refreshSessionInfo(rec))
	}
	return out, nil
}

func (e *defaultEngine) RevokeRefreshSession(ctx context.Context, in RevokeRefreshSessionInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	if in.SessionID == uuid.Nil {
		return fmt.Errorf("%w: session_id is required", ErrInvalidConfig)
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	rec, err := e.refreshSessionManager.GetByID(ctx, in.SessionID)
	if err != nil {
		if errors.Is(err, storesession.ErrSessionNotFound) {
			return ErrNotFound
		}
		return err
	}
	if rec.UserID != auth.UserID {
		return ErrUnauthorized
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		reason = "revoked by user"
	}
	revoked, err := e.refreshSessionManager.RevokeByID(ctx, in.SessionID, time.Now().UTC(), reason)
	if err != nil {
		return err
	}
	_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.session_revoked", UserID: &auth.UserID, UserRef: auth.UserRef, SessionID: &revoked.ID, Message: reason})
	return nil
}

func (e *defaultEngine) RevokeOtherRefreshSessions(ctx context.Context, in RevokeOtherRefreshSessionsInput) (int, error) {
	if err := e.Ready(ctx); err != nil {
		return 0, err
	}
	if in.CurrentSessionID == uuid.Nil {
		return 0, fmt.Errorf("%w: current_session_id is required", ErrInvalidConfig)
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return 0, err
	}
	current, err := e.refreshSessionManager.GetByID(ctx, in.CurrentSessionID)
	if err != nil {
		if errors.Is(err, storesession.ErrSessionNotFound) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	if current.UserID != auth.UserID {
		return 0, ErrUnauthorized
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		reason = "revoked other sessions"
	}
	records, err := e.refreshSessionManager.ListByUser(ctx, auth.UserID)
	if err != nil {
		return 0, err
	}
	revokedCount := 0
	for _, rec := range records {
		if rec.ID == in.CurrentSessionID || rec.Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		if _, err := e.refreshSessionManager.RevokeByID(ctx, rec.ID, time.Now().UTC(), reason); err != nil {
			return revokedCount, err
		}
		revokedCount++
	}
	_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.session_revoked", UserID: &auth.UserID, UserRef: auth.UserRef, SessionID: &in.CurrentSessionID, Message: reason})
	return revokedCount, nil
}

func (e *defaultEngine) handleRefreshTokenReuse(ctx context.Context, rec domainauth.RefreshSession) error {
	now := time.Now().UTC()
	_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.refresh_reuse_detected", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "consumed refresh token reused"})
	count, err := e.refreshSessionManager.RevokeFamily(ctx, rec.TokenFamilyID, now, "refresh token reuse detected")
	if err != nil {
		return err
	}
	if count > 0 {
		_ = e.recordAuthAuditEvent(context.Background(), domainauth.AuthAuditEvent{Type: "auth.session_family_revoked", UserID: &rec.UserID, UserRef: rec.UserRef, SessionID: &rec.ID, Message: "refresh token family revoked after reuse detection"})
	}
	return nil
}

func refreshSessionRefreshable(rec domainauth.RefreshSession, now time.Time) bool {
	return rec.Status == domainauth.RefreshSessionStatusActive && !refreshSessionExpired(rec, now)
}

func refreshSessionExpired(rec domainauth.RefreshSession, now time.Time) bool {
	return !rec.IdleExpiresAt.After(now) || !rec.AbsoluteExpiresAt.After(now)
}

func (e *defaultEngine) authenticateAccount(ctx context.Context, ref identity.UserRef, password string) (identity.User, error) {
	if ref == "" || password == "" {
		return identity.User{}, fmt.Errorf("%w: user_ref and password are required", ErrInvalidCredentials)
	}
	account, err := e.userManager.Authenticate(ctx, ref, password)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) || errors.Is(err, user.ErrInvalidInput) {
			return identity.User{}, ErrInvalidCredentials
		}
		return identity.User{}, err
	}
	if account.Status != identity.UserStatusActive {
		return identity.User{}, ErrInvalidCredentials
	}
	return account, nil
}

func (e *defaultEngine) mintAccessToken(ctx context.Context, account identity.User) (AccessToken, time.Time, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(e.accessTokenTTL)
	uid := account.ID.String()
	accessToken := AccessToken(uuid.NewString())
	roles, err := e.accessManager.SystemRolesForUser(ctx, account.ID)
	if err != nil {
		return "", time.Time{}, err
	}
	claims := authClaims{
		Iss:      "mycel",
		Sub:      "user:" + uid,
		Aud:      "mycel",
		JTI:      uuid.NewString(),
		IAT:      now.Unix(),
		EXP:      expiresAt.Unix(),
		UserID:   account.ID,
		UserRef:  account.Ref,
		Roles:    roles,
		OwnerIDs: []identity.UserID{account.ID},
		SpaceIDs: []domainspace.SpaceID{},
		Scopes:   scopesForSystemRoles(roles),
	}

	e.authMu.Lock()
	if e.authCache == nil {
		e.authCache = map[AccessToken]authClaims{}
	}
	e.authCache[accessToken] = claims
	e.authMu.Unlock()
	return accessToken, expiresAt, nil
}

func (e *defaultEngine) recordAuthAuditEvent(ctx context.Context, event domainauth.AuthAuditEvent) error {
	if e.refreshSessionManager == nil {
		return nil
	}
	_, err := e.refreshSessionManager.RecordAuditEvent(ctx, event)
	return err
}

func refreshSessionInfo(rec domainauth.RefreshSession) RefreshSessionInfo {
	return RefreshSessionInfo{
		ID:                rec.ID,
		UserID:            rec.UserID,
		UserRef:           rec.UserRef,
		Status:            rec.Status,
		TokenFamilyID:     string(rec.TokenFamilyID),
		RotationCounter:   rec.RotationCounter,
		CreatedAt:         rec.CreatedAt,
		LastUsedAt:        rec.LastUsedAt,
		IdleExpiresAt:     rec.IdleExpiresAt,
		AbsoluteExpiresAt: rec.AbsoluteExpiresAt,
		RevokedAt:         rec.RevokedAt,
		RevokedReason:     rec.RevokedReason,
		Metadata:          rec.Metadata,
	}
}

func (e *defaultEngine) CurrentUser(ctx context.Context, in CurrentUserInput) (identity.User, error) {
	if err := e.Ready(ctx); err != nil {
		return identity.User{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return identity.User{}, err
	}
	account, err := e.userManager.GetByID(ctx, auth.UserID)
	if err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return identity.User{}, ErrNotFound
		}
		return identity.User{}, err
	}
	return account, nil
}

func (e *defaultEngine) CreateUser(ctx context.Context, in CreateUserInput) (identity.User, error) {
	if err := e.Ready(ctx); err != nil {
		return identity.User{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return identity.User{}, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
	if err != nil {
		return identity.User{}, err
	}
	if !canManageUsers {
		return identity.User{}, ErrUnauthorized
	}
	created, err := e.userManager.Create(ctx, user.CreateInput{User: in.User, Password: in.Password})
	if err != nil {
		if errors.Is(err, user.ErrInvalidInput) {
			return identity.User{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		return identity.User{}, err
	}
	return created, nil
}

func (e *defaultEngine) ListUsers(ctx context.Context, in ListUsersInput) ([]identity.User, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
	if err != nil {
		return nil, err
	}
	if !canManageUsers {
		return nil, ErrUnauthorized
	}
	return e.userManager.List(ctx)
}

func (e *defaultEngine) DeleteUser(ctx context.Context, in DeleteUserInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	canManageUsers, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageUsers)
	if err != nil {
		return err
	}
	if !canManageUsers {
		return ErrUnauthorized
	}
	if in.UserID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidConfig)
	}
	if _, err := e.userManager.GetByID(ctx, in.UserID); err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return ErrNotFound
		}
		return err
	}
	if err := e.ensureNotLastSuperuser(ctx, in.UserID); err != nil {
		return err
	}
	ownedSpaces, err := e.spaceManager.ListByOwner(ctx, in.UserID)
	if err != nil {
		return err
	}
	for _, sp := range ownedSpaces {
		if err := e.deleteSpaceByID(ctx, sp.SpaceID); err != nil {
			return err
		}
	}
	if err := e.accessManager.DeleteForUser(ctx, in.UserID); err != nil {
		return err
	}
	if err := e.userManager.DeleteByID(ctx, in.UserID); err != nil {
		if errors.Is(err, user.ErrUserNotFound) {
			return ErrNotFound
		}
		return err
	}
	e.purgeCachedClaimsForUser(in.UserID)
	return nil
}

func (e *defaultEngine) CreateSpace(ctx context.Context, in CreateSpaceInput) (SpaceInfo, error) {
	if err := e.Ready(ctx); err != nil {
		return SpaceInfo{}, err
	}
	if in.Name == "" {
		return SpaceInfo{}, fmt.Errorf("%w: space name is required", ErrInvalidConfig)
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return SpaceInfo{}, err
	}
	if auth.UserID == uuid.Nil {
		return SpaceInfo{}, ErrUnauthorized
	}
	canCreateSpace, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionCreateSpaces)
	if err != nil {
		return SpaceInfo{}, err
	}
	if !canCreateSpace {
		return SpaceInfo{}, ErrUnauthorized
	}

	ownerID, err := e.resolveCreateSpaceOwner(ctx, auth.UserID, in)
	if err != nil {
		return SpaceInfo{}, err
	}
	if ownerID != auth.UserID {
		canManageAccess, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
		if err != nil {
			return SpaceInfo{}, err
		}
		if !canManageAccess {
			return SpaceInfo{}, ErrUnauthorized
		}
	}

	sp, err := e.spaceManager.Create(ctx, spaces.CreateInput{OwnerID: ownerID, Name: in.Name})
	if err != nil {
		return SpaceInfo{}, err
	}
	var defaultDomain graph.Domain
	if strings.TrimSpace(in.DefaultDomainKey) != "" {
		name := in.DefaultDomainName
		if strings.TrimSpace(name) == "" {
			name = in.DefaultDomainKey
		}
		defaultDomain, err = e.domainManager.Create(ctx, storedomains.CreateInput{SpaceID: sp.SpaceID, Key: in.DefaultDomainKey, Name: name, Default: true})
	} else {
		defaultDomain, err = e.domainManager.EnsureDefault(ctx, sp.SpaceID)
	}
	if err != nil {
		return SpaceInfo{}, err
	}
	if _, err := e.accessManager.Grant(ctx, acl.GrantInput{
		SpaceID:     sp.SpaceID,
		UserID:      ownerID,
		Permissions: []access.SpacePermission{access.SpacePermissionAdmin},
	}); err != nil {
		return SpaceInfo{}, err
	}

	if ownerID == auth.UserID {
		e.grantSpaceToCachedClaims(in.AccessToken, sp.SpaceID)
	}
	return SpaceInfo{OwnerID: sp.OwnerID, SpaceID: sp.SpaceID, Name: sp.Name, DefaultDomainID: defaultDomain.ID}, nil
}

func (e *defaultEngine) resolveCreateSpaceOwner(ctx context.Context, authenticatedUserID identity.UserID, in CreateSpaceInput) (identity.UserID, error) {
	if in.OwnerUserID != nil && *in.OwnerUserID != uuid.Nil && in.OwnerRef != "" {
		owner, err := e.userManager.GetByRef(ctx, in.OwnerRef)
		if err != nil {
			if errors.Is(err, user.ErrUserNotFound) {
				return uuid.Nil, ErrNotFound
			}
			return uuid.Nil, err
		}
		if owner.ID != *in.OwnerUserID {
			return uuid.Nil, fmt.Errorf("%w: owner_user_id and owner_ref refer to different users", ErrInvalidConfig)
		}
		return owner.ID, nil
	}
	if in.OwnerUserID != nil && *in.OwnerUserID != uuid.Nil {
		if _, err := e.userManager.GetByID(ctx, *in.OwnerUserID); err != nil {
			if errors.Is(err, user.ErrUserNotFound) {
				return uuid.Nil, ErrNotFound
			}
			return uuid.Nil, err
		}
		return *in.OwnerUserID, nil
	}
	if in.OwnerRef != "" {
		owner, err := e.userManager.GetByRef(ctx, in.OwnerRef)
		if err != nil {
			if errors.Is(err, user.ErrUserNotFound) {
				return uuid.Nil, ErrNotFound
			}
			return uuid.Nil, err
		}
		return owner.ID, nil
	}
	return authenticatedUserID, nil
}

func (e *defaultEngine) ListSpaces(ctx context.Context, in ListSpacesInput) ([]domainspace.Space, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	spaces, err := e.spaceManager.List(ctx)
	if err != nil {
		return nil, err
	}
	canManageAccess, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return nil, err
	}
	if canManageAccess {
		return spaces, nil
	}
	accessible := []domainspace.Space{}
	for _, sp := range spaces {
		canRead, err := e.canReadSpace(ctx, auth.UserID, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		if canRead {
			accessible = append(accessible, sp)
		}
	}
	return accessible, nil
}

func (e *defaultEngine) CreateDomain(ctx context.Context, in CreateDomainInput) (graph.Domain, error) {
	if err := e.Ready(ctx); err != nil {
		return graph.Domain{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return graph.Domain{}, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return graph.Domain{}, err
	}
	return e.domainManager.Create(ctx, storedomains.CreateInput{SpaceID: in.SpaceID, Key: in.Key, Name: in.Name, Description: in.Description, Default: in.Default})
}

func (e *defaultEngine) ListDomains(ctx context.Context, in ListDomainsInput) ([]graph.Domain, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if in.SpaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	canRead, err := e.canReadSpace(ctx, auth.UserID, in.SpaceID)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrUnauthorized
	}
	return e.domainManager.ListBySpace(ctx, in.SpaceID)
}

func (e *defaultEngine) GetDomain(ctx context.Context, in GetDomainInput) (graph.Domain, error) {
	if err := e.Ready(ctx); err != nil {
		return graph.Domain{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return graph.Domain{}, err
	}
	domain, err := e.resolveDomain(ctx, in.SpaceID, in.DomainID, in.Key)
	if err != nil {
		return graph.Domain{}, err
	}
	canRead, err := e.canReadSpace(ctx, auth.UserID, domain.SpaceID)
	if err != nil {
		return graph.Domain{}, err
	}
	if !canRead {
		return graph.Domain{}, ErrUnauthorized
	}
	return domain, nil
}

func (e *defaultEngine) SetDomainEmbeddingPolicy(ctx context.Context, in SetDomainEmbeddingPolicyInput) (domainembedding.DomainEmbeddingPolicy, error) {
	if err := e.Ready(ctx); err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.Policy.SpaceID); err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	return e.domainManager.SetEmbeddingPolicy(ctx, in.Policy)
}

func (e *defaultEngine) GetDomainEmbeddingPolicy(ctx context.Context, in GetDomainEmbeddingPolicyInput) (domainembedding.DomainEmbeddingPolicy, error) {
	if err := e.Ready(ctx); err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	domain, err := e.resolveDomain(ctx, in.SpaceID, in.DomainID, in.Key)
	if err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	canRead, err := e.canReadSpace(ctx, auth.UserID, domain.SpaceID)
	if err != nil {
		return domainembedding.DomainEmbeddingPolicy{}, err
	}
	if !canRead {
		return domainembedding.DomainEmbeddingPolicy{}, ErrUnauthorized
	}
	policy, err := e.domainManager.GetEmbeddingPolicy(ctx, domain.SpaceID, domain.ID)
	if errors.Is(err, storedomains.ErrDomainNotFound) {
		return domainembedding.DomainEmbeddingPolicy{}, ErrNotFound
	}
	return policy, err
}

func (e *defaultEngine) DeleteSpace(ctx context.Context, in DeleteSpaceInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return err
	}
	return e.deleteSpaceByID(ctx, in.SpaceID)
}

func (e *defaultEngine) GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (access.SystemAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return access.SystemAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return access.SystemAccessRule{}, err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return access.SystemAccessRule{}, err
	}
	if !canManage {
		return access.SystemAccessRule{}, ErrUnauthorized
	}
	return e.accessManager.GrantSystemRole(ctx, acl.GrantSystemRoleInput{UserID: in.UserID, Roles: in.Roles})
}

func (e *defaultEngine) RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return err
	}
	if !canManage {
		return ErrUnauthorized
	}
	return e.accessManager.RevokeSystemRole(ctx, acl.RevokeSystemRoleInput{UserID: in.UserID})
}

func (e *defaultEngine) ListSystemAccess(ctx context.Context, in ListSystemAccessInput) ([]access.SystemAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	canManage, err := e.accessManager.CanSystem(ctx, auth.UserID, access.SystemPermissionManageAccess)
	if err != nil {
		return nil, err
	}
	if !canManage {
		return nil, ErrUnauthorized
	}
	return e.accessManager.SystemRules(ctx)
}

func (e *defaultEngine) GrantSpaceAccess(ctx context.Context, in GrantSpaceAccessInput) (access.SpaceAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return access.SpaceAccessRule{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return access.SpaceAccessRule{}, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return access.SpaceAccessRule{}, err
	}
	return e.accessManager.Grant(ctx, acl.GrantInput{SpaceID: in.SpaceID, UserID: in.UserID, Permissions: in.Permissions})
}

func (e *defaultEngine) RevokeSpaceAccess(ctx context.Context, in RevokeSpaceAccessInput) error {
	if err := e.Ready(ctx); err != nil {
		return err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return err
	}
	return e.accessManager.Revoke(ctx, acl.RevokeInput{SpaceID: in.SpaceID, UserID: in.UserID})
}

func (e *defaultEngine) ListSpaceAccess(ctx context.Context, in ListSpaceAccessInput) ([]access.SpaceAccessRule, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if err := e.ensureSpaceAdmin(ctx, auth.UserID, in.SpaceID); err != nil {
		return nil, err
	}
	return e.accessManager.RulesForSpace(ctx, in.SpaceID)
}

func (e *defaultEngine) ensureSpaceAdmin(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID) error {
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, spaces.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	canAdmin, err := e.canAdminSpace(ctx, userID, spaceID)
	if err != nil {
		return err
	}
	if !canAdmin {
		return ErrUnauthorized
	}
	return nil
}

func (e *defaultEngine) OpenSession(ctx context.Context, in OpenSessionInput) (domainsession.Session, error) {
	if err := e.Ready(ctx); err != nil {
		return nil, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	if auth.UserID == uuid.Nil {
		return nil, ErrUnauthorized
	}

	spaceID := in.SpaceID
	if spaceID == uuid.Nil && len(auth.SpaceIDs) > 0 {
		spaceID = auth.SpaceIDs[0]
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}

	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, spaces.ErrSpaceNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	canRead, err := e.canReadSpace(ctx, auth.UserID, spaceID)
	if err != nil {
		return nil, err
	}
	if !canRead {
		return nil, ErrUnauthorized
	}
	canWrite, err := e.canWriteSpace(ctx, auth.UserID, spaceID)
	if err != nil {
		return nil, err
	}
	canAdmin, err := e.canAdminSpace(ctx, auth.UserID, spaceID)
	if err != nil {
		return nil, err
	}
	domain, err := e.resolveDomain(ctx, spaceID, nilDomainID(in.DomainID), in.DomainKey)
	if err != nil {
		return nil, err
	}

	if err := ensureGraphSpaceDir(e.dataDir, spaceID); err != nil {
		return nil, err
	}
	store, err := e.graphStore(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	return domainsession.NewSessionWithStoreConfig(
		graphsDir(e.dataDir),
		blobsDir(e.dataDir),
		spaceID,
		e.templateManager,
		domainsession.Permissions{Read: canRead, Write: canWrite, Admin: canAdmin},
		domainsession.Errors{Closed: ErrClosed, NotFound: ErrNotFound, Unauthorized: ErrUnauthorized, Conflict: ErrConflict},
		store,
		domainsession.Config{BlobLimits: e.blobLimits, BlobStaleTmpAge: e.blobStaleTmpAge, CurrentUserID: auth.UserID, EmbeddingManager: e.embeddingManager, DomainID: domain.ID},
	), nil
}

func nilDomainID(id *graph.DomainID) graph.DomainID {
	if id == nil {
		return uuid.Nil
	}
	return *id
}

func (e *defaultEngine) resolveDomain(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, key string) (graph.Domain, error) {
	var d graph.Domain
	var err error
	if domainID != uuid.Nil {
		d, err = e.domainManager.GetByID(ctx, domainID)
	} else if strings.TrimSpace(key) != "" {
		if spaceID == uuid.Nil {
			return graph.Domain{}, fmt.Errorf("%w: space_id is required when resolving domain by key", ErrInvalidConfig)
		}
		d, err = e.domainManager.FindBySpaceAndKey(ctx, spaceID, key)
	} else {
		if spaceID == uuid.Nil {
			return graph.Domain{}, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
		}
		d, err = e.domainManager.GetDefault(ctx, spaceID)
	}
	if err != nil {
		if errors.Is(err, storedomains.ErrDomainNotFound) {
			return graph.Domain{}, ErrNotFound
		}
		return graph.Domain{}, err
	}
	if spaceID != uuid.Nil && d.SpaceID != spaceID {
		return graph.Domain{}, fmt.Errorf("%w: domain does not belong to space", ErrInvalidConfig)
	}
	return d, nil
}

func (e *defaultEngine) Close() error {
	e.state = EngineStateClose
	e.authMu.Lock()
	e.authCache = map[AccessToken]authClaims{}
	e.authMu.Unlock()
	e.storeMu.Lock()
	defer e.storeMu.Unlock()
	var err error
	for spaceID, store := range e.storeCache {
		if closeErr := store.Close(); err == nil {
			err = closeErr
		}
		delete(e.storeCache, spaceID)
	}
	return err
}

func (e *defaultEngine) graphStore(ctx context.Context, spaceID domainspace.SpaceID) (*graphstorage.LocalStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.storeMu.Lock()
	defer e.storeMu.Unlock()
	if store, ok := e.storeCache[spaceID]; ok {
		return store, nil
	}
	if e.storeCache == nil {
		e.storeCache = map[domainspace.SpaceID]*graphstorage.LocalStore{}
	}
	store, err := graphstorage.Open(ctx, filepath.Join(graphsDir(e.dataDir), spaceID.String()))
	if err != nil {
		return nil, err
	}
	e.storeCache[spaceID] = store
	return store, nil
}

func (e *defaultEngine) authClaimsForAccessToken(ctx context.Context, accessToken AccessToken) (authClaims, error) {
	if err := ctx.Err(); err != nil {
		return authClaims{}, err
	}
	if accessToken == "" {
		return authClaims{}, ErrUnauthorized
	}

	e.authMu.Lock()
	defer e.authMu.Unlock()
	claims, ok := e.authCache[accessToken]
	if !ok {
		return authClaims{}, ErrUnauthorized
	}
	if claims.EXP <= time.Now().Unix() {
		delete(e.authCache, accessToken)
		return authClaims{}, ErrUnauthorized
	}
	return claims, nil
}

func (e *defaultEngine) grantSpaceToCachedClaims(accessToken AccessToken, spaceID domainspace.SpaceID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	claims, ok := e.authCache[accessToken]
	if !ok || containsSpaceID(claims.SpaceIDs, spaceID) {
		return
	}
	claims.SpaceIDs = append(claims.SpaceIDs, spaceID)
	e.authCache[accessToken] = claims
}

func (e *defaultEngine) deleteSpaceByID(ctx context.Context, spaceID domainspace.SpaceID) error {
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	if _, err := e.spaceManager.GetByID(ctx, spaceID); err != nil {
		if errors.Is(err, spaces.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	if err := e.templateManager.DeleteForSpace(ctx, spaceID); err != nil {
		return err
	}
	if err := e.domainManager.DeleteForSpace(ctx, spaceID); err != nil {
		return err
	}
	if err := e.accessManager.DeleteForSpace(ctx, spaceID); err != nil {
		return err
	}
	e.closeCachedStore(spaceID)
	if err := os.RemoveAll(filepath.Join(graphsDir(e.dataDir), spaceID.String())); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(blobsDir(e.dataDir), spaceID.String())); err != nil {
		return err
	}
	if err := e.spaceManager.DeleteByID(ctx, spaceID); err != nil {
		if errors.Is(err, spaces.ErrSpaceNotFound) {
			return ErrNotFound
		}
		return err
	}
	e.purgeCachedSpace(spaceID)
	return nil
}

func (e *defaultEngine) closeCachedStore(spaceID domainspace.SpaceID) {
	e.storeMu.Lock()
	defer e.storeMu.Unlock()
	if store, ok := e.storeCache[spaceID]; ok {
		_ = store.Close()
		delete(e.storeCache, spaceID)
	}
}

func (e *defaultEngine) ensureNotLastSuperuser(ctx context.Context, userID identity.UserID) error {
	roles, err := e.accessManager.SystemRolesForUser(ctx, userID)
	if err != nil {
		return err
	}
	if !containsSystemRole(roles, access.SystemRoleSuperuser) {
		return nil
	}
	rules, err := e.accessManager.SystemRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.UserID != userID && containsSystemRole(rule.Roles, access.SystemRoleSuperuser) {
			return nil
		}
	}
	return acl.ErrLastSuperuser
}

func (e *defaultEngine) purgeCachedClaimsForUser(userID identity.UserID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	for token, claims := range e.authCache {
		if claims.UserID == userID {
			delete(e.authCache, token)
		}
	}
}

func (e *defaultEngine) purgeCachedSpace(spaceID domainspace.SpaceID) {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	for token, claims := range e.authCache {
		filtered := claims.SpaceIDs[:0]
		for _, existing := range claims.SpaceIDs {
			if existing != spaceID {
				filtered = append(filtered, existing)
			}
		}
		claims.SpaceIDs = filtered
		e.authCache[token] = claims
	}
}

func (e *defaultEngine) canReadSpace(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionRead)
}

func (e *defaultEngine) canWriteSpace(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionWrite)
}

func (e *defaultEngine) canAdminSpace(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID) (bool, error) {
	if canAdmin, err := e.canAdminSystem(ctx, userID); err != nil || canAdmin {
		return canAdmin, err
	}
	return e.accessManager.Can(ctx, userID, spaceID, access.SpacePermissionAdmin)
}

func (e *defaultEngine) canAdminSystem(ctx context.Context, userID identity.UserID) (bool, error) {
	return e.accessManager.CanSystem(ctx, userID, access.SystemPermissionManageAccess)
}

func scopesForSystemRoles(roles []access.SystemRole) []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(scope string) {
		if _, ok := seen[scope]; ok {
			return
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	for _, role := range roles {
		if access.RoleAllows(role, access.SystemPermissionManageUsers) {
			add("admin:users")
		}
		if access.RoleAllows(role, access.SystemPermissionCreateSpaces) {
			add("space:create")
		}
		if access.RoleAllows(role, access.SystemPermissionManageAccess) {
			add("access:manage")
			add("template:write")
			add("graph:read")
			add("graph:write")
		}
		if access.RoleAllows(role, access.SystemPermissionOperateSystem) {
			add("system:operate")
		}
	}
	return out
}

type metadataStoreState struct {
	complete bool
	empty    bool
	missing  []string
}

func inspectMetadataStore(dataDir string) (metadataStoreState, error) {
	required := []string{
		filepath.Join(metaDir(dataDir), "users.json"),
		filepath.Join(metaDir(dataDir), "spaces.json"),
		filepath.Join(metaDir(dataDir), "access.json"),
	}
	existing := 0
	missing := []string{}
	for _, path := range required {
		exists, err := regularFileExists(path)
		if err != nil {
			return metadataStoreState{}, err
		}
		if exists {
			existing++
		} else {
			missing = append(missing, path)
		}
	}
	return metadataStoreState{complete: existing == len(required), empty: existing == 0, missing: missing}, nil
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("%w: metadata path is a directory: %s", ErrInvalidConfig, path)
	}
	return true, nil
}

func ensureStorageLayout(dataDir string) error {
	for _, dir := range []string{dataDir, metaDir(dataDir), graphsDir(dataDir), blobsDir(dataDir), templatesDir(dataDir)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func metaDir(dataDir string) string {
	return filepath.Join(dataDir, "meta")
}

func graphsDir(dataDir string) string {
	return filepath.Join(dataDir, "graphs")
}

func blobsDir(dataDir string) string {
	return filepath.Join(dataDir, "blobs")
}

func templatesDir(dataDir string) string {
	return filepath.Join(metaDir(dataDir), "templates")
}

func ensureGraphSpaceDir(dataDir string, spaceID domainspace.SpaceID) error {
	path := filepath.Join(graphsDir(dataDir), spaceID.String())
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, ".space"), []byte("active\n"), 0o600)
}

func contains(items []string, wanted string) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsUserID(items []identity.UserID, wanted identity.UserID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSpaceID(items []domainspace.SpaceID, wanted domainspace.SpaceID) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}

func containsSystemRole(items []access.SystemRole, wanted access.SystemRole) bool {
	for _, it := range items {
		if it == wanted {
			return true
		}
	}
	return false
}
