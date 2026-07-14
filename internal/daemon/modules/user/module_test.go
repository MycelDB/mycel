package user

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleInitCreatesUserStore(t *testing.T) {
	module := initUserModule(t, t.TempDir())
	users, err := module.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("expected empty user store, got %d", len(users))
	}
}

func TestModuleQuiesceRejectsCreateUser(t *testing.T) {
	ctx := context.Background()
	module := initUserModule(t, t.TempDir())
	lease, err := module.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = module.CreateUser(ctx, CreateUserInput{Username: "blocked", Password: "pass"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateUser() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestModuleCreateUpdateDisableEnableDeleteUser(t *testing.T) {
	dataDir := t.TempDir()
	module := initUserModule(t, dataDir)
	created, err := module.CreateUser(context.Background(), CreateUserInput{Username: "Alice", Password: "alice-pass"})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if created.ID == "" || created.Username != "Alice" || created.State != UserStateActive {
		t.Fatalf("unexpected created user: %+v", created)
	}
	storeContent := readUserFile(t, dataDir)
	if strings.Contains(storeContent, "alice-pass") || strings.Contains(strings.ToLower(storeContent), "password_hash\": \"alice-pass") {
		t.Fatalf("user store leaked plaintext password: %s", storeContent)
	}
	persisted, err := module.store.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("store.GetByID() error = %v", err)
	}
	if err := VerifyPassword(persisted.PasswordHash, "alice-pass"); err != nil {
		t.Fatalf("stored password hash does not verify: %v", err)
	}

	disabled, err := module.DisableUser(context.Background(), created.ID)
	if err != nil || disabled.State != UserStateDisabled {
		t.Fatalf("DisableUser() = %+v, %v", disabled, err)
	}
	enabled, err := module.EnableUser(context.Background(), created.ID)
	if err != nil || enabled.State != UserStateActive {
		t.Fatalf("EnableUser() = %+v, %v", enabled, err)
	}
	deleted, err := module.DeleteUser(context.Background(), created.ID)
	if err != nil || deleted.State != UserStateDeleted {
		t.Fatalf("DeleteUser() = %+v, %v", deleted, err)
	}
}

func TestModuleRejectsDuplicateUsername(t *testing.T) {
	module := initUserModule(t, t.TempDir())
	if _, err := module.CreateUser(context.Background(), CreateUserInput{Username: "alice", Password: "pass"}); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if _, err := module.CreateUser(context.Background(), CreateUserInput{Username: "ALICE", Password: "pass"}); err != ErrDuplicateUser {
		t.Fatalf("expected duplicate username, got %v", err)
	}
}

func TestModuleUserSessionsRevoke(t *testing.T) {
	module := initUserModule(t, t.TempDir())
	created, err := module.CreateUser(context.Background(), CreateUserInput{Username: "alice", Password: "pass"})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	userID := uuid.MustParse(created.ID)
	sessionID := uuid.New()
	tokenHash, err := domainauth.HashRefreshToken("refresh-token")
	if err != nil {
		t.Fatalf("HashRefreshToken() error = %v", err)
	}
	_, err = module.sessions.Create(context.Background(), domainauth.RefreshSession{ID: sessionID, UserID: userID, UserRef: "alice", Status: domainauth.RefreshSessionStatusActive, TokenFamilyID: domainauth.TokenFamilyID(uuid.NewString()), RefreshTokenHash: tokenHash, CreatedAt: time.Now().UTC(), LastUsedAt: time.Now().UTC(), IdleExpiresAt: time.Now().Add(time.Hour), AbsoluteExpiresAt: time.Now().Add(2 * time.Hour), Metadata: domainauth.RefreshSessionMetadata{ClientName: "test"}})
	if err != nil {
		t.Fatalf("session Create() error = %v", err)
	}
	sessions, err := module.ListUserSessions(context.Background(), created.ID)
	if err != nil || len(sessions) != 1 {
		t.Fatalf("ListUserSessions() = %d, %v", len(sessions), err)
	}
	if err := module.RevokeUserSession(context.Background(), created.ID, sessionID.String()); err != nil {
		t.Fatalf("RevokeUserSession() error = %v", err)
	}
	sessions, err = module.ListUserSessions(context.Background(), created.ID)
	if err != nil || sessions[0].Status != domainauth.RefreshSessionStatusRevoked {
		t.Fatalf("expected revoked session, got %+v err=%v", sessions, err)
	}
	count, err := module.RevokeUserSessions(context.Background(), created.ID)
	if err != nil || count != 0 {
		t.Fatalf("expected no active sessions left, got count=%d err=%v", count, err)
	}
}

func initUserModule(t *testing.T, dataDir string) *Module {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text"}, Logger: logger}
	module := NewModule()
	result := module.Init(context.Background(), rt)
	if !result.OK {
		t.Fatalf("Init() result = %+v logs=%s", result.Error, logs.String())
	}
	return module
}

func readUserFile(t *testing.T, dataDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "users", StoreFilename))
	if err != nil {
		t.Fatalf("read user store: %v", err)
	}
	return string(data)
}
