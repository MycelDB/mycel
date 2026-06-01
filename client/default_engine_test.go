package client

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"knot_db/core/identity"
)

func TestDefaultEngine_StandaloneSuccess(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb")

	engine, err := DefaultEngine(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
		AdminUsername:   "admin@example.com",
		AdminPassword:   "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected engine open success, got error: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	if err := engine.Ready(context.Background()); err != nil {
		t.Fatalf("expected engine ready, got error: %v", err)
	}
}

func TestRuntimeEngine_OpenMethod(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-open")

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
		AdminUsername:   "admin",
		AdminPassword:   "password",
	}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}

	if err := engine.Ready(context.Background()); err != nil {
		t.Fatalf("expected engine ready after open, got error: %v", err)
	}

	usersPath := filepath.Join(dataDir, "users.json")
	raw, err := os.ReadFile(usersPath)
	if err != nil {
		t.Fatalf("expected users.json to be created, got error: %v", err)
	}

	var users []identity.User
	if err := json.Unmarshal(raw, &users); err != nil {
		t.Fatalf("expected valid users.json user array, got error: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected exactly one bootstrap user, got: %d", len(users))
	}
	if users[0].Ref != identity.UserRef("admin") {
		t.Fatalf("unexpected bootstrap user ref: %s", users[0].Ref)
	}
	if users[0].Username == nil || *users[0].Username != "admin" {
		t.Fatalf("unexpected bootstrap username: %#v", users[0].Username)
	}
	if users[0].Status != identity.UserStatusActive {
		t.Fatalf("unexpected bootstrap status: %s", users[0].Status)
	}
}

func TestRuntimeEngine_OpenMethod_CreateIfMissingFalse(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "does-not-exist")

	engine := &defaultEngine{}
	err := engine.Open(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: false,
	})
	if err == nil {
		t.Fatal("expected error when CreateIfMissing is false and data dir is missing")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestRuntimeEngine_OpenMethod_CreateIfMissingTrueRequiresAdminCredentials(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "missing-admin-creds")

	engine := &defaultEngine{}
	err := engine.Open(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
	})
	if err == nil {
		t.Fatal("expected error when CreateIfMissing is true without admin credentials")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestRuntimeEngine_Authenticate_Success(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-auth")

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
		AdminUsername:   "admin@example.com",
		AdminPassword:   "change-me-now",
	}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}

	token, err := engine.Authenticate(context.Background(), AuthInput{
		UserRef:  identity.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}
	if token.AccessToken == "" {
		t.Fatal("expected non-empty access token")
	}
}

func TestRuntimeEngine_Authenticate_InvalidPassword(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-auth-invalid")

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
		AdminUsername:   "admin@example.com",
		AdminPassword:   "change-me-now",
	}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}

	_, err := engine.Authenticate(context.Background(), AuthInput{
		UserRef:  identity.UserRef("admin@example.com"),
		Password: "wrong-password",
	})
	if err == nil {
		t.Fatal("expected invalid credentials error")
	}
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got: %v", err)
	}
}
