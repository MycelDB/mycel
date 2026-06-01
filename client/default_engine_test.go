package client

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultEngine_StandaloneSuccess(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb")

	engine, err := DefaultEngine(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
		AdminUsername:   "admin",
		AdminPassword:   "password",
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

	var uf struct {
		Users []struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"users"`
	}
	if err := json.Unmarshal(raw, &uf); err != nil {
		t.Fatalf("expected valid users.json, got error: %v", err)
	}
	if len(uf.Users) != 1 {
		t.Fatalf("expected exactly one bootstrap user, got: %d", len(uf.Users))
	}
	if uf.Users[0].Username != "admin" || uf.Users[0].Password != "password" {
		t.Fatalf("unexpected bootstrap admin record: %#v", uf.Users[0])
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
