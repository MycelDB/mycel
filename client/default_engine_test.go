package client

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"knot_db/api"
	"knot_db/model"
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
	if _, err := os.Stat(usersPath); err != nil {
		t.Fatalf("expected users.json to be created, got error: %v", err)
	}

	if _, err := engine.Authenticate(context.Background(), AuthInput{
		UserRef:  model.UserRef("admin"),
		Password: "password",
	}); err != nil {
		t.Fatalf("expected bootstrap admin auth success, got error: %v", err)
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
		UserRef:  model.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}
	if token.AccessToken == "" {
		t.Fatal("expected non-empty access token")
	}

	engine.authMu.RLock()
	claims, ok := engine.authCache[token.AccessToken]
	engine.authMu.RUnlock()
	if !ok {
		t.Fatal("expected cached auth claims")
	}
	if claims.JTI == "" {
		t.Fatal("expected non-empty claims JTI")
	}
	if claims.Iss != "knotdb" || claims.Aud != "knotdb" {
		t.Fatalf("unexpected claims issuer/audience: %s/%s", claims.Iss, claims.Aud)
	}
	if claims.UserRef != model.UserRef("admin@example.com") {
		t.Fatalf("unexpected user_ref: %s", claims.UserRef)
	}
	if claims.UserID == uuid.Nil {
		t.Fatal("expected non-zero user_id")
	}
	if claims.IAT <= 0 || claims.EXP <= claims.IAT {
		t.Fatalf("invalid claims timestamps iat=%d exp=%d", claims.IAT, claims.EXP)
	}
	if len(claims.Roles) == 0 || len(claims.Scopes) == 0 {
		t.Fatal("expected claims roles and scopes")
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
		UserRef:  model.UserRef("admin@example.com"),
		Password: "wrong-password",
	})
	if err == nil {
		t.Fatal("expected invalid credentials error")
	}
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials, got: %v", err)
	}
}

func TestRuntimeEngine_CreateSpace_Success(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-create-space")

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
		UserRef:  model.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}

	spaceInfo, err := engine.CreateSpace(context.Background(), CreateSpaceInput{
		AccessToken: token.AccessToken,
		Name:        "default",
	})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}
	if spaceInfo.OwnerID == uuid.Nil || spaceInfo.SpaceID == uuid.Nil || spaceInfo.Name != "default" {
		t.Fatalf("unexpected space info: %#v", spaceInfo)
	}

	if _, err := os.Stat(filepath.Join(dataDir, "owners.json")); !os.IsNotExist(err) {
		t.Fatalf("expected owners.json not to exist, got: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "spaces.json")); err != nil {
		t.Fatalf("expected spaces.json to exist: %v", err)
	}
}

func TestRuntimeEngine_CreateSpace_UnauthorizedWithoutScope(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-create-space-no-scope")

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
		UserRef:  model.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}
	engine.authMu.Lock()
	claims := engine.authCache[token.AccessToken]
	claims.Roles = nil
	claims.Scopes = []string{"graph:read"}
	engine.authCache[token.AccessToken] = claims
	engine.authMu.Unlock()

	_, err = engine.CreateSpace(context.Background(), CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err == nil {
		t.Fatal("expected unauthorized error")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got: %v", err)
	}
}

func TestRuntimeEngine_AddNodeToNewSpace(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-add-node")
	ctx := context.Background()

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

	token, err := engine.Authenticate(ctx, AuthInput{
		UserRef:  model.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}

	spaceInfo, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}

	session, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: token.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	defer session.Close()

	node, err := session.AddNode(ctx, api.NodeInput{
		Content: "Hello Knotbase",
		Props: map[string]any{
			"kind": "note",
		},
	})
	if err != nil {
		t.Fatalf("expected add node success, got error: %v", err)
	}
	if node.ID == uuid.Nil || node.Content != "Hello Knotbase" || node.Props["kind"] != "note" {
		t.Fatalf("unexpected node: %#v", node)
	}

	got, err := session.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("expected get node success, got error: %v", err)
	}
	if got.ID != node.ID || got.Content != node.Content || got.Props["kind"] != "note" {
		t.Fatalf("unexpected persisted node: %#v", got)
	}
}

func TestRuntimeEngine_OpenSession_Success(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-open-session")

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
		UserRef:  model.UserRef("admin@example.com"),
		Password: "change-me-now",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}

	spaceInfo, err := engine.CreateSpace(context.Background(), CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}

	session, err := engine.OpenSession(context.Background(), OpenSessionInput{AccessToken: token.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("expected close session success, got error: %v", err)
	}

	defaultSession, err := engine.OpenSession(context.Background(), OpenSessionInput{AccessToken: token.AccessToken})
	if err != nil {
		t.Fatalf("expected open session with cached default space success, got error: %v", err)
	}
	if err := defaultSession.Close(); err != nil {
		t.Fatalf("expected close default session success, got error: %v", err)
	}
}
