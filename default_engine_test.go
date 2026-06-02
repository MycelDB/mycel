package knotdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
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

	usersPath := filepath.Join(dataDir, "meta", "users.json")
	if _, err := os.Stat(usersPath); err != nil {
		t.Fatalf("expected meta/users.json to be created, got error: %v", err)
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

	if _, err := os.Stat(filepath.Join(dataDir, "meta", "owners.json")); !os.IsNotExist(err) {
		t.Fatalf("expected meta/owners.json not to exist, got: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "meta", "spaces.json")); err != nil {
		t.Fatalf("expected meta/spaces.json to exist: %v", err)
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

	node, err := session.AddNode(ctx, graph.NodeInput{
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
	if _, err := os.Stat(filepath.Join(dataDir, "graphs", spaceInfo.SpaceID.String(), "nodes.json")); err != nil {
		t.Fatalf("expected graph nodes file to exist: %v", err)
	}
}

func TestRuntimeEngine_ImportTemplatesAndValidateNode(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-template-node")
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

	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}
	spaceInfo, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}

	templates, err := engine.ImportTemplates(ctx, ImportTemplatesInput{
		AccessToken: token.AccessToken,
		SpaceID:     spaceInfo.SpaceID,
		Document:    nodeTemplateDocument(),
	})
	if err != nil {
		t.Fatalf("expected import templates success, got error: %v", err)
	}
	templateByKey := map[string]graph.Template{}
	for _, tmpl := range templates {
		templateByKey[tmpl.Key] = tmpl
	}
	noteTemplate := templateByKey["note"]
	taskTemplate := templateByKey["task"]
	if noteTemplate.ID == uuid.Nil || taskTemplate.ID == uuid.Nil {
		t.Fatalf("expected note and task templates, got: %#v", templates)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "meta", "templates", spaceInfo.SpaceID.String()+".json")); err != nil {
		t.Fatalf("expected template file to exist: %v", err)
	}

	session, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: token.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	defer session.Close()

	_, err = session.AddNode(ctx, graph.NodeInput{TemplateID: &noteTemplate.ID, Props: map[string]any{}})
	if err == nil {
		t.Fatal("expected missing required property error")
	}
	_, err = session.AddNode(ctx, graph.NodeInput{TemplateID: &noteTemplate.ID, Props: map[string]any{"title": "Parent", "secret": "x"}})
	if err == nil {
		t.Fatal("expected forbidden property error")
	}
	_, err = session.AddNode(ctx, graph.NodeInput{TemplateID: &noteTemplate.ID, Props: map[string]any{"title": "Parent", "unknown": "x"}})
	if err == nil {
		t.Fatal("expected unknown property error")
	}

	parent, err := session.AddNode(ctx, graph.NodeInput{TemplateID: &noteTemplate.ID, Props: map[string]any{"title": "Parent"}})
	if err != nil {
		t.Fatalf("expected parent node success, got error: %v", err)
	}
	child, err := session.AddNode(ctx, graph.NodeInput{TemplateID: &taskTemplate.ID, ParentID: &parent.ID, Props: map[string]any{}})
	if err != nil {
		t.Fatalf("expected allowed child node success, got error: %v", err)
	}
	if child.Props["done"] != false {
		t.Fatalf("expected default done=false, got node: %#v", child)
	}

	_, err = session.AddNode(ctx, graph.NodeInput{TemplateID: &noteTemplate.ID, ParentID: &child.ID, Props: map[string]any{"title": "Nested"}})
	if err == nil {
		t.Fatal("expected child rejection for template that disallows children")
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

func nodeTemplateDocument() coretemplate.ImportDocument {
	return coretemplate.ImportDocument{
		SchemaVersion: 1,
		Templates: []coretemplate.TemplateImport{
			{
				Key:         "note",
				Version:     "1.0.0",
				DisplayName: "Note",
				Properties: coretemplate.PropertyPolicyImport{
					AllowExtra: false,
					Allowed: []coretemplate.TemplatePropertyImport{
						{Name: "title", Type: graph.PropertyTypeString, Required: true},
					},
					Forbidden: []string{"secret"},
				},
				Children: coretemplate.ChildPolicyImport{
					Allowed: true,
					AllowedTemplates: []coretemplate.TemplateRefImport{
						{Key: "task", Version: "1.0.0"},
					},
				},
			},
			{
				Key:         "task",
				Version:     "1.0.0",
				DisplayName: "Task",
				Properties: coretemplate.PropertyPolicyImport{
					AllowExtra: false,
					Allowed: []coretemplate.TemplatePropertyImport{
						{Name: "done", Type: graph.PropertyTypeBool, Default: false},
					},
				},
				Children: coretemplate.ChildPolicyImport{Allowed: false},
			},
		},
	}
}
