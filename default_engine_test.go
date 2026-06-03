package knotdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	coreuser "martinbeauvais.com/mbgit/knotbase/knotdb/core/user"
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
	if len(claims.Roles) == 0 || claims.Roles[0] != model.SystemRoleSuperuser || len(claims.Scopes) == 0 {
		t.Fatalf("expected superuser claims roles and scopes, got roles=%v scopes=%v", claims.Roles, claims.Scopes)
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

func TestRuntimeEngine_GrantSystemRoleAndRevokeLastSuperuserFails(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-system-roles")
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

	adminToken, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected admin auth success, got error: %v", err)
	}
	status := model.UserStatusActive
	operator, err := engine.userManager.Create(ctx, coreuser.CreateInput{
		User:     model.UserInput{Ref: model.UserRef("operator@example.com"), Status: status},
		Password: "operator-password",
	})
	if err != nil {
		t.Fatalf("expected create operator success, got error: %v", err)
	}
	if _, err := engine.GrantSystemRole(ctx, GrantSystemRoleInput{
		AccessToken: adminToken.AccessToken,
		UserID:      operator.ID,
		Roles:       []model.SystemRole{model.SystemRoleOperator},
	}); err != nil {
		t.Fatalf("expected grant system role success, got error: %v", err)
	}
	roles, err := engine.accessManager.SystemRolesForUser(ctx, operator.ID)
	if err != nil || len(roles) != 1 || roles[0] != model.SystemRoleOperator {
		t.Fatalf("unexpected operator roles=%v err=%v", roles, err)
	}
	err = engine.RevokeSystemRole(ctx, RevokeSystemRoleInput{AccessToken: adminToken.AccessToken, UserID: engine.authCache[adminToken.AccessToken].UserID})
	if err == nil {
		t.Fatal("expected last superuser revoke error")
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
	if _, err := os.Stat(filepath.Join(dataDir, "meta", "access.json")); err != nil {
		t.Fatalf("expected meta/access.json to exist: %v", err)
	}
}

func TestRuntimeEngine_CreateSpace_UnauthorizedWithoutSystemRole(t *testing.T) {
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

	status := model.UserStatusActive
	_, err := engine.userManager.Create(context.Background(), coreuser.CreateInput{
		User:     model.UserInput{Ref: model.UserRef("regular@example.com"), Status: status},
		Password: "regular-password",
	})
	if err != nil {
		t.Fatalf("expected create regular user success, got error: %v", err)
	}
	token, err := engine.Authenticate(context.Background(), AuthInput{
		UserRef:  model.UserRef("regular@example.com"),
		Password: "regular-password",
	})
	if err != nil {
		t.Fatalf("expected authenticate success, got error: %v", err)
	}

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

func TestRuntimeEngine_SpaceAccessReadOnlyUserCannotWrite(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-access-read-only")
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

	adminToken, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected admin authenticate success, got error: %v", err)
	}
	spaceInfo, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: adminToken.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}
	adminSession, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: adminToken.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected admin open session success, got error: %v", err)
	}
	node, err := adminSession.AddNode(ctx, graph.NodeInput{Content: "readable"})
	if err != nil {
		t.Fatalf("expected admin add node success, got error: %v", err)
	}
	_ = adminSession.Close()

	status := model.UserStatusActive
	reader, err := engine.userManager.Create(ctx, coreuser.CreateInput{
		User:     model.UserInput{Ref: model.UserRef("reader@example.com"), Status: status},
		Password: "reader-password",
	})
	if err != nil {
		t.Fatalf("expected create reader success, got error: %v", err)
	}
	if _, err := engine.GrantSpaceAccess(ctx, GrantSpaceAccessInput{
		AccessToken: adminToken.AccessToken,
		SpaceID:     spaceInfo.SpaceID,
		UserID:      reader.ID,
		Permissions: []model.SpacePermission{model.SpacePermissionRead},
	}); err != nil {
		t.Fatalf("expected grant read success, got error: %v", err)
	}

	readerToken, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("reader@example.com"), Password: "reader-password"})
	if err != nil {
		t.Fatalf("expected reader authenticate success, got error: %v", err)
	}
	readerSession, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: readerToken.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected reader open session success, got error: %v", err)
	}
	if _, err := readerSession.GetNode(ctx, node.ID); err != nil {
		t.Fatalf("expected reader get node success, got error: %v", err)
	}
	if _, err := readerSession.AddNode(ctx, graph.NodeInput{Content: "should fail"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for reader write, got: %v", err)
	}
}

func TestRuntimeEngine_RevokeLastSpaceAdminFails(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-access-last-admin")
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

	rules, err := engine.ListSpaceAccess(ctx, ListSpaceAccessInput{AccessToken: token.AccessToken, SpaceID: spaceInfo.SpaceID})
	if err != nil {
		t.Fatalf("expected list access success, got error: %v", err)
	}
	if len(rules) != 1 || rules[0].UserID != spaceInfo.OwnerID || rules[0].Permissions[0] != model.SpacePermissionAdmin {
		t.Fatalf("unexpected access rules: %#v", rules)
	}
	err = engine.RevokeSpaceAccess(ctx, RevokeSpaceAccessInput{AccessToken: token.AccessToken, SpaceID: spaceInfo.SpaceID, UserID: spaceInfo.OwnerID})
	if err == nil {
		t.Fatal("expected last admin revoke error")
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
func TestRuntimeEngine_DeleteUserCascadesOwnedSpaces(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-delete-user-cascade")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	adminToken, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected admin auth success, got error: %v", err)
	}
	bob, err := engine.CreateUser(ctx, CreateUserInput{AccessToken: adminToken.AccessToken, User: model.UserInput{Ref: model.UserRef("bob@example.com"), Status: model.UserStatusActive}, Password: "bob-password"})
	if err != nil {
		t.Fatalf("expected create user success, got error: %v", err)
	}
	if _, err := engine.GrantSystemRole(ctx, GrantSystemRoleInput{AccessToken: adminToken.AccessToken, UserID: bob.ID, Roles: []model.SystemRole{model.SystemRoleSuperuser}}); err != nil {
		t.Fatalf("expected grant superuser success, got error: %v", err)
	}
	bobToken, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("bob@example.com"), Password: "bob-password"})
	if err != nil {
		t.Fatalf("expected bob auth success, got error: %v", err)
	}
	sp, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: bobToken.AccessToken, Name: "owned"})
	if err != nil {
		t.Fatalf("expected bob create space success, got error: %v", err)
	}
	sess, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: bobToken.AccessToken, SpaceID: sp.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	if _, err := sess.AddNode(ctx, graph.NodeInput{Content: "will be deleted"}); err != nil {
		t.Fatalf("expected add node success, got error: %v", err)
	}
	_ = sess.Close()

	if err := engine.DeleteUser(ctx, DeleteUserInput{AccessToken: adminToken.AccessToken, UserID: bob.ID}); err != nil {
		t.Fatalf("expected delete user success, got error: %v", err)
	}
	if _, err := engine.userManager.GetByID(ctx, bob.ID); !errors.Is(err, coreuser.ErrUserNotFound) {
		t.Fatalf("expected user manager not found, got: %v", err)
	}
	if _, err := engine.spaceManager.GetByID(ctx, sp.SpaceID); err == nil {
		t.Fatal("expected owned space to be deleted")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "graphs", sp.SpaceID.String())); !os.IsNotExist(err) {
		t.Fatalf("expected graph directory removed, got: %v", err)
	}
	if _, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("bob@example.com"), Password: "bob-password"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected deleted user authentication to fail, got: %v", err)
	}
}

func TestRuntimeEngine_DeleteNodeRequiresRecursiveForChildren(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-delete-node-recursive")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected auth success, got error: %v", err)
	}
	sp, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}
	sess, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: token.AccessToken, SpaceID: sp.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	parent, err := sess.AddNode(ctx, graph.NodeInput{Content: "parent"})
	if err != nil {
		t.Fatalf("expected add parent success, got error: %v", err)
	}
	child, err := sess.AddNode(ctx, graph.NodeInput{ParentID: &parent.ID, Content: "child"})
	if err != nil {
		t.Fatalf("expected add child success, got error: %v", err)
	}
	if _, err := sess.AddEdge(ctx, graph.EdgeInput{FromID: parent.ID, ToID: child.ID, Kind: graph.EdgeKindContains}); err != nil {
		t.Fatalf("expected add edge success, got error: %v", err)
	}
	if err := sess.DeleteNode(ctx, graph.DeleteNodeInput{ID: parent.ID}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict deleting parent without recursive, got: %v", err)
	}
	if err := sess.DeleteNode(ctx, graph.DeleteNodeInput{ID: parent.ID, Recursive: true}); err != nil {
		t.Fatalf("expected recursive delete success, got error: %v", err)
	}
	if _, err := sess.GetNode(ctx, child.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected child to be deleted, got: %v", err)
	}
	_ = sess.Close()
}
func TestRuntimeEngine_DeleteSpaceInvalidatesOpenSession(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-delete-space-invalidates-session")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected auth success, got error: %v", err)
	}
	sp, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}
	sess, err := engine.OpenSession(ctx, OpenSessionInput{AccessToken: token.AccessToken, SpaceID: sp.SpaceID})
	if err != nil {
		t.Fatalf("expected open session success, got error: %v", err)
	}
	if err := engine.DeleteSpace(ctx, DeleteSpaceInput{AccessToken: token.AccessToken, SpaceID: sp.SpaceID}); err != nil {
		t.Fatalf("expected delete space success, got error: %v", err)
	}
	if _, err := sess.AddNode(ctx, graph.NodeInput{Content: "stale write"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected stale session write to fail with ErrNotFound, got: %v", err)
	}
	_ = sess.Close()
}
func TestRuntimeEngine_ListUsersAndSpaces(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-list-users-spaces")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected auth success, got error: %v", err)
	}
	bob, err := engine.CreateUser(ctx, CreateUserInput{AccessToken: token.AccessToken, User: model.UserInput{Ref: model.UserRef("bob@example.com"), Status: model.UserStatusActive}, Password: "bob-password"})
	if err != nil {
		t.Fatalf("expected create user success, got error: %v", err)
	}
	sp, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}

	users, err := engine.ListUsers(ctx, ListUsersInput{AccessToken: token.AccessToken})
	if err != nil {
		t.Fatalf("expected list users success, got error: %v", err)
	}
	if !hasUserID(users, bob.ID) {
		t.Fatalf("expected listed users to contain bob %s: %v", bob.ID, users)
	}
	spaces, err := engine.ListSpaces(ctx, ListSpacesInput{AccessToken: token.AccessToken})
	if err != nil {
		t.Fatalf("expected list spaces success, got error: %v", err)
	}
	if !hasSpaceID(spaces, sp.SpaceID) {
		t.Fatalf("expected listed spaces to contain %s: %v", sp.SpaceID, spaces)
	}
}

func hasUserID(users []model.User, id model.UserID) bool {
	for _, u := range users {
		if u.ID == id {
			return true
		}
	}
	return false
}

func hasSpaceID(spaces []model.Space, id model.SpaceID) bool {
	for _, sp := range spaces {
		if sp.SpaceID == id {
			return true
		}
	}
	return false
}
func TestRuntimeEngine_ListSystemAccess(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-list-system-access")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected auth success, got error: %v", err)
	}
	rules, err := engine.ListSystemAccess(ctx, ListSystemAccessInput{AccessToken: token.AccessToken})
	if err != nil {
		t.Fatalf("expected list system access success, got error: %v", err)
	}
	if len(rules) != 1 || !containsSystemRole(rules[0].Roles, model.SystemRoleSuperuser) {
		t.Fatalf("expected bootstrap superuser rule, got: %v", rules)
	}
}
func TestRuntimeEngine_ImportAndListTemplates(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb-list-templates")
	ctx := context.Background()

	engine := &defaultEngine{}
	if err := engine.Open(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now"}); err != nil {
		t.Fatalf("expected open success, got error: %v", err)
	}
	token, err := engine.Authenticate(ctx, AuthInput{UserRef: model.UserRef("admin@example.com"), Password: "change-me-now"})
	if err != nil {
		t.Fatalf("expected auth success, got error: %v", err)
	}
	sp, err := engine.CreateSpace(ctx, CreateSpaceInput{AccessToken: token.AccessToken, Name: "default"})
	if err != nil {
		t.Fatalf("expected create space success, got error: %v", err)
	}
	doc := coretemplate.ImportDocument{SchemaVersion: 1, Templates: []coretemplate.TemplateImport{{Key: "note", Version: "1.0.0", DisplayName: "Note", Properties: coretemplate.PropertyPolicyImport{AllowExtra: true}}}}
	imported, err := engine.ImportTemplates(ctx, ImportTemplatesInput{AccessToken: token.AccessToken, SpaceID: sp.SpaceID, Document: doc})
	if err != nil {
		t.Fatalf("expected import templates success, got error: %v", err)
	}
	listed, err := engine.ListTemplates(ctx, ListTemplatesInput{AccessToken: token.AccessToken, SpaceID: sp.SpaceID})
	if err != nil {
		t.Fatalf("expected list templates success, got error: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != imported[0].ID || listed[0].Key != "note" {
		t.Fatalf("unexpected listed templates: %v imported=%v", listed, imported)
	}
}
