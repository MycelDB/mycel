package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myceldb/mycel/domain/access"
	"github.com/myceldb/mycel/domain/identity"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
)

func TestSemanticProvisioningCLI(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "mycel")
	eng, err := mycelengine.NewEngine(mycelengine.EngineConfig{DataDir: dataDir, Mode: mycelengine.EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("init engine failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, mycelengine.AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, mycelengine.CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Personal PKM"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	if _, err := eng.CreateDomain(ctx, mycelengine.CreateDomainInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, Key: "personal-pkm", Name: "Personal PKM"}); err != nil {
		t.Fatalf("create domain failed: %v", err)
	}
	bob, err := eng.CreateUser(ctx, mycelengine.CreateUserInput{AccessToken: auth.AccessToken, User: identity.UserInput{Ref: identity.UserRef("bob"), Status: identity.UserStatusActive}, Password: "pass"})
	if err != nil {
		t.Fatalf("create bob failed: %v", err)
	}
	if _, err := eng.CreateUser(ctx, mycelengine.CreateUserInput{AccessToken: auth.AccessToken, User: identity.UserInput{Ref: identity.UserRef("charlie"), Status: identity.UserStatusActive}, Password: "pass"}); err != nil {
		t.Fatalf("create charlie failed: %v", err)
	}
	if _, err := eng.GrantSpaceAccess(ctx, mycelengine.GrantSpaceAccessInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, UserID: bob.ID, Permissions: []access.SpacePermission{access.SpacePermissionAdmin}}); err != nil {
		t.Fatalf("grant bob space admin failed: %v", err)
	}
	_ = eng.Close()

	pkgPath := filepath.Join(t.TempDir(), "openai.yaml")
	if err := os.WriteFile(pkgPath, []byte(`name: standard-openai
version: "2026.06"
model_endpoints:
  - key: openai-public
    name: OpenAI Public API
    connector_type: openai-compatible
    endpoint_url: https://api.openai.com/v1
    network_class: external_https
    privacy_class: third_party
    auth_modes: [api_key]
    operations: [embeddings]
    enabled: true
models:
  - key: openai/text-embedding-3-small
    operation: embeddings
    model_name: text-embedding-3-small
    connector_types: [openai-compatible]
    dimensions: 1536
    modality: text
    vector_space_key: openai/text-embedding-3-small
model_endpoint_capabilities:
  - model_endpoint: openai-public
    model: openai/text-embedding-3-small
    operation: embeddings
    enabled: true
`), 0o600); err != nil {
		t.Fatalf("write package failed: %v", err)
	}

	expectMycelCommandError(t, "-d", dataDir, "-u", "bob", "-p", "pass", "inference", "package", "apply", pkgPath)
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "package", "apply", pkgPath)
	runMycelCommand(t, "-d", dataDir, "-u", "bob", "-p", "pass", "semantic", "index", "add", "bob-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--template-key", "logseq.page", "--source", "subtree", "--model-endpoint", "openai-public", "--model", "openai/text-embedding-3-small")
	expectMycelCommandError(t, "-d", dataDir, "-u", "charlie", "-p", "pass", "semantic", "index", "add", "charlie-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--template-key", "logseq.page", "--source", "subtree", "--model-endpoint", "openai-public", "--model", "openai/text-embedding-3-small")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "semantic", "index", "add", "notes-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--template-key", "logseq.page", "--source", "subtree", "--model-endpoint", "openai-public", "--model", "openai/text-embedding-3-small")
	expectMycelCommandError(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "credential", "add", "bad-inline", "--model-endpoint", "openai-public", "--owner-user", "martin", "--api-key", "sk-test-secret")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "credential", "add", "martin-openai", "--model-endpoint", "openai-public", "--owner-user", "martin", "--external-ref", "vault://test/openai")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "credential", "grant", "martin-openai", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--semantic-index", "notes-search", "--operation", "embeddings", "--allow-background-use")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "policy", "allow", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--operation", "embeddings", "--privacy-class", "third_party")

	for _, rel := range []string{
		"meta/inference/packages.json",
		"meta/inference/model_endpoints.json",
		"meta/inference/models.json",
		"meta/inference/model_endpoint_capabilities.json",
		"meta/credentials/credentials.json",
		"meta/secrets/secrets.json",
		filepath.Join("graphs", space.SpaceID.String(), "semantic", "indexes.json"),
		filepath.Join("graphs", space.SpaceID.String(), "semantic", "credential_grants.json"),
		filepath.Join("graphs", space.SpaceID.String(), "semantic", "inference_policies.json"),
		"meta/semantic_events/semantic-config-000001.ksem",
	} {
		if _, err := os.Stat(filepath.Join(dataDir, rel)); err != nil {
			t.Fatalf("expected %s to exist: %v", rel, err)
		}
	}
	secretsRaw, err := os.ReadFile(filepath.Join(dataDir, "meta", "secrets", "secrets.json"))
	if err != nil {
		t.Fatalf("read secrets failed: %v", err)
	}
	if strings.Contains(string(secretsRaw), "sk-test-secret") {
		t.Fatalf("secret store contains plaintext API key: %s", string(secretsRaw))
	}
	eventsRaw, err := os.ReadFile(filepath.Join(dataDir, "meta", "semantic_events", "semantic-config-000001.ksem"))
	if err != nil {
		t.Fatalf("read events failed: %v", err)
	}
	for _, expected := range []string{"inference_package_applied", "semantic_index_changed", "credential_changed", "credential_grant_changed", "inference_policy_changed"} {
		if !strings.Contains(string(eventsRaw), expected) {
			t.Fatalf("expected semantic config event %q in %s", expected, string(eventsRaw))
		}
	}
}

func runMycelCommand(t *testing.T, args ...string) {
	t.Helper()
	a := &app.App{}
	cmd := NewRootCommand(a, false)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("mycel %v failed: %v", args, err)
	}
}

func expectMycelCommandError(t *testing.T, args ...string) {
	t.Helper()
	a := &app.App{}
	cmd := NewRootCommand(a, false)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err == nil {
		t.Fatalf("expected mycel %v to fail", args)
	}
}
