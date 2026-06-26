package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/session"
)

func TestSemanticIndexBackfillCLI(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "mycel")
	var providerCalls int
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Fatalf("unexpected provider path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-cli-test" {
			t.Fatalf("unexpected auth header %q", got)
		}
		providerCalls++
		w.Header().Set("X-Request-Id", "req-cli")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[1,0,0]}],"usage":{"prompt_tokens":4,"total_tokens":4}}`))
	}))
	defer provider.Close()
	t.Setenv("MYCEL_TEST_OPENAI_KEY", "sk-cli-test")

	eng, err := mycelengine.NewEngine(mycelengine.EngineConfig{DataDir: dataDir, Mode: mycelengine.EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("init engine failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, mycelengine.AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, mycelengine.CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Backfill Space"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	if _, err := eng.CreateDomain(ctx, mycelengine.CreateDomainInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, Key: "personal-pkm", Name: "Personal PKM"}); err != nil {
		t.Fatalf("create domain failed: %v", err)
	}
	if _, err := eng.CreateDomain(ctx, mycelengine.CreateDomainInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, Key: "archive", Name: "Archive"}); err != nil {
		t.Fatalf("create archive domain failed: %v", err)
	}
	sess, err := eng.OpenSession(ctx, mycelengine.OpenSessionInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID, DomainKey: "personal-pkm"})
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	templates, err := sess.ImportTemplates(ctx, session.ImportTemplatesInput{Document: session.ImportDocument{SchemaVersion: 1, Templates: []session.TemplateImport{{Key: "note", Version: "1.0.0", DisplayName: "Note", Children: session.ChildPolicyImport{Allowed: true, AllowedTemplates: []session.TemplateRefImport{{Key: "note", Version: "1.0.0"}}}}}}})
	if err != nil {
		t.Fatalf("import templates failed: %v", err)
	}
	root, err := sess.AddNode(ctx, session.AddNodeInput{TemplateID: &templates[0].ID, Content: "root note"})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := sess.AddNode(ctx, session.AddNodeInput{TemplateID: &templates[0].ID, Content: "child note"})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	if _, err := sess.AddEdge(ctx, session.AddEdgeInput{FromID: root.ID, ToID: child.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 1}}); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	_ = sess.Close()
	_ = eng.Close()

	pkgPath := filepath.Join(t.TempDir(), "openai.yaml")
	if err := os.WriteFile(pkgPath, []byte(`name: test-openai
version: "2026.06"
model_endpoints:
  - key: test-openai
    name: Test OpenAI
    connector_type: openai-compatible
    endpoint_url: `+provider.URL+`/v1
    network_class: external_https
    privacy_class: third_party
    auth_modes: [api_key]
    operations: [embeddings]
    enabled: true
models:
  - key: test/text-embedding
    operation: embeddings
    model_name: text-embedding-test
    connector_types: [openai-compatible]
    dimensions: 3
    modality: text
    vector_space_key: test/text-embedding
model_endpoint_capabilities:
  - model_endpoint: test-openai
    model: test/text-embedding
    operation: embeddings
    enabled: true
`), 0o600); err != nil {
		t.Fatalf("write package failed: %v", err)
	}

	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "package", "apply", pkgPath)
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "credential", "add", "test-openai-key", "--model-endpoint", "test-openai", "--owner-user", "admin", "--external-ref", "env:MYCEL_TEST_OPENAI_KEY")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "semantic", "index", "add", "notes-search", "--space-id", space.SpaceID.String(), "--domain", "archive", "--template-key", "note", "--source", "subtree", "--model-endpoint", "test-openai", "--model", "test/text-embedding")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "semantic", "index", "add", "notes-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--template-key", "note", "--source", "subtree", "--model-endpoint", "test-openai", "--model", "test/text-embedding")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "credential", "grant", "test-openai-key", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--semantic-index", "notes-search", "--operation", "embeddings", "--allow-background-use")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "inference", "policy", "allow", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm", "--operation", "embeddings", "--privacy-class", "third_party")
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "semantic", "index", "backfill", "notes-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm")
	if providerCalls != 1 {
		t.Fatalf("expected one provider call, got %d", providerCalls)
	}
	runMycelCommand(t, "-d", dataDir, "-u", "admin", "-p", "pass", "semantic", "index", "backfill", "notes-search", "--space-id", space.SpaceID.String(), "--domain", "personal-pkm")
	if providerCalls != 1 {
		t.Fatalf("expected second backfill to skip current hash, got %d calls", providerCalls)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "graphs", space.SpaceID.String(), "semantic", "indexes")); err != nil {
		t.Fatalf("expected semantic vector index directory: %v", err)
	}
	usageRaw, err := os.ReadFile(filepath.Join(dataDir, "meta", "accounting", "inference-usage-000001.kusag"))
	if err != nil {
		t.Fatalf("read accounting ledger failed: %v", err)
	}
	if !strings.Contains(string(usageRaw), "req-cli") || !strings.Contains(string(usageRaw), "semantic_backfill") {
		t.Fatalf("expected backfill accounting event, got %s", string(usageRaw))
	}
	var line map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(string(usageRaw)), "\n") {
		if raw != "" {
			_ = json.Unmarshal([]byte(raw), &line)
		}
	}
	if line["status"] != "success" {
		t.Fatalf("expected successful accounting status, got %+v", line)
	}
}
