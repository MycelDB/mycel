package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

func TestAdminSemanticRuleCreateUsesDaemonGRPC(t *testing.T) {
	dataDir, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "admin-semantic-user", "semantic-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Admin Semantic Space", "--owner-username", "admin-semantic-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	ids := seedDaemonSemanticGlobals(t, dataDir)
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "create", "admin-notes", "--space-id", spaceID, "--domain", domainID, "--source", "self", "--profile", "admin-test-profile", "--vector-store", ids.vectorStoreID.String())
	if err != nil {
		t.Fatalf("semantic rule create failed: %v\n%s", err, out)
	}
	var added clientv1.SemanticGenerationRuleSummary
	if err := json.Unmarshal([]byte(out), &added); err != nil {
		t.Fatalf("decode semantic rule create: %v\n%s", err, out)
	}
	if added.GetKey() != "admin-notes" || added.GetSemanticRuleId() == "" {
		t.Fatalf("unexpected added rule: %#v", &added)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin-semantic-user", "-p", "semantic-pass", "--output", "json", "semantic", "rule", "list", "--space-id", spaceID, "--domain", domainID)
	if err != nil {
		t.Fatalf("semantic rule list failed: %v\n%s", err, out)
	}
	var list clientv1.ListSemanticRulesResponse
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("decode semantic rule list: %v\n%s", err, out)
	}
	if len(list.GetRules()) != 1 || list.GetRules()[0].GetKey() != "admin-notes" {
		t.Fatalf("unexpected rule list: %#v", list.GetRules())
	}
	ruleFile := filepath.Join(t.TempDir(), "semantic-rule.yaml")
	if err := os.WriteFile(ruleFile, []byte("space_id: "+spaceID+"\ndomain_id: "+domainID+"\nkey: file-notes\ndisplay_name: File Notes\nenabled: true\ntrigger:\n  events: [changed]\n  labels: [Note]\nselector:\n  mode: node_type\n  labels: [Note]\nsource:\n  mode: self\nembeddings:\n  - key: search\n    purpose: search\n    intelligence_profile: admin-test-profile\n    vector_store: mycel-file\n    enabled: true\nstorage:\n  searchable: true\n  physical_index: exact\n"), 0o600); err != nil {
		t.Fatalf("write semantic rule file: %v", err)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "validate", "--file", ruleFile)
	if err != nil {
		t.Fatalf("semantic rule validate failed: %v\n%s", err, out)
	}
	var validation adminv1.ValidateSemanticRuleResponse
	if err := json.Unmarshal([]byte(out), &validation); err != nil || !validation.GetValid() {
		t.Fatalf("unexpected validation response: %#v err=%v out=%s", &validation, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "create", "--file", ruleFile)
	if err != nil {
		t.Fatalf("semantic rule file create failed: %v\n%s", err, out)
	}
	var fileRule clientv1.SemanticGenerationRuleSummary
	if err := json.Unmarshal([]byte(out), &fileRule); err != nil || fileRule.GetKey() != "file-notes" {
		t.Fatalf("unexpected file rule: %#v err=%v out=%s", &fileRule, err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "disable", "file-notes", "--space-id", spaceID, "--domain", domainID)
	if err != nil {
		t.Fatalf("semantic rule disable failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "enable", fileRule.GetSemanticRuleId(), "--space-id", spaceID, "--domain", domainID)
	if err != nil {
		t.Fatalf("semantic rule enable failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "rule", "delete", fileRule.GetSemanticRuleId(), "--space-id", spaceID, "--domain", domainID, "--purge-vectors")
	if err != nil {
		t.Fatalf("semantic rule delete failed: %v\n%s", err, out)
	}
}

type daemonSemanticGlobalIDs struct {
	endpointID    domainsemantic.ModelEndpointID
	modelID       domainsemantic.InferenceModelID
	vectorStoreID domainsemantic.VectorStoreID
}

func seedDaemonSemanticGlobals(t *testing.T, dataDir string) daemonSemanticGlobalIDs {
	t.Helper()
	ctx := context.Background()
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(dataDir, "meta")); err != nil {
		t.Fatalf("init global semantic manager: %v", err)
	}
	vectorStore, err := global.EnsureDefaultVectorStore(ctx)
	if err != nil {
		t.Fatalf("ensure vector store: %v", err)
	}
	now := time.Now().UTC()
	endpoint, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "admin-test-endpoint", Name: "Admin Test Endpoint", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "http://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	model, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{Key: "admin-test/embedding", Kind: domainsemantic.ModelKindEmbedding, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "admin-test/embedding", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := global.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ModelEndpointID: endpoint.ID, ModelID: model.ID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	return daemonSemanticGlobalIDs{endpointID: endpoint.ID, modelID: model.ID, vectorStoreID: vectorStore.ID}
}
