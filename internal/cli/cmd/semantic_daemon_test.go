package cmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

func TestSemanticServiceCommandsUseDaemonGRPC(t *testing.T) {
	dataDir, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "semantic-user", "semantic-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Semantic Space", "--owner-username", "semantic-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	ruleID := seedDaemonSemanticRule(t, dataDir, spaceID, domainID)
	base := []string{"--daemon-addr", addr, "-u", "semantic-user", "-p", "semantic-pass", "--output", "json"}
	out, err = runCLI(t, append(base, "semantic", "rule", "list", "--space-id", spaceID, "--domain", graph.DefaultDomainKey)...)
	if err != nil {
		t.Fatalf("semantic rule list failed: %v\n%s", err, out)
	}
	var list clientv1.ListSemanticRulesResponse
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("decode rule list: %v\n%s", err, out)
	}
	if len(list.GetRules()) != 1 || list.GetRules()[0].GetSemanticRuleId() != ruleID.String() || list.GetRules()[0].GetKey() != "notes-search" {
		t.Fatalf("unexpected semantic rules: %#v", list.GetRules())
	}
	out, err = runCLI(t, append(base, "semantic", "search", "--space-id", spaceID, "--domain", graph.DefaultDomainKey, "--rule", "notes-search", "--binding", "search", "--text", "hello")...)
	if err == nil {
		t.Fatalf("expected fail-closed semantic search without a physical search index, out=%s", out)
	}
}

func seedDaemonSemanticRule(t *testing.T, dataDir string, spaceIDText string, domainIDText string) domainsemantic.SemanticRuleID {
	t.Helper()
	ctx := context.Background()
	spaceID, err := uuid.Parse(spaceIDText)
	if err != nil {
		t.Fatalf("parse space id: %v", err)
	}
	domainID, err := uuid.Parse(domainIDText)
	if err != nil {
		t.Fatalf("parse domain id: %v", err)
	}
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(dataDir, "meta")); err != nil {
		t.Fatalf("init global semantic manager: %v", err)
	}
	vectorStore, err := global.EnsureDefaultVectorStore(ctx)
	if err != nil {
		t.Fatalf("ensure vector store: %v", err)
	}
	now := time.Now().UTC()
	endpoint, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "test-endpoint", Name: "Test Endpoint", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "http://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert endpoint: %v", err)
	}
	model, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{Key: "test/embedding", Kind: domainsemantic.ModelKindEmbedding, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "test/embedding", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	capability, err := global.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ModelEndpointID: endpoint.ID, ModelID: model.ID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(dataDir, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatalf("init space semantic manager: %v", err)
	}
	_ = capability
	rule, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{SpaceID: spaceID, DomainID: domainID, Key: "notes-search", DisplayName: "Notes Search", Enabled: true, Trigger: domainsemantic.SemanticTriggerPolicy{Events: []string{domainsemantic.DefaultSemanticTriggerEventChanged}, Labels: []string{"Note"}}, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Note"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), IntelligenceProfile: "test-profile", VectorStoreID: vectorStore.ID, Enabled: true}}, Storage: domainsemantic.SemanticStoragePolicy{Searchable: true, PhysicalIndex: domainsemantic.SemanticPhysicalIndexExact}, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert semantic rule: %v", err)
	}
	return rule.ID
}
