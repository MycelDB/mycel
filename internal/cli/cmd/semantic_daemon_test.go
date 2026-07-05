package cmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	storesemantic "github.com/myceldb/mycel/store/semantic"
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
	indexID := seedDaemonSemanticIndex(t, dataDir, spaceID, domainID)
	base := []string{"--daemon-addr", addr, "-u", "semantic-user", "-p", "semantic-pass", "--output", "json"}
	out, err = runCLI(t, append(base, "semantic", "index", "list", "--space-id", spaceID, "--domain", graph.DefaultDomainKey)...)
	if err != nil {
		t.Fatalf("semantic index list failed: %v\n%s", err, out)
	}
	var list clientv1.ListSemanticIndexesResponse
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("decode index list: %v\n%s", err, out)
	}
	if len(list.GetIndexes()) != 1 || list.GetIndexes()[0].GetSemanticIndexId() != indexID.String() || list.GetIndexes()[0].GetKey() != "notes-search" {
		t.Fatalf("unexpected semantic indexes: %#v", list.GetIndexes())
	}
	out, err = runCLI(t, append(base, "semantic", "search", "--space-id", spaceID, "--domain", graph.DefaultDomainKey, "--index", "notes-search", "--text", "hello")...)
	if err != nil {
		t.Fatalf("semantic search failed: %v\n%s", err, out)
	}
	var search clientv1.SemanticSearchResponse
	if err := json.Unmarshal([]byte(out), &search); err != nil {
		t.Fatalf("decode search: %v\n%s", err, out)
	}
	if len(search.GetWarnings()) == 0 {
		t.Fatalf("expected safe warning for unprovisioned policy/grant, got %#v", search)
	}
}

func seedDaemonSemanticIndex(t *testing.T, dataDir string, spaceIDText string, domainIDText string) domainsemantic.SemanticIndexID {
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
	model, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{Key: "test/embedding", Operation: domainsemantic.OperationEmbeddings, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "test/embedding", CreatedAt: now, UpdatedAt: now})
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
	index, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "notes-search", Name: "Notes Search", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: endpoint.ID, ModelID: model.ID, ModelEndpointCapabilityID: capability.ID, VectorStoreID: vectorStore.ID, Enabled: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert semantic index: %v", err)
	}
	return index.ID
}
