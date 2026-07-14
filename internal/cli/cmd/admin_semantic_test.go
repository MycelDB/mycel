package cmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

func TestAdminSemanticIndexAddUsesDaemonGRPC(t *testing.T) {
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
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "semantic", "index", "add", "admin-notes", "--space-id", spaceID, "--domain", domainID, "--source", "self", "--model-endpoint", ids.endpointID.String(), "--model", ids.modelID.String(), "--vector-store", ids.vectorStoreID.String())
	if err != nil {
		t.Fatalf("semantic index add failed: %v\n%s", err, out)
	}
	var added clientv1.SemanticIndex
	if err := json.Unmarshal([]byte(out), &added); err != nil {
		t.Fatalf("decode semantic index add: %v\n%s", err, out)
	}
	if added.GetKey() != "admin-notes" || added.GetSemanticIndexId() == "" {
		t.Fatalf("unexpected added index: %#v", &added)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin-semantic-user", "-p", "semantic-pass", "--output", "json", "semantic", "index", "list", "--space-id", spaceID, "--domain", domainID)
	if err != nil {
		t.Fatalf("semantic index list failed: %v\n%s", err, out)
	}
	var list clientv1.ListSemanticIndexesResponse
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("decode semantic index list: %v\n%s", err, out)
	}
	if len(list.GetIndexes()) != 1 || list.GetIndexes()[0].GetKey() != "admin-notes" {
		t.Fatalf("unexpected index list: %#v", list.GetIndexes())
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
	model, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{Key: "admin-test/embedding", Operation: domainsemantic.OperationEmbeddings, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "admin-test/embedding", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatalf("upsert model: %v", err)
	}
	if _, err := global.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ModelEndpointID: endpoint.ID, ModelID: model.ID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert capability: %v", err)
	}
	return daemonSemanticGlobalIDs{endpointID: endpoint.ID, modelID: model.ID, vectorStoreID: vectorStore.ID}
}
