package backfill

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	"github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	"github.com/myceldb/mycel/internal/graph/filesession"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestRunnerBackfillsSemanticIndexAndSkipsCurrentHash(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, child := env.addRootWithChild(t, "root note", "child note")
	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil {
		t.Fatalf("backfill failed: %v", err)
	}
	if result.SelectedCount != 1 || result.GeneratedCount != 1 || result.Records[0].NodeID != root.ID {
		t.Fatalf("unexpected backfill result: %+v child=%s", result, child.ID)
	}
	if len(env.connector.calls) != 1 || !strings.Contains(env.connector.calls[0], "child note") {
		t.Fatalf("expected subtree source sent to connector, calls=%+v", env.connector.calls)
	}
	search, err := env.vector.Search(ctx, vectorstore.SearchInput{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: env.index.ID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(search) != 1 || search[0].NodeID != root.ID || search[0].Record.CredentialGrantID != env.grant.ID {
		t.Fatalf("expected stored semantic record with grant provenance, got %+v", search)
	}
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil {
		t.Fatalf("second backfill failed: %v", err)
	}
	if result.GeneratedCount != 0 || result.SkippedCount != 1 || len(env.connector.calls) != 1 {
		t.Fatalf("expected current hash skip without connector call, result=%+v calls=%+v", result, env.connector.calls)
	}
	if _, err := env.sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: root.ID, TemplateID: root.TemplateID, Content: "changed root", Props: root.Props}); err != nil {
		t.Fatalf("update root failed: %v", err)
	}
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil || result.GeneratedCount != 1 || len(env.connector.calls) != 2 {
		t.Fatalf("expected changed source to regenerate, result=%+v calls=%+v err=%v", result, env.connector.calls, err)
	}
	if _, err := env.sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: root.ID, TemplateID: root.TemplateID, Content: "root note", Props: root.Props}); err != nil {
		t.Fatalf("restore root failed: %v", err)
	}
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil || result.GeneratedCount != 1 || len(env.connector.calls) != 3 {
		t.Fatalf("expected restored historical source to regenerate because latest active hash differs, result=%+v calls=%+v err=%v", result, env.connector.calls, err)
	}
	newModelID := domainsemantic.InferenceModelID(uuid.New())
	newCapID := domainsemantic.ModelEndpointCapabilityID(uuid.New())
	if _, err := env.globalMgr.UpsertModel(ctx, domainsemantic.InferenceModel{ID: newModelID, Key: "test/embedding-v2", Operation: domainsemantic.OperationEmbeddings, ModelName: "embedding-v2", Dimensions: 3, VectorSpaceKey: "test/embedding-v2", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("new model upsert failed: %v", err)
	}
	if _, err := env.globalMgr.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ID: newCapID, ModelEndpointID: env.index.ModelEndpointID, ModelID: newModelID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("new capability upsert failed: %v", err)
	}
	env.grant.ModelID = &newModelID
	if _, err := env.spaceMgr.UpsertCredentialGrant(ctx, env.grant); err != nil {
		t.Fatalf("grant model update failed: %v", err)
	}
	env.index.ModelID = newModelID
	env.index.ModelEndpointCapabilityID = newCapID
	if _, err := env.spaceMgr.UpsertSemanticIndex(ctx, env.index); err != nil {
		t.Fatalf("semantic index model update failed: %v", err)
	}
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil {
		t.Fatalf("backfill after model change failed: %v", err)
	}
	if result.GeneratedCount != 1 || len(env.connector.calls) != 4 {
		t.Fatalf("expected model binding change to regenerate despite same source hash, result=%+v calls=%+v", result, env.connector.calls)
	}
}

func TestRunnerRequiresPolicyAndBackgroundGrant(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	env.addRootWithChild(t, "root", "child")
	if _, err := env.spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{ID: uuid.New(), Scope: domainsemantic.ProcessingScope{SpaceID: env.spaceID, DomainID: env.domainID}, Effect: domainsemantic.PolicyEffectDeny, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("deny policy upsert failed: %v", err)
	}
	if _, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID}); err == nil || !strings.Contains(err.Error(), "denies") {
		t.Fatalf("expected deny policy error, got %v", err)
	}

	env = newBackfillTestEnv(t)
	env.addRootWithChild(t, "root", "child")
	env.grant.AllowBackgroundUse = false
	if _, err := env.spaceMgr.UpsertCredentialGrant(ctx, env.grant); err != nil {
		t.Fatalf("grant update failed: %v", err)
	}
	if _, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID}); err == nil || !strings.Contains(err.Error(), "no background credential grant") {
		t.Fatalf("expected missing background grant error, got %v", err)
	}
}

func TestRunnerExplicitRootsAndForce(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, child := env.addRootWithChild(t, "root note", "child note")
	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID, NodeIDs: []graph.NodeID{child.ID}, Force: true})
	if err != nil {
		t.Fatalf("forced explicit backfill failed: %v", err)
	}
	if result.SelectedCount != 1 || result.GeneratedCount != 1 || result.Records[0].NodeID != child.ID || result.Records[0].NodeID == root.ID {
		t.Fatalf("expected explicit child root, got %+v", result)
	}
}

type backfillTestEnv struct {
	spaceID   domainspace.SpaceID
	domainID  graph.DomainID
	userID    identity.UserID
	sess      sessionapi.Session
	globalMgr storesemantic.GlobalManager
	spaceMgr  storesemantic.SpaceManager
	vector    vectorstore.MycelFileBackend
	connector *fakeConnector
	runner    Runner
	template  graph.Template
	index     domainsemantic.SemanticIndex
	grant     domainsemantic.CredentialGrant
}

func newBackfillTestEnv(t *testing.T) *backfillTestEnv {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	userID := identity.UserID(uuid.New())
	spacePath := filepath.Join(root, "graphs", spaceID.String())
	if err := os.MkdirAll(spacePath, 0o755); err != nil {
		t.Fatalf("create space path failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(spacePath, ".space"), []byte(""), 0o600); err != nil {
		t.Fatalf("create space marker failed: %v", err)
	}
	tmplMgr := storetemplate.NewManager()
	if err := tmplMgr.Init(ctx, filepath.Join(root, "meta", "templates")); err != nil {
		t.Fatalf("template init failed: %v", err)
	}
	sess := filesession.NewWithStoreConfig(filepath.Join(root, "graphs"), filepath.Join(root, "blobs"), spaceID, tmplMgr, sessionapi.Permissions{Read: true, Write: true, Admin: true}, sessionapi.Errors{NotFound: errNotFound{}}, nil, filesession.Config{CurrentUserID: userID, DomainID: domainID})
	templates, err := sess.ImportTemplates(ctx, sessionapi.ImportTemplatesInput{Document: sessionapi.ImportDocument{SchemaVersion: 1, Templates: []sessionapi.TemplateImport{{Key: "note", Version: "1.0.0", DisplayName: "Note", Children: sessionapi.ChildPolicyImport{Allowed: true, AllowedTemplates: []sessionapi.TemplateRefImport{{Key: "note", Version: "1.0.0"}}}}}}})
	if err != nil {
		t.Fatalf("import templates failed: %v", err)
	}
	globalMgr := storesemantic.NewGlobalManager()
	if err := globalMgr.Init(ctx, filepath.Join(root, "meta")); err != nil {
		t.Fatalf("global init failed: %v", err)
	}
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatalf("space semantic init failed: %v", err)
	}
	now := time.Now().UTC()
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	capID := domainsemantic.ModelEndpointCapabilityID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())
	if _, err := globalMgr.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{ID: endpointID, Key: "openai", Name: "OpenAI", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "http://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("endpoint upsert failed: %v", err)
	}
	if _, err := globalMgr.UpsertModel(ctx, domainsemantic.InferenceModel{ID: modelID, Key: "test/embedding", Operation: domainsemantic.OperationEmbeddings, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "test/embedding", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("model upsert failed: %v", err)
	}
	if _, err := globalMgr.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ID: capID, ModelEndpointID: endpointID, ModelID: modelID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("capability upsert failed: %v", err)
	}
	if _, err := globalMgr.UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("vector store upsert failed: %v", err)
	}
	if _, err := globalMgr.UpsertCredential(ctx, domainsemantic.InferenceCredential{ID: credentialID, Key: "cred", Name: "Credential", ModelEndpointID: endpointID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: userID.String(), AuthType: domainsemantic.AuthModeNone, Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("credential upsert failed: %v", err)
	}
	index, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "notes", Name: "Notes", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{TemplateKeys: []string{"note"}, Extraction: domainsemantic.SourceExtractionSubtree}, ModelEndpointID: endpointID, ModelID: modelID, ModelEndpointCapabilityID: capID, VectorStoreID: storeID, Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	grant, err := spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: credentialID, Scope: domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, ModelEndpointID: &endpointID, ModelID: &modelID, AllowBackgroundUse: true, CreatedAt: now})
	if err != nil {
		t.Fatalf("grant upsert failed: %v", err)
	}
	if _, err := spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{Scope: domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID}, Effect: domainsemantic.PolicyEffectAllow, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowedPrivacyClasses: []domainsemantic.PrivacyClass{domainsemantic.PrivacyClassThirdParty}, CreatedAt: now}); err != nil {
		t.Fatalf("policy upsert failed: %v", err)
	}
	connector := &fakeConnector{}
	vector := vectorstore.MycelFileBackend{GraphsDir: filepath.Join(root, "graphs")}
	env := &backfillTestEnv{spaceID: spaceID, domainID: domainID, userID: userID, sess: sess, globalMgr: globalMgr, spaceMgr: spaceMgr, vector: vector, connector: connector, template: templates[0], index: index, grant: grant}
	env.runner = Runner{Session: sess, GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connector, VectorBackend: vector}
	return env
}

func (e *backfillTestEnv) addRootWithChild(t *testing.T, rootText, childText string) (graph.Node, graph.Node) {
	t.Helper()
	ctx := context.Background()
	root, err := e.sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &e.template.ID, Content: rootText})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := e.sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &e.template.ID, Content: childText})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	if _, err := e.sess.AddEdge(ctx, sessionapi.AddEdgeInput{FromID: root.ID, ToID: child.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 1}}); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	return root, child
}

type fakeConnector struct{ calls []string }

func (f *fakeConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated"}, nil
}

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }
