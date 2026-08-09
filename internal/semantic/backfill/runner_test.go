package backfill

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
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
	if len(env.connector.inputs) != 1 || env.connector.inputs[0].OnBehalfOfPrincipalID != env.userID || env.connector.inputs[0].EffectivePrincipalID != env.userID {
		t.Fatalf("expected backfill attribution to credential owner %s, got %+v", env.userID, env.connector.inputs)
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
	env.graph.UpdateNode(root.ID, "changed root")
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil || result.GeneratedCount != 1 || len(env.connector.calls) != 2 {
		t.Fatalf("expected changed source to regenerate, result=%+v calls=%+v err=%v", result, env.connector.calls, err)
	}
	env.graph.UpdateNode(root.ID, "root note")
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

func TestRunnerTombstonesCurrentVectorWhenSourceBecomesEmpty(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, child := env.addRootWithChild(t, "root note", "child note")
	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil || result.GeneratedCount != 1 || len(env.connector.calls) != 1 {
		t.Fatalf("initial backfill result=%+v calls=%+v err=%v", result, env.connector.calls, err)
	}
	env.graph.UpdateNode(root.ID, "   ")
	env.graph.UpdateNode(child.ID, "\n\t  ")

	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil {
		t.Fatalf("empty-source backfill failed: %v", err)
	}
	if result.GeneratedCount != 0 || result.SkippedCount != 1 || len(env.connector.calls) != 1 {
		t.Fatalf("expected empty source skip without provider call, result=%+v calls=%+v", result, env.connector.calls)
	}
	search, err := env.vector.Search(ctx, vectorstore.SearchInput{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: env.index.ID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(search) != 0 {
		t.Fatalf("expected tombstoned vector to be absent from search results, got %+v", search)
	}
}

func TestRunnerAccountingAttributesBackfillToCredentialOwner(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, _ := env.addRootWithChild(t, "accounted root", "accounted child")
	accountingMgr := storeaccounting.NewManager()
	if err := accountingMgr.Init(ctx, t.TempDir()); err != nil {
		t.Fatalf("accounting init failed: %v", err)
	}
	provider := &fakeProviderConnector{}
	actorID := identity.UserID(uuid.New())
	runner := env.runner
	runner.Connector = connectors.Service{GlobalManager: env.globalMgr, Accounting: accountingMgr, ActorPrincipalID: actorID, Connectors: map[domainsemantic.ConnectorType]connectors.Connector{domainsemantic.ConnectorOpenAICompatible: provider}}
	result, err := runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticIndexID: env.index.ID})
	if err != nil {
		t.Fatalf("backfill failed: %v", err)
	}
	if result.GeneratedCount != 1 || len(provider.requests) != 1 {
		t.Fatalf("expected one accounted embedding call, result=%+v provider_requests=%+v", result, provider.requests)
	}
	events, err := accountingMgr.List(ctx, storeaccounting.Filter{})
	if err != nil {
		t.Fatalf("list accounting events failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one accounting event, got %+v", events)
	}
	event := events[0]
	if event.Status != "success" || event.Operation != string(domainsemantic.OperationEmbeddings) || event.Reason != "semantic_backfill" {
		t.Fatalf("unexpected accounting event status/operation/reason: %+v", event)
	}
	if event.ActorPrincipalID != actorID || event.EffectivePrincipalID != env.userID || event.OnBehalfOfPrincipalID != env.userID {
		t.Fatalf("expected backfill accounting actor=%s effective/on_behalf=%s, got %+v", actorID, env.userID, event)
	}
	if event.SpaceID != env.spaceID || event.DomainID != env.domainID || event.SemanticIndexID != env.index.ID || event.TargetNodeID != root.ID {
		t.Fatalf("unexpected accounting target attribution: %+v", event)
	}
	if event.ModelEndpointID != env.index.ModelEndpointID || event.ModelID != env.index.ModelID || event.CredentialID != env.grant.CredentialID || event.CredentialGrantID != env.grant.ID {
		t.Fatalf("unexpected accounting semantic provenance: %+v", event)
	}
	if event.InputTokens == 0 || event.TotalTokens == 0 || event.TokenCountSource != "provider_reported" || event.ProviderRequestID != "fake-provider-request" {
		t.Fatalf("unexpected accounting usage metrics: %+v", event)
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
	graph     *memoryGraphSource
	globalMgr storesemantic.GlobalManager
	spaceMgr  storesemantic.SpaceManager
	vector    vectorstore.MycelFileBackend
	connector *fakeConnector
	runner    Runner
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
	graphReader := &memoryGraphSource{domainID: domainID}
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
	index, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "notes", Name: "Notes", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{RecordTypes: []string{"note"}, Extraction: domainsemantic.SourceExtractionSubtree}, ModelEndpointID: endpointID, ModelID: modelID, ModelEndpointCapabilityID: capID, VectorStoreID: storeID, Enabled: true})
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
	env := &backfillTestEnv{spaceID: spaceID, domainID: domainID, userID: userID, graph: graphReader, globalMgr: globalMgr, spaceMgr: spaceMgr, vector: vector, connector: connector, index: index, grant: grant}
	env.runner = Runner{GraphReader: graphReader, GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connector, VectorBackend: vector}
	return env
}

func (e *backfillTestEnv) addRootWithChild(t *testing.T, rootText, childText string) (graph.Node, graph.Node) {
	t.Helper()
	root := e.graph.AddNode(rootText)
	child := e.graph.AddNode(childText)
	e.graph.AddEdge(root.ID, child.ID)
	return root, child
}

type memoryGraphSource struct {
	domainID graph.DomainID
	nodes    []graph.Node
	edges    []graph.Edge
}

func (s *memoryGraphSource) AddNode(content string) graph.Node {
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: s.domainID, Content: content}
	s.nodes = append(s.nodes, node)
	return node
}

func (s *memoryGraphSource) UpdateNode(id graph.NodeID, content string) {
	for i := range s.nodes {
		if s.nodes[i].ID == id {
			s.nodes[i].Content = content
			return
		}
	}
}

func (s *memoryGraphSource) AddEdge(fromID, toID graph.NodeID) graph.Edge {
	edge := graph.Edge{ID: graph.EdgeID(uuid.New()), DomainID: s.domainID, FromID: fromID, ToID: toID, Labels: []string{"contains"}, Properties: map[string]any{"order": 1}}
	s.edges = append(s.edges, edge)
	return edge
}

func (s *memoryGraphSource) ListNodes(_ context.Context, domainID graph.DomainID) ([]graph.Node, error) {
	out := []graph.Node{}
	for _, node := range s.nodes {
		if node.DomainID == domainID {
			out = append(out, node)
		}
	}
	return out, nil
}

func (s *memoryGraphSource) ListEdges(_ context.Context, domainID graph.DomainID) ([]graph.Edge, error) {
	out := []graph.Edge{}
	for _, edge := range s.edges {
		if edge.DomainID == domainID {
			out = append(out, edge)
		}
	}
	return out, nil
}

type fakeConnector struct {
	calls  []string
	inputs []connectors.EmbedInput
}

func (f *fakeConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	f.inputs = append(f.inputs, in)
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated"}, nil
}

type fakeProviderConnector struct {
	requests []connectors.EmbeddingRequest
}

func (f *fakeProviderConnector) Embed(ctx context.Context, in connectors.EmbeddingRequest) (connectors.EmbeddingResponse, error) {
	f.requests = append(f.requests, in)
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "provider_reported", ProviderRequestID: "fake-provider-request"}, nil
}
