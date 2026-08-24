package backfill

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	inferenceconnectors "github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
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
	if len(env.connector.inputs) != 1 || env.connector.inputs[0].InferenceProfile != "semantic-profile" {
		t.Fatalf("expected profile-backed connector input, got %+v", env.connector.inputs)
	}
	search, err := env.vector.Search(ctx, vectorstore.SearchInput{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: env.index.ID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(search) != 1 || search[0].NodeID != root.ID {
		t.Fatalf("expected stored semantic record, got %+v", search)
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
	if _, err := env.globalMgr.UpsertModel(ctx, domainsemantic.InferenceModel{ID: newModelID, Key: "test/embedding-v2", Kind: domainsemantic.ModelKindEmbedding, ModelName: "embedding-v2", Dimensions: 3, VectorSpaceKey: "test/embedding-v2", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
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

func TestRunnerBackfillsThroughStandaloneInferenceProfile(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, _ := env.addRootWithChild(t, "standalone root", "standalone child")
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	fake := &inferenceconnectors.FakeConnector{Vector: []float64{0.2, 0.8, 0}}
	inference.SetConnector(domaininference.ConnectorOpenAICompatible, fake)
	profileID := uuid.New()
	rule, err := env.spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(env.index.ID), SpaceID: env.spaceID, DomainID: env.domainID, Key: "standalone-rule", DisplayName: "Standalone Rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorExplicit, NodeIDs: []graph.NodeID{root.ID}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSubtree}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), IntelligenceProfileID: domainsemantic.IntelligenceProfileID(profileID), VectorStoreID: env.index.VectorStoreID, Enabled: true}}})
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	seedStandaloneInferenceForBackfill(t, ctx, inference, env, profileID)
	runner := env.runner
	runner.Connector = connectors.InferenceAdapter{Manager: inference}

	result, err := runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID})
	if err != nil {
		t.Fatalf("backfill through inference failed: %v", err)
	}
	if result.GeneratedCount != 1 || result.Records[0].NodeID != root.ID || result.Records[0].SemanticRuleID != rule.ID || result.Records[0].EmbeddingBindingKey != "search" || result.Records[0].PolicyDecisionID == uuid.Nil {
		t.Fatalf("expected generated rule record with policy decision, result=%+v", result)
	}
	if len(env.connector.calls) != 0 {
		t.Fatalf("legacy semantic connector should not be called, calls=%+v", env.connector.calls)
	}
	embedCalls, _ := fake.Calls()
	if embedCalls != 1 {
		t.Fatalf("expected standalone inference fake connector call, got %d", embedCalls)
	}
	searchPlanner := semanticsearch.Planner{GlobalManager: env.globalMgr, SpaceManager: env.spaceMgr, Connector: connectors.InferenceAdapter{Manager: inference}, VectorBackend: env.vector}
	search, err := searchPlanner.Search(ctx, semanticsearch.Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "standalone", Limit: 10, Purpose: domainsemantic.SemanticIndexPurposeSearch, ActorPrincipalID: env.userID})
	if err != nil {
		t.Fatalf("semantic search through inference failed: %v", err)
	}
	if len(search.Results) != 1 || len(search.Warnings) != 0 {
		t.Fatalf("unexpected semantic search result: %+v", search)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 2 || events[0].Status != domaininference.UsageStatusSucceeded || events[1].Status != domaininference.UsageStatusSucceeded || events[0].SemanticRuleID != rule.ID.String() || events[1].SemanticRuleID != rule.ID.String() || events[0].EmbeddingBindingKey != "search" || events[1].EmbeddingBindingKey != "search" {
		t.Fatalf("unexpected standalone inference usage events: %#v err=%v", events, err)
	}
}

func seedStandaloneInferenceForBackfill(t *testing.T, ctx context.Context, inference *inferenceservice.Module, env *backfillTestEnv, profileID uuid.UUID) {
	t.Helper()
	if _, err := inference.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{ID: domaininference.EndpointID(env.index.ModelEndpointID), Key: "openai", ConnectorType: domaininference.ConnectorOpenAICompatible, NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, Enabled: true}); err != nil {
		t.Fatalf("upsert inference endpoint: %v", err)
	}
	if _, err := inference.GlobalManager().UpsertModel(ctx, domaininference.Model{ID: domaininference.ModelID(env.index.ModelID), Key: "test/embedding", Kind: domaininference.ModelKindEmbedding, ProviderModelName: "embedding", EmbeddingDims: 3, VectorSpace: "test/embedding", Enabled: true}); err != nil {
		t.Fatalf("upsert inference model: %v", err)
	}
	if _, err := inference.GlobalManager().UpsertCapability(ctx, domaininference.Capability{ID: domaininference.CapabilityID(env.index.ModelEndpointCapabilityID), EndpointID: domaininference.EndpointID(env.index.ModelEndpointID), ModelID: domaininference.ModelID(env.index.ModelID), Operation: domaininference.OperationEmbeddings, Enabled: true}); err != nil {
		t.Fatalf("upsert inference capability: %v", err)
	}
	if _, err := inference.GlobalManager().UpsertSecret(ctx, domaininference.Secret{ID: domaininference.SecretID(uuid.New()), OwnerType: domaininference.CredentialOwnerPrincipal, OwnerID: env.userID.String(), Kind: "none"}); err != nil {
		t.Fatalf("upsert inference secret: %v", err)
	}
	if _, err := inference.GlobalManager().UpsertCredential(ctx, domaininference.Credential{ID: domaininference.CredentialID(env.grant.CredentialID), Key: "cred", EndpointID: domaininference.EndpointID(env.index.ModelEndpointID), OwnerType: domaininference.CredentialOwnerSystem, OwnerID: "semantic", AuthType: domaininference.CredentialAuthNone, SecretID: domaininference.SecretID(uuid.New()), Status: domaininference.CredentialStatusActive}); err != nil {
		t.Fatalf("upsert inference credential: %v", err)
	}
	spaceMgr, err := inference.SpaceManager(ctx, env.spaceID.String())
	if err != nil {
		t.Fatalf("inference space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{ID: domaininference.ProfileID(profileID), SpaceID: env.spaceID.String(), Key: "semantic-profile", Operation: domaininference.OperationEmbeddings, DomainIDs: []string{env.domainID.String()}, EndpointRefs: []string{env.index.ModelEndpointID.String()}, ModelRefs: []string{env.index.ModelID.String()}, CapabilityRefs: []string{env.index.ModelEndpointCapabilityID.String()}, Enabled: true}); err != nil {
		t.Fatalf("upsert inference profile: %v", err)
	}
	if _, err := spaceMgr.UpsertCredentialGrant(ctx, domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(env.grant.ID), SpaceID: env.spaceID.String(), CredentialID: domaininference.CredentialID(env.grant.CredentialID), Scope: domaininference.Scope{SpaceID: env.spaceID.String(), DomainID: env.domainID.String(), SemanticIndexID: env.index.ID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{profileID.String()}, EndpointRefs: []string{env.index.ModelEndpointID.String()}, ModelRefs: []string{env.index.ModelID.String()}, UsageModes: []domaininference.UsageMode{domaininference.UsageModeSemantic}, State: domaininference.GrantStateActive}); err != nil {
		t.Fatalf("upsert inference grant: %v", err)
	}
	if _, err := spaceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: env.spaceID.String(), Scope: domaininference.Scope{SpaceID: env.spaceID.String(), DomainID: env.domainID.String(), SemanticIndexID: env.index.ID.String()}, Operations: []domaininference.Operation{domaininference.OperationEmbeddings}, ProfileRefs: []string{profileID.String()}, Action: domaininference.PolicyActionAllow, AllowedPrivacyClasses: []domaininference.PrivacyClass{domaininference.PrivacyClassThirdParty}, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert inference policy: %v", err)
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
	userID    identity.PrincipalID
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
	userID := identity.PrincipalID(uuid.NewString())
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
	if _, err := globalMgr.UpsertModel(ctx, domainsemantic.InferenceModel{ID: modelID, Key: "test/embedding", Kind: domainsemantic.ModelKindEmbedding, ModelName: "embedding", Dimensions: 3, VectorSpaceKey: "test/embedding", CreatedAt: now, UpdatedAt: now}); err != nil {
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
	index, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "notes", Name: "Notes", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{RecordTypes: []string{"note"}, Extraction: domainsemantic.SourceExtractionSubtree}, ModelEndpointID: endpointID, ModelID: modelID, ModelEndpointCapabilityID: capID, VectorStoreID: storeID, Enabled: true, Metadata: map[string]any{"inference_profile": "semantic-profile"}})
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
	connector := &fakeConnector{endpointID: endpointID, modelID: modelID, capID: capID}
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
	calls      []string
	inputs     []connectors.EmbedInput
	endpointID domainsemantic.ModelEndpointID
	modelID    domainsemantic.InferenceModelID
	capID      domainsemantic.ModelEndpointCapabilityID
}

func (f *fakeConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	f.inputs = append(f.inputs, in)
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated", EndpointID: f.endpointID, ModelID: f.modelID, CapabilityID: f.capID}, nil
}

func TestRunnerBackfillsSemanticRuleBinding(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, child := env.addRootWithChild(t, "root note", "child note")
	env.graph.nodes[0].Labels = []string{"Note"}
	env.graph.nodes[1].Labels = []string{"Note"}
	rule := env.semanticRule(t, []domainsemantic.SemanticEmbeddingBinding{env.semanticBinding("search", true), env.semanticBinding("summary", true)})

	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: "search"})
	if err != nil {
		t.Fatalf("rule backfill failed: %v", err)
	}
	if result.SemanticRuleID != rule.ID || result.EmbeddingBindingKey != "search" || result.SelectedCount != 1 || result.GeneratedCount != 1 {
		t.Fatalf("unexpected rule backfill result: %+v", result)
	}
	if len(env.connector.inputs) != 1 || env.connector.inputs[0].SemanticRuleID != rule.ID || env.connector.inputs[0].EmbeddingBindingKey != "search" || env.connector.inputs[0].InferenceProfile != "semantic-profile" {
		t.Fatalf("unexpected connector input: %+v", env.connector.inputs)
	}
	if !strings.Contains(env.connector.calls[0], "child note") {
		t.Fatalf("expected subtree source in connector call, got %+v", env.connector.calls)
	}
	if len(result.Records) != 1 || result.Records[0].SemanticRuleID != rule.ID || result.Records[0].EmbeddingBindingKey != "search" || result.Records[0].TargetNodeID != root.ID || result.Records[0].NodeID != root.ID {
		t.Fatalf("record missing rule/binding attribution: %+v child=%s", result.Records, child.ID)
	}
	search, err := env.vector.Search(ctx, vectorstore.SearchInput{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil || len(search) != 1 || search[0].Record.SemanticRuleID != rule.ID || search[0].Record.EmbeddingBindingKey != "search" {
		t.Fatalf("unexpected rule vector search: search=%+v err=%v", search, err)
	}
}

func TestRunnerBackfillsAllEnabledRuleBindingsAndSkipsCurrentHash(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	env.addRootWithChild(t, "root", "child")
	env.graph.nodes[0].Labels = []string{"Note"}
	env.graph.nodes[1].Labels = []string{"Note"}
	rule := env.semanticRule(t, []domainsemantic.SemanticEmbeddingBinding{env.semanticBinding("search", true), env.semanticBinding("summary", true), env.semanticBinding("disabled", false)})

	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID})
	if err != nil {
		t.Fatalf("rule backfill failed: %v", err)
	}
	if result.GeneratedCount != 2 || len(env.connector.inputs) != 2 {
		t.Fatalf("expected two enabled bindings, result=%+v inputs=%+v", result, env.connector.inputs)
	}
	result, err = env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID})
	if err != nil {
		t.Fatalf("second rule backfill failed: %v", err)
	}
	if result.GeneratedCount != 0 || result.SkippedCount != 2 || len(env.connector.inputs) != 2 {
		t.Fatalf("expected current hash skip per binding, result=%+v inputs=%+v", result, env.connector.inputs)
	}
}

func TestRunnerRuleBackfillTombstonesShortSource(t *testing.T) {
	ctx := context.Background()
	env := newBackfillTestEnv(t)
	root, child := env.addRootWithChild(t, "root note", "child note")
	env.graph.nodes[0].Labels = []string{"Note"}
	env.graph.nodes[1].Labels = []string{"Note"}
	rule := env.semanticRule(t, []domainsemantic.SemanticEmbeddingBinding{env.semanticBinding("search", true)})
	if _, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID}); err != nil {
		t.Fatalf("initial rule backfill failed: %v", err)
	}
	env.graph.UpdateNode(root.ID, " ")
	env.graph.UpdateNode(child.ID, " ")
	result, err := env.runner.Run(ctx, Input{SpaceID: env.spaceID, SemanticRuleID: rule.ID})
	if err != nil {
		t.Fatalf("short source backfill failed: %v", err)
	}
	if result.GeneratedCount != 0 || result.SkippedCount != 1 {
		t.Fatalf("expected skipped tombstone, result=%+v", result)
	}
	search, err := env.vector.Search(ctx, vectorstore.SearchInput{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(search) != 0 {
		t.Fatalf("expected tombstoned rule vector to be absent, got %+v", search)
	}
}

func (e *backfillTestEnv) semanticBinding(key string, enabled bool) domainsemantic.SemanticEmbeddingBinding {
	return domainsemantic.SemanticEmbeddingBinding{Key: key, Purpose: "semantic_search", IntelligenceProfile: "semantic-profile", VectorStoreID: e.index.VectorStoreID, Enabled: enabled}
}

func (e *backfillTestEnv) semanticRule(t *testing.T, bindings []domainsemantic.SemanticEmbeddingBinding) domainsemantic.SemanticGenerationRule {
	t.Helper()
	return e.semanticRuleWithID(t, uuid.Nil, bindings)
}

func (e *backfillTestEnv) semanticRuleWithID(t *testing.T, id domainsemantic.SemanticRuleID, bindings []domainsemantic.SemanticEmbeddingBinding) domainsemantic.SemanticGenerationRule {
	t.Helper()
	rule, err := e.spaceMgr.UpsertSemanticRule(context.Background(), domainsemantic.SemanticGenerationRule{ID: id, SpaceID: e.spaceID, DomainID: e.domainID, Key: "notes-rule-" + uuid.NewString(), DisplayName: "Notes Rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Note"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSubtree}, Storage: domainsemantic.DefaultSemanticStoragePolicy(), Embeddings: bindings})
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	return rule
}
