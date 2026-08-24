package search

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	identity "github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestPlannerSearchesRuleBindingsAndWarnsForSkipped(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	allowed := env.addRule(t, "allowed", []domainsemantic.SemanticEmbeddingBinding{env.binding("search", env.vectorStore.ID)})
	env.addRule(t, "missing-store", []domainsemantic.SemanticEmbeddingBinding{env.binding("search", domainsemantic.VectorStoreID(uuid.New()))})
	nodeID := graph.NodeID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	if _, err := env.vector.Upsert(ctx, env.record(allowed.ID, "search", nodeID, []float64{1, 0, 0}, grantID)); err != nil {
		t.Fatalf("upsert vector failed: %v", err)
	}

	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "focus notes", Limit: 10, ActorPrincipalID: env.userID})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(result.Results) != 1 || result.Results[0].SemanticRuleID != allowed.ID || result.Results[0].EmbeddingBindingKey != "search" || result.Results[0].CredentialGrantID != grantID {
		t.Fatalf("expected profiled rule/binding result with record grant provenance, got %+v", result)
	}
	if len(env.connector.calls) != 1 || !strings.Contains(env.connector.calls[0], "focus notes") {
		t.Fatalf("expected one query embedding call, calls=%+v", env.connector.calls)
	}
	warnings := strings.Join(result.Warnings, "\n")
	if !strings.Contains(warnings, "missing-store") || !strings.Contains(warnings, "enabled vector store not found") {
		t.Fatalf("expected warning for missing vector store, got %+v", result.Warnings)
	}
}

func TestPlannerSearchesBindingsIndividuallyAndMergesDuplicateTargets(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	rule := env.addRule(t, "multi", []domainsemantic.SemanticEmbeddingBinding{env.binding("search", env.vectorStore.ID), env.binding("summary", env.vectorStore.ID)})
	nodeID := graph.NodeID(uuid.New())
	if _, err := env.vector.Upsert(ctx, env.record(rule.ID, "search", nodeID, []float64{0.90, 0.10, 0}, domainsemantic.CredentialGrantID(uuid.New()))); err != nil {
		t.Fatalf("upsert search vector failed: %v", err)
	}
	if _, err := env.vector.Upsert(ctx, env.record(rule.ID, "summary", nodeID, []float64{1, 0, 0}, domainsemantic.CredentialGrantID(uuid.New()))); err != nil {
		t.Fatalf("upsert summary vector failed: %v", err)
	}

	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "grouped", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(env.connector.inputs) != 2 || env.connector.inputs[0].EmbeddingBindingKey == env.connector.inputs[1].EmbeddingBindingKey {
		t.Fatalf("expected one query embedding per binding, inputs=%+v", env.connector.inputs)
	}
	if len(result.Groups) != 2 {
		t.Fatalf("expected two binding groups, got %+v", result.Groups)
	}
	if len(result.Results) != 1 || result.Results[0].NodeID != nodeID || len(result.Results[0].MatchedBindings) != 2 || len(result.Results[0].MatchedRecordIDs) != 2 {
		t.Fatalf("expected duplicate target merge with provenance, got %+v", result.Results)
	}
	if result.Results[0].EmbeddingBindingKey != "summary" {
		t.Fatalf("expected best-scoring binding to be primary result, got %+v", result.Results[0])
	}
}

func TestPlannerSkipsBindingWhenProfileResolutionFails(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	rule := env.addRule(t, "multi", []domainsemantic.SemanticEmbeddingBinding{env.binding("search", env.vectorStore.ID), env.binding("summary", env.vectorStore.ID)})
	if _, err := env.vector.Upsert(ctx, env.record(rule.ID, "search", graph.NodeID(uuid.New()), []float64{1, 0, 0}, domainsemantic.CredentialGrantID(uuid.New()))); err != nil {
		t.Fatalf("upsert search vector failed: %v", err)
	}
	if _, err := env.vector.Upsert(ctx, env.record(rule.ID, "summary", graph.NodeID(uuid.New()), []float64{1, 0, 0}, domainsemantic.CredentialGrantID(uuid.New()))); err != nil {
		t.Fatalf("upsert summary vector failed: %v", err)
	}
	env.connector.denyBindings = map[string]error{rule.ID.String() + "/summary": errors.New("policy denies semantic binding")}

	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "query", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(env.connector.inputs) != 2 {
		t.Fatalf("expected embedding resolution for both bindings, inputs=%+v", env.connector.inputs)
	}
	if len(result.Results) != 1 || result.Results[0].EmbeddingBindingKey != "search" {
		t.Fatalf("expected only allowed binding result, got %+v", result.Results)
	}
	if !strings.Contains(strings.Join(result.Warnings, "\n"), "policy denies semantic binding") {
		t.Fatalf("expected denied binding warning, got %+v", result.Warnings)
	}
}

func TestPlannerAllBindingsSkippedReturnsError(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	rule := env.addRule(t, "denied", []domainsemantic.SemanticEmbeddingBinding{env.binding("search", env.vectorStore.ID)})
	env.connector.denyBindings = map[string]error{rule.ID.String() + "/search": errors.New("policy denies semantic binding")}
	_, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "query", Limit: 10})
	if err == nil || !strings.Contains(err.Error(), "all semantic search bindings") {
		t.Fatalf("expected all-skipped error, got %v", err)
	}
}

type searchEnv struct {
	spaceID     domainspace.SpaceID
	domainID    graph.DomainID
	userID      identity.PrincipalID
	globalMgr   storesemantic.GlobalManager
	spaceMgr    storesemantic.SpaceManager
	vector      vectorstore.MycelFileBackend
	connector   *fakeSearchConnector
	planner     Planner
	endpoint    domainsemantic.ModelEndpoint
	model       domainsemantic.InferenceModel
	capability  domainsemantic.ModelEndpointCapability
	vectorStore domainsemantic.VectorStoreBackend
	credential  domainsemantic.InferenceCredential
}

func newSearchEnv(t *testing.T) *searchEnv {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	userID := identity.PrincipalID(uuid.NewString())
	globalMgr := storesemantic.NewGlobalManager()
	if err := globalMgr.Init(ctx, filepath.Join(root, "meta")); err != nil {
		t.Fatalf("global init failed: %v", err)
	}
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(root, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		t.Fatalf("space init failed: %v", err)
	}
	now := time.Now().UTC()
	endpoint := domainsemantic.ModelEndpoint{ID: uuid.New(), Key: "endpoint", Name: "Endpoint", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "http://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now}
	model := domainsemantic.InferenceModel{ID: uuid.New(), Key: "model", Kind: domainsemantic.ModelKindEmbedding, ModelName: "model", Dimensions: 3, VectorSpaceKey: "test/model", CreatedAt: now, UpdatedAt: now}
	cap := domainsemantic.ModelEndpointCapability{ID: uuid.New(), ModelEndpointID: endpoint.ID, ModelID: model.ID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, CreatedAt: now, UpdatedAt: now}
	vectorStore := domainsemantic.VectorStoreBackend{ID: uuid.New(), Key: "mycel-file", Name: "mycel-file", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}
	credential := domainsemantic.InferenceCredential{ID: uuid.New(), Key: "cred", Name: "Credential", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: userID.String(), AuthType: domainsemantic.AuthModeNone, Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	for _, upsert := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := globalMgr.UpsertModelEndpoint(ctx, endpoint); return err },
		func(ctx context.Context) error { _, err := globalMgr.UpsertModel(ctx, model); return err },
		func(ctx context.Context) error {
			_, err := globalMgr.UpsertModelEndpointCapability(ctx, cap)
			return err
		},
		func(ctx context.Context) error { _, err := globalMgr.UpsertVectorStore(ctx, vectorStore); return err },
		func(ctx context.Context) error { _, err := globalMgr.UpsertCredential(ctx, credential); return err },
	} {
		if err := upsert(ctx); err != nil {
			t.Fatalf("upsert setup failed: %v", err)
		}
	}
	connector := &fakeSearchConnector{endpoint: endpoint, model: model, capability: cap}
	vector := vectorstore.MycelFileBackend{GraphsDir: filepath.Join(root, "graphs")}
	env := &searchEnv{spaceID: spaceID, domainID: domainID, userID: userID, globalMgr: globalMgr, spaceMgr: spaceMgr, vector: vector, connector: connector, endpoint: endpoint, model: model, capability: cap, vectorStore: vectorStore, credential: credential}
	env.planner = Planner{GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connector, VectorBackend: vector}
	return env
}

func (e *searchEnv) binding(key string, vectorStoreID domainsemantic.VectorStoreID) domainsemantic.SemanticEmbeddingBinding {
	return domainsemantic.SemanticEmbeddingBinding{Key: key, Purpose: string(domainsemantic.SemanticIndexPurposeSearch), IntelligenceProfile: "semantic-profile", VectorStoreID: vectorStoreID, Enabled: true}
}

func (e *searchEnv) addRule(t *testing.T, key string, bindings []domainsemantic.SemanticEmbeddingBinding) domainsemantic.SemanticGenerationRule {
	t.Helper()
	rule, err := e.spaceMgr.UpsertSemanticRule(context.Background(), domainsemantic.SemanticGenerationRule{SpaceID: e.spaceID, DomainID: e.domainID, Key: key, DisplayName: key, Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Note"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf, IncludeProperties: []string{"payload.text"}}, Embeddings: bindings, Storage: domainsemantic.DefaultSemanticStoragePolicy(), OwnerPrincipalID: e.userID, CreatedByPrincipalID: e.userID})
	if err != nil {
		t.Fatalf("rule upsert failed: %v", err)
	}
	return rule
}

func (e *searchEnv) record(ruleID domainsemantic.SemanticRuleID, bindingKey string, nodeID graph.NodeID, vector []float64, grantID domainsemantic.CredentialGrantID) domainsemantic.AdvancedEmbeddingRecord {
	return domainsemantic.AdvancedEmbeddingRecord{SpaceID: e.spaceID, DomainID: e.domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "sha256:" + bindingKey, SourceMode: "self", ModelEndpointID: e.endpoint.ID, ModelID: e.model.ID, ModelEndpointCapabilityID: e.capability.ID, CredentialID: e.credential.ID, CredentialGrantID: grantID, VectorStoreID: e.vectorStore.ID, VectorSpaceKey: e.model.VectorSpaceKey, Dimensions: len(vector), Vector: vector, CreatedAt: time.Now().UTC()}
}

type fakeSearchConnector struct {
	calls        []string
	inputs       []connectors.EmbedInput
	denyBindings map[string]error
	endpoint     domainsemantic.ModelEndpoint
	model        domainsemantic.InferenceModel
	capability   domainsemantic.ModelEndpointCapability
}

func (f *fakeSearchConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	f.inputs = append(f.inputs, in)
	if err := f.denyBindings[in.SemanticRuleID.String()+"/"+in.EmbeddingBindingKey]; err != nil {
		return connectors.EmbeddingResponse{}, err
	}
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated", EndpointID: f.endpoint.ID, ModelID: f.model.ID, CapabilityID: f.capability.ID, CredentialGrantID: domainsemantic.CredentialGrantID(uuid.New())}, nil
}
