package search

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestPlannerSearchesAllowedIndexesAndWarnsForSkipped(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	allowed := env.addIndex(t, "allowed", env.domainID, true, true)
	unprofiled := env.addIndex(t, "unprofiled", env.domainID, true, true)
	unprofiled.Metadata = nil
	if _, err := env.spaceMgr.UpsertSemanticIndex(ctx, unprofiled); err != nil {
		t.Fatalf("update unprofiled index: %v", err)
	}
	if _, err := env.vector.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: allowed.ID, NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:1", SourceMode: "self", ModelEndpointID: env.endpoint.ID, ModelID: env.model.ID, ModelEndpointCapabilityID: env.capability.ID, CredentialID: env.credential.ID, CredentialGrantID: env.grants[allowed.ID].ID, VectorStoreID: env.vectorStore.ID, VectorSpaceKey: env.model.VectorSpaceKey, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("upsert vector failed: %v", err)
	}
	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "focus notes", Limit: 10, ActorPrincipalID: env.userID})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(result.Results) != 1 || result.Results[0].SemanticIndexID != allowed.ID || result.Results[0].CredentialGrantID != env.grants[allowed.ID].ID {
		t.Fatalf("expected profiled index result with record grant provenance, got %+v", result)
	}
	if len(env.connector.calls) != 1 || !strings.Contains(env.connector.calls[0], "focus notes") {
		t.Fatalf("expected one query embedding call, calls=%+v", env.connector.calls)
	}
	warnings := strings.Join(result.Warnings, "\n")
	if !strings.Contains(warnings, unprofiled.Key) || !strings.Contains(warnings, "does not declare an inference profile") {
		t.Fatalf("expected warning for unprofiled index %s, got %+v", unprofiled.Key, result.Warnings)
	}
}

func TestPlannerResolvesCompatibleIndexesIndividually(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	idx1 := env.addIndex(t, "idx1", env.domainID, true, false)
	idx2 := env.addIndex(t, "idx2", env.domainID, true, false)
	sharedGrant, err := env.spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: env.credential.ID, Scope: domainsemantic.ProcessingScope{SpaceID: env.spaceID, DomainID: env.domainID}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, ModelEndpointID: &env.endpoint.ID, ModelID: &env.model.ID, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("shared grant upsert failed: %v", err)
	}
	for _, idx := range []domainsemantic.SemanticIndex{idx1, idx2} {
		if _, err := env.vector.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: idx.ID, NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:" + idx.Key, SourceMode: "self", ModelEndpointID: env.endpoint.ID, ModelID: env.model.ID, ModelEndpointCapabilityID: env.capability.ID, CredentialID: env.credential.ID, CredentialGrantID: sharedGrant.ID, VectorStoreID: env.vectorStore.ID, VectorSpaceKey: env.model.VectorSpaceKey, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("upsert vector failed: %v", err)
		}
	}
	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "grouped", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(env.connector.calls) != 2 || env.connector.inputs[0].SemanticIndexID == env.connector.inputs[1].SemanticIndexID {
		t.Fatalf("expected one query embedding per semantic index, inputs=%+v", env.connector.inputs)
	}
	if len(result.Results) != 2 || len(result.Groups) != 2 {
		t.Fatalf("expected two results in two per-index groups, got %+v", result)
	}
}

func TestPlannerSkipsIndexWhenProfileResolutionFails(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	idx1 := env.addIndex(t, "idx1", env.domainID, true, false)
	idx2 := env.addIndex(t, "idx2", env.domainID, true, false)
	sharedGrant, err := env.spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: env.credential.ID, Scope: domainsemantic.ProcessingScope{SpaceID: env.spaceID, DomainID: env.domainID}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, ModelEndpointID: &env.endpoint.ID, ModelID: &env.model.ID, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("shared grant upsert failed: %v", err)
	}
	for _, idx := range []domainsemantic.SemanticIndex{idx1, idx2} {
		if _, err := env.vector.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: idx.ID, NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:" + idx.Key, SourceMode: "self", ModelEndpointID: env.endpoint.ID, ModelID: env.model.ID, ModelEndpointCapabilityID: env.capability.ID, CredentialID: env.credential.ID, CredentialGrantID: sharedGrant.ID, VectorStoreID: env.vectorStore.ID, VectorSpaceKey: env.model.VectorSpaceKey, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("upsert vector failed: %v", err)
		}
	}
	env.connector.denyIndexes = map[domainsemantic.SemanticIndexID]error{idx2.ID: errors.New("policy denies semantic index")}
	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "query", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(env.connector.inputs) != 2 {
		t.Fatalf("expected embedding resolution for both indexes, inputs=%+v", env.connector.inputs)
	}
	if len(result.Results) != 1 || result.Results[0].SemanticIndexID != idx1.ID {
		t.Fatalf("expected only allowed index result, got %+v", result.Results)
	}
	if !strings.Contains(strings.Join(result.Warnings, "\n"), "policy denies semantic index") {
		t.Fatalf("expected denied index warning, got %+v", result.Warnings)
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
	grants      map[domainsemantic.SemanticIndexID]domainsemantic.CredentialGrant
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
	model := domainsemantic.InferenceModel{ID: uuid.New(), Key: "model", Operation: domainsemantic.OperationEmbeddings, ModelName: "model", Dimensions: 3, VectorSpaceKey: "test/model", CreatedAt: now, UpdatedAt: now}
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
	connector := &fakeSearchConnector{}
	vector := vectorstore.MycelFileBackend{GraphsDir: filepath.Join(root, "graphs")}
	env := &searchEnv{spaceID: spaceID, domainID: domainID, userID: userID, globalMgr: globalMgr, spaceMgr: spaceMgr, vector: vector, connector: connector, endpoint: endpoint, model: model, capability: cap, vectorStore: vectorStore, credential: credential, grants: map[domainsemantic.SemanticIndexID]domainsemantic.CredentialGrant{}}
	env.planner = Planner{GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connector, VectorBackend: vector}
	return env
}

func (e *searchEnv) addIndex(t *testing.T, key string, domainID graph.DomainID, allowed bool, grant bool) domainsemantic.SemanticIndex {
	t.Helper()
	ctx := context.Background()
	idx, err := e.spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: e.spaceID, DomainID: domainID, Key: key, Name: key, Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: e.endpoint.ID, ModelID: e.model.ID, ModelEndpointCapabilityID: e.capability.ID, VectorStoreID: e.vectorStore.ID, Enabled: true, Metadata: map[string]any{"inference_profile": "semantic-profile"}})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	effect := domainsemantic.PolicyEffectDeny
	privacy := []domainsemantic.PrivacyClass(nil)
	if allowed {
		effect = domainsemantic.PolicyEffectAllow
		privacy = []domainsemantic.PrivacyClass{domainsemantic.PrivacyClassThirdParty}
	}
	if _, err := e.spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{Scope: domainsemantic.ProcessingScope{SpaceID: e.spaceID, DomainID: domainID, SemanticIndexID: idx.ID}, Effect: effect, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowedPrivacyClasses: privacy, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("policy upsert failed: %v", err)
	}
	if grant {
		g, err := e.spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: e.credential.ID, Scope: domainsemantic.ProcessingScope{SpaceID: e.spaceID, DomainID: domainID, SemanticIndexID: idx.ID}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, ModelEndpointID: &e.endpoint.ID, ModelID: &e.model.ID, CreatedAt: time.Now().UTC()})
		if err != nil {
			t.Fatalf("grant upsert failed: %v", err)
		}
		e.grants[idx.ID] = g
	}
	return idx
}

type fakeSearchConnector struct {
	calls       []string
	inputs      []connectors.EmbedInput
	denyIndexes map[domainsemantic.SemanticIndexID]error
}

func (f *fakeSearchConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	f.inputs = append(f.inputs, in)
	if err := f.denyIndexes[in.SemanticIndexID]; err != nil {
		return connectors.EmbeddingResponse{}, err
	}
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated"}, nil
}
