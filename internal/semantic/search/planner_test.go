package search

import (
	"context"
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
	missingGrant := env.addIndex(t, "missing-grant", env.domainID, true, false)
	denied := env.addIndex(t, "denied", env.domainID, false, true)
	if _, err := env.vector.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: allowed.ID, NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:1", SourceMode: "self", ModelEndpointID: env.endpoint.ID, ModelID: env.model.ID, ModelEndpointCapabilityID: env.capability.ID, CredentialID: env.credential.ID, CredentialGrantID: env.grants[allowed.ID].ID, VectorStoreID: env.vectorStore.ID, VectorSpaceKey: env.model.VectorSpaceKey, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("upsert vector failed: %v", err)
	}
	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "focus notes", Limit: 10, ActorPrincipalID: env.userID})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(result.Results) != 1 || result.Results[0].SemanticIndexID != allowed.ID || result.Results[0].CredentialGrantID != env.grants[allowed.ID].ID {
		t.Fatalf("expected allowed index result with query grant provenance, got %+v", result)
	}
	if len(env.connector.calls) != 1 || !strings.Contains(env.connector.calls[0], "focus notes") {
		t.Fatalf("expected one query embedding call, calls=%+v", env.connector.calls)
	}
	warnings := strings.Join(result.Warnings, "\n")
	if !strings.Contains(warnings, missingGrant.Key) || !strings.Contains(warnings, denied.Key) {
		t.Fatalf("expected warnings for skipped indexes %s and %s, got %+v", missingGrant.Key, denied.Key, result.Warnings)
	}
}

func TestPlannerGroupsCompatibleIndexesByVectorSpace(t *testing.T) {
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
	if len(env.connector.calls) != 1 {
		t.Fatalf("expected one query embedding for shared vector space and grant, calls=%+v", env.connector.calls)
	}
	if len(result.Results) != 2 || len(result.Groups) != 1 || result.Groups[0].ResultCount != 2 {
		t.Fatalf("expected two results in one group, got %+v", result)
	}
}

func TestPlannerSeparatesGroupsByCredentialGrant(t *testing.T) {
	ctx := context.Background()
	env := newSearchEnv(t)
	idx1 := env.addIndex(t, "idx1", env.domainID, true, true)
	idx2 := env.addIndex(t, "idx2", env.domainID, true, true)
	for _, idx := range []domainsemantic.SemanticIndex{idx1, idx2} {
		if _, err := env.vector.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: env.spaceID, DomainID: env.domainID, SemanticIndexID: idx.ID, NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:" + idx.Key, SourceMode: "self", ModelEndpointID: env.endpoint.ID, ModelID: env.model.ID, ModelEndpointCapabilityID: env.capability.ID, CredentialID: env.credential.ID, CredentialGrantID: env.grants[idx.ID].ID, VectorStoreID: env.vectorStore.ID, VectorSpaceKey: env.model.VectorSpaceKey, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("upsert vector failed: %v", err)
		}
	}
	result, err := env.planner.Search(ctx, Input{SpaceID: env.spaceID, DomainID: env.domainID, Text: "separate grants", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(env.connector.calls) != 2 || len(result.Groups) != 2 || len(result.Results) != 2 {
		t.Fatalf("expected separate query calls/groups for separate grants, calls=%+v result=%+v", env.connector.calls, result)
	}
	seen := map[domainsemantic.CredentialGrantID]bool{}
	for _, r := range result.Results {
		seen[r.CredentialGrantID] = true
	}
	if !seen[env.grants[idx1.ID].ID] || !seen[env.grants[idx2.ID].ID] {
		t.Fatalf("expected result grant provenance for both grants, seen=%+v grants=%+v", seen, env.grants)
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
	idx, err := e.spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: e.spaceID, DomainID: domainID, Key: key, Name: key, Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: e.endpoint.ID, ModelID: e.model.ID, ModelEndpointCapabilityID: e.capability.ID, VectorStoreID: e.vectorStore.ID, Enabled: true})
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

type fakeSearchConnector struct{ calls []string }

func (f *fakeSearchConnector) Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error) {
	f.calls = append(f.calls, in.Input)
	return connectors.EmbeddingResponse{Vector: []float64{1, 0, 0}, InputTokens: len(strings.Fields(in.Input)), TotalTokens: len(strings.Fields(in.Input)), TokenCountSource: "estimated"}, nil
}
