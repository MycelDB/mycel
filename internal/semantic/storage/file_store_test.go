package semantic

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGlobalManagerInitCreatesFilesAndDefaultVectorStore(t *testing.T) {
	ctx := context.Background()
	metaDir := t.TempDir()
	mgr := NewGlobalManager()
	if err := mgr.Init(ctx, metaDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for _, path := range []string{
		filepath.Join(metaDir, inferenceDirName, packagesFileName),
		filepath.Join(metaDir, inferenceDirName, modelEndpointsFileName),
		filepath.Join(metaDir, inferenceDirName, modelsFileName),
		filepath.Join(metaDir, inferenceDirName, modelEndpointCapsFileName),
		filepath.Join(metaDir, inferenceDirName, vectorStoresFileName),
		filepath.Join(metaDir, secretsDirName, secretsFileName),
		filepath.Join(metaDir, credentialsDirName, credentialsFileName),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}

	first, err := mgr.EnsureDefaultVectorStore(ctx)
	if err != nil {
		t.Fatalf("ensure default vector store failed: %v", err)
	}
	if first.Key != defaultVectorStoreKey || first.Type != domainsemantic.VectorStoreMycelFile || first.PrivacyClass != domainsemantic.PrivacyClassLocalOnly || !first.Enabled {
		t.Fatalf("unexpected default vector store: %+v", first)
	}
	second, err := mgr.EnsureDefaultVectorStore(ctx)
	if err != nil {
		t.Fatalf("second ensure default vector store failed: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("default vector store should be idempotent: first=%s second=%s", first.ID, second.ID)
	}
	stores, err := mgr.ListVectorStores(ctx)
	if err != nil {
		t.Fatalf("list vector stores failed: %v", err)
	}
	if len(stores) != 1 {
		t.Fatalf("expected one vector store, got %+v", stores)
	}

	reloaded := NewGlobalManager()
	if err := reloaded.Init(ctx, metaDir); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	reloadedStores, err := reloaded.ListVectorStores(ctx)
	if err != nil {
		t.Fatalf("list reloaded vector stores failed: %v", err)
	}
	if len(reloadedStores) != 1 || reloadedStores[0].ID != first.ID {
		t.Fatalf("unexpected reloaded stores: %+v", reloadedStores)
	}
}

func TestGlobalManagerUpsertsDefinitions(t *testing.T) {
	ctx := context.Background()
	mgr := NewGlobalManager()
	if err := mgr.Init(ctx, t.TempDir()); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	endpoint, err := mgr.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{
		Key:           "OpenAI-Public",
		Name:          "OpenAI Public",
		ConnectorType: domainsemantic.ConnectorOpenAICompatible,
		EndpointURL:   "https://api.openai.com/v1",
		NetworkClass:  domainsemantic.NetworkClassExternalHTTPS,
		PrivacyClass:  domainsemantic.PrivacyClassThirdParty,
		AuthModes:     []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey},
		Operations:    []domainsemantic.Operation{domainsemantic.OperationEmbeddings},
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("upsert endpoint failed: %v", err)
	}
	if endpoint.ID == uuid.Nil || endpoint.Key != "openai-public" {
		t.Fatalf("unexpected endpoint: %+v", endpoint)
	}
	model, err := mgr.UpsertModel(ctx, domainsemantic.InferenceModel{
		Key:            "OpenAI/Text-Embedding-3-Small",
		Operation:      domainsemantic.OperationEmbeddings,
		ModelName:      "text-embedding-3-small",
		ConnectorTypes: []domainsemantic.ConnectorType{domainsemantic.ConnectorOpenAICompatible},
		Dimensions:     1536,
		Modality:       "text",
		VectorSpaceKey: "openai/text-embedding-3-small",
	})
	if err != nil {
		t.Fatalf("upsert model failed: %v", err)
	}
	capability, err := mgr.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{
		ModelEndpointID: endpoint.ID,
		ModelID:         model.ID,
		Operation:       domainsemantic.OperationEmbeddings,
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("upsert capability failed: %v", err)
	}
	if capability.ID == uuid.Nil {
		t.Fatalf("expected capability id")
	}
	secret, err := mgr.UpsertSecret(ctx, domainsemantic.Secret{
		OwnerType:   domainsemantic.CredentialOwnerUser,
		OwnerID:     uuid.NewString(),
		Kind:        domainsemantic.SecretKindExternalRef,
		ExternalRef: "vault://secret/openai",
	})
	if err != nil {
		t.Fatalf("upsert secret failed: %v", err)
	}
	credential, err := mgr.UpsertCredential(ctx, domainsemantic.InferenceCredential{
		Key:             "Martin-OpenAI",
		Name:            "Martin OpenAI",
		ModelEndpointID: endpoint.ID,
		OwnerType:       domainsemantic.CredentialOwnerUser,
		OwnerID:         uuid.NewString(),
		AuthType:        domainsemantic.AuthModeAPIKey,
		SecretRef:       secret.ID,
		Status:          domainsemantic.CredentialStatusActive,
	})
	if err != nil {
		t.Fatalf("upsert credential failed: %v", err)
	}
	if credential.ID == uuid.Nil || credential.Key != "martin-openai" {
		t.Fatalf("unexpected credential: %+v", credential)
	}
	pkg, err := mgr.UpsertPackage(ctx, domainsemantic.InferencePackage{Name: "Standard-OpenAI", Version: "2026.06"})
	if err != nil {
		t.Fatalf("upsert package failed: %v", err)
	}
	pkgAgain, err := mgr.UpsertPackage(ctx, domainsemantic.InferencePackage{Name: "standard-openai", Version: "2026.06", Source: "file://package.yaml"})
	if err != nil {
		t.Fatalf("second package upsert failed: %v", err)
	}
	if pkgAgain.ID != pkg.ID {
		t.Fatalf("package upsert should be idempotent by name/version")
	}
	models, err := mgr.ListModels(ctx)
	if err != nil || len(models) != 1 {
		t.Fatalf("unexpected models: models=%+v err=%v", models, err)
	}
	credentials, err := mgr.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 {
		t.Fatalf("unexpected credentials: credentials=%+v err=%v", credentials, err)
	}
}

func TestGlobalManagerRejectsEmbeddingModelWithoutVectorSpace(t *testing.T) {
	ctx := context.Background()
	mgr := NewGlobalManager()
	if err := mgr.Init(ctx, t.TempDir()); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	_, err := mgr.UpsertModel(ctx, domainsemantic.InferenceModel{Key: "bad", Operation: domainsemantic.OperationEmbeddings, Dimensions: 1536})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestSpaceManagerUpsertsScopedSemanticResources(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	modelEndpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	vectorStoreID := domainsemantic.VectorStoreID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())

	location := t.TempDir()
	mgr := NewSpaceManager()
	if err := mgr.Init(ctx, location, spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for _, path := range []string{
		filepath.Join(location, semanticIndexesFileName),
		filepath.Join(location, credentialGrantsFileName),
		filepath.Join(location, inferencePoliciesFileName),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
	index, err := mgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{
		SpaceID:         spaceID,
		DomainID:        domainID,
		Key:             "Notes-Search",
		Name:            "Notes Search",
		Purpose:         domainsemantic.SemanticIndexPurposeSearch,
		SourcePolicy:    domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree, TemplateKeys: []string{"logseq.page"}},
		ModelEndpointID: modelEndpointID,
		ModelID:         modelID,
		VectorStoreID:   vectorStoreID,
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("upsert index failed: %v", err)
	}
	if index.ID == uuid.Nil || index.Key != "notes-search" {
		t.Fatalf("unexpected index: %+v", index)
	}
	grant, err := mgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{
		CredentialID:       credentialID,
		Scope:              domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: index.ID},
		Operations:         []domainsemantic.Operation{domainsemantic.OperationEmbeddings},
		AllowBackgroundUse: true,
	})
	if err != nil {
		t.Fatalf("upsert grant failed: %v", err)
	}
	if grant.ID == uuid.Nil {
		t.Fatalf("expected grant id")
	}
	policy, err := mgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{
		Scope:      domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID},
		Effect:     domainsemantic.PolicyEffectAllow,
		Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings},
	})
	if err != nil {
		t.Fatalf("upsert policy failed: %v", err)
	}
	if policy.ID == uuid.Nil {
		t.Fatalf("expected policy id")
	}

	reloaded := NewSpaceManager()
	if err := reloaded.Init(ctx, location, spaceID); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	indexes, err := reloaded.ListSemanticIndexes(ctx)
	if err != nil || len(indexes) != 1 || indexes[0].ID != index.ID {
		t.Fatalf("unexpected reloaded indexes: indexes=%+v err=%v", indexes, err)
	}
}

func TestSpaceManagerNormalizesLegacySearchPurpose(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	mgr := NewSpaceManager()
	if err := mgr.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	index, err := mgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{
		SpaceID:         spaceID,
		DomainID:        domainID,
		Key:             "notes-search",
		Name:            "Notes Search",
		Purpose:         domainsemantic.SemanticIndexPurpose("search"),
		SourcePolicy:    domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree},
		ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()),
		ModelID:         domainsemantic.InferenceModelID(uuid.New()),
		VectorStoreID:   domainsemantic.VectorStoreID(uuid.New()),
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("upsert index failed: %v", err)
	}
	if index.Purpose != domainsemantic.SemanticIndexPurposeSearch {
		t.Fatalf("expected canonical search purpose, got %q", index.Purpose)
	}
}

func TestMaintenanceManagerPersistsDirtyEventsAndWork(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	location := t.TempDir()
	mgr := NewMaintenanceManager()
	if err := mgr.Init(ctx, location, spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	txnID := uuid.New()
	event := domainsemantic.GraphDirtyEvent{TxnID: txnID, GraphRevision: 7, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}}
	first, err := mgr.AppendGraphDirtyEvent(ctx, event)
	if err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	second, err := mgr.AppendGraphDirtyEvent(ctx, event)
	if err != nil {
		t.Fatalf("append duplicate failed: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected idempotent append, first=%s second=%s", first.ID, second.ID)
	}
	checkpoint := MaintenanceCheckpoint{Consumer: "analyzer", SpaceID: spaceID, LastGraphRevision: 7, LastGraphDirtyEventID: first.ID}
	if err := mgr.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatalf("save checkpoint failed: %v", err)
	}
	item, err := mgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: nodeID, SourceTxnIDs: []uuid.UUID{txnID}, FirstGraphRevision: 7, LastGraphRevision: 7})
	if err != nil {
		t.Fatalf("upsert work failed: %v", err)
	}
	if item.ID == uuid.Nil || item.Status != domainsemantic.SemanticDirtyWorkStatusPending || item.Action != domainsemantic.SemanticDirtyWorkActionRefresh {
		t.Fatalf("unexpected item defaults: %+v", item)
	}
	now := time.Now().UTC()
	claimed, err := mgr.ClaimReadyWork(ctx, ClaimReadyWorkInput{Now: now, Limit: 1, LeaseDuration: time.Minute, ClaimedBy: "worker-1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim work: claimed=%+v err=%v", claimed, err)
	}
	if claimed[0].Status != domainsemantic.SemanticDirtyWorkStatusRunning || claimed[0].ClaimedBy != "worker-1" || claimed[0].ClaimedUntil == nil || claimed[0].Attempts != 1 {
		t.Fatalf("unexpected claim: %+v", claimed[0])
	}
	if err := mgr.CompleteWork(ctx, item.ID, WorkResult{CompletedAt: now.Add(time.Second)}); err != nil {
		t.Fatalf("complete work: %v", err)
	}

	retryNodeID := graph.NodeID(uuid.New())
	retryItem, err := mgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: retryNodeID})
	if err != nil {
		t.Fatalf("upsert retry work failed: %v", err)
	}
	if _, err := mgr.ClaimReadyWork(ctx, ClaimReadyWorkInput{Now: now, Limit: 1, LeaseDuration: time.Minute, ClaimedBy: "worker-2"}); err != nil {
		t.Fatalf("claim retry work: %v", err)
	}
	nextRun := now.Add(time.Hour)
	if err := mgr.FailWork(ctx, retryItem.ID, WorkFailure{FailedAt: now.Add(2 * time.Second), Category: "rate_limited", Message: "slow down", Retryable: true, NextRunAt: &nextRun}); err != nil {
		t.Fatalf("fail retry work: %v", err)
	}

	reloaded := NewMaintenanceManager()
	if err := reloaded.Init(ctx, location, spaceID); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	events, err := reloaded.ListGraphDirtyEvents(ctx)
	if err != nil || len(events) != 1 || events[0].TxnID != txnID {
		t.Fatalf("unexpected events: %+v err=%v", events, err)
	}
	gotCheckpoint, err := reloaded.GetCheckpoint(ctx, "analyzer")
	if err != nil || gotCheckpoint.LastGraphRevision != 7 || gotCheckpoint.LastGraphDirtyEventID != first.ID {
		t.Fatalf("unexpected checkpoint: %+v err=%v", gotCheckpoint, err)
	}
	items, err := reloaded.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 2 {
		t.Fatalf("unexpected items: %+v err=%v", items, err)
	}
	byID := map[uuid.UUID]domainsemantic.SemanticDirtyWorkItem{}
	for _, item := range items {
		byID[item.ID] = item
	}
	if byID[item.ID].Status != domainsemantic.SemanticDirtyWorkStatusComplete || byID[item.ID].CompletedAt == nil {
		t.Fatalf("completed item not persisted: %+v", byID[item.ID])
	}
	if byID[retryItem.ID].Status != domainsemantic.SemanticDirtyWorkStatusPending || byID[retryItem.ID].EarliestRunAt == nil || byID[retryItem.ID].LastErrorCategory != "rate_limited" {
		t.Fatalf("retry item not persisted: %+v", byID[retryItem.ID])
	}

	if err := os.Remove(filepath.Join(location, workStateDirName, workStateFileName)); err != nil {
		t.Fatalf("remove materialized state: %v", err)
	}
	rebuilt := NewMaintenanceManager()
	if err := rebuilt.Init(ctx, location, spaceID); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	rebuiltItems, err := rebuilt.ListDirtyWorkItems(ctx)
	if err != nil || len(rebuiltItems) != 2 {
		t.Fatalf("unexpected rebuilt items: %+v err=%v", rebuiltItems, err)
	}
}

func TestMaintenanceManagerReclaimsExpiredLeases(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	mgr := NewMaintenanceManager()
	if err := mgr.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	item, err := mgr.UpsertDirtyWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, SemanticIndexID: indexID, TargetNodeID: nodeID})
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	now := time.Now().UTC()
	if _, err := mgr.ClaimReadyWork(ctx, ClaimReadyWorkInput{Now: now, Limit: 1, LeaseDuration: time.Second, ClaimedBy: "first"}); err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	second, err := mgr.ClaimReadyWork(ctx, ClaimReadyWorkInput{Now: now.Add(500 * time.Millisecond), Limit: 1, LeaseDuration: time.Second, ClaimedBy: "second"})
	if err != nil {
		t.Fatalf("second claim before expiry failed: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("expected no claim before lease expiry, got %+v", second)
	}
	reclaimed, err := mgr.ClaimReadyWork(ctx, ClaimReadyWorkInput{Now: now.Add(2 * time.Second), Limit: 1, LeaseDuration: time.Second, ClaimedBy: "second"})
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim after expiry: %+v err=%v", reclaimed, err)
	}
	if reclaimed[0].ID != item.ID || reclaimed[0].ClaimedBy != "second" || reclaimed[0].Attempts != 2 {
		t.Fatalf("unexpected reclaimed item: %+v", reclaimed[0])
	}
}

func TestSpaceManagerRejectsMismatchedSpaceScope(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	mgr := NewSpaceManager()
	if err := mgr.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	_, err := mgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{
		Scope:  domainsemantic.ProcessingScope{SpaceID: domainspace.SpaceID(uuid.New())},
		Effect: domainsemantic.PolicyEffectAllow,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}
