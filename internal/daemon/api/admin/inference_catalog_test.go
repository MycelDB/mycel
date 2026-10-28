package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyInferencePackageSyncsDerivedInferenceStoreWithRejectingLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, rejectingAdminWriteHost{Host: runtimetest.New(dataDir, logger)}); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	if _, err := inference.GlobalManager().UpsertEndpoint(ctx, domaininference.Endpoint{Key: "direct-openai", ConnectorType: domaininference.ConnectorOpenAICompatible, PrivacyClass: domaininference.PrivacyClassThirdParty}); err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected normal inference write to be gated, got %v", err)
	}

	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	res, err := svc.ApplyInferencePackage(authenticatedContext(), &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
		Name:    "test-openai",
		Version: "1",
		ModelEndpoints: []*adminv1.ModelEndpoint{{
			Key:           "openai",
			Name:          "OpenAI",
			ConnectorType: "openai-compatible",
			EndpointUrl:   "https://api.openai.com/v1",
			NetworkClass:  "external_https",
			PrivacyClass:  "third_party",
			AuthModes:     []string{"api_key"},
			Operations:    []string{"embeddings"},
			Enabled:       true,
		}},
		Models: []*adminv1.InferenceModel{{
			Key:              "openai/text-embedding-3-small",
			Kind:             "embedding",
			ModelName:        "text-embedding-3-small",
			Dimensions:       1536,
			VectorSpaceKey:   "openai/text-embedding-3-small",
			ConnectorTypes:   []string{"openai-compatible"},
			InputModalities:  []string{"text"},
			OutputModalities: []string{"embedding"},
		}},
		ModelEndpointCapabilities: []*adminv1.ModelEndpointCapabilityDefinition{{
			ModelEndpoint: "openai",
			Model:         "openai/text-embedding-3-small",
			Operation:     "embeddings",
		}},
	})
	if err != nil {
		t.Fatalf("ApplyInferencePackage() error = %v", err)
	}
	if res.GetPackage().GetName() != "test-openai" || len(res.GetModelEndpoints()) != 1 || len(res.GetModels()) != 1 || len(res.GetModelEndpointCapabilities()) != 1 {
		t.Fatalf("unexpected apply response: %#v", res)
	}
	endpoints, err := inference.GlobalManager().ListEndpoints(ctx)
	if err != nil {
		t.Fatalf("list derived endpoints: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0].Key != "openai" {
		t.Fatalf("derived inference endpoint was not synced: %#v", endpoints)
	}
}

func TestApplyInferencePackageEnforcesVectorStoreIdentityAndKeepsRuleReferenceAcrossRename(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	apply := func(name string, stores ...*adminv1.VectorStore) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
		return svc.ApplyInferencePackage(authenticatedContext(), &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{Name: name, Version: "1", VectorStores: stores})
	}
	created, err := apply("vector-create", &adminv1.VectorStore{Key: "  Search-Primary ", Name: "Primary Search", Type: "mycel-file", PrivacyClass: "local_only", Enabled: true})
	if err != nil {
		t.Fatalf("create vector store: %v", err)
	}
	if len(created.GetVectorStores()) != 1 {
		t.Fatalf("unexpected create response: %#v", created)
	}
	storeID := created.GetVectorStores()[0].GetVectorStoreId()
	if _, err := uuid.Parse(storeID); err != nil || created.GetVectorStores()[0].GetKey() != "search-primary" {
		t.Fatalf("unexpected vector store: %#v err=%v", created.GetVectorStores()[0], err)
	}

	_, err = apply("vector-batch-conflict",
		&adminv1.VectorStore{Key: "batch-one", Name: "Batch store", Type: "mycel-file", PrivacyClass: "local_only"},
		&adminv1.VectorStore{Key: "batch-two", Name: " batch STORE ", Type: "mycel-file", PrivacyClass: "local_only"},
	)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("in-package name conflict code=%v err=%v", status.Code(err), err)
	}
	catalogAfterRejectedBatch, listErr := semantic.GlobalManager().ListVectorStores(ctx)
	if listErr != nil {
		t.Fatalf("list after rejected package: %v", listErr)
	}
	for _, item := range catalogAfterRejectedBatch {
		if strings.HasPrefix(item.Key, "batch-") {
			t.Fatalf("rejected package partially published vector store: %+v", item)
		}
	}

	_, err = apply("vector-name-conflict", &adminv1.VectorStore{Key: "other", Name: " primary search ", Type: "mycel-file", PrivacyClass: "local_only"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("name conflict code=%v err=%v", status.Code(err), err)
	}
	st := status.Convert(err)
	if len(st.Details()) != 1 {
		t.Fatalf("name conflict details=%#v", st.Details())
	}
	detail, ok := st.Details()[0].(*adminv1.VectorStoreCatalogConflict)
	if !ok || detail.GetReason() != adminv1.VectorStoreCatalogConflictReason_VECTOR_STORE_CATALOG_CONFLICT_REASON_NAME_ALREADY_EXISTS || detail.GetNormalizedValue() != "primary search" || len(detail.GetConflictingVectorStoreIds()) != 1 || detail.GetConflictingVectorStoreIds()[0] != storeID {
		t.Fatalf("unexpected name conflict detail: %#v", st.Details()[0])
	}

	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graphmodel.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	spaceMgr, err := semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertSemanticRule(ctx, domainsemantic.SemanticGenerationRule{
		ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "rename-safe", Enabled: false,
		Selector:   domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}},
		Source:     domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf},
		Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfileID: domainsemantic.IntelligenceProfileID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.MustParse(storeID)), Enabled: true}},
	}); err != nil {
		t.Fatalf("create semantic rule: %v", err)
	}

	renamed, err := apply("vector-rename", &adminv1.VectorStore{VectorStoreId: storeID, Key: "search-primary", Name: "Renamed Search", Type: "mycel-file", PrivacyClass: "local_only", Enabled: true})
	if err != nil {
		t.Fatalf("rename vector store: %v", err)
	}
	if got := renamed.GetVectorStores()[0]; got.GetVectorStoreId() != storeID || got.GetName() != "Renamed Search" {
		t.Fatalf("rename changed identity: %#v", got)
	}
	projected, err := inference.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		t.Fatalf("list projected vector stores: %v", err)
	}
	if len(projected) != 1 || projected[0].ID.String() != storeID || projected[0].DisplayName != "Renamed Search" {
		t.Fatalf("derived vector-store projection changed identity: %#v", projected)
	}
	rules, err := spaceMgr.ListSemanticRules(ctx)
	if err != nil || len(rules) != 1 || len(rules[0].Embeddings) != 1 || rules[0].Embeddings[0].VectorStoreID.String() != storeID {
		t.Fatalf("rule reference after rename=%+v err=%v", rules, err)
	}
}

func TestCreateIntelligenceProfileUsesSemanticAuthorityWithRejectingLocalWriteGate(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, rejectingAdminWriteHost{Host: runtimetest.New(dataDir, logger)}); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	spaceID := "8e603d31-3f2f-4d35-9362-22854399cb27"
	if _, err := inference.SpaceManager(ctx, spaceID); err != nil {
		t.Fatalf("open inference space manager: %v", err)
	}
	spaceMgr, err := inference.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	if _, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: spaceID, Key: "direct", Operation: domaininference.OperationEmbeddings, Enabled: true}); err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected normal inference profile write to be gated, got %v", err)
	}

	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	res, err := svc.CreateIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{SpaceId: spaceID, Key: "Semantic-Embeddings", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS, Purpose: "semantic_search", Enabled: true})
	if err != nil {
		t.Fatalf("CreateIntelligenceProfile() error = %v", err)
	}
	if res.GetIntelligenceProfile().GetKey() != "semantic-embeddings" || res.GetIntelligenceProfile().GetIntelligenceProfileId() == "" {
		t.Fatalf("unexpected profile response: %#v", res.GetIntelligenceProfile())
	}
	projected, err := spaceMgr.ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list projected profiles: %v", err)
	}
	if len(projected) != 1 || projected[0].Key != "semantic-embeddings" {
		t.Fatalf("derived inference profile was not synced: %#v", projected)
	}
}

type rejectingAdminWriteHost struct{ *runtimetest.Host }

func (h rejectingAdminWriteHost) RequireLocalWriteAllowed() error { return errors.New("rejected") }
