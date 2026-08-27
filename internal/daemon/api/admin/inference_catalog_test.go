package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
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
