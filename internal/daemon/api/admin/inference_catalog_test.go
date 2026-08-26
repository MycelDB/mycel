package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
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

type rejectingAdminWriteHost struct{ *runtimetest.Host }

func (h rejectingAdminWriteHost) RequireLocalWriteAllowed() error { return errors.New("rejected") }
