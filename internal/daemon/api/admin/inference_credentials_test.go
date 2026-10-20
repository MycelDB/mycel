package admin

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
)

func TestCreateCredentialAllowsCredentialFreeEndpoint(t *testing.T) {
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
	catalog, err := svc.ApplyInferencePackage(authenticatedContext(), &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
		Name: "credential-free", Version: "1",
		ModelEndpoints:            []*adminv1.ModelEndpoint{{Key: "fixture", Name: "fixture", ConnectorType: "fake", NetworkClass: "local", PrivacyClass: "local_only", AuthModes: []string{"none"}, Operations: []string{"embeddings"}, Enabled: true}},
		Models:                    []*adminv1.InferenceModel{{Key: "fixture/embedding", Kind: "embedding", ModelName: "fixture/embedding", ConnectorTypes: []string{"fake"}, Dimensions: 8, VectorSpaceKey: "fixture/8", Enabled: true}},
		ModelEndpointCapabilities: []*adminv1.ModelEndpointCapabilityDefinition{{ModelEndpoint: "fixture", Model: "fixture/embedding", Operation: "embeddings"}},
	})
	if err != nil {
		t.Fatalf("ApplyInferencePackage() error = %v", err)
	}
	response, err := svc.CreateCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialRequest{
		Key: "fixture-none", DisplayName: "fixture-none", ModelEndpointId: catalog.GetModelEndpoints()[0].GetModelEndpointId(), OwnerType: "space", OwnerId: "space-1", AuthType: "none", IsDefault: true,
	})
	if err != nil {
		t.Fatalf("CreateCredential() error = %v", err)
	}
	if response.GetSecret() != nil {
		t.Fatalf("credential-free endpoint persisted a secret: %+v", response.GetSecret())
	}
	if response.GetCredential() == nil || response.GetCredential().GetAuthType() != "none" || response.GetCredential().GetSecretId() != "" {
		t.Fatalf("unexpected credential-free response: %+v", response.GetCredential())
	}
	secrets, err := semantic.GlobalManager().ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("credential-free semantic secrets=%+v err=%v", secrets, err)
	}
	credentials, err := inference.GlobalManager().ListCredentials(ctx)
	if err != nil || len(credentials) != 1 || credentials[0].AuthType != domaininference.CredentialAuthNone || credentials[0].SecretID != uuid.Nil {
		t.Fatalf("credential-free derived credentials=%+v err=%v", credentials, err)
	}
}
