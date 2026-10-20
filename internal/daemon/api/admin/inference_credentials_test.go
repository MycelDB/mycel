package admin

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/encryption"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type credentialEncryptionHost struct {
	*runtimetest.Host
	service *encryption.Service
}

func (h credentialEncryptionHost) EncryptionService() *encryption.Service { return h.service }

func TestCredentialAuthModeAndSecretLifecycle(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	host := runtimetest.New(dataDir, logger)
	encryptionService, err := encryption.NewService(ctx, encryption.Config{AtRest: encryption.ModeEnabled, KEKProvider: encryption.ProviderStaticEnv, StaticKeyB64: base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))})
	if err != nil {
		t.Fatal(err)
	}
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, credentialEncryptionHost{Host: host, service: encryptionService}); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, host); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	catalog, err := svc.ApplyInferencePackage(authenticatedContext(), &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
		Name: "credential-modes", Version: "1",
		ModelEndpoints: []*adminv1.ModelEndpoint{
			{Key: "fixture", Name: "fixture", ConnectorType: "fake", NetworkClass: "local", PrivacyClass: "local_only", AuthModes: []string{"none"}, Operations: []string{"embeddings"}, Enabled: true},
			{Key: "api", Name: "api", ConnectorType: "openai-compatible", NetworkClass: "external_https", PrivacyClass: "third_party", AuthModes: []string{"api_key"}, Operations: []string{"embeddings"}, Enabled: true},
		},
		Models:                    []*adminv1.InferenceModel{{Key: "embedding", Kind: "embedding", ModelName: "embedding", ConnectorTypes: []string{"fake", "openai-compatible"}, Dimensions: 8, VectorSpaceKey: "fixture/8", Enabled: true}},
		ModelEndpointCapabilities: []*adminv1.ModelEndpointCapabilityDefinition{{ModelEndpoint: "fixture", Model: "embedding", Operation: "embeddings"}},
	})
	if err != nil {
		t.Fatalf("ApplyInferencePackage() error = %v", err)
	}
	endpointIDs := map[string]string{}
	for _, endpoint := range catalog.GetModelEndpoints() {
		endpointIDs[endpoint.GetKey()] = endpoint.GetModelEndpointId()
	}
	if endpointIDs["fixture"] == "" || endpointIDs["api"] == "" {
		t.Fatalf("missing catalog endpoints: %+v", catalog.GetModelEndpoints())
	}

	create := func(key, endpointID, authType, secret string) (*adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialResponse, error) {
		return svc.CreateCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialRequest{Key: key, DisplayName: key, ModelEndpointId: endpointID, OwnerType: "space", OwnerId: "space-1", AuthType: authType, SecretValue: secret, IsDefault: true})
	}
	if _, err := create("missing", uuid.NewString(), "none", ""); status.Code(err) != codes.NotFound {
		t.Fatalf("nonexistent endpoint error=%v, want NotFound", err)
	}
	if _, err := create("none-on-api", endpointIDs["api"], "none", ""); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("none on api_key endpoint error=%v, want InvalidArgument", err)
	}
	if _, err := create("api-on-none", endpointIDs["fixture"], "api_key", "secret"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("api_key on none endpoint error=%v, want InvalidArgument", err)
	}

	noneResponse, err := create("fixture-none", endpointIDs["fixture"], "none", "")
	if err != nil {
		t.Fatalf("CreateCredential(none) error = %v", err)
	}
	if noneResponse.GetSecret() != nil || noneResponse.GetCredential() == nil || noneResponse.GetCredential().GetAuthType() != "none" || noneResponse.GetCredential().GetSecretId() != "" {
		t.Fatalf("unexpected credential-free response: %+v", noneResponse)
	}
	if _, err := svc.RotateCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialRequest{CredentialId: noneResponse.GetCredential().GetCredentialId(), SecretValue: "must-not-persist"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RotateCredential(none) error=%v, want FailedPrecondition", err)
	}
	if _, err := svc.SetCredentialStatus(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceSetCredentialStatusRequest{CredentialId: noneResponse.GetCredential().GetCredentialId(), Status: "revkoed"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SetCredentialStatus(unknown) error=%v, want InvalidArgument", err)
	}
	semanticSecrets, err := semantic.GlobalManager().ListSecrets(ctx)
	if err != nil || len(semanticSecrets) != 0 {
		t.Fatalf("credential-free semantic secrets=%+v err=%v", semanticSecrets, err)
	}
	derivedSecrets, err := inference.GlobalManager().ListSecrets(ctx)
	if err != nil || len(derivedSecrets) != 0 {
		t.Fatalf("credential-free derived secrets=%+v err=%v", derivedSecrets, err)
	}
	derivedCredentials, err := inference.GlobalManager().ListCredentials(ctx)
	if err != nil || len(derivedCredentials) != 1 || derivedCredentials[0].AuthType != domaininference.CredentialAuthNone || derivedCredentials[0].SecretID != uuid.Nil || derivedCredentials[0].Status != domaininference.CredentialStatusActive {
		t.Fatalf("credential-free derived credentials=%+v err=%v", derivedCredentials, err)
	}

	apiResponse, err := create("api-key", endpointIDs["api"], "api_key", "first-secret")
	if err != nil {
		t.Fatalf("CreateCredential(api_key) error = %v", err)
	}
	secretID := apiResponse.GetCredential().GetSecretId()
	if secretID == "" || apiResponse.GetSecret().GetSecretId() != secretID {
		t.Fatalf("api-key secret identity mismatch: %+v", apiResponse)
	}
	for _, value := range []string{"second-secret", "third-secret"} {
		rotated, err := svc.RotateCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialRequest{CredentialId: apiResponse.GetCredential().GetCredentialId(), SecretValue: value})
		if err != nil {
			t.Fatalf("RotateCredential(%q) error = %v", value, err)
		}
		if rotated.GetSecret().GetSecretId() != secretID || rotated.GetCredential().GetSecretId() != secretID {
			t.Fatalf("rotation allocated a new secret: %+v", rotated)
		}
	}
	semanticSecrets, err = semantic.GlobalManager().ListSecrets(ctx)
	if err != nil || len(semanticSecrets) != 1 || semanticSecrets[0].ID.String() != secretID {
		t.Fatalf("rotated semantic secrets=%+v err=%v", semanticSecrets, err)
	}
	derivedSecrets, err = inference.GlobalManager().ListSecrets(ctx)
	if err != nil || len(derivedSecrets) != 1 || derivedSecrets[0].ID.String() != secretID {
		t.Fatalf("rotated derived secrets=%+v err=%v", derivedSecrets, err)
	}
	if _, err := svc.DeleteCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest{CredentialId: apiResponse.GetCredential().GetCredentialId(), DeleteSecret: true}); err != nil {
		t.Fatalf("DeleteCredential(api_key) error = %v", err)
	}
	semanticSecrets, err = semantic.GlobalManager().ListSecrets(ctx)
	if err != nil || len(semanticSecrets) != 0 {
		t.Fatalf("deleted semantic secrets=%+v err=%v", semanticSecrets, err)
	}
	derivedSecrets, err = inference.GlobalManager().ListSecrets(ctx)
	if err != nil || len(derivedSecrets) != 0 {
		t.Fatalf("deleted derived secrets=%+v err=%v", derivedSecrets, err)
	}
}
