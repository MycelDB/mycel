package semantic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func TestModelEndpointCannotInvalidateReferencedCredential(t *testing.T) {
	ctx := context.Background()
	manager := NewGlobalManager()
	if err := manager.Init(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	endpoint, err := manager.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api", Name: "api", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(50, 0).UTC()
	secretID := domainsemantic.SecretID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())
	secret := domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "cipher"}, SecretSuffix: "pher", CreatedAt: now, UpdatedAt: now}
	credential := domainsemantic.InferenceCredential{ID: credentialID, Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretID, SecretSuffix: "pher", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
	lifecycle := manager.(CredentialLifecycleManager)
	if _, _, err := lifecycle.CreateCredentialLifecycle(ctx, credential, &secret); err != nil {
		t.Fatal(err)
	}
	retrySecret := secret
	retrySecret.ID = domainsemantic.SecretID(uuid.New())
	retrySecret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "retry"}
	retryCredential := credential
	retryCredential.SecretRef = retrySecret.ID
	storedCredential, storedSecret, err := lifecycle.CreateCredentialLifecycle(ctx, retryCredential, &retrySecret)
	if err != nil {
		t.Fatalf("ambiguous create retry: %v", err)
	}
	if storedCredential.ID != credentialID || storedCredential.SecretRef != secretID || storedSecret == nil || storedSecret.ID != secretID {
		t.Fatalf("ambiguous create changed lifecycle identity: credential=%+v secret=%+v", storedCredential, storedSecret)
	}
	secrets, err := manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 1 {
		t.Fatalf("ambiguous create orphaned a secret: %+v err=%v", secrets, err)
	}
	incompatible := endpoint
	incompatible.AuthModes = []domainsemantic.AuthMode{domainsemantic.AuthModeNone}
	if _, err := manager.UpsertModelEndpoint(ctx, incompatible); err == nil {
		t.Fatal("incompatible endpoint update error=nil")
	}
	if err := manager.DeleteModelEndpoint(ctx, endpoint.ID); err == nil {
		t.Fatal("referenced endpoint delete error=nil")
	}
	endpoints, err := manager.ListModelEndpoints(ctx)
	if err != nil || len(endpoints) != 1 || !endpointSupportsCredentialAuth(endpoints[0], domainsemantic.AuthModeAPIKey) {
		t.Fatalf("endpoint changed after rejected operations: %+v err=%v", endpoints, err)
	}
	if err := lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.UpsertModelEndpoint(ctx, incompatible); err != nil {
		t.Fatalf("endpoint update after credential deletion: %v", err)
	}
	if err := manager.DeleteModelEndpoint(ctx, endpoint.ID); err != nil {
		t.Fatalf("endpoint delete after credential deletion: %v", err)
	}
}

func TestCredentialLifecycleRotationRepairsAfterPartialPersistence(t *testing.T) {
	ctx := context.Background()
	metaDir := t.TempDir()
	manager := NewGlobalManager()
	if err := manager.Init(ctx, metaDir); err != nil {
		t.Fatal(err)
	}
	endpoint, err := manager.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: "api", Name: "api", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: "https://example.invalid/v1", NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	secretID := domainsemantic.SecretID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())
	secret := domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "old"}, SecretSuffix: "old", CreatedAt: now, UpdatedAt: now}
	credential := domainsemantic.InferenceCredential{ID: credentialID, Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretID, SecretSuffix: "old", Status: domainsemantic.CredentialStatusRevoked, CreatedAt: now, UpdatedAt: now}
	lifecycle := manager.(CredentialLifecycleManager)
	if _, _, err := lifecycle.CreateCredentialLifecycle(ctx, credential, &secret); err != nil {
		t.Fatal(err)
	}

	concrete := manager.(*globalManager)
	failCredentialPersist := true
	concrete.persistHook = func(path string, value any) error {
		if failCredentialPersist && strings.HasSuffix(path, credentialsFileName) {
			failCredentialPersist = false
			return errors.New("injected credential persistence failure")
		}
		return persistJSON(path, value)
	}
	rotated := secret
	rotated.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "new"}
	rotated.SecretSuffix = "new"
	rotated.UpdatedAt = now.Add(time.Second)
	if _, _, err := lifecycle.RotateCredentialLifecycle(ctx, credentialID, secretID, rotated); err == nil {
		t.Fatal("RotateCredentialLifecycle() error=nil, want injected credential persistence failure")
	}
	credentials, err := manager.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 {
		t.Fatalf("credentials=%+v err=%v", credentials, err)
	}
	if credentials[0].Status != domainsemantic.CredentialStatusRevoked || credentials[0].SecretRef != secretID {
		t.Fatalf("partial rotation changed protected credential fields: %+v", credentials[0])
	}
	secrets, err := manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 1 || secrets[0].Ciphertext.CipherB64 != "new" {
		t.Fatalf("partial rotation secret=%+v err=%v", secrets, err)
	}

	concrete.persistHook = nil
	storedCredential, storedSecret, err := lifecycle.RotateCredentialLifecycle(ctx, credentialID, secretID, rotated)
	if err != nil {
		t.Fatalf("retry rotation: %v", err)
	}
	if storedCredential.Status != domainsemantic.CredentialStatusRevoked || storedCredential.SecretRef != secretID || storedCredential.SecretSuffix != "new" || storedSecret.ID != secretID {
		t.Fatalf("repaired lifecycle state is invalid: credential=%+v secret=%+v", storedCredential, storedSecret)
	}

	reloaded := NewGlobalManager()
	if err := reloaded.Init(ctx, metaDir); err != nil {
		t.Fatal(err)
	}
	reloadedCredentials, err := reloaded.ListCredentials(ctx)
	if err != nil || len(reloadedCredentials) != 1 || reloadedCredentials[0].Status != domainsemantic.CredentialStatusRevoked || reloadedCredentials[0].SecretSuffix != "new" {
		t.Fatalf("reloaded credentials=%+v err=%v", reloadedCredentials, err)
	}

	failSecretPersist := true
	concrete.persistHook = func(path string, value any) error {
		if failSecretPersist && strings.HasSuffix(path, secretsFileName) {
			failSecretPersist = false
			return errors.New("injected secret persistence failure")
		}
		return persistJSON(path, value)
	}
	if err := lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true); err == nil {
		t.Fatal("DeleteCredentialLifecycle() error=nil, want injected secret persistence failure")
	}
	credentials, err = manager.ListCredentials(ctx)
	if err != nil || len(credentials) != 0 {
		t.Fatalf("partial delete resurrected credential: %+v err=%v", credentials, err)
	}
	secrets, err = manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 1 {
		t.Fatalf("partial delete lost retryable secret state: %+v err=%v", secrets, err)
	}
	concrete.persistHook = nil
	if err := lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	secrets, err = manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("retry delete left orphan secret: %+v err=%v", secrets, err)
	}
}
