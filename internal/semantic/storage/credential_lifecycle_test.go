package semantic

import (
	"context"
	"errors"
	"path/filepath"
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

func TestCredentialLifecycleIntentRecoversOnRestartAtEveryBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		operation             string
		failFile              string
		wantVisibleCiphertext string
		wantCiphertext        string
	}{
		{name: "intent", operation: "write", failFile: credentialLifecycleIntentFileName, wantVisibleCiphertext: "old", wantCiphertext: "old"},
		{name: "secrets", operation: "write", failFile: secretsFileName, wantVisibleCiphertext: "old", wantCiphertext: "new"},
		{name: "credentials", operation: "write", failFile: credentialsFileName, wantVisibleCiphertext: "old", wantCiphertext: "new"},
		{name: "intent clear", operation: "remove", failFile: credentialLifecycleIntentFileName, wantVisibleCiphertext: "new", wantCiphertext: "new"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			now := time.Unix(75, 0).UTC()
			secretID := domainsemantic.SecretID(uuid.New())
			credentialID := domainsemantic.InferenceCredentialID(uuid.New())
			secret := domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "old"}, SecretSuffix: "old", CreatedAt: now, UpdatedAt: now}
			credential := domainsemantic.InferenceCredential{ID: credentialID, Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretID, SecretSuffix: "old", Status: domainsemantic.CredentialStatusRevoked, CreatedAt: now, UpdatedAt: now}
			lifecycle := manager.(CredentialLifecycleManager)
			if _, _, err := lifecycle.CreateCredentialLifecycle(ctx, credential, &secret); err != nil {
				t.Fatal(err)
			}
			rotated := secret
			rotated.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "new"}
			rotated.SecretSuffix = "new"
			rotated.UpdatedAt = now.Add(time.Second)
			concrete := manager.(*globalManager)
			failed := false
			concrete.persistFailureHook = func(operation, path string) error {
				if !failed && operation == tc.operation && filepath.Base(path) == tc.failFile {
					failed = true
					return errors.New("injected lifecycle persistence failure")
				}
				return nil
			}
			if _, _, err := lifecycle.RotateCredentialLifecycle(ctx, credentialID, secretID, rotated); err == nil {
				t.Fatal("rotation error=nil, want injected failure")
			}
			concrete.persistFailureHook = nil
			visibleCredentials, _ := manager.ListCredentials(ctx)
			visibleSecrets, _ := manager.ListSecrets(ctx)
			if len(visibleCredentials) != 1 || visibleCredentials[0].Status != domainsemantic.CredentialStatusRevoked || visibleCredentials[0].SecretSuffix != tc.wantVisibleCiphertext || len(visibleSecrets) != 1 || visibleSecrets[0].Ciphertext.CipherB64 != tc.wantVisibleCiphertext {
				t.Fatalf("failure exposed a non-atomic state: credentials=%+v secrets=%+v", visibleCredentials, visibleSecrets)
			}
			restarted := NewGlobalManager()
			if err := restarted.Init(ctx, metaDir); err != nil {
				t.Fatalf("restart recovery: %v", err)
			}
			credentials, err := restarted.ListCredentials(ctx)
			if err != nil || len(credentials) != 1 || credentials[0].Status != domainsemantic.CredentialStatusRevoked || credentials[0].SecretRef != secretID {
				t.Fatalf("restarted credentials=%+v err=%v", credentials, err)
			}
			secrets, err := restarted.ListSecrets(ctx)
			if err != nil || len(secrets) != 1 || secrets[0].ID != secretID || secrets[0].Ciphertext.CipherB64 != tc.wantCiphertext {
				t.Fatalf("restarted secrets=%+v err=%v", secrets, err)
			}
			wantSuffix := "old"
			if tc.wantCiphertext == "new" {
				wantSuffix = "new"
			}
			if credentials[0].SecretSuffix != wantSuffix {
				t.Fatalf("restarted credential suffix=%q want %q", credentials[0].SecretSuffix, wantSuffix)
			}
		})
	}
}

func TestCredentialLifecycleOperationsRecoverAcrossPersistenceBoundaries(t *testing.T) {
	boundaries := []struct {
		name      string
		operation string
		file      string
		committed bool
	}{
		{name: "intent", operation: "write", file: credentialLifecycleIntentFileName},
		{name: "secrets", operation: "write", file: secretsFileName, committed: true},
		{name: "credentials", operation: "write", file: credentialsFileName, committed: true},
		{name: "intent-clear", operation: "remove", file: credentialLifecycleIntentFileName, committed: true},
	}
	for _, operation := range []string{"create", "status", "delete"} {
		for _, boundary := range boundaries {
			t.Run(operation+"/"+boundary.name, func(t *testing.T) {
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
				now := time.Unix(90, 0).UTC()
				secretID := domainsemantic.SecretID(uuid.New())
				credentialID := domainsemantic.InferenceCredentialID(uuid.New())
				secret := domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "test", CipherB64: "cipher"}, SecretSuffix: "pher", CreatedAt: now, UpdatedAt: now}
				credential := domainsemantic.InferenceCredential{ID: credentialID, Key: "api", Name: "api", ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: "owner", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secretID, SecretSuffix: "pher", Status: domainsemantic.CredentialStatusActive, CreatedAt: now, UpdatedAt: now}
				lifecycle := manager.(CredentialLifecycleManager)
				if operation != "create" {
					if _, _, err := lifecycle.CreateCredentialLifecycle(ctx, credential, &secret); err != nil {
						t.Fatal(err)
					}
				}
				concrete := manager.(*globalManager)
				failed := false
				concrete.persistFailureHook = func(operation, path string) error {
					if !failed && operation == boundary.operation && filepath.Base(path) == boundary.file {
						failed = true
						return errors.New("injected lifecycle persistence failure")
					}
					return nil
				}
				switch operation {
				case "create":
					_, _, err = lifecycle.CreateCredentialLifecycle(ctx, credential, &secret)
				case "status":
					_, err = lifecycle.SetCredentialStatusLifecycle(ctx, credentialID, domainsemantic.CredentialStatusRevoked, now.Add(time.Second))
				case "delete":
					err = lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true)
				}
				if err == nil {
					t.Fatal("operation error=nil, want injected failure")
				}
				concrete.persistFailureHook = nil
				restarted := NewGlobalManager()
				if err := restarted.Init(ctx, metaDir); err != nil {
					t.Fatalf("restart recovery: %v", err)
				}
				credentials, err := restarted.ListCredentials(ctx)
				if err != nil {
					t.Fatal(err)
				}
				secrets, err := restarted.ListSecrets(ctx)
				if err != nil {
					t.Fatal(err)
				}
				switch operation {
				case "create":
					want := 0
					if boundary.committed {
						want = 1
					}
					if len(credentials) != want || len(secrets) != want {
						t.Fatalf("create recovery credentials=%+v secrets=%+v want=%d", credentials, secrets, want)
					}
				case "status":
					wantStatus := domainsemantic.CredentialStatusActive
					if boundary.committed {
						wantStatus = domainsemantic.CredentialStatusRevoked
					}
					if len(credentials) != 1 || len(secrets) != 1 || credentials[0].Status != wantStatus {
						t.Fatalf("status recovery credentials=%+v secrets=%+v", credentials, secrets)
					}
				case "delete":
					want := 1
					if boundary.committed {
						want = 0
					}
					if len(credentials) != want || len(secrets) != want {
						t.Fatalf("delete recovery credentials=%+v secrets=%+v want=%d", credentials, secrets, want)
					}
				}
			})
		}
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
	concrete.persistFailureHook = func(_ string, path string) error {
		if failCredentialPersist && strings.HasSuffix(path, credentialsFileName) {
			failCredentialPersist = false
			return errors.New("injected credential persistence failure")
		}
		return nil
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
	if err != nil || len(secrets) != 1 || secrets[0].Ciphertext.CipherB64 != "old" {
		t.Fatalf("failed rotation became visible before intent recovery: %+v err=%v", secrets, err)
	}

	concrete.persistFailureHook = nil
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
	concrete.persistFailureHook = func(_ string, path string) error {
		if failSecretPersist && strings.HasSuffix(path, secretsFileName) {
			failSecretPersist = false
			return errors.New("injected secret persistence failure")
		}
		return nil
	}
	if err := lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true); err == nil {
		t.Fatal("DeleteCredentialLifecycle() error=nil, want injected secret persistence failure")
	}
	credentials, err = manager.ListCredentials(ctx)
	if err != nil || len(credentials) != 1 || credentials[0].ID != credentialID {
		t.Fatalf("failed delete became visible before intent recovery: %+v err=%v", credentials, err)
	}
	secrets, err = manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 1 {
		t.Fatalf("failed delete lost retryable secret state: %+v err=%v", secrets, err)
	}
	concrete.persistFailureHook = nil
	if err := lifecycle.DeleteCredentialLifecycle(ctx, credentialID, secretID, true); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	secrets, err = manager.ListSecrets(ctx)
	if err != nil || len(secrets) != 0 {
		t.Fatalf("retry delete left orphan secret: %+v err=%v", secrets, err)
	}
}
