package semantic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

const credentialLifecycleIntentVersion = 1

type credentialLifecycleIntent struct {
	Version      int                                  `json:"version"`
	Operation    string                               `json:"operation"`
	CredentialID domainsemantic.InferenceCredentialID `json:"credential_id"`
	Secrets      secretsState                         `json:"secrets"`
	Credentials  credentialsState                     `json:"credentials"`
}

func (m *globalManager) CreateCredentialLifecycle(ctx context.Context, credential domainsemantic.InferenceCredential, secret *domainsemantic.Secret) (domainsemantic.InferenceCredential, *domainsemantic.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.recoverCredentialLifecycleLocked(ctx); err != nil {
		return domainsemantic.InferenceCredential{}, nil, err
	}
	if err := validateCredential(ctx, credential); err != nil {
		return domainsemantic.InferenceCredential{}, nil, err
	}
	for _, existing := range m.credentials.Credentials {
		if normalizeKey(existing.Key) != normalizeKey(credential.Key) {
			continue
		}
		if existing.ModelEndpointID != credential.ModelEndpointID || existing.OwnerType != credential.OwnerType || existing.OwnerID != credential.OwnerID || existing.AuthType != credential.AuthType {
			return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: credential key already exists with different identity", ErrConflict)
		}
		if existing.SecretRef == uuid.Nil {
			return existing, nil, nil
		}
		secretIndex := m.secretIndexLocked(existing.SecretRef)
		if secretIndex < 0 {
			return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: existing credential secret %s", ErrNotFound, existing.SecretRef)
		}
		storedSecret := m.secrets.Secrets[secretIndex]
		return existing, &storedSecret, nil
	}
	endpoint, ok := m.endpointByIDLocked(credential.ModelEndpointID)
	if !ok {
		return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: model endpoint %s", ErrNotFound, credential.ModelEndpointID)
	}
	if !endpointSupportsCredentialAuth(endpoint, credential.AuthType) {
		return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: endpoint does not support credential auth type %q", ErrConflict, credential.AuthType)
	}
	if credential.AuthType == domainsemantic.AuthModeNone {
		if secret != nil || credential.SecretRef != uuid.Nil {
			return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: auth_type none cannot persist secret material", ErrInvalidInput)
		}
	} else if credential.AuthType == domainsemantic.AuthModeAPIKey {
		if secret == nil || secret.ID == uuid.Nil || credential.SecretRef != secret.ID {
			return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: api_key credential requires matching secret material", ErrInvalidInput)
		}
	} else {
		return domainsemantic.InferenceCredential{}, nil, fmt.Errorf("%w: unsupported credential auth type %q", ErrInvalidInput, credential.AuthType)
	}
	nextSecrets := cloneSecretsState(m.secrets)
	if secret != nil {
		if err := validateSecret(ctx, *secret); err != nil {
			return domainsemantic.InferenceCredential{}, nil, err
		}
		upsertSecretState(&nextSecrets, *secret)
	}
	nextCredentials := cloneCredentialsState(m.credentials)
	upsertCredentialState(&nextCredentials, credential)
	if err := m.persistCredentialLifecycleLocked(ctx, "create", credential.ID, nextSecrets, nextCredentials); err != nil {
		return domainsemantic.InferenceCredential{}, nil, err
	}
	return credential, secret, nil
}

func (m *globalManager) RotateCredentialLifecycle(ctx context.Context, credentialID domainsemantic.InferenceCredentialID, expectedSecretRef domainsemantic.SecretID, secret domainsemantic.Secret) (domainsemantic.InferenceCredential, domainsemantic.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.recoverCredentialLifecycleLocked(ctx); err != nil {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, err
	}
	credentialIndex := m.credentialIndexLocked(credentialID)
	if credentialIndex < 0 {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, ErrNotFound
	}
	credential := m.credentials.Credentials[credentialIndex]
	if credential.AuthType != domainsemantic.AuthModeAPIKey {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, fmt.Errorf("%w: only api_key credentials can rotate secret material", ErrConflict)
	}
	if expectedSecretRef == uuid.Nil || credential.SecretRef != expectedSecretRef || secret.ID != expectedSecretRef {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, fmt.Errorf("%w: credential secret reference changed", ErrConflict)
	}
	if err := validateSecret(ctx, secret); err != nil {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, err
	}
	if m.secretIndexLocked(expectedSecretRef) < 0 {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, fmt.Errorf("%w: credential secret %s", ErrNotFound, expectedSecretRef)
	}
	endpoint, ok := m.endpointByIDLocked(credential.ModelEndpointID)
	if !ok {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, fmt.Errorf("%w: model endpoint %s", ErrNotFound, credential.ModelEndpointID)
	}
	if !endpointSupportsCredentialAuth(endpoint, credential.AuthType) {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, fmt.Errorf("%w: endpoint does not support credential auth type %q", ErrConflict, credential.AuthType)
	}
	nextSecrets := cloneSecretsState(m.secrets)
	upsertSecretState(&nextSecrets, secret)
	nextCredentials := cloneCredentialsState(m.credentials)
	// Preserve every current credential field, especially status.
	credential.SecretSuffix = secret.SecretSuffix
	credential.UpdatedAt = secret.UpdatedAt
	nextCredentials.Credentials[credentialIndex] = credential
	if err := m.persistCredentialLifecycleLocked(ctx, "rotate", credentialID, nextSecrets, nextCredentials); err != nil {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, err
	}
	return credential, secret, nil
}

func (m *globalManager) SetCredentialStatusLifecycle(ctx context.Context, credentialID domainsemantic.InferenceCredentialID, status domainsemantic.CredentialStatus, updatedAt time.Time) (domainsemantic.InferenceCredential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.recoverCredentialLifecycleLocked(ctx); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	index := m.credentialIndexLocked(credentialID)
	if index < 0 {
		return domainsemantic.InferenceCredential{}, ErrNotFound
	}
	credential := m.credentials.Credentials[index]
	credential.Status = status
	credential.UpdatedAt = updatedAt
	if err := validateCredential(ctx, credential); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	nextCredentials := cloneCredentialsState(m.credentials)
	nextCredentials.Credentials[index] = credential
	if err := m.persistCredentialLifecycleLocked(ctx, "set_status", credentialID, cloneSecretsState(m.secrets), nextCredentials); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	return credential, nil
}

func (m *globalManager) DeleteCredentialLifecycle(ctx context.Context, credentialID domainsemantic.InferenceCredentialID, expectedSecretRef domainsemantic.SecretID, deleteSecret bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.recoverCredentialLifecycleLocked(ctx); err != nil {
		return err
	}
	nextCredentials := cloneCredentialsState(m.credentials)
	credentialIndex := credentialIndexInState(nextCredentials, credentialID)
	if credentialIndex >= 0 {
		credential := nextCredentials.Credentials[credentialIndex]
		if expectedSecretRef != uuid.Nil && credential.SecretRef != expectedSecretRef {
			return fmt.Errorf("%w: credential secret reference changed", ErrConflict)
		}
		nextCredentials.Credentials = append(nextCredentials.Credentials[:credentialIndex], nextCredentials.Credentials[credentialIndex+1:]...)
	}
	nextSecrets := cloneSecretsState(m.secrets)
	if deleteSecret && expectedSecretRef != uuid.Nil {
		for _, credential := range nextCredentials.Credentials {
			if credential.SecretRef == expectedSecretRef {
				return fmt.Errorf("%w: secret is referenced by credential %s", ErrConflict, credential.ID)
			}
		}
		if secretIndex := secretIndexInState(nextSecrets, expectedSecretRef); secretIndex >= 0 {
			nextSecrets.Secrets = append(nextSecrets.Secrets[:secretIndex], nextSecrets.Secrets[secretIndex+1:]...)
		}
	}
	return m.persistCredentialLifecycleLocked(ctx, "delete", credentialID, nextSecrets, nextCredentials)
}

func (m *globalManager) RecoverCredentialLifecycle(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recoverCredentialLifecycleLocked(ctx)
}

func (m *globalManager) persistCredentialLifecycleLocked(ctx context.Context, operation string, credentialID domainsemantic.InferenceCredentialID, nextSecrets secretsState, nextCredentials credentialsState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	intent := credentialLifecycleIntent{Version: credentialLifecycleIntentVersion, Operation: operation, CredentialID: credentialID, Secrets: nextSecrets, Credentials: nextCredentials}
	if err := m.persistLocked(m.path(credentialLifecycleIntentFileName), intent); err != nil {
		return err
	}
	if err := m.persistLocked(m.path(secretsFileName), nextSecrets); err != nil {
		return err
	}
	if err := m.persistLocked(m.path(credentialsFileName), nextCredentials); err != nil {
		return err
	}
	m.secrets = nextSecrets
	m.credentials = nextCredentials
	return m.clearCredentialLifecycleIntentLocked()
}

func (m *globalManager) recoverCredentialLifecycleLocked(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path := m.path(credentialLifecycleIntentFileName)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	var intent credentialLifecycleIntent
	if err := loadJSON(path, &intent, credentialLifecycleIntent{}); err != nil {
		return false, err
	}
	if intent.Version != credentialLifecycleIntentVersion || intent.CredentialID == uuid.Nil || intent.Operation == "" {
		return false, fmt.Errorf("%w: invalid credential lifecycle intent", ErrInvalidInput)
	}
	if err := m.persistLocked(m.path(secretsFileName), intent.Secrets); err != nil {
		return false, err
	}
	if err := m.persistLocked(m.path(credentialsFileName), intent.Credentials); err != nil {
		return false, err
	}
	m.secrets = intent.Secrets
	m.credentials = intent.Credentials
	if err := m.clearCredentialLifecycleIntentLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (m *globalManager) clearCredentialLifecycleIntentLocked() error {
	path := m.path(credentialLifecycleIntentFileName)
	if m.persistFailureHook != nil {
		if err := m.persistFailureHook("remove", path); err != nil {
			return err
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func cloneSecretsState(state secretsState) secretsState {
	return secretsState{Secrets: append([]domainsemantic.Secret(nil), state.Secrets...)}
}

func cloneCredentialsState(state credentialsState) credentialsState {
	return credentialsState{Credentials: append([]domainsemantic.InferenceCredential(nil), state.Credentials...)}
}

func upsertSecretState(state *secretsState, secret domainsemantic.Secret) {
	for index, existing := range state.Secrets {
		if existing.ID == secret.ID {
			state.Secrets[index] = secret
			return
		}
	}
	state.Secrets = append(state.Secrets, secret)
}

func upsertCredentialState(state *credentialsState, credential domainsemantic.InferenceCredential) {
	for index, existing := range state.Credentials {
		if existing.ID == credential.ID || normalizeKey(existing.Key) == normalizeKey(credential.Key) {
			state.Credentials[index] = credential
			return
		}
	}
	state.Credentials = append(state.Credentials, credential)
}

func credentialIndexInState(state credentialsState, id domainsemantic.InferenceCredentialID) int {
	for index, credential := range state.Credentials {
		if credential.ID == id {
			return index
		}
	}
	return -1
}

func secretIndexInState(state secretsState, id domainsemantic.SecretID) int {
	for index, secret := range state.Secrets {
		if secret.ID == id {
			return index
		}
	}
	return -1
}

func (m *globalManager) endpointByIDLocked(id domainsemantic.ModelEndpointID) (domainsemantic.ModelEndpoint, bool) {
	for _, endpoint := range m.endpoints.ModelEndpoints {
		if endpoint.ID == id {
			return endpoint, true
		}
	}
	return domainsemantic.ModelEndpoint{}, false
}

func (m *globalManager) credentialIndexLocked(id domainsemantic.InferenceCredentialID) int {
	return credentialIndexInState(m.credentials, id)
}

func (m *globalManager) secretIndexLocked(id domainsemantic.SecretID) int {
	return secretIndexInState(m.secrets, id)
}
