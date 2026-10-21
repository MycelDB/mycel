package semantic

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func (m *globalManager) CreateCredentialLifecycle(_ context.Context, credential domainsemantic.InferenceCredential, secret *domainsemantic.Secret) (domainsemantic.InferenceCredential, *domainsemantic.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validateCredential(context.Background(), credential); err != nil {
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
	if secret != nil {
		if err := validateSecret(context.Background(), *secret); err != nil {
			return domainsemantic.InferenceCredential{}, nil, err
		}
		storedSecret := *secret
		m.upsertSecretLocked(storedSecret)
		if err := m.persistLocked(m.path(secretsFileName), m.secrets); err != nil {
			return domainsemantic.InferenceCredential{}, nil, err
		}
		secret = &storedSecret
	}
	m.upsertCredentialLocked(credential)
	if err := m.persistLocked(m.path(credentialsFileName), m.credentials); err != nil {
		return domainsemantic.InferenceCredential{}, secret, err
	}
	return credential, secret, nil
}

func (m *globalManager) RotateCredentialLifecycle(_ context.Context, credentialID domainsemantic.InferenceCredentialID, expectedSecretRef domainsemantic.SecretID, secret domainsemantic.Secret) (domainsemantic.InferenceCredential, domainsemantic.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	if err := validateSecret(context.Background(), secret); err != nil {
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
	m.upsertSecretLocked(secret)
	if err := m.persistLocked(m.path(secretsFileName), m.secrets); err != nil {
		return domainsemantic.InferenceCredential{}, domainsemantic.Secret{}, err
	}
	// Re-read and preserve every current credential field, especially status.
	credential = m.credentials.Credentials[credentialIndex]
	credential.SecretSuffix = secret.SecretSuffix
	credential.UpdatedAt = secret.UpdatedAt
	m.credentials.Credentials[credentialIndex] = credential
	if err := m.persistLocked(m.path(credentialsFileName), m.credentials); err != nil {
		return domainsemantic.InferenceCredential{}, secret, err
	}
	return credential, secret, nil
}

func (m *globalManager) SetCredentialStatusLifecycle(_ context.Context, credentialID domainsemantic.InferenceCredentialID, status domainsemantic.CredentialStatus, updatedAt time.Time) (domainsemantic.InferenceCredential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := m.credentialIndexLocked(credentialID)
	if index < 0 {
		return domainsemantic.InferenceCredential{}, ErrNotFound
	}
	previous := m.credentials.Credentials[index]
	credential := previous
	credential.Status = status
	credential.UpdatedAt = updatedAt
	if err := validateCredential(context.Background(), credential); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	m.credentials.Credentials[index] = credential
	if err := m.persistLocked(m.path(credentialsFileName), m.credentials); err != nil {
		m.credentials.Credentials[index] = previous
		return domainsemantic.InferenceCredential{}, err
	}
	return credential, nil
}

func (m *globalManager) DeleteCredentialLifecycle(_ context.Context, credentialID domainsemantic.InferenceCredentialID, expectedSecretRef domainsemantic.SecretID, deleteSecret bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	credentialIndex := m.credentialIndexLocked(credentialID)
	if credentialIndex >= 0 {
		credential := m.credentials.Credentials[credentialIndex]
		if expectedSecretRef != uuid.Nil && credential.SecretRef != expectedSecretRef {
			return fmt.Errorf("%w: credential secret reference changed", ErrConflict)
		}
		previous := append([]domainsemantic.InferenceCredential(nil), m.credentials.Credentials...)
		m.credentials.Credentials = append(m.credentials.Credentials[:credentialIndex], m.credentials.Credentials[credentialIndex+1:]...)
		if err := m.persistLocked(m.path(credentialsFileName), m.credentials); err != nil {
			m.credentials.Credentials = previous
			return err
		}
	}
	if deleteSecret && expectedSecretRef != uuid.Nil {
		for _, credential := range m.credentials.Credentials {
			if credential.SecretRef == expectedSecretRef {
				return fmt.Errorf("%w: secret is referenced by credential %s", ErrConflict, credential.ID)
			}
		}
		secretIndex := m.secretIndexLocked(expectedSecretRef)
		if secretIndex >= 0 {
			previous := append([]domainsemantic.Secret(nil), m.secrets.Secrets...)
			m.secrets.Secrets = append(m.secrets.Secrets[:secretIndex], m.secrets.Secrets[secretIndex+1:]...)
			if err := m.persistLocked(m.path(secretsFileName), m.secrets); err != nil {
				m.secrets.Secrets = previous
				return err
			}
		}
	}
	return nil
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
	for index, credential := range m.credentials.Credentials {
		if credential.ID == id {
			return index
		}
	}
	return -1
}

func (m *globalManager) secretIndexLocked(id domainsemantic.SecretID) int {
	for index, secret := range m.secrets.Secrets {
		if secret.ID == id {
			return index
		}
	}
	return -1
}

func (m *globalManager) upsertSecretLocked(secret domainsemantic.Secret) {
	for index, existing := range m.secrets.Secrets {
		if existing.ID == secret.ID {
			m.secrets.Secrets[index] = secret
			return
		}
	}
	m.secrets.Secrets = append(m.secrets.Secrets, secret)
}

func (m *globalManager) upsertCredentialLocked(credential domainsemantic.InferenceCredential) {
	for index, existing := range m.credentials.Credentials {
		if existing.ID == credential.ID || normalizeKey(existing.Key) == normalizeKey(credential.Key) {
			m.credentials.Credentials[index] = credential
			return
		}
	}
	m.credentials.Credentials = append(m.credentials.Credentials, credential)
}
