package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

type credentialCreateMutation struct {
	Credential domainsemantic.InferenceCredential `json:"credential"`
	Secret     *domainsemantic.Secret             `json:"secret,omitempty"`
}

type credentialRotateMutation struct {
	CredentialID      domainsemantic.InferenceCredentialID `json:"credential_id"`
	ExpectedSecretRef domainsemantic.SecretID              `json:"expected_secret_ref"`
	Secret            domainsemantic.Secret                `json:"secret"`
}

type credentialStatusMutation struct {
	CredentialID domainsemantic.InferenceCredentialID `json:"credential_id"`
	Status       domainsemantic.CredentialStatus      `json:"status"`
	UpdatedAt    time.Time                            `json:"updated_at"`
}

type credentialDeleteMutation struct {
	CredentialID      domainsemantic.InferenceCredentialID `json:"credential_id"`
	ExpectedSecretRef domainsemantic.SecretID              `json:"expected_secret_ref,omitempty"`
	DeleteSecret      bool                                 `json:"delete_secret"`
}

func (m *Module) CreateInferenceCredential(ctx context.Context, in CredentialCreateInput) (CredentialLifecycleResult, error) {
	canonical := &walGlobalManager{inner: m.credentialGlobalManager(), module: m}
	var secret *domainsemantic.Secret
	if in.Secret != nil {
		value, err := canonical.canonicalSecret(ctx, *in.Secret)
		if err != nil {
			return CredentialLifecycleResult{}, err
		}
		secret = &value
		in.Credential.SecretRef = value.ID
		in.Credential.SecretSuffix = value.SecretSuffix
	}
	credential, err := canonical.canonicalCredential(ctx, in.Credential)
	if err != nil {
		return CredentialLifecycleResult{}, err
	}
	mutation := credentialCreateMutation{Credential: credential, Secret: secret}
	if err := m.commitCredentialLifecycle(ctx, semanticMutationRecord{Kind: "credential.create", Payload: raw(mutation)}); err != nil {
		return CredentialLifecycleResult{}, err
	}
	return m.credentialLifecycleResult(ctx, credential.ID, credential.Key)
}

func (m *Module) RotateInferenceCredential(ctx context.Context, in CredentialRotateInput) (CredentialLifecycleResult, error) {
	if in.CredentialID == uuid.Nil || in.ExpectedSecretRef == uuid.Nil {
		return CredentialLifecycleResult{}, fmt.Errorf("%w: credential_id and expected_secret_ref are required", storesemantic.ErrInvalidInput)
	}
	in.Secret.ID = in.ExpectedSecretRef
	canonical := &walGlobalManager{inner: m.credentialGlobalManager(), module: m}
	secret, err := canonical.canonicalSecret(ctx, in.Secret)
	if err != nil {
		return CredentialLifecycleResult{}, err
	}
	mutation := credentialRotateMutation{CredentialID: in.CredentialID, ExpectedSecretRef: in.ExpectedSecretRef, Secret: secret}
	if err := m.commitCredentialLifecycle(ctx, semanticMutationRecord{Kind: "credential.rotate", Payload: raw(mutation)}); err != nil {
		return CredentialLifecycleResult{}, err
	}
	return m.credentialLifecycleResult(ctx, in.CredentialID, "")
}

func (m *Module) SetInferenceCredentialStatus(ctx context.Context, in CredentialStatusInput) (CredentialLifecycleResult, error) {
	if in.CredentialID == uuid.Nil {
		return CredentialLifecycleResult{}, fmt.Errorf("%w: credential_id is required", storesemantic.ErrInvalidInput)
	}
	mutation := credentialStatusMutation{CredentialID: in.CredentialID, Status: in.Status, UpdatedAt: time.Now().UTC()}
	if err := m.commitCredentialLifecycle(ctx, semanticMutationRecord{Kind: "credential.set_status", Payload: raw(mutation)}); err != nil {
		return CredentialLifecycleResult{}, err
	}
	return m.credentialLifecycleResult(ctx, in.CredentialID, "")
}

func (m *Module) DeleteInferenceCredential(ctx context.Context, in CredentialDeleteInput) error {
	if in.CredentialID == uuid.Nil {
		return fmt.Errorf("%w: credential_id is required", storesemantic.ErrInvalidInput)
	}
	mutation := credentialDeleteMutation{CredentialID: in.CredentialID, ExpectedSecretRef: in.ExpectedSecretRef, DeleteSecret: in.DeleteSecret}
	return m.commitCredentialLifecycle(ctx, semanticMutationRecord{Kind: "credential.delete_lifecycle", Payload: raw(mutation)})
}

func (m *Module) recoverCredentialLifecycle(ctx context.Context) error {
	lifecycle, ok := m.globalBase.(storesemantic.CredentialLifecycleManager)
	if !ok {
		return nil
	}
	recovered, err := lifecycle.RecoverCredentialLifecycle(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if recovered {
		m.credentialProjectionRepairNeeded = true
	}
	repairProjection := m.credentialProjectionRepairNeeded
	m.mu.Unlock()
	if !repairProjection {
		return nil
	}
	if err := m.rebuildInferenceGlobalProjection(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.credentialProjectionRepairNeeded = false
	m.mu.Unlock()
	return nil
}

func (m *Module) credentialGlobalManager() storesemantic.GlobalManager {
	if m.wal != nil || m.raftGroups != nil {
		return m.globalBase
	}
	return m.GlobalManager()
}

func (m *Module) commitCredentialLifecycle(ctx context.Context, rec semanticMutationRecord) error {
	if m.wal != nil || m.raftGroups != nil {
		return m.commitSemanticMutation(ctx, recordTypeSemanticGlobal, rec)
	}
	m.globalMutationMu.Lock()
	defer m.globalMutationMu.Unlock()
	if err := applySemanticGlobalMutation(ctx, m.credentialGlobalManager(), rec); err != nil {
		return err
	}
	return m.projectSemanticGlobalMutation(ctx, rec)
}

func (m *Module) credentialLifecycleResult(ctx context.Context, id domainsemantic.InferenceCredentialID, key string) (CredentialLifecycleResult, error) {
	manager := m.credentialGlobalManager()
	credentials, err := manager.ListCredentials(ctx)
	if err != nil {
		return CredentialLifecycleResult{}, err
	}
	var credential domainsemantic.InferenceCredential
	for _, candidate := range credentials {
		if candidate.ID == id || (strings.TrimSpace(key) != "" && semanticKey(candidate.Key) == semanticKey(key)) {
			credential = candidate
			break
		}
	}
	if credential.ID == uuid.Nil {
		return CredentialLifecycleResult{}, storesemantic.ErrNotFound
	}
	result := CredentialLifecycleResult{Credential: credential}
	if credential.SecretRef == uuid.Nil {
		return result, nil
	}
	secrets, err := manager.ListSecrets(ctx)
	if err != nil {
		return CredentialLifecycleResult{}, err
	}
	for _, secret := range secrets {
		if secret.ID == credential.SecretRef {
			value := secret
			result.Secret = &value
			return result, nil
		}
	}
	return CredentialLifecycleResult{}, fmt.Errorf("%w: credential secret not found", storesemantic.ErrNotFound)
}

func applyCredentialCreateMutation(ctx context.Context, manager storesemantic.GlobalManager, mutation credentialCreateMutation) error {
	if lifecycle, ok := manager.(storesemantic.CredentialLifecycleManager); ok {
		_, _, err := lifecycle.CreateCredentialLifecycle(ctx, mutation.Credential, mutation.Secret)
		return err
	}
	endpoint, err := credentialEndpoint(ctx, manager, mutation.Credential.ModelEndpointID)
	if err != nil {
		return err
	}
	if !semanticEndpointSupportsAuth(endpoint, mutation.Credential.AuthType) {
		return fmt.Errorf("%w: endpoint does not support credential auth type %q", storesemantic.ErrConflict, mutation.Credential.AuthType)
	}
	if mutation.Credential.AuthType == domainsemantic.AuthModeNone {
		if mutation.Secret != nil || mutation.Credential.SecretRef != uuid.Nil {
			return fmt.Errorf("%w: auth_type none cannot persist secret material", storesemantic.ErrInvalidInput)
		}
	} else if mutation.Credential.AuthType == domainsemantic.AuthModeAPIKey {
		if mutation.Secret == nil || mutation.Secret.ID == uuid.Nil || mutation.Credential.SecretRef != mutation.Secret.ID {
			return fmt.Errorf("%w: api_key credential requires matching secret material", storesemantic.ErrInvalidInput)
		}
	} else {
		return fmt.Errorf("%w: unsupported credential auth type %q", storesemantic.ErrInvalidInput, mutation.Credential.AuthType)
	}
	if mutation.Secret != nil {
		if _, err := manager.UpsertSecret(ctx, *mutation.Secret); err != nil {
			return err
		}
	}
	_, err = manager.UpsertCredential(ctx, mutation.Credential)
	return err
}

func applyCredentialRotateMutation(ctx context.Context, manager storesemantic.GlobalManager, mutation credentialRotateMutation) error {
	if lifecycle, ok := manager.(storesemantic.CredentialLifecycleManager); ok {
		_, _, err := lifecycle.RotateCredentialLifecycle(ctx, mutation.CredentialID, mutation.ExpectedSecretRef, mutation.Secret)
		return err
	}
	credential, err := credentialByIDFromManager(ctx, manager, mutation.CredentialID)
	if err != nil {
		return err
	}
	if credential.AuthType != domainsemantic.AuthModeAPIKey {
		return fmt.Errorf("%w: only api_key credentials can rotate secret material", storesemantic.ErrConflict)
	}
	if credential.SecretRef != mutation.ExpectedSecretRef || mutation.Secret.ID != mutation.ExpectedSecretRef {
		return fmt.Errorf("%w: credential secret reference changed", storesemantic.ErrConflict)
	}
	endpoint, err := credentialEndpoint(ctx, manager, credential.ModelEndpointID)
	if err != nil {
		return err
	}
	if !semanticEndpointSupportsAuth(endpoint, credential.AuthType) {
		return fmt.Errorf("%w: endpoint does not support credential auth type %q", storesemantic.ErrConflict, credential.AuthType)
	}
	if _, err := manager.UpsertSecret(ctx, mutation.Secret); err != nil {
		return err
	}
	credential.SecretSuffix = mutation.Secret.SecretSuffix
	credential.UpdatedAt = mutation.Secret.UpdatedAt
	_, err = manager.UpsertCredential(ctx, credential)
	return err
}

func applyCredentialStatusMutation(ctx context.Context, manager storesemantic.GlobalManager, mutation credentialStatusMutation) error {
	if lifecycle, ok := manager.(storesemantic.CredentialLifecycleManager); ok {
		_, err := lifecycle.SetCredentialStatusLifecycle(ctx, mutation.CredentialID, mutation.Status, mutation.UpdatedAt)
		return err
	}
	credential, err := credentialByIDFromManager(ctx, manager, mutation.CredentialID)
	if err != nil {
		return err
	}
	credential.Status = mutation.Status
	credential.UpdatedAt = mutation.UpdatedAt
	_, err = manager.UpsertCredential(ctx, credential)
	return err
}

func applyCredentialDeleteMutation(ctx context.Context, manager storesemantic.GlobalManager, mutation credentialDeleteMutation) error {
	if lifecycle, ok := manager.(storesemantic.CredentialLifecycleManager); ok {
		return lifecycle.DeleteCredentialLifecycle(ctx, mutation.CredentialID, mutation.ExpectedSecretRef, mutation.DeleteSecret)
	}
	credential, err := credentialByIDFromManager(ctx, manager, mutation.CredentialID)
	if err == nil {
		if mutation.ExpectedSecretRef != uuid.Nil && credential.SecretRef != mutation.ExpectedSecretRef {
			return fmt.Errorf("%w: credential secret reference changed", storesemantic.ErrConflict)
		}
		if err := manager.DeleteCredential(ctx, mutation.CredentialID); err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
			return err
		}
	} else if !errors.Is(err, storesemantic.ErrNotFound) {
		return err
	}
	if mutation.DeleteSecret && mutation.ExpectedSecretRef != uuid.Nil {
		if err := manager.DeleteSecret(ctx, mutation.ExpectedSecretRef); err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
			return err
		}
	}
	return nil
}

func credentialByIDFromManager(ctx context.Context, manager storesemantic.GlobalManager, id domainsemantic.InferenceCredentialID) (domainsemantic.InferenceCredential, error) {
	items, err := manager.ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return domainsemantic.InferenceCredential{}, storesemantic.ErrNotFound
}

func credentialEndpoint(ctx context.Context, manager storesemantic.GlobalManager, id domainsemantic.ModelEndpointID) (domainsemantic.ModelEndpoint, error) {
	items, err := manager.ListModelEndpoints(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return domainsemantic.ModelEndpoint{}, fmt.Errorf("%w: model endpoint %s", storesemantic.ErrNotFound, id)
}

func semanticEndpointSupportsAuth(endpoint domainsemantic.ModelEndpoint, authType domainsemantic.AuthMode) bool {
	for _, supported := range endpoint.AuthModes {
		if supported == authType {
			return true
		}
	}
	return false
}
