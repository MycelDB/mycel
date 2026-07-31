package service

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func semanticNewID() uuid.UUID {
	id, err := uuid.NewV7()
	if err == nil {
		return id
	}
	return uuid.New()
}

func semanticKey(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func (w *walGlobalManager) canonicalPackage(ctx context.Context, v domainsemantic.InferencePackage) (domainsemantic.InferencePackage, error) {
	v.Name = semanticKey(v.Name)
	v.Version = strings.TrimSpace(v.Version)
	items, err := w.inner.ListPackages(ctx)
	if err != nil {
		return domainsemantic.InferencePackage{}, err
	}
	for _, existing := range items {
		if semanticKey(existing.Name) == v.Name && strings.TrimSpace(existing.Version) == v.Version {
			v.ID = existing.ID
			if v.InstalledAt.IsZero() {
				v.InstalledAt = existing.InstalledAt
			}
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.InstalledAt.IsZero() {
		v.InstalledAt = time.Now().UTC()
	}
	return v, nil
}

func (w *walGlobalManager) canonicalEndpoint(ctx context.Context, v domainsemantic.ModelEndpoint) (domainsemantic.ModelEndpoint, error) {
	now := time.Now().UTC()
	v.Key = semanticKey(v.Key)
	if v.Name == "" {
		v.Name = v.Key
	}
	items, err := w.inner.ListModelEndpoints(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	for _, existing := range items {
		if semanticKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalModel(ctx context.Context, v domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error) {
	now := time.Now().UTC()
	v.Key = semanticKey(v.Key)
	if strings.TrimSpace(v.ModelName) == "" {
		v.ModelName = v.Key
	}
	items, err := w.inner.ListModels(ctx)
	if err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	for _, existing := range items {
		if semanticKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalCapability(ctx context.Context, v domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error) {
	now := time.Now().UTC()
	items, err := w.inner.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	for _, existing := range items {
		if existing.ModelEndpointID == v.ModelEndpointID && existing.ModelID == v.ModelID && existing.Operation == v.Operation {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalVectorStore(ctx context.Context, v domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error) {
	now := time.Now().UTC()
	v.Key = semanticKey(v.Key)
	if v.Name == "" {
		v.Name = v.Key
	}
	items, err := w.inner.ListVectorStores(ctx)
	if err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	for _, existing := range items {
		if semanticKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalSecret(ctx context.Context, v domainsemantic.Secret) (domainsemantic.Secret, error) {
	now := time.Now().UTC()
	if v.ID != uuid.Nil {
		items, err := w.inner.ListSecrets(ctx)
		if err != nil {
			return domainsemantic.Secret{}, err
		}
		for _, existing := range items {
			if existing.ID == v.ID {
				v.CreatedAt = existing.CreatedAt
				v.UpdatedAt = now
				return v, nil
			}
		}
	} else {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalCredential(ctx context.Context, v domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error) {
	now := time.Now().UTC()
	v.Key = semanticKey(v.Key)
	if v.Name == "" {
		v.Name = v.Key
	}
	if v.Status == "" {
		v.Status = domainsemantic.CredentialStatusActive
	}
	items, err := w.inner.ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	for _, existing := range items {
		if semanticKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walSpaceManager) canonicalSemanticIndex(ctx context.Context, v domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) {
	now := time.Now().UTC()
	v.Purpose = domainsemantic.NormalizeSemanticIndexPurpose(v.Purpose)
	v.Key = semanticKey(v.Key)
	if v.Name == "" {
		v.Name = v.Key
	}
	items, err := w.inner.ListSemanticIndexes(ctx)
	if err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	for _, existing := range items {
		if existing.SpaceID == v.SpaceID && existing.DomainID == v.DomainID && semanticKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walSpaceManager) canonicalGrant(ctx context.Context, v domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	if v.ID != uuid.Nil {
		items, err := w.inner.ListCredentialGrants(ctx)
		if err != nil {
			return domainsemantic.CredentialGrant{}, err
		}
		for _, existing := range items {
			if existing.ID == v.ID {
				if v.CreatedAt.IsZero() {
					v.CreatedAt = existing.CreatedAt
				}
				return v, nil
			}
		}
	} else {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return v, nil
}

func (w *walSpaceManager) canonicalPolicy(ctx context.Context, v domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	if v.ID != uuid.Nil {
		items, err := w.inner.ListInferencePolicies(ctx)
		if err != nil {
			return domainsemantic.InferencePolicy{}, err
		}
		for _, existing := range items {
			if existing.ID == v.ID {
				if v.CreatedAt.IsZero() {
					v.CreatedAt = existing.CreatedAt
				}
				return v, nil
			}
		}
	} else {
		v.ID = semanticNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return v, nil
}
