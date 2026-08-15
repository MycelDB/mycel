package service

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	model "github.com/myceldb/mycel/internal/inference/model"
)

func (w *walGlobalManager) canonicalPackage(ctx context.Context, v model.InferencePackage) (model.InferencePackage, error) {
	v.Name = inferenceKey(v.Name)
	v.Version = strings.TrimSpace(v.Version)
	items, err := w.inner.ListPackages(ctx)
	if err != nil {
		return model.InferencePackage{}, err
	}
	for _, existing := range items {
		if inferenceKey(existing.Name) == v.Name && strings.TrimSpace(existing.Version) == v.Version {
			v.ID = existing.ID
			if v.InstalledAt.IsZero() {
				v.InstalledAt = existing.InstalledAt
			}
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.InstalledAt.IsZero() {
		v.InstalledAt = time.Now().UTC()
	}
	return v, nil
}

func (w *walGlobalManager) canonicalEndpoint(ctx context.Context, v model.Endpoint) (model.Endpoint, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	items, err := w.inner.ListEndpoints(ctx)
	if err != nil {
		return model.Endpoint{}, err
	}
	for _, existing := range items {
		if inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalModel(ctx context.Context, v model.Model) (model.Model, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	if strings.TrimSpace(v.ProviderModelName) == "" {
		v.ProviderModelName = v.Key
	}
	items, err := w.inner.ListModels(ctx)
	if err != nil {
		return model.Model{}, err
	}
	for _, existing := range items {
		if inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalCapability(ctx context.Context, v model.Capability) (model.Capability, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	items, err := w.inner.ListCapabilities(ctx)
	if err != nil {
		return model.Capability{}, err
	}
	for _, existing := range items {
		if existing.EndpointID == v.EndpointID && existing.ModelID == v.ModelID && existing.Operation == v.Operation {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			if v.Key == "" {
				v.Key = existing.Key
			}
			return v, nil
		}
		if v.Key != "" && inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalVectorStore(ctx context.Context, v model.VectorStore) (model.VectorStore, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	items, err := w.inner.ListVectorStores(ctx)
	if err != nil {
		return model.VectorStore{}, err
	}
	for _, existing := range items {
		if inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalSecret(ctx context.Context, v model.Secret) (model.Secret, error) {
	now := time.Now().UTC()
	if v.ID != uuid.Nil {
		items, err := w.inner.ListSecrets(ctx)
		if err != nil {
			return model.Secret{}, err
		}
		for _, existing := range items {
			if existing.ID == v.ID {
				v.CreatedAt = existing.CreatedAt
				v.UpdatedAt = now
				return v, nil
			}
		}
	} else {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walGlobalManager) canonicalCredential(ctx context.Context, v model.Credential) (model.Credential, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	if v.Status == "" {
		v.Status = model.CredentialStatusActive
	}
	items, err := w.inner.ListCredentials(ctx)
	if err != nil {
		return model.Credential{}, err
	}
	for _, existing := range items {
		if inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walSpaceManager) canonicalProfile(ctx context.Context, v model.Profile) (model.Profile, error) {
	now := time.Now().UTC()
	v.Key = inferenceKey(v.Key)
	if strings.TrimSpace(v.SpaceID) == "" {
		v.SpaceID = w.spaceID
	}
	items, err := w.inner.ListProfiles(ctx)
	if err != nil {
		return model.Profile{}, err
	}
	for _, existing := range items {
		if existing.SpaceID == v.SpaceID && inferenceKey(existing.Key) == v.Key {
			v.ID = existing.ID
			v.CreatedAt = existing.CreatedAt
			v.UpdatedAt = now
			return v, nil
		}
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = now
	}
	v.UpdatedAt = now
	return v, nil
}

func (w *walSpaceManager) canonicalGrant(ctx context.Context, v model.CredentialGrant) (model.CredentialGrant, error) {
	if strings.TrimSpace(v.SpaceID) == "" {
		v.SpaceID = w.spaceID
	}
	if v.ID != uuid.Nil {
		return v, nil
	}
	v.ID = inferenceNewID()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return v, nil
}

func (w *walSpaceManager) canonicalPolicy(ctx context.Context, v model.Policy) (model.Policy, error) {
	if strings.TrimSpace(v.SpaceID) == "" {
		v.SpaceID = w.spaceID
	}
	if v.ID != uuid.Nil {
		return v, nil
	}
	v.ID = inferenceNewID()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return v, nil
}

func (w *walSpaceManager) canonicalDecision(ctx context.Context, v model.PolicyDecision) (model.PolicyDecision, error) {
	if strings.TrimSpace(v.SpaceID) == "" {
		v.SpaceID = w.spaceID
	}
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.DecidedAt.IsZero() {
		v.DecidedAt = time.Now().UTC()
	}
	return v, nil
}
