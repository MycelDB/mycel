package service

import (
	"context"
	"strings"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func (w *walGlobalManager) resolvePackage(ctx context.Context, v domainsemantic.InferencePackage) (domainsemantic.InferencePackage, error) {
	items, err := w.inner.ListPackages(ctx)
	if err != nil {
		return domainsemantic.InferencePackage{}, err
	}
	name := semanticKey(v.Name)
	version := strings.TrimSpace(v.Version)
	for _, item := range items {
		if semanticKey(item.Name) == name && strings.TrimSpace(item.Version) == version {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveEndpoint(ctx context.Context, v domainsemantic.ModelEndpoint) (domainsemantic.ModelEndpoint, error) {
	items, err := w.inner.ListModelEndpoints(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveModel(ctx context.Context, v domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error) {
	items, err := w.inner.ListModels(ctx)
	if err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveCapability(ctx context.Context, v domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error) {
	items, err := w.inner.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	for _, item := range items {
		if item.ModelEndpointID == v.ModelEndpointID && item.ModelID == v.ModelID && item.Operation == v.Operation {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveVectorStore(ctx context.Context, v domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error) {
	items, err := w.inner.ListVectorStores(ctx)
	if err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveSecret(ctx context.Context, v domainsemantic.Secret) (domainsemantic.Secret, error) {
	if v.ID == uuid.Nil {
		return v, nil
	}
	items, err := w.inner.ListSecrets(ctx)
	if err != nil {
		return domainsemantic.Secret{}, err
	}
	for _, item := range items {
		if item.ID == v.ID {
			return item, nil
		}
	}
	return v, nil
}

func (w *walGlobalManager) resolveCredential(ctx context.Context, v domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error) {
	items, err := w.inner.ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walSpaceManager) resolveSemanticRule(ctx context.Context, v domainsemantic.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error) {
	items, err := w.inner.ListSemanticRules(ctx)
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if item.SpaceID == v.SpaceID && item.DomainID == v.DomainID && semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walSpaceManager) resolveSemanticIndex(ctx context.Context, v domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) {
	items, err := w.inner.ListSemanticIndexes(ctx)
	if err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if item.SpaceID == v.SpaceID && item.DomainID == v.DomainID && semanticKey(item.Key) == key {
			return item, nil
		}
	}
	return v, nil
}

func (w *walSpaceManager) resolveIntelligenceProfile(ctx context.Context, v domainsemantic.IntelligenceProfile) (domainsemantic.IntelligenceProfile, error) {
	items, err := w.inner.ListIntelligenceProfiles(ctx)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	key := semanticKey(v.Key)
	for _, item := range items {
		if item.ID == v.ID || (item.SpaceID == v.SpaceID && semanticKey(item.Key) == key) {
			return item, nil
		}
	}
	return v, nil
}

func (w *walSpaceManager) resolveGrant(ctx context.Context, v domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	items, err := w.inner.ListCredentialGrants(ctx)
	if err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	for _, item := range items {
		if item.ID == v.ID {
			return item, nil
		}
	}
	return v, nil
}

func (w *walSpaceManager) resolvePolicy(ctx context.Context, v domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	items, err := w.inner.ListInferencePolicies(ctx)
	if err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	for _, item := range items {
		if item.ID == v.ID {
			return item, nil
		}
	}
	return v, nil
}
