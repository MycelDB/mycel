package semantic

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

func (w *walGlobalManager) Init(ctx context.Context, metaDir string) error {
	return w.inner.Init(ctx, metaDir)
}
func (w *walGlobalManager) EnsureDefaultVectorStore(ctx context.Context) (domainsemantic.VectorStoreBackend, error) {
	return w.inner.EnsureDefaultVectorStore(ctx)
}
func (w *walGlobalManager) ListPackages(ctx context.Context) ([]domainsemantic.InferencePackage, error) {
	return w.inner.ListPackages(ctx)
}
func (w *walGlobalManager) ListModelEndpoints(ctx context.Context) ([]domainsemantic.ModelEndpoint, error) {
	return w.inner.ListModelEndpoints(ctx)
}
func (w *walGlobalManager) ListModels(ctx context.Context) ([]domainsemantic.InferenceModel, error) {
	return w.inner.ListModels(ctx)
}
func (w *walGlobalManager) ListModelEndpointCapabilities(ctx context.Context) ([]domainsemantic.ModelEndpointCapability, error) {
	return w.inner.ListModelEndpointCapabilities(ctx)
}
func (w *walGlobalManager) ListVectorStores(ctx context.Context) ([]domainsemantic.VectorStoreBackend, error) {
	return w.inner.ListVectorStores(ctx)
}
func (w *walGlobalManager) ListSecrets(ctx context.Context) ([]domainsemantic.Secret, error) {
	return w.inner.ListSecrets(ctx)
}
func (w *walGlobalManager) ListCredentials(ctx context.Context) ([]domainsemantic.InferenceCredential, error) {
	return w.inner.ListCredentials(ctx)
}
func (w *walGlobalManager) UpsertPackage(ctx context.Context, v domainsemantic.InferencePackage) (domainsemantic.InferencePackage, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "package.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.InferencePackage{}, err
	}
	return v, nil
}
func (w *walGlobalManager) UpsertModelEndpoint(ctx context.Context, v domainsemantic.ModelEndpoint) (domainsemantic.ModelEndpoint, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "endpoint.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteModelEndpoint(ctx context.Context, v domainsemantic.ModelEndpointID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "endpoint.delete", Payload: raw(v)})
}
func (w *walGlobalManager) UpsertModel(ctx context.Context, v domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "model.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteModel(ctx context.Context, v domainsemantic.InferenceModelID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "model.delete", Payload: raw(v)})
}
func (w *walGlobalManager) UpsertModelEndpointCapability(ctx context.Context, v domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "capability.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteModelEndpointCapability(ctx context.Context, v domainsemantic.ModelEndpointCapabilityID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "capability.delete", Payload: raw(v)})
}
func (w *walGlobalManager) UpsertVectorStore(ctx context.Context, v domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteVectorStore(ctx context.Context, v domainsemantic.VectorStoreID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "vector_store.delete", Payload: raw(v)})
}
func (w *walGlobalManager) UpsertSecret(ctx context.Context, v domainsemantic.Secret) (domainsemantic.Secret, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "secret.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.Secret{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteSecret(ctx context.Context, v domainsemantic.SecretID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "secret.delete", Payload: raw(v)})
}
func (w *walGlobalManager) UpsertCredential(ctx context.Context, v domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "credential.upsert", Payload: raw(v)}); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteCredential(ctx context.Context, v domainsemantic.InferenceCredentialID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticGlobal, semanticMutationRecord{Kind: "credential.delete", Payload: raw(v)})
}
