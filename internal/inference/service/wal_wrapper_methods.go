package service

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	model "github.com/myceldb/mycel/internal/inference/model"
)

func inferenceNewID() uuid.UUID {
	id, err := uuid.NewV7()
	if err == nil {
		return id
	}
	return uuid.New()
}

func inferenceKey(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func (w *walGlobalManager) Init(ctx context.Context, metaDir string) error {
	return w.inner.Init(ctx, metaDir)
}
func (w *walGlobalManager) ListPackages(ctx context.Context) ([]model.InferencePackage, error) {
	return w.inner.ListPackages(ctx)
}
func (w *walGlobalManager) ListEndpoints(ctx context.Context) ([]model.Endpoint, error) {
	return w.inner.ListEndpoints(ctx)
}
func (w *walGlobalManager) ListModels(ctx context.Context) ([]model.Model, error) {
	return w.inner.ListModels(ctx)
}
func (w *walGlobalManager) ListCapabilities(ctx context.Context) ([]model.Capability, error) {
	return w.inner.ListCapabilities(ctx)
}
func (w *walGlobalManager) ListVectorStores(ctx context.Context) ([]model.VectorStore, error) {
	return w.inner.ListVectorStores(ctx)
}
func (w *walGlobalManager) ListSecrets(ctx context.Context) ([]model.Secret, error) {
	return w.inner.ListSecrets(ctx)
}
func (w *walGlobalManager) ListCredentials(ctx context.Context) ([]model.Credential, error) {
	return w.inner.ListCredentials(ctx)
}

func (w *walGlobalManager) UpsertPackage(ctx context.Context, v model.InferencePackage) (model.InferencePackage, error) {
	v, err := w.canonicalPackage(ctx, v)
	if err != nil {
		return model.InferencePackage{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "package.upsert", Payload: rawInference(v)}); err != nil {
		return model.InferencePackage{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeletePackage(ctx context.Context, id model.InferencePackageID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "package.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertEndpoint(ctx context.Context, v model.Endpoint) (model.Endpoint, error) {
	v, err := w.canonicalEndpoint(ctx, v)
	if err != nil {
		return model.Endpoint{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "endpoint.upsert", Payload: rawInference(v)}); err != nil {
		return model.Endpoint{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteEndpoint(ctx context.Context, id model.EndpointID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "endpoint.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertModel(ctx context.Context, v model.Model) (model.Model, error) {
	v, err := w.canonicalModel(ctx, v)
	if err != nil {
		return model.Model{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "model.upsert", Payload: rawInference(v)}); err != nil {
		return model.Model{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteModel(ctx context.Context, id model.ModelID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "model.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertCapability(ctx context.Context, v model.Capability) (model.Capability, error) {
	v, err := w.canonicalCapability(ctx, v)
	if err != nil {
		return model.Capability{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "capability.upsert", Payload: rawInference(v)}); err != nil {
		return model.Capability{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteCapability(ctx context.Context, id model.CapabilityID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "capability.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertVectorStore(ctx context.Context, v model.VectorStore) (model.VectorStore, error) {
	v, err := w.canonicalVectorStore(ctx, v)
	if err != nil {
		return model.VectorStore{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "vector_store.upsert", Payload: rawInference(v)}); err != nil {
		return model.VectorStore{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteVectorStore(ctx context.Context, id model.VectorStoreID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "vector_store.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertSecret(ctx context.Context, v model.Secret) (model.Secret, error) {
	v, err := w.canonicalSecret(ctx, v)
	if err != nil {
		return model.Secret{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "secret.upsert", Payload: rawInference(v)}); err != nil {
		return model.Secret{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteSecret(ctx context.Context, id model.SecretID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "secret.delete", Payload: rawInference(id)})
}
func (w *walGlobalManager) UpsertCredential(ctx context.Context, v model.Credential) (model.Credential, error) {
	v, err := w.canonicalCredential(ctx, v)
	if err != nil {
		return model.Credential{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "credential.upsert", Payload: rawInference(v)}); err != nil {
		return model.Credential{}, err
	}
	return v, nil
}
func (w *walGlobalManager) DeleteCredential(ctx context.Context, id model.CredentialID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceGlobal, inferenceMutationRecord{Kind: "credential.delete", Payload: rawInference(id)})
}

func (w *walSpaceManager) Init(ctx context.Context, location string, spaceID string) error {
	return w.inner.Init(ctx, location, spaceID)
}
func (w *walSpaceManager) ListProfiles(ctx context.Context) ([]model.Profile, error) {
	return w.inner.ListProfiles(ctx)
}
func (w *walSpaceManager) ListCredentialGrants(ctx context.Context) ([]model.CredentialGrant, error) {
	return w.inner.ListCredentialGrants(ctx)
}
func (w *walSpaceManager) ListPolicies(ctx context.Context) ([]model.Policy, error) {
	return w.inner.ListPolicies(ctx)
}
func (w *walSpaceManager) ListPolicyDecisions(ctx context.Context) ([]model.PolicyDecision, error) {
	return w.inner.ListPolicyDecisions(ctx)
}
func (w *walSpaceManager) UpsertProfile(ctx context.Context, v model.Profile) (model.Profile, error) {
	v, err := w.canonicalProfile(ctx, v)
	if err != nil {
		return model.Profile{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "profile.upsert", SpaceID: w.spaceID, Payload: rawInference(v)}); err != nil {
		return model.Profile{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeleteProfile(ctx context.Context, id model.ProfileID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "profile.delete", SpaceID: w.spaceID, Payload: rawInference(id)})
}
func (w *walSpaceManager) UpsertCredentialGrant(ctx context.Context, v model.CredentialGrant) (model.CredentialGrant, error) {
	v, err := w.canonicalGrant(ctx, v)
	if err != nil {
		return model.CredentialGrant{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "credential_grant.upsert", SpaceID: w.spaceID, Payload: rawInference(v)}); err != nil {
		return model.CredentialGrant{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeleteCredentialGrant(ctx context.Context, id model.CredentialGrantID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "credential_grant.delete", SpaceID: w.spaceID, Payload: rawInference(id)})
}
func (w *walSpaceManager) UpsertPolicy(ctx context.Context, v model.Policy) (model.Policy, error) {
	v, err := w.canonicalPolicy(ctx, v)
	if err != nil {
		return model.Policy{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "policy.upsert", SpaceID: w.spaceID, Payload: rawInference(v)}); err != nil {
		return model.Policy{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeletePolicy(ctx context.Context, id model.PolicyID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "policy.delete", SpaceID: w.spaceID, Payload: rawInference(id)})
}
func (w *walSpaceManager) UpsertPolicyDecision(ctx context.Context, v model.PolicyDecision) (model.PolicyDecision, error) {
	v, err := w.canonicalDecision(ctx, v)
	if err != nil {
		return model.PolicyDecision{}, err
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "policy_decision.upsert", SpaceID: w.spaceID, Payload: rawInference(v)}); err != nil {
		return model.PolicyDecision{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeletePolicyDecision(ctx context.Context, id model.PolicyDecisionID) error {
	return w.module.commitInferenceMutation(ctx, recordTypeInferenceSpace, inferenceMutationRecord{Kind: "policy_decision.delete", SpaceID: w.spaceID, Payload: rawInference(id)})
}

func (w *walUsageLedger) Init(ctx context.Context, location string) error {
	return w.inner.Init(ctx, location)
}
func (w *walUsageLedger) ListUsageEvents(ctx context.Context) ([]model.UsageEvent, error) {
	return w.inner.ListUsageEvents(ctx)
}
func (w *walUsageLedger) AppendUsageEvent(ctx context.Context, v model.UsageEvent) (model.UsageEvent, error) {
	if v.ID == uuid.Nil {
		v.ID = inferenceNewID()
	}
	if v.StartedAt.IsZero() {
		v.StartedAt = time.Now().UTC()
	}
	if err := w.module.commitInferenceMutation(ctx, recordTypeInferenceUsage, inferenceMutationRecord{Kind: "usage_event.append", Payload: rawInference(v)}); err != nil {
		return model.UsageEvent{}, err
	}
	return v, nil
}
