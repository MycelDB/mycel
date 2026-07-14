package semantic

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (w *walSpaceManager) Init(ctx context.Context, loc string, sid domainspace.SpaceID) error {
	return w.inner.Init(ctx, loc, sid)
}
func (w *walSpaceManager) ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error) {
	return w.inner.ListSemanticIndexes(ctx)
}
func (w *walSpaceManager) ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error) {
	return w.inner.ListCredentialGrants(ctx)
}
func (w *walSpaceManager) ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error) {
	return w.inner.ListInferencePolicies(ctx)
}
func (w *walSpaceManager) ListIndexStates(ctx context.Context) ([]domainsemantic.SemanticIndexState, error) {
	return w.inner.ListIndexStates(ctx)
}
func (w *walSpaceManager) ListPolicyDecisions(ctx context.Context) ([]domainsemantic.PolicyDecision, error) {
	return w.inner.ListPolicyDecisions(ctx)
}
func (w *walSpaceManager) UpsertIndexState(ctx context.Context, v domainsemantic.SemanticIndexState) (domainsemantic.SemanticIndexState, error) {
	return w.inner.UpsertIndexState(ctx, v)
}
func (w *walSpaceManager) UpsertPolicyDecision(ctx context.Context, v domainsemantic.PolicyDecision) (domainsemantic.PolicyDecision, error) {
	return w.inner.UpsertPolicyDecision(ctx, v)
}
func (w *walSpaceManager) UpsertSemanticIndex(ctx context.Context, v domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeleteSemanticIndex(ctx context.Context, id domainsemantic.SemanticIndexID, purge bool) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_index.delete", SpaceID: w.spaceID, Payload: raw(id), Flag: purge})
}
func (w *walSpaceManager) UpsertCredentialGrant(ctx context.Context, v domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "credential_grant.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeleteCredentialGrant(ctx context.Context, id domainsemantic.CredentialGrantID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "credential_grant.delete", SpaceID: w.spaceID, Payload: raw(id)})
}
func (w *walSpaceManager) UpsertInferencePolicy(ctx context.Context, v domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "inference_policy.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	return v, nil
}
func (w *walSpaceManager) DeleteInferencePolicy(ctx context.Context, id domainsemantic.InferencePolicyID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "inference_policy.delete", SpaceID: w.spaceID, Payload: raw(id)})
}
