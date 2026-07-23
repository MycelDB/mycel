package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (w *walSpaceManager) Init(ctx context.Context, loc string, sid domainspace.SpaceID) error {
	return w.inner.Init(ctx, loc, sid)
}
func (w *walSpaceManager) ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticIndexesResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_indexes", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Indexes, nil
	}
	return w.inner.ListSemanticIndexes(ctx)
}
func (w *walSpaceManager) ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticGrantsResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_grants", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Grants, nil
	}
	return w.inner.ListCredentialGrants(ctx)
}
func (w *walSpaceManager) ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticPoliciesResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_policies", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Policies, nil
	}
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
	if v.ID == uuid.Nil {
		v.ID = uuid.New()
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
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
