package service

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (w *walSpaceManager) Init(ctx context.Context, loc string, sid domainspace.SpaceID) error {
	return w.inner.Init(ctx, loc, sid)
}
func (w *walSpaceManager) ListSemanticRules(ctx context.Context) ([]domainsemantic.SemanticGenerationRule, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticRulesResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_rules", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Rules, nil
	}
	return w.inner.ListSemanticRules(ctx)
}

func (w *walSpaceManager) UpsertSemanticRule(ctx context.Context, v domainsemantic.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error) {
	v, err := w.canonicalSemanticRule(ctx, v)
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_rule.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.SemanticGenerationRule{}, err
	}
	return w.resolveSemanticRule(ctx, v)
}

func (w *walSpaceManager) DeleteSemanticRule(ctx context.Context, id domainsemantic.SemanticRuleID, purge bool) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_rule.delete", SpaceID: w.spaceID, Payload: raw(id), Flag: purge})
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
func (w *walSpaceManager) ListIntelligenceProfiles(ctx context.Context) ([]domainsemantic.IntelligenceProfile, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticProfilesResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_profiles", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Profiles, nil
	}
	return w.inner.ListIntelligenceProfiles(ctx)
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
func (w *walSpaceManager) ListSemanticRuleStates(ctx context.Context) ([]domainsemantic.SemanticRuleState, error) {
	return w.inner.ListSemanticRuleStates(ctx)
}
func (w *walSpaceManager) UpsertSemanticRuleState(ctx context.Context, v domainsemantic.SemanticRuleState) (domainsemantic.SemanticRuleState, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_rule_state.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.SemanticRuleState{}, err
	}
	return v, nil
}
func (w *walSpaceManager) ListSearchIndexStates(ctx context.Context) ([]domainsemantic.SemanticSearchIndexState, error) {
	return w.inner.ListSearchIndexStates(ctx)
}
func (w *walSpaceManager) UpsertSearchIndexState(ctx context.Context, v domainsemantic.SemanticSearchIndexState) (domainsemantic.SemanticSearchIndexState, error) {
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_search_index_state.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	return v, nil
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
	v, err := w.canonicalSemanticIndex(ctx, v)
	if err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	return w.resolveSemanticIndex(ctx, v)
}
func (w *walSpaceManager) DeleteSemanticIndex(ctx context.Context, id domainsemantic.SemanticIndexID, purge bool) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "semantic_index.delete", SpaceID: w.spaceID, Payload: raw(id), Flag: purge})
}
func (w *walSpaceManager) UpsertIntelligenceProfile(ctx context.Context, v domainsemantic.IntelligenceProfile) (domainsemantic.IntelligenceProfile, error) {
	v, err := w.canonicalIntelligenceProfile(ctx, v)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "intelligence_profile.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	return w.resolveIntelligenceProfile(ctx, v)
}
func (w *walSpaceManager) DeleteIntelligenceProfile(ctx context.Context, id domainsemantic.IntelligenceProfileID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "intelligence_profile.delete", SpaceID: w.spaceID, Payload: raw(id)})
}
func (w *walSpaceManager) UpsertCredentialGrant(ctx context.Context, v domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	v, err := w.canonicalGrant(ctx, v)
	if err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "credential_grant.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	return w.resolveGrant(ctx, v)
}
func (w *walSpaceManager) DeleteCredentialGrant(ctx context.Context, id domainsemantic.CredentialGrantID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "credential_grant.delete", SpaceID: w.spaceID, Payload: raw(id)})
}
func (w *walSpaceManager) UpsertInferencePolicy(ctx context.Context, v domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	v, err := w.canonicalPolicy(ctx, v)
	if err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	if err := w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "inference_policy.upsert", SpaceID: w.spaceID, Payload: raw(v)}); err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	return w.resolvePolicy(ctx, v)
}
func (w *walSpaceManager) DeleteInferencePolicy(ctx context.Context, id domainsemantic.InferencePolicyID) error {
	return w.module.commitSemanticMutation(ctx, recordTypeSemanticSpace, semanticMutationRecord{Kind: "inference_policy.delete", SpaceID: w.spaceID, Payload: raw(id)})
}
