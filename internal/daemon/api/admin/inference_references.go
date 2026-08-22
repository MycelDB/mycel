package admin

import (
	"context"
	"fmt"
	"strings"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reference-safety helpers for AdminInferenceService lifecycle operations.

func (s *AdminInferenceService) modelEndpointReferences(ctx context.Context, id domainsemantic.ModelEndpointID) ([]string, error) {
	refs := []string{}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.EndpointID == id })
	if err != nil {
		return nil, err
	}
	refs = append(refs, usageRefs...)
	global := s.semantic.GlobalManager()
	caps, err := global.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == id {
			refs = append(refs, "capability:"+cap.ID.String())
		}
	}
	credentials, err := global.ListCredentials(ctx)
	if err != nil {
		return nil, err
	}
	for _, credential := range credentials {
		if credential.ModelEndpointID == id {
			refs = append(refs, "credential:"+credential.ID.String())
		}
	}
	standaloneGlobalRefs, err := s.standaloneGlobalReferences(ctx, func(capability domaininference.Capability, credential domaininference.Credential) []string {
		out := []string{}
		if capability.EndpointID == id {
			out = append(out, "capability:"+capability.ID.String())
		}
		if credential.EndpointID == id {
			out = append(out, "credential:"+credential.ID.String())
		}
		return out
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, standaloneGlobalRefs...)
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelEndpointID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.ModelEndpointID != nil && *grant.ModelEndpointID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
		decisions, err := space.Manager.ListPolicyDecisions(ctx)
		if err != nil {
			return nil, err
		}
		for _, decision := range decisions {
			if decision.ModelEndpointID == id {
				refs = append(refs, "policy_decision:"+decision.ID.String())
			}
		}
		standaloneRefs, err := s.standaloneSpaceReferences(ctx, space.SpaceID.String(), func(profile domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
			out := []string{}
			idText := id.String()
			if stringSliceContains(profile.EndpointRefs, idText) || stringSliceContains(grant.EndpointRefs, idText) {
				if stringSliceContains(profile.EndpointRefs, idText) {
					out = append(out, "intelligence_profile:"+profile.ID.String())
				}
				if stringSliceContains(grant.EndpointRefs, idText) {
					out = append(out, "credential_grant:"+grant.ID.String())
				}
			}
			if decision.EndpointID == id {
				out = append(out, "policy_decision:"+decision.ID.String())
			}
			return out
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, standaloneRefs...)
	}
	return refs, nil
}

func (s *AdminInferenceService) modelReferences(ctx context.Context, id domainsemantic.InferenceModelID) ([]string, error) {
	refs := []string{}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.ModelID == id })
	if err != nil {
		return nil, err
	}
	refs = append(refs, usageRefs...)
	caps, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, cap := range caps {
		if cap.ModelID == id {
			refs = append(refs, "capability:"+cap.ID.String())
		}
	}
	standaloneGlobalRefs, err := s.standaloneGlobalReferences(ctx, func(capability domaininference.Capability, credential domaininference.Credential) []string {
		if capability.ModelID == id {
			return []string{"capability:" + capability.ID.String()}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, standaloneGlobalRefs...)
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.ModelID != nil && *grant.ModelID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
		decisions, err := space.Manager.ListPolicyDecisions(ctx)
		if err != nil {
			return nil, err
		}
		for _, decision := range decisions {
			if decision.ModelID == id {
				refs = append(refs, "policy_decision:"+decision.ID.String())
			}
		}
		standaloneRefs, err := s.standaloneSpaceReferences(ctx, space.SpaceID.String(), func(profile domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
			out := []string{}
			idText := id.String()
			if stringSliceContains(profile.ModelRefs, idText) {
				out = append(out, "intelligence_profile:"+profile.ID.String())
			}
			if stringSliceContains(grant.ModelRefs, idText) {
				out = append(out, "credential_grant:"+grant.ID.String())
			}
			if decision.ModelID == id {
				out = append(out, "policy_decision:"+decision.ID.String())
			}
			return out
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, standaloneRefs...)
	}
	return refs, nil
}

func (s *AdminInferenceService) vectorStoreReferences(ctx context.Context, id domainsemantic.VectorStoreID) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.VectorStoreID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) capabilityReferences(ctx context.Context, id domainsemantic.ModelEndpointCapabilityID) ([]string, error) {
	refs := []string{}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.CapabilityID == id })
	if err != nil {
		return nil, err
	}
	refs = append(refs, usageRefs...)
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if index.ModelEndpointCapabilityID == id {
				refs = append(refs, "semantic_index:"+index.ID.String())
			}
		}
		standaloneRefs, err := s.standaloneSpaceReferences(ctx, space.SpaceID.String(), func(profile domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
			out := []string{}
			idText := id.String()
			if stringSliceContains(profile.CapabilityRefs, idText) {
				out = append(out, "intelligence_profile:"+profile.ID.String())
			}
			if stringSliceContains(grant.CapabilityRefs, idText) {
				out = append(out, "credential_grant:"+grant.ID.String())
			}
			if decision.CapabilityID == id {
				out = append(out, "policy_decision:"+decision.ID.String())
			}
			return out
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, standaloneRefs...)
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialGrantReferences(ctx context.Context, id domainsemantic.InferenceCredentialID) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, err
	}
	for _, space := range spaces {
		grants, err := space.Manager.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.CredentialID == id {
				refs = append(refs, "credential_grant:"+grant.ID.String())
			}
		}
		standaloneRefs, err := s.standaloneSpaceReferences(ctx, space.SpaceID.String(), func(profile domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
			if grant.CredentialID == id {
				return []string{"credential_grant:" + grant.ID.String()}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, standaloneRefs...)
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialVectorReferences(ctx context.Context, id domainsemantic.InferenceCredentialID) ([]string, error) {
	refs, err := s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialID == id })
	if err != nil {
		return nil, err
	}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.CredentialID == id })
	if err != nil {
		return nil, err
	}
	return append(refs, usageRefs...), nil
}

func (s *AdminInferenceService) credentialGrantVectorReferences(ctx context.Context, id domainsemantic.CredentialGrantID) ([]string, error) {
	refs, err := s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialGrantID == id })
	if err != nil {
		return nil, err
	}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.CredentialGrantID == id })
	if err != nil {
		return nil, err
	}
	return append(refs, usageRefs...), nil
}

func (s *AdminInferenceService) profileReferences(ctx context.Context, profile domaininference.Profile) ([]string, error) {
	refs := []string{}
	idText := profile.ID.String()
	key := strings.TrimSpace(profile.Key)
	if s.semantic != nil {
		spaces, err := s.semantic.ListSpaceManagers(ctx)
		if err != nil {
			return nil, err
		}
		for _, space := range spaces {
			indexes, err := space.Manager.ListSemanticIndexes(ctx)
			if err != nil {
				return nil, err
			}
			for _, index := range indexes {
				if metadataMatchesAny(index.Metadata, []string{"intelligence_profile_id", "intelligence_profile", "intelligence_profile_key", "embedding_profile"}, idText, key) {
					refs = append(refs, "semantic_index:"+index.ID.String())
				}
			}
		}
	}
	usageRefs, err := s.standaloneUsageReferences(ctx, func(event domaininference.UsageEvent) bool { return event.ProfileID == profile.ID })
	if err != nil {
		return nil, err
	}
	refs = append(refs, usageRefs...)
	spaceRefs, err := s.standaloneSpaceReferences(ctx, profile.SpaceID, func(existing domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
		out := []string{}
		if stringSliceContains(grant.ProfileRefs, idText) || (key != "" && stringSliceContains(grant.ProfileRefs, key)) {
			out = append(out, "credential_grant:"+grant.ID.String())
		}
		if stringSliceContains(policy.ProfileRefs, idText) || (key != "" && stringSliceContains(policy.ProfileRefs, key)) {
			out = append(out, "inference_policy:"+policy.ID.String())
		}
		if decision.ProfileID == profile.ID {
			out = append(out, "policy_decision:"+decision.ID.String())
		}
		return out
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, spaceRefs...)
	return refs, nil
}

func (s *AdminInferenceService) vectorRecordReferences(ctx context.Context, match func(domainsemantic.AdvancedEmbeddingRecord) bool) ([]string, error) {
	refs := []string{}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list semantic spaces")
	}
	for _, space := range spaces {
		indexes, err := space.Manager.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, mapAdminInferenceError(err, "list semantic indexes")
		}
		for _, index := range indexes {
			records, err := s.semantic.ListVectorRecords(ctx, space.SpaceID, index.ID)
			if err != nil {
				return nil, mapAdminInferenceError(err, "list vector records")
			}
			for _, rec := range records {
				if !rec.Tombstone && match(rec) {
					refs = append(refs, "vector_record:"+rec.ID.String()+":index:"+index.ID.String())
				}
			}
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) secretCredentialReferences(ctx context.Context, id domainsemantic.SecretID, excluding domainsemantic.InferenceCredentialID) ([]string, error) {
	refs := []string{}
	credentials, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credentials")
	}
	for _, credential := range credentials {
		if credential.ID != excluding && credential.SecretRef == id {
			refs = append(refs, "credential:"+credential.ID.String())
		}
	}
	standaloneRefs, err := s.standaloneGlobalReferences(ctx, func(capability domaininference.Capability, credential domaininference.Credential) []string {
		if credential.ID != excluding && credential.SecretID == id {
			return []string{"credential:" + credential.ID.String()}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return append(refs, standaloneRefs...), nil
}

func (s *AdminInferenceService) credentialByID(ctx context.Context, id domainsemantic.InferenceCredentialID) (domainsemantic.InferenceCredential, error) {
	credentials, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, mapAdminInferenceError(err, "list credentials")
	}
	for _, credential := range credentials {
		if credential.ID == id {
			return credential, nil
		}
	}
	return domainsemantic.InferenceCredential{}, status.Error(codes.NotFound, "credential not found")
}

func referencedPrecondition(resource string, refs []string) error {
	if len(refs) > 20 {
		refs = append(append([]string{}, refs[:20]...), fmt.Sprintf("...%d more", len(refs)-20))
	}
	return status.Errorf(codes.FailedPrecondition, "%s is referenced by %s", resource, strings.Join(refs, ", "))
}

func (s *AdminInferenceService) standaloneDecisionReferences(ctx context.Context, spaceID string, match func(domaininference.PolicyDecision) bool) ([]string, error) {
	return s.standaloneSpaceReferences(ctx, spaceID, func(profile domaininference.Profile, grant domaininference.CredentialGrant, policy domaininference.Policy, decision domaininference.PolicyDecision) []string {
		if match(decision) {
			return []string{"policy_decision:" + decision.ID.String()}
		}
		return nil
	})
}

func (s *AdminInferenceService) standaloneUsageReferences(ctx context.Context, match func(domaininference.UsageEvent) bool) ([]string, error) {
	if s.inference == nil || s.inference.UsageLedger() == nil {
		return nil, nil
	}
	events, err := s.inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference usage")
	}
	refs := []string{}
	for _, event := range events {
		if match(event) {
			refs = append(refs, "usage_event:"+event.ID.String())
		}
	}
	return refs, nil
}

func (s *AdminInferenceService) standaloneGlobalReferences(ctx context.Context, match func(domaininference.Capability, domaininference.Credential) []string) ([]string, error) {
	if s.inference == nil || s.inference.GlobalManager() == nil {
		return nil, nil
	}
	global := s.inference.GlobalManager()
	capabilities, err := global.ListCapabilities(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference capabilities")
	}
	credentials, err := global.ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference credentials")
	}
	refs := []string{}
	for _, capability := range capabilities {
		refs = append(refs, match(capability, domaininference.Credential{})...)
	}
	for _, credential := range credentials {
		refs = append(refs, match(domaininference.Capability{}, credential)...)
	}
	return refs, nil
}

func (s *AdminInferenceService) standaloneSpaceReferences(ctx context.Context, spaceID string, match func(domaininference.Profile, domaininference.CredentialGrant, domaininference.Policy, domaininference.PolicyDecision) []string) ([]string, error) {
	if s.inference == nil || strings.TrimSpace(spaceID) == "" {
		return nil, nil
	}
	mgr, err := s.inference.SpaceManager(ctx, strings.TrimSpace(spaceID))
	if err != nil {
		return nil, mapAdminInferenceError(err, "open standalone inference space")
	}
	profiles, err := mgr.ListProfiles(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference profiles")
	}
	grants, err := mgr.ListCredentialGrants(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference grants")
	}
	policies, err := mgr.ListPolicies(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone access policies")
	}
	decisions, err := mgr.ListPolicyDecisions(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference decisions")
	}
	refs := []string{}
	for _, profile := range profiles {
		refs = append(refs, match(profile, domaininference.CredentialGrant{}, domaininference.Policy{}, domaininference.PolicyDecision{})...)
	}
	for _, grant := range grants {
		refs = append(refs, match(domaininference.Profile{}, grant, domaininference.Policy{}, domaininference.PolicyDecision{})...)
	}
	for _, policy := range policies {
		refs = append(refs, match(domaininference.Profile{}, domaininference.CredentialGrant{}, policy, domaininference.PolicyDecision{})...)
	}
	for _, decision := range decisions {
		refs = append(refs, match(domaininference.Profile{}, domaininference.CredentialGrant{}, domaininference.Policy{}, decision)...)
	}
	return refs, nil
}

func stringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func metadataMatchesAny(metadata map[string]any, keys []string, values ...string) bool {
	if len(metadata) == 0 {
		return false
	}
	for _, key := range keys {
		raw, ok := metadata[key]
		if !ok {
			continue
		}
		text, ok := raw.(string)
		if !ok {
			continue
		}
		for _, value := range values {
			if strings.TrimSpace(text) != "" && strings.TrimSpace(text) == strings.TrimSpace(value) {
				return true
			}
		}
	}
	return false
}
