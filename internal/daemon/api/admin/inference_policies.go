package admin

import (
	"context"
	"sort"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Inference policy RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) CreateAccessPolicy(ctx context.Context, req *adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyRequest) (*adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse, error) {
	principal, err := s.requireInferenceCapability(ctx, capAccessPolicyManage, inferenceScope(req.GetSpaceId(), ""))
	if err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	scope, err := processingScopeFromProto(req.GetScope(), spaceID)
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	policy, err := spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{Scope: scope, Effect: domainsemantic.PolicyEffect(req.GetEffect()), Operations: operationsFromStringsAdmin(req.GetOperations()), NoInference: req.GetNoIntelligence(), AllowedPrivacyClasses: privacyClassesFromStringsAdmin(req.GetAllowedPrivacyClasses()), DisallowThirdParty: req.GetDisallowThirdParty(), RequireLocalEndpoint: req.GetRequireLocalEndpoint(), Reason: req.GetReason(), CreatedBy: principal.PrincipalID, ExpiresAt: timeFromProto(req.GetExpiresAt())})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert access policy")
	}
	if err := s.syncAccessPolicy(ctx, spaceID.String(), policy); err != nil {
		return nil, mapAdminInferenceError(err, "sync access policy")
	}
	return &adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse{AccessPolicy: mapAccessPolicy(policy)}, nil
}

func (s *AdminInferenceService) ListAccessPolicies(ctx context.Context, req *adminv1.AdminIntelligenceAccessPolicyServiceListAccessPoliciesRequest) (*adminv1.AdminIntelligenceAccessPolicyServiceListAccessPoliciesResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capAccessPolicyManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	items, err := spaceMgr.ListInferencePolicies(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list access policies")
	}
	if strings.TrimSpace(req.GetEffect()) != "" {
		items = filterPoliciesByEffect(items, domainsemantic.PolicyEffect(req.GetEffect()))
	}
	if !req.GetIncludeExpired() {
		items = filterPoliciesUnexpired(items, time.Now())
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminIntelligenceAccessPolicyServiceListAccessPoliciesResponse{AccessPolicies: mapAccessPolicies(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ExpireAccessPolicy(ctx context.Context, req *adminv1.AdminIntelligenceAccessPolicyServiceExpireAccessPolicyRequest) (*adminv1.AdminIntelligenceAccessPolicyServiceExpireAccessPolicyResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capAccessPolicyManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	policyID, err := parseSemanticUUID[domainsemantic.InferencePolicyID](req.GetAccessPolicyId(), "inference_policy_id")
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	items, err := spaceMgr.ListInferencePolicies(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list access policies")
	}
	for _, item := range items {
		if item.ID == policyID {
			now := time.Now().UTC()
			item.ExpiresAt = &now
			stored, err := spaceMgr.UpsertInferencePolicy(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "expire access policy")
			}
			if err := s.syncAccessPolicy(ctx, spaceID.String(), stored); err != nil {
				return nil, mapAdminInferenceError(err, "sync access policy")
			}
			return &adminv1.AdminIntelligenceAccessPolicyServiceExpireAccessPolicyResponse{AccessPolicy: mapAccessPolicy(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "access policy not found")
}

func (s *AdminInferenceService) DeleteAccessPolicy(ctx context.Context, req *adminv1.AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyRequest) (*adminv1.AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capAccessPolicyManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	policyID, err := parseSemanticUUID[domainsemantic.InferencePolicyID](req.GetAccessPolicyId(), "inference_policy_id")
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	decisions, err := spaceMgr.ListPolicyDecisions(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list policy decisions")
	}
	refs := []string{}
	for _, decision := range decisions {
		for _, matched := range decision.MatchedPolicyIDs {
			if matched == policyID {
				refs = append(refs, "policy_decision:"+decision.ID.String())
			}
		}
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("access policy", refs)
	}
	standaloneRefs, err := s.standaloneDecisionReferences(ctx, spaceID.String(), func(decision domaininference.PolicyDecision) bool {
		for _, matched := range decision.MatchedPolicyIDs {
			if matched == policyID.String() {
				return true
			}
		}
		return false
	})
	if err != nil {
		return nil, mapAdminInferenceError(err, "list standalone access policy decisions")
	}
	if len(standaloneRefs) > 0 {
		return nil, referencedPrecondition("access policy", standaloneRefs)
	}
	if err := spaceMgr.DeleteInferencePolicy(ctx, policyID); err != nil {
		return nil, mapAdminInferenceError(err, "delete access policy")
	}
	if s.inference != nil {
		inferenceSpace, err := s.inference.SpaceManager(ctx, spaceID.String())
		if err != nil {
			return nil, mapAdminInferenceError(err, "open inference space manager")
		}
		if err := inferenceSpace.DeletePolicy(ctx, policyID); err != nil {
			return nil, mapAdminInferenceError(err, "delete standalone access policy")
		}
	}
	return &adminv1.AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyResponse{AccessPolicyId: policyID.String()}, nil
}

func (s *AdminInferenceService) GetPolicyDecision(ctx context.Context, req *adminv1.AdminIntelligenceAccessPolicyServiceGetPolicyDecisionRequest) (*adminv1.AdminIntelligenceAccessPolicyServiceGetPolicyDecisionResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceAuditRead, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	decisionID, err := parseSemanticUUID[domainsemantic.PolicyDecisionID](req.GetPolicyDecisionId(), "policy_decision_id")
	if err != nil {
		return nil, err
	}
	spaceMgr, err := s.inference.SpaceManager(ctx, spaceID.String())
	if err != nil {
		return nil, mapAdminInferenceError(err, "open inference space manager")
	}
	items, err := spaceMgr.ListPolicyDecisions(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list policy decisions")
	}
	for _, item := range items {
		if item.ID == decisionID {
			return &adminv1.AdminIntelligenceAccessPolicyServiceGetPolicyDecisionResponse{PolicyDecision: mapStandalonePolicyDecision(item)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "policy decision not found")
}
