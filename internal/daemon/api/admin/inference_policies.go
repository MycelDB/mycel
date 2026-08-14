package admin

import (
	"context"
	"sort"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Inference policy RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) CreateInferencePolicy(ctx context.Context, req *adminv1.AdminInferencePolicyServiceCreateInferencePolicyRequest) (*adminv1.AdminInferencePolicyServiceCreateInferencePolicyResponse, error) {
	principal, err := s.requireInferenceManage(ctx)
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
	policy, err := spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{Scope: scope, Effect: domainsemantic.PolicyEffect(req.GetEffect()), Operations: operationsFromStringsAdmin(req.GetOperations()), NoInference: req.GetNoInference(), AllowedPrivacyClasses: privacyClassesFromStringsAdmin(req.GetAllowedPrivacyClasses()), DisallowThirdParty: req.GetDisallowThirdParty(), RequireLocalEndpoint: req.GetRequireLocalEndpoint(), Reason: req.GetReason(), CreatedBy: principal.PrincipalID, ExpiresAt: timeFromProto(req.GetExpiresAt())})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert inference policy")
	}
	return &adminv1.AdminInferencePolicyServiceCreateInferencePolicyResponse{InferencePolicy: mapInferencePolicy(policy)}, nil
}

func (s *AdminInferenceService) ListInferencePolicies(ctx context.Context, req *adminv1.AdminInferencePolicyServiceListInferencePoliciesRequest) (*adminv1.AdminInferencePolicyServiceListInferencePoliciesResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
		return nil, mapAdminInferenceError(err, "list inference policies")
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
	return &adminv1.AdminInferencePolicyServiceListInferencePoliciesResponse{InferencePolicies: mapInferencePolicies(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ExpireInferencePolicy(ctx context.Context, req *adminv1.AdminInferencePolicyServiceExpireInferencePolicyRequest) (*adminv1.AdminInferencePolicyServiceExpireInferencePolicyResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	policyID, err := parseSemanticUUID[domainsemantic.InferencePolicyID](req.GetInferencePolicyId(), "inference_policy_id")
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
		return nil, mapAdminInferenceError(err, "list inference policies")
	}
	for _, item := range items {
		if item.ID == policyID {
			now := time.Now().UTC()
			item.ExpiresAt = &now
			stored, err := spaceMgr.UpsertInferencePolicy(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "expire inference policy")
			}
			return &adminv1.AdminInferencePolicyServiceExpireInferencePolicyResponse{InferencePolicy: mapInferencePolicy(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "inference policy not found")
}

func (s *AdminInferenceService) DeleteInferencePolicy(ctx context.Context, req *adminv1.AdminInferencePolicyServiceDeleteInferencePolicyRequest) (*adminv1.AdminInferencePolicyServiceDeleteInferencePolicyResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	policyID, err := parseSemanticUUID[domainsemantic.InferencePolicyID](req.GetInferencePolicyId(), "inference_policy_id")
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
		return nil, referencedPrecondition("inference policy", refs)
	}
	if err := spaceMgr.DeleteInferencePolicy(ctx, policyID); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference policy")
	}
	return &adminv1.AdminInferencePolicyServiceDeleteInferencePolicyResponse{InferencePolicyId: policyID.String()}, nil
}
