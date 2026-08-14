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

// Credential grant RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) CreateCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceGrantServiceCreateCredentialGrantRequest) (*adminv1.AdminInferenceGrantServiceCreateCredentialGrantResponse, error) {
	principal, err := s.requireInferenceCapability(ctx, capInferenceGrantManage, inferenceScope(req.GetSpaceId(), ""))
	if err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	credentialID, err := s.resolveCredentialID(ctx, firstNonEmptyAdmin(req.GetCredentialId(), req.GetCredential()))
	if err != nil {
		return nil, err
	}
	scope, err := processingScopeFromProto(req.GetScope(), spaceID)
	if err != nil {
		return nil, err
	}
	var endpointID *domainsemantic.ModelEndpointID
	if ref := firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()); strings.TrimSpace(ref) != "" {
		id, err := s.resolveModelEndpointID(ctx, ref)
		if err != nil {
			return nil, err
		}
		endpointID = &id
	}
	var modelID *domainsemantic.InferenceModelID
	if ref := firstNonEmptyAdmin(req.GetModelId(), req.GetModel()); strings.TrimSpace(ref) != "" {
		id, err := s.resolveModelID(ctx, ref)
		if err != nil {
			return nil, err
		}
		modelID = &id
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
	grant, err := spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: credentialID, Scope: scope, Operations: operationsFromStringsAdmin(req.GetOperations()), ModelEndpointID: endpointID, ModelID: modelID, Priority: int(req.GetPriority()), IsDefault: req.GetIsDefault(), AllowBackgroundUse: req.GetAllowBackgroundUse(), GranteePrincipalIDs: cleanStringListAdmin(req.GetGranteePrincipalIds()), AllowOnBehalfOfPrincipalIDs: cleanStringListAdmin(req.GetAllowOnBehalfOfPrincipalIds()), GrantedBy: principal.PrincipalID, ExpiresAt: timeFromProto(req.GetExpiresAt())})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert credential grant")
	}
	if err := s.syncInferenceCredentialGrant(ctx, spaceID.String(), grant); err != nil {
		return nil, mapAdminInferenceError(err, "sync inference credential grant")
	}
	return &adminv1.AdminInferenceGrantServiceCreateCredentialGrantResponse{CredentialGrant: mapCredentialGrant(grant)}, nil
}

func (s *AdminInferenceService) ListCredentialGrants(ctx context.Context, req *adminv1.AdminInferenceGrantServiceListCredentialGrantsRequest) (*adminv1.AdminInferenceGrantServiceListCredentialGrantsResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceGrantManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
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
	items, err := spaceMgr.ListCredentialGrants(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credential grants")
	}
	if req.CredentialId != nil {
		id, err := parseSemanticUUID[domainsemantic.InferenceCredentialID](req.GetCredentialId(), "credential_id")
		if err != nil {
			return nil, err
		}
		items = filterGrantsByCredential(items, id)
	}
	if !req.GetIncludeExpired() {
		items = filterGrantsUnexpired(items, time.Now())
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceGrantServiceListCredentialGrantsResponse{CredentialGrants: mapCredentialGrants(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ExpireCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceGrantServiceExpireCredentialGrantRequest) (*adminv1.AdminInferenceGrantServiceExpireCredentialGrantResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceGrantManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	grantID, err := parseSemanticUUID[domainsemantic.CredentialGrantID](req.GetCredentialGrantId(), "credential_grant_id")
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
	items, err := spaceMgr.ListCredentialGrants(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credential grants")
	}
	for _, item := range items {
		if item.ID == grantID {
			now := time.Now().UTC()
			item.ExpiresAt = &now
			stored, err := spaceMgr.UpsertCredentialGrant(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "expire credential grant")
			}
			if err := s.syncInferenceCredentialGrant(ctx, spaceID.String(), stored); err != nil {
				return nil, mapAdminInferenceError(err, "sync inference credential grant")
			}
			return &adminv1.AdminInferenceGrantServiceExpireCredentialGrantResponse{CredentialGrant: mapCredentialGrant(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "credential grant not found")
}

func (s *AdminInferenceService) DeleteCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceGrantServiceDeleteCredentialGrantRequest) (*adminv1.AdminInferenceGrantServiceDeleteCredentialGrantResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceGrantManage, inferenceScope(req.GetSpaceId(), "")); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	grantID, err := parseSemanticUUID[domainsemantic.CredentialGrantID](req.GetCredentialGrantId(), "credential_grant_id")
	if err != nil {
		return nil, err
	}
	if refs, err := s.credentialGrantVectorReferences(ctx, grantID); err != nil {
		return nil, err
	} else if len(refs) > 0 {
		return nil, referencedPrecondition("credential grant", refs)
	}
	if refs, err := s.standaloneDecisionReferences(ctx, spaceID.String(), func(decision domaininference.PolicyDecision) bool { return decision.CredentialGrantID == grantID }); err != nil {
		return nil, mapAdminInferenceError(err, "list standalone inference policy decisions")
	} else if len(refs) > 0 {
		return nil, referencedPrecondition("credential grant", refs)
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
	if err := spaceMgr.DeleteCredentialGrant(ctx, grantID); err != nil {
		return nil, mapAdminInferenceError(err, "delete credential grant")
	}
	if s.inference != nil {
		inferenceSpace, err := s.inference.SpaceManager(ctx, spaceID.String())
		if err != nil {
			return nil, mapAdminInferenceError(err, "open inference space manager")
		}
		if err := inferenceSpace.DeleteCredentialGrant(ctx, grantID); err != nil {
			return nil, mapAdminInferenceError(err, "delete inference credential grant")
		}
	}
	return &adminv1.AdminInferenceGrantServiceDeleteCredentialGrantResponse{CredentialGrantId: grantID.String()}, nil
}
