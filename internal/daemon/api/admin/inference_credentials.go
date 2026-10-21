package admin

import (
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Credential and secret RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) CreateCredential(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialRequest) (*adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceCredentialManage, inferenceScope("", "")); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	endpointID, err := s.resolveModelEndpointID(ctx, firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()))
	if err != nil {
		return nil, err
	}
	ownerType := firstNonEmptyAdmin(req.GetOwnerType(), string(domainsemantic.CredentialOwnerUser))
	ownerID := strings.TrimSpace(req.GetOwnerId())
	if ownerID == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}
	authType := domainsemantic.AuthMode(firstNonEmptyAdmin(req.GetAuthType(), string(domainsemantic.AuthModeAPIKey)))
	if authType != domainsemantic.AuthModeAPIKey && authType != domainsemantic.AuthModeNone {
		return nil, status.Error(codes.InvalidArgument, "auth_type must be api_key or none")
	}
	if err := s.requireEndpointAuthMode(ctx, endpointID, authType); err != nil {
		return nil, err
	}
	secretValue := strings.TrimSpace(req.GetSecretValue())
	if authType == domainsemantic.AuthModeAPIKey && secretValue == "" {
		return nil, status.Error(codes.InvalidArgument, "api key is required")
	}
	if authType == domainsemantic.AuthModeNone && secretValue != "" {
		return nil, status.Error(codes.InvalidArgument, "secret_value must be empty when auth_type is none")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	input := semanticservice.CredentialCreateInput{Credential: domainsemantic.InferenceCredential{Key: req.GetKey(), Name: firstNonEmptyAdmin(req.GetDisplayName(), req.GetKey()), ModelEndpointID: endpointID, OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID, AuthType: authType, Status: domainsemantic.CredentialStatusActive, IsDefault: req.GetIsDefault()}}
	if authType == domainsemantic.AuthModeAPIKey {
		ciphertext, err := s.semantic.EncryptSecret(ctx, secretValue)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "encrypt secret: %v", err)
		}
		secret := domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID, Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: ciphertext, SecretSuffix: secretSuffix(secretValue)}
		input.Secret = &secret
	}
	stored, err := s.semantic.CreateInferenceCredential(ctx, input)
	if err != nil {
		return nil, mapAdminInferenceError(err, "create credential lifecycle")
	}
	response := &adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialResponse{Credential: mapCredential(stored.Credential)}
	if stored.Secret != nil {
		response.Secret = mapSecret(*stored.Secret)
	}
	return response, nil
}

func (s *AdminInferenceService) ListCredentials(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceListCredentialsRequest) (*adminv1.AdminIntelligenceAccessCredentialServiceListCredentialsResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceCredentialRead, inferenceScope("", "")); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credentials")
	}
	if strings.TrimSpace(req.GetOwnerType()) != "" {
		items = filterCredentialsByOwnerType(items, domainsemantic.CredentialOwnerType(req.GetOwnerType()))
	}
	if strings.TrimSpace(req.GetOwnerId()) != "" {
		items = filterCredentialsByOwnerID(items, req.GetOwnerId())
	}
	if req.ModelEndpointId != nil {
		id, err := parseSemanticUUID[domainsemantic.ModelEndpointID](req.GetModelEndpointId(), "model_endpoint_id")
		if err != nil {
			return nil, err
		}
		items = filterCredentialsByEndpoint(items, id)
	}
	if !req.GetIncludeInactive() {
		items = filterCredentialsActive(items)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminIntelligenceAccessCredentialServiceListCredentialsResponse{Credentials: mapCredentials(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) SetCredentialStatus(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceSetCredentialStatusRequest) (*adminv1.AdminIntelligenceAccessCredentialServiceSetCredentialStatusResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceCredentialManage, inferenceScope("", "")); err != nil {
		return nil, err
	}
	id, err := s.resolveCredentialID(ctx, firstNonEmptyAdmin(req.GetCredentialId(), req.GetCredential()))
	if err != nil {
		return nil, err
	}
	statusValue := domainsemantic.CredentialStatus(strings.ToLower(strings.TrimSpace(req.GetStatus())))
	switch statusValue {
	case domainsemantic.CredentialStatusActive, domainsemantic.CredentialStatusDisabled, domainsemantic.CredentialStatusRevoked, domainsemantic.CredentialStatusExpired:
	default:
		return nil, status.Error(codes.InvalidArgument, "status must be active, disabled, revoked, or expired")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	items, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credentials")
	}
	for _, item := range items {
		if item.ID == id {
			stored, err := s.semantic.SetInferenceCredentialStatus(ctx, semanticservice.CredentialStatusInput{CredentialID: id, Status: statusValue})
			if err != nil {
				return nil, mapAdminInferenceError(err, "update credential status")
			}
			return &adminv1.AdminIntelligenceAccessCredentialServiceSetCredentialStatusResponse{Credential: mapCredential(stored.Credential)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "credential not found")
}

func (s *AdminInferenceService) RotateCredential(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialRequest) (*adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceCredentialManage, inferenceScope("", "")); err != nil {
		return nil, err
	}
	id, err := s.resolveCredentialID(ctx, firstNonEmptyAdmin(req.GetCredentialId(), req.GetCredential()))
	if err != nil {
		return nil, err
	}
	credential, err := s.credentialByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateCredentialFromInferenceStore(ctx, &credential); err != nil {
		return nil, err
	}
	if credential.AuthType != domainsemantic.AuthModeAPIKey {
		return nil, status.Error(codes.FailedPrecondition, "only api_key credentials can rotate secret material")
	}
	if err := s.requireEndpointAuthMode(ctx, credential.ModelEndpointID, credential.AuthType); err != nil {
		return nil, err
	}
	secret, err := s.rotatedSecretFromRequest(ctx, req, credential)
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	stored, err := s.semantic.RotateInferenceCredential(ctx, semanticservice.CredentialRotateInput{CredentialID: credential.ID, ExpectedSecretRef: credential.SecretRef, Secret: secret})
	if err != nil {
		return nil, mapAdminInferenceError(err, "rotate credential lifecycle")
	}
	if stored.Secret == nil {
		return nil, status.Error(codes.Internal, "rotated credential secret is missing")
	}
	return &adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialResponse{Secret: mapSecret(*stored.Secret), Credential: mapCredential(stored.Credential)}, nil
}

func (s *AdminInferenceService) rotatedSecretFromRequest(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceRotateCredentialRequest, credential domainsemantic.InferenceCredential) (domainsemantic.Secret, error) {
	secretValue := strings.TrimSpace(req.GetSecretValue())
	if secretValue == "" {
		return domainsemantic.Secret{}, status.Error(codes.InvalidArgument, "api key is required")
	}
	ciphertext, err := s.semantic.EncryptSecret(ctx, secretValue)
	if err != nil {
		return domainsemantic.Secret{}, status.Errorf(codes.FailedPrecondition, "encrypt secret: %v", err)
	}
	return domainsemantic.Secret{ID: credential.SecretRef, OwnerType: credential.OwnerType, OwnerID: credential.OwnerID, Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: ciphertext, SecretSuffix: secretSuffix(secretValue)}, nil
}

func (s *AdminInferenceService) requireEndpointAuthMode(ctx context.Context, endpointID domainsemantic.ModelEndpointID, authType domainsemantic.AuthMode) error {
	items, err := s.semantic.GlobalManager().ListModelEndpoints(ctx)
	if err != nil {
		return mapAdminInferenceError(err, "list model endpoints")
	}
	for _, endpoint := range items {
		if endpoint.ID != endpointID {
			continue
		}
		for _, supported := range endpoint.AuthModes {
			if supported == authType {
				return nil
			}
		}
		return status.Errorf(codes.InvalidArgument, "model endpoint does not support auth_type %q", authType)
	}
	return status.Error(codes.NotFound, "model endpoint not found")
}

func (s *AdminInferenceService) hydrateCredentialFromInferenceStore(ctx context.Context, credential *domainsemantic.InferenceCredential) error {
	if credential == nil || credential.ModelEndpointID != uuid.Nil || s.inference == nil {
		return nil
	}
	items, err := s.inference.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return mapAdminInferenceError(err, "list standalone inference credentials")
	}
	for _, item := range items {
		if item.ID == uuid.UUID(credential.ID) || strings.EqualFold(strings.TrimSpace(item.Key), strings.TrimSpace(credential.Key)) {
			credential.ModelEndpointID = domainsemantic.ModelEndpointID(item.EndpointID)
			if credential.AuthType == "" {
				credential.AuthType = domainsemantic.AuthMode(item.AuthType)
			}
			return nil
		}
	}
	return nil
}

func (s *AdminInferenceService) DeleteCredential(ctx context.Context, req *adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest) (*adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capIntelligenceCredentialManage, inferenceScope("", "")); err != nil {
		return nil, err
	}
	id, err := s.resolveCredentialID(ctx, firstNonEmptyAdmin(req.GetCredentialId(), req.GetCredential()))
	if err != nil {
		return nil, err
	}
	credential, err := s.credentialByID(ctx, id)
	credentialExists := err == nil
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	grantRefs, err := s.credentialGrantReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	vectorRefs, err := s.credentialVectorReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(vectorRefs) > 0 {
		return nil, referencedPrecondition("credential", vectorRefs)
	}
	if len(grantRefs) > 0 && !req.GetDeleteGrants() {
		return nil, referencedPrecondition("credential", grantRefs)
	}
	spaces, err := s.semantic.ListSpaceManagers(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list semantic spaces")
	}
	standaloneDecisionRefs := []string{}
	for _, space := range spaces {
		refs, err := s.standaloneDecisionReferences(ctx, space.SpaceID.String(), func(decision domaininference.PolicyDecision) bool { return decision.CredentialID == id })
		if err != nil {
			return nil, mapAdminInferenceError(err, "list standalone access policy decisions")
		}
		standaloneDecisionRefs = append(standaloneDecisionRefs, refs...)
	}
	if len(standaloneDecisionRefs) > 0 {
		return nil, referencedPrecondition("credential", standaloneDecisionRefs)
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	deletedGrants := int32(0)
	if req.GetDeleteGrants() {
		deletedStandaloneGrants := map[string]struct{}{}
		for _, space := range spaces {
			grants, err := space.Manager.ListCredentialGrants(ctx)
			if err != nil {
				return nil, mapAdminInferenceError(err, "list credential grants")
			}
			for _, grant := range grants {
				if grant.CredentialID == id {
					if refs, err := s.credentialGrantVectorReferences(ctx, grant.ID); err != nil {
						return nil, err
					} else if len(refs) > 0 {
						return nil, referencedPrecondition("credential grant", refs)
					}
					if err := space.Manager.DeleteCredentialGrant(ctx, grant.ID); err != nil {
						return nil, mapAdminInferenceError(err, "delete credential grant")
					}
					if err := s.deleteSyncedIntelligenceCredentialGrant(ctx, space.SpaceID.String(), grant.ID); err != nil {
						return nil, mapAdminInferenceError(err, "delete inference credential grant")
					}
					deletedStandaloneGrants[grant.ID.String()] = struct{}{}
					deletedGrants++
				}
			}
			if s.inference != nil {
				inferenceSpace, err := s.inference.SpaceManager(ctx, space.SpaceID.String())
				if err != nil {
					return nil, mapAdminInferenceError(err, "open inference space manager")
				}
				standaloneGrants, err := inferenceSpace.ListCredentialGrants(ctx)
				if err != nil {
					return nil, mapAdminInferenceError(err, "list inference credential grants")
				}
				for _, grant := range standaloneGrants {
					if grant.CredentialID != id {
						continue
					}
					if refs, err := s.credentialGrantVectorReferences(ctx, grant.ID); err != nil {
						return nil, err
					} else if len(refs) > 0 {
						return nil, referencedPrecondition("credential grant", refs)
					}
					if err := s.deleteSyncedIntelligenceCredentialGrant(ctx, space.SpaceID.String(), domainsemantic.CredentialGrantID(grant.ID)); err != nil {
						return nil, mapAdminInferenceError(err, "delete inference credential grant")
					}
					if _, seen := deletedStandaloneGrants[grant.ID.String()]; !seen {
						deletedGrants++
					}
				}
			}
		}
	}
	deleteSecret := req.GetDeleteSecret() && credential.SecretRef != uuid.Nil
	if deleteSecret {
		if refs, err := s.secretCredentialReferences(ctx, credential.SecretRef, id); err != nil {
			return nil, err
		} else if len(refs) > 0 {
			return nil, referencedPrecondition("secret", refs)
		}
	}
	if err := s.semantic.DeleteInferenceCredential(ctx, semanticservice.CredentialDeleteInput{CredentialID: id, ExpectedSecretRef: credential.SecretRef, DeleteSecret: deleteSecret}); err != nil {
		return nil, mapAdminInferenceError(err, "delete credential lifecycle")
	}
	return &adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialResponse{CredentialId: id.String(), CredentialGrantsDeleted: deletedGrants, SecretDeleted: deleteSecret || (!credentialExists && req.GetDeleteSecret())}, nil
}

func secretSuffix(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 4 {
		return value
	}
	return value[len(value)-4:]
}
