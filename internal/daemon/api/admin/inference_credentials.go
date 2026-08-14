package admin

import (
	"context"
	"sort"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Credential and secret RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) CreateCredential(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceCreateCredentialRequest) (*adminv1.AdminInferenceCredentialServiceCreateCredentialResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
	secret := domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID}
	if inline := req.GetInlineSecret(); inline != nil {
		secret.Kind = domainsemantic.SecretKindInlineEncrypted
		secret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: inline.GetAlgorithm(), NonceB64: inline.GetNonceB64(), CipherB64: inline.GetCipherB64()}
	} else if strings.TrimSpace(req.GetSecretValue()) != "" {
		ciphertext, err := s.semantic.EncryptSecret(ctx, req.GetSecretValue())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "encrypt secret: %v", err)
		}
		secret.Kind = domainsemantic.SecretKindInlineEncrypted
		secret.Ciphertext = ciphertext
	} else if strings.TrimSpace(req.GetExternalRef()) != "" {
		if err := validateExternalSecretRef(req.GetExternalRef()); err != nil {
			return nil, err
		}
		secret.Kind = domainsemantic.SecretKindExternalRef
		secret.ExternalRef = req.GetExternalRef()
	} else {
		return nil, status.Error(codes.InvalidArgument, "secret_value, inline_secret, or external_ref is required")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	mgr := s.semantic.GlobalManager()
	storedSecret, err := mgr.UpsertSecret(ctx, secret)
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert secret")
	}
	if err := s.syncInferenceSecret(ctx, storedSecret); err != nil {
		return nil, mapAdminInferenceError(err, "sync inference secret")
	}
	credential, err := mgr.UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: req.GetKey(), Name: firstNonEmptyAdmin(req.GetDisplayName(), req.GetKey()), ModelEndpointID: endpointID, OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID, AuthType: domainsemantic.AuthMode(firstNonEmptyAdmin(req.GetAuthType(), string(domainsemantic.AuthModeAPIKey))), SecretRef: storedSecret.ID, Status: domainsemantic.CredentialStatusActive, IsDefault: req.GetIsDefault()})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert credential")
	}
	if err := s.syncInferenceCredential(ctx, credential); err != nil {
		return nil, mapAdminInferenceError(err, "sync inference credential")
	}
	return &adminv1.AdminInferenceCredentialServiceCreateCredentialResponse{Secret: mapSecret(storedSecret), Credential: mapCredential(credential)}, nil
}

func (s *AdminInferenceService) ListCredentials(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceListCredentialsRequest) (*adminv1.AdminInferenceCredentialServiceListCredentialsResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
	return &adminv1.AdminInferenceCredentialServiceListCredentialsResponse{Credentials: mapCredentials(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) SetCredentialStatus(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceSetCredentialStatusRequest) (*adminv1.AdminInferenceCredentialServiceSetCredentialStatusResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveCredentialID(ctx, firstNonEmptyAdmin(req.GetCredentialId(), req.GetCredential()))
	if err != nil {
		return nil, err
	}
	statusValue := domainsemantic.CredentialStatus(req.GetStatus())
	if statusValue == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
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
			item.Status = statusValue
			if err := s.hydrateCredentialFromInferenceStore(ctx, &item); err != nil {
				return nil, err
			}
			stored, err := s.semantic.GlobalManager().UpsertCredential(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "update credential")
			}
			if err := s.syncInferenceCredential(ctx, stored); err != nil {
				return nil, mapAdminInferenceError(err, "sync inference credential")
			}
			return &adminv1.AdminInferenceCredentialServiceSetCredentialStatusResponse{Credential: mapCredential(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "credential not found")
}

func (s *AdminInferenceService) RotateCredential(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceRotateCredentialRequest) (*adminv1.AdminInferenceCredentialServiceRotateCredentialResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
	secret, err := s.rotatedSecretFromRequest(ctx, req, credential)
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	storedSecret, err := s.semantic.GlobalManager().UpsertSecret(ctx, secret)
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert rotated secret")
	}
	if err := s.syncInferenceSecret(ctx, storedSecret); err != nil {
		return nil, mapAdminInferenceError(err, "sync rotated inference secret")
	}
	credential.SecretRef = storedSecret.ID
	storedCredential, err := s.semantic.GlobalManager().UpsertCredential(ctx, credential)
	if err != nil {
		return nil, mapAdminInferenceError(err, "update rotated credential")
	}
	if err := s.syncInferenceCredential(ctx, storedCredential); err != nil {
		return nil, mapAdminInferenceError(err, "sync rotated inference credential")
	}
	return &adminv1.AdminInferenceCredentialServiceRotateCredentialResponse{Secret: mapSecret(storedSecret), Credential: mapCredential(storedCredential)}, nil
}

func (s *AdminInferenceService) rotatedSecretFromRequest(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceRotateCredentialRequest, credential domainsemantic.InferenceCredential) (domainsemantic.Secret, error) {
	secret := domainsemantic.Secret{OwnerType: credential.OwnerType, OwnerID: credential.OwnerID}
	if inline := req.GetInlineSecret(); inline != nil {
		secret.Kind = domainsemantic.SecretKindInlineEncrypted
		secret.Ciphertext = &domainsemantic.EncryptedSecretPayload{Algorithm: inline.GetAlgorithm(), NonceB64: inline.GetNonceB64(), CipherB64: inline.GetCipherB64()}
		return secret, nil
	}
	if strings.TrimSpace(req.GetSecretValue()) != "" {
		ciphertext, err := s.semantic.EncryptSecret(ctx, req.GetSecretValue())
		if err != nil {
			return domainsemantic.Secret{}, status.Errorf(codes.FailedPrecondition, "encrypt secret: %v", err)
		}
		secret.Kind = domainsemantic.SecretKindInlineEncrypted
		secret.Ciphertext = ciphertext
		return secret, nil
	}
	if strings.TrimSpace(req.GetExternalRef()) != "" {
		if err := validateExternalSecretRef(req.GetExternalRef()); err != nil {
			return domainsemantic.Secret{}, err
		}
		secret.Kind = domainsemantic.SecretKindExternalRef
		secret.ExternalRef = req.GetExternalRef()
		return secret, nil
	}
	return domainsemantic.Secret{}, status.Error(codes.InvalidArgument, "secret_value, inline_secret, or external_ref is required")
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

func (s *AdminInferenceService) DeleteCredential(ctx context.Context, req *adminv1.AdminInferenceCredentialServiceDeleteCredentialRequest) (*adminv1.AdminInferenceCredentialServiceDeleteCredentialResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	deletedGrants := int32(0)
	if req.GetDeleteGrants() {
		spaces, err := s.semantic.ListSpaceManagers(ctx)
		if err != nil {
			return nil, mapAdminInferenceError(err, "list semantic spaces")
		}
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
					deletedGrants++
				}
			}
		}
	}
	secretDeleted := false
	if req.GetDeleteSecret() && credential.SecretRef != uuid.Nil {
		if refs, err := s.secretCredentialReferences(ctx, credential.SecretRef, id); err != nil {
			return nil, err
		} else if len(refs) > 0 {
			return nil, referencedPrecondition("secret", refs)
		}
		if err := s.semantic.GlobalManager().DeleteSecret(ctx, credential.SecretRef); err != nil {
			return nil, mapAdminInferenceError(err, "delete secret")
		}
		if s.inference != nil {
			if err := s.inference.GlobalManager().DeleteSecret(ctx, credential.SecretRef); err != nil {
				return nil, mapAdminInferenceError(err, "delete inference secret")
			}
		}
		secretDeleted = true
	}
	if err := s.semantic.GlobalManager().DeleteCredential(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete credential")
	}
	if s.inference != nil {
		if err := s.inference.GlobalManager().DeleteCredential(ctx, id); err != nil {
			return nil, mapAdminInferenceError(err, "delete inference credential")
		}
	}
	return &adminv1.AdminInferenceCredentialServiceDeleteCredentialResponse{CredentialId: id.String(), CredentialGrantsDeleted: deletedGrants, SecretDeleted: secretDeleted}, nil
}

func validateExternalSecretRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return status.Error(codes.InvalidArgument, "external_ref is required")
	}
	if !strings.HasPrefix(ref, "env://") || strings.TrimSpace(strings.TrimPrefix(ref, "env://")) == "" {
		return status.Error(codes.InvalidArgument, "external_ref must use env://NAME")
	}
	return nil
}
