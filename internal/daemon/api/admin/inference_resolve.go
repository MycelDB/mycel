package admin

import (
	"context"
	"strings"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reference resolution and authorization helpers for AdminInferenceService.

func (s *AdminInferenceService) resolveModelEndpointID(ctx context.Context, ref string) (domainsemantic.ModelEndpointID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		return domainsemantic.ModelEndpointID(id), nil
	}
	items, err := s.semantic.GlobalManager().ListModelEndpoints(ctx)
	if err != nil {
		return uuid.Nil, mapAdminInferenceError(err, "list model endpoints")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item.ID, nil
		}
	}
	return uuid.Nil, status.Errorf(codes.NotFound, "model endpoint %q not found", ref)
}

func (s *AdminInferenceService) resolveModelID(ctx context.Context, ref string) (domainsemantic.InferenceModelID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		return domainsemantic.InferenceModelID(id), nil
	}
	items, err := s.semantic.GlobalManager().ListModels(ctx)
	if err != nil {
		return uuid.Nil, mapAdminInferenceError(err, "list models")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item.ID, nil
		}
	}
	return uuid.Nil, status.Errorf(codes.NotFound, "model %q not found", ref)
}

func (s *AdminInferenceService) resolveVectorStoreID(ctx context.Context, ref string) (domainsemantic.VectorStoreID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		return domainsemantic.VectorStoreID(id), nil
	}
	items, err := s.semantic.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return uuid.Nil, mapAdminInferenceError(err, "list vector stores")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item.ID, nil
		}
	}
	return uuid.Nil, status.Errorf(codes.NotFound, "vector store %q not found", ref)
}

func (s *AdminInferenceService) resolveCapability(ctx context.Context, capIDText, endpointRef, modelRef, operation string) (domainsemantic.ModelEndpointCapability, error) {
	items, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, mapAdminInferenceError(err, "list model endpoint capabilities")
	}
	if strings.TrimSpace(capIDText) != "" {
		capID, err := parseSemanticUUID[domainsemantic.ModelEndpointCapabilityID](capIDText, "model_endpoint_capability_id")
		if err != nil {
			return domainsemantic.ModelEndpointCapability{}, err
		}
		for _, item := range items {
			if item.ID == capID {
				return item, nil
			}
		}
		return domainsemantic.ModelEndpointCapability{}, status.Error(codes.NotFound, "model endpoint capability not found")
	}
	endpointID, err := s.resolveModelEndpointID(ctx, endpointRef)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	modelID, err := s.resolveModelID(ctx, modelRef)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	op := domainsemantic.Operation(firstNonEmptyAdmin(operation, string(domainsemantic.OperationEmbeddings)))
	for _, item := range items {
		if item.ModelEndpointID == endpointID && item.ModelID == modelID && item.Operation == op {
			return item, nil
		}
	}
	return domainsemantic.ModelEndpointCapability{}, status.Error(codes.NotFound, "model endpoint capability not found")
}

func (s *AdminInferenceService) resolveCredentialID(ctx context.Context, ref string) (domainsemantic.InferenceCredentialID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(ref)); err == nil && id != uuid.Nil {
		return domainsemantic.InferenceCredentialID(id), nil
	}
	items, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return uuid.Nil, mapAdminInferenceError(err, "list credentials")
	}
	key := strings.ToLower(strings.TrimSpace(ref))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Key)) == key {
			return item.ID, nil
		}
	}
	return uuid.Nil, status.Errorf(codes.NotFound, "credential %q not found", ref)
}

const (
	capInferenceCatalogRead      = "inference.catalog.read"
	capInferenceCatalogManage    = "inference.catalog.manage"
	capInferenceProfileRead      = "inference.profile.read"
	capInferenceProfileManage    = "inference.profile.manage"
	capInferenceCredentialRead   = "inference.credential.read"
	capInferenceCredentialManage = "inference.credential.manage"
	capInferenceGrantManage      = "inference.grant.manage"
	capInferencePolicyManage     = "inference.policy.manage"
	capInferenceAuditRead        = "inference.audit.read"
)

type ScopedOperatorAuthorizer interface {
	Authorize(ctx context.Context, principalID string, capability string, scope principalservice.AccessScope) error
}

func (s *AdminInferenceService) requireInferenceCapability(ctx context.Context, capability string, scope principalservice.AccessScope) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
		if err := scoped.Authorize(ctx, principal.PrincipalID, capability, scope); err != nil {
			return daemonauth.Principal{}, err
		}
		return principal, nil
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability)
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required inference capability")
	}
	return principal, nil
}

func inferenceScope(spaceID string, domainID string) principalservice.AccessScope {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if domainID != "" {
		return principalservice.AccessScope{Type: "domain", SpaceID: spaceID, DomainID: domainID}
	}
	if spaceID != "" {
		return principalservice.AccessScope{Type: "space", SpaceID: spaceID}
	}
	return principalservice.AccessScope{Type: "system"}
}

func (s *AdminInferenceService) requireInferenceManage(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireInferenceCapability(ctx, capInferenceCatalogManage, principalservice.AccessScope{Type: "system"})
}
