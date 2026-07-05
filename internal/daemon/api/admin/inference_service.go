package admin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adminInferenceMaxPageSize = 500

type AdminInferenceService struct {
	adminv1.UnimplementedAdminInferenceServiceServer
	semantic   daemonsemantic.Manager
	authorizer OperatorAuthorizer
}

func NewAdminInferenceService(semantic daemonsemantic.Manager, authorizer OperatorAuthorizer) *AdminInferenceService {
	return &AdminInferenceService{semantic: semantic, authorizer: authorizer}
}

func (s *AdminInferenceService) ApplyInferencePackage(ctx context.Context, req *adminv1.AdminInferenceServiceApplyInferencePackageRequest) (*adminv1.AdminInferenceServiceApplyInferencePackageResponse, error) {
	principal, err := s.requireInferenceManage(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetName()) == "" || strings.TrimSpace(req.GetVersion()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name and version are required")
	}
	mgr := s.semantic.GlobalManager()
	endpoints := make([]domainsemantic.ModelEndpoint, 0, len(req.GetModelEndpoints()))
	for _, protoEndpoint := range req.GetModelEndpoints() {
		endpoint, err := modelEndpointFromProto(protoEndpoint)
		if err != nil {
			return nil, err
		}
		stored, err := mgr.UpsertModelEndpoint(ctx, endpoint)
		if err != nil {
			return nil, mapAdminInferenceError(err, "upsert model endpoint")
		}
		endpoints = append(endpoints, stored)
	}
	models := make([]domainsemantic.InferenceModel, 0, len(req.GetModels()))
	for _, protoModel := range req.GetModels() {
		model, err := inferenceModelFromProto(protoModel)
		if err != nil {
			return nil, err
		}
		stored, err := mgr.UpsertModel(ctx, model)
		if err != nil {
			return nil, mapAdminInferenceError(err, "upsert model")
		}
		models = append(models, stored)
	}
	stores := make([]domainsemantic.VectorStoreBackend, 0, len(req.GetVectorStores()))
	for _, protoStore := range req.GetVectorStores() {
		store, err := vectorStoreFromProto(protoStore)
		if err != nil {
			return nil, err
		}
		stored, err := mgr.UpsertVectorStore(ctx, store)
		if err != nil {
			return nil, mapAdminInferenceError(err, "upsert vector store")
		}
		stores = append(stores, stored)
	}
	capabilities := make([]domainsemantic.ModelEndpointCapability, 0, len(req.GetModelEndpointCapabilities()))
	for _, def := range req.GetModelEndpointCapabilities() {
		endpointID, err := s.resolveModelEndpointID(ctx, firstNonEmptyAdmin(def.GetModelEndpointId(), def.GetModelEndpoint()))
		if err != nil {
			return nil, err
		}
		modelID, err := s.resolveModelID(ctx, firstNonEmptyAdmin(def.GetModelId(), def.GetModel()))
		if err != nil {
			return nil, err
		}
		enabled := true
		if def.Enabled != nil {
			enabled = def.GetEnabled()
		}
		capability, err := mgr.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ModelEndpointID: endpointID, ModelID: modelID, Operation: domainsemantic.Operation(firstNonEmptyAdmin(def.GetOperation(), string(domainsemantic.OperationEmbeddings))), Enabled: enabled, ModelNameOverride: def.GetModelNameOverride(), Metadata: structToMap(def.GetMetadata())})
		if err != nil {
			return nil, mapAdminInferenceError(err, "upsert model endpoint capability")
		}
		capabilities = append(capabilities, capability)
	}
	counts := map[string]int{"model_endpoints": len(req.GetModelEndpoints()), "models": len(req.GetModels()), "model_endpoint_capabilities": len(req.GetModelEndpointCapabilities()), "vector_stores": len(req.GetVectorStores())}
	pkg, err := mgr.UpsertPackage(ctx, domainsemantic.InferencePackage{Name: req.GetName(), Version: req.GetVersion(), Source: req.GetSource(), Checksum: req.GetChecksum(), InstalledBy: principal.OperatorID, DefinitionCounts: counts})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert inference package")
	}
	return &adminv1.AdminInferenceServiceApplyInferencePackageResponse{Package: mapInferencePackage(pkg), ModelEndpoints: mapModelEndpoints(endpoints), Models: mapInferenceModels(models), VectorStores: mapVectorStores(stores), ModelEndpointCapabilities: mapModelEndpointCapabilities(capabilities)}, nil
}

func (s *AdminInferenceService) ListInferencePackages(ctx context.Context, req *adminv1.AdminInferenceServiceListInferencePackagesRequest) (*adminv1.AdminInferenceServiceListInferencePackagesResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListPackages(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list inference packages")
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Name+items[i].Version < items[j].Name+items[j].Version })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceServiceListInferencePackagesResponse{Packages: mapInferencePackages(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModelEndpoints(ctx context.Context, req *adminv1.AdminInferenceServiceListModelEndpointsRequest) (*adminv1.AdminInferenceServiceListModelEndpointsResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListModelEndpoints(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list model endpoints")
	}
	items = filterEndpoints(items, req.GetIncludeDisabled())
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceServiceListModelEndpointsResponse{ModelEndpoints: mapModelEndpoints(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModels(ctx context.Context, req *adminv1.AdminInferenceServiceListModelsRequest) (*adminv1.AdminInferenceServiceListModelsResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListModels(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list models")
	}
	operation := strings.TrimSpace(req.GetOperation())
	if operation != "" {
		filtered := items[:0]
		for _, item := range items {
			if string(item.Operation) == operation {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceServiceListModelsResponse{Models: mapInferenceModels(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListVectorStores(ctx context.Context, req *adminv1.AdminInferenceServiceListVectorStoresRequest) (*adminv1.AdminInferenceServiceListVectorStoresResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list vector stores")
	}
	items = filterVectorStores(items, req.GetIncludeDisabled())
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceServiceListVectorStoresResponse{VectorStores: mapVectorStores(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModelEndpointCapabilities(ctx context.Context, req *adminv1.AdminInferenceServiceListModelEndpointCapabilitiesRequest) (*adminv1.AdminInferenceServiceListModelEndpointCapabilitiesResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list model endpoint capabilities")
	}
	if req.ModelEndpointId != nil {
		id, err := parseSemanticUUID[domainsemantic.ModelEndpointID](req.GetModelEndpointId(), "model_endpoint_id")
		if err != nil {
			return nil, err
		}
		items = filterCapabilitiesByEndpoint(items, id)
	}
	if req.ModelId != nil {
		id, err := parseSemanticUUID[domainsemantic.InferenceModelID](req.GetModelId(), "model_id")
		if err != nil {
			return nil, err
		}
		items = filterCapabilitiesByModel(items, id)
	}
	operation := strings.TrimSpace(req.GetOperation())
	if operation != "" {
		items = filterCapabilitiesByOperation(items, domainsemantic.Operation(operation))
	}
	if !req.GetIncludeDisabled() {
		items = filterCapabilitiesEnabled(items)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ID.String() < items[j].ID.String() })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceServiceListModelEndpointCapabilitiesResponse{ModelEndpointCapabilities: mapModelEndpointCapabilities(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) CreateCredential(ctx context.Context, req *adminv1.AdminInferenceServiceCreateCredentialRequest) (*adminv1.AdminInferenceServiceCreateCredentialResponse, error) {
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
		secret.Kind = domainsemantic.SecretKindExternalRef
		secret.ExternalRef = req.GetExternalRef()
	} else {
		return nil, status.Error(codes.InvalidArgument, "secret_value, inline_secret, or external_ref is required")
	}
	mgr := s.semantic.GlobalManager()
	storedSecret, err := mgr.UpsertSecret(ctx, secret)
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert secret")
	}
	credential, err := mgr.UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: req.GetKey(), Name: firstNonEmptyAdmin(req.GetDisplayName(), req.GetKey()), ModelEndpointID: endpointID, OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID, AuthType: domainsemantic.AuthMode(firstNonEmptyAdmin(req.GetAuthType(), string(domainsemantic.AuthModeAPIKey))), SecretRef: storedSecret.ID, Status: domainsemantic.CredentialStatusActive, IsDefault: req.GetIsDefault()})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert credential")
	}
	return &adminv1.AdminInferenceServiceCreateCredentialResponse{Secret: mapSecret(storedSecret), Credential: mapCredential(credential)}, nil
}

func (s *AdminInferenceService) ListCredentials(ctx context.Context, req *adminv1.AdminInferenceServiceListCredentialsRequest) (*adminv1.AdminInferenceServiceListCredentialsResponse, error) {
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
	return &adminv1.AdminInferenceServiceListCredentialsResponse{Credentials: mapCredentials(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) CreateCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceServiceCreateCredentialGrantRequest) (*adminv1.AdminInferenceServiceCreateCredentialGrantResponse, error) {
	principal, err := s.requireInferenceManage(ctx)
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
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	grant, err := spaceMgr.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{CredentialID: credentialID, Scope: scope, Operations: operationsFromStringsAdmin(req.GetOperations()), ModelEndpointID: endpointID, ModelID: modelID, Priority: int(req.GetPriority()), IsDefault: req.GetIsDefault(), AllowBackgroundUse: req.GetAllowBackgroundUse(), GrantedBy: principal.OperatorID, ExpiresAt: timeFromProto(req.GetExpiresAt())})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert credential grant")
	}
	return &adminv1.AdminInferenceServiceCreateCredentialGrantResponse{CredentialGrant: mapCredentialGrant(grant)}, nil
}

func (s *AdminInferenceService) ListCredentialGrants(ctx context.Context, req *adminv1.AdminInferenceServiceListCredentialGrantsRequest) (*adminv1.AdminInferenceServiceListCredentialGrantsResponse, error) {
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
	return &adminv1.AdminInferenceServiceListCredentialGrantsResponse{CredentialGrants: mapCredentialGrants(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) CreateInferencePolicy(ctx context.Context, req *adminv1.AdminInferenceServiceCreateInferencePolicyRequest) (*adminv1.AdminInferenceServiceCreateInferencePolicyResponse, error) {
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
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	policy, err := spaceMgr.UpsertInferencePolicy(ctx, domainsemantic.InferencePolicy{Scope: scope, Effect: domainsemantic.PolicyEffect(req.GetEffect()), Operations: operationsFromStringsAdmin(req.GetOperations()), NoInference: req.GetNoInference(), AllowedPrivacyClasses: privacyClassesFromStringsAdmin(req.GetAllowedPrivacyClasses()), DisallowThirdParty: req.GetDisallowThirdParty(), RequireLocalEndpoint: req.GetRequireLocalEndpoint(), Reason: req.GetReason(), CreatedBy: principal.OperatorID, ExpiresAt: timeFromProto(req.GetExpiresAt())})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert inference policy")
	}
	return &adminv1.AdminInferenceServiceCreateInferencePolicyResponse{InferencePolicy: mapInferencePolicy(policy)}, nil
}

func (s *AdminInferenceService) ListInferencePolicies(ctx context.Context, req *adminv1.AdminInferenceServiceListInferencePoliciesRequest) (*adminv1.AdminInferenceServiceListInferencePoliciesResponse, error) {
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
	return &adminv1.AdminInferenceServiceListInferencePoliciesResponse{InferencePolicies: mapInferencePolicies(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) SetModelEndpointEnabled(ctx context.Context, req *adminv1.AdminInferenceServiceSetModelEndpointEnabledRequest) (*adminv1.AdminInferenceServiceSetModelEndpointEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveModelEndpointID(ctx, firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()))
	if err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListModelEndpoints(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list model endpoints")
	}
	for _, item := range items {
		if item.ID == id {
			item.Enabled = req.GetEnabled()
			stored, err := s.semantic.GlobalManager().UpsertModelEndpoint(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "update model endpoint")
			}
			return &adminv1.AdminInferenceServiceSetModelEndpointEnabledResponse{ModelEndpoint: mapModelEndpoint(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "model endpoint not found")
}

func (s *AdminInferenceService) SetVectorStoreEnabled(ctx context.Context, req *adminv1.AdminInferenceServiceSetVectorStoreEnabledRequest) (*adminv1.AdminInferenceServiceSetVectorStoreEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveVectorStoreID(ctx, firstNonEmptyAdmin(req.GetVectorStoreId(), req.GetVectorStore()))
	if err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list vector stores")
	}
	for _, item := range items {
		if item.ID == id {
			item.Enabled = req.GetEnabled()
			stored, err := s.semantic.GlobalManager().UpsertVectorStore(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "update vector store")
			}
			return &adminv1.AdminInferenceServiceSetVectorStoreEnabledResponse{VectorStore: mapVectorStore(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "vector store not found")
}

func (s *AdminInferenceService) SetModelEndpointCapabilityEnabled(ctx context.Context, req *adminv1.AdminInferenceServiceSetModelEndpointCapabilityEnabledRequest) (*adminv1.AdminInferenceServiceSetModelEndpointCapabilityEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	cap, err := s.resolveCapability(ctx, req.GetModelEndpointCapabilityId(), firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()), firstNonEmptyAdmin(req.GetModelId(), req.GetModel()), req.GetOperation())
	if err != nil {
		return nil, err
	}
	cap.Enabled = req.GetEnabled()
	stored, err := s.semantic.GlobalManager().UpsertModelEndpointCapability(ctx, cap)
	if err != nil {
		return nil, mapAdminInferenceError(err, "update model endpoint capability")
	}
	return &adminv1.AdminInferenceServiceSetModelEndpointCapabilityEnabledResponse{ModelEndpointCapability: mapModelEndpointCapability(stored)}, nil
}

func (s *AdminInferenceService) SetCredentialStatus(ctx context.Context, req *adminv1.AdminInferenceServiceSetCredentialStatusRequest) (*adminv1.AdminInferenceServiceSetCredentialStatusResponse, error) {
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
	items, err := s.semantic.GlobalManager().ListCredentials(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list credentials")
	}
	for _, item := range items {
		if item.ID == id {
			item.Status = statusValue
			stored, err := s.semantic.GlobalManager().UpsertCredential(ctx, item)
			if err != nil {
				return nil, mapAdminInferenceError(err, "update credential")
			}
			return &adminv1.AdminInferenceServiceSetCredentialStatusResponse{Credential: mapCredential(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "credential not found")
}

func (s *AdminInferenceService) ExpireCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceServiceExpireCredentialGrantRequest) (*adminv1.AdminInferenceServiceExpireCredentialGrantResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
			return &adminv1.AdminInferenceServiceExpireCredentialGrantResponse{CredentialGrant: mapCredentialGrant(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "credential grant not found")
}

func (s *AdminInferenceService) ExpireInferencePolicy(ctx context.Context, req *adminv1.AdminInferenceServiceExpireInferencePolicyRequest) (*adminv1.AdminInferenceServiceExpireInferencePolicyResponse, error) {
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
			return &adminv1.AdminInferenceServiceExpireInferencePolicyResponse{InferencePolicy: mapInferencePolicy(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "inference policy not found")
}

func (s *AdminInferenceService) DeleteModelEndpoint(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteModelEndpointRequest) (*adminv1.AdminInferenceServiceDeleteModelEndpointResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveModelEndpointID(ctx, firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()))
	if err != nil {
		return nil, err
	}
	refs, err := s.modelEndpointReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("model endpoint", refs)
	}
	if err := s.semantic.GlobalManager().DeleteModelEndpoint(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model endpoint")
	}
	return &adminv1.AdminInferenceServiceDeleteModelEndpointResponse{ModelEndpointId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteModel(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteModelRequest) (*adminv1.AdminInferenceServiceDeleteModelResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveModelID(ctx, firstNonEmptyAdmin(req.GetModelId(), req.GetModel()))
	if err != nil {
		return nil, err
	}
	refs, err := s.modelReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("model", refs)
	}
	if err := s.semantic.GlobalManager().DeleteModel(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model")
	}
	return &adminv1.AdminInferenceServiceDeleteModelResponse{ModelId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteVectorStore(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteVectorStoreRequest) (*adminv1.AdminInferenceServiceDeleteVectorStoreResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveVectorStoreID(ctx, firstNonEmptyAdmin(req.GetVectorStoreId(), req.GetVectorStore()))
	if err != nil {
		return nil, err
	}
	refs, err := s.vectorStoreReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("vector store", refs)
	}
	if err := s.semantic.GlobalManager().DeleteVectorStore(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete vector store")
	}
	return &adminv1.AdminInferenceServiceDeleteVectorStoreResponse{VectorStoreId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteModelEndpointCapability(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteModelEndpointCapabilityRequest) (*adminv1.AdminInferenceServiceDeleteModelEndpointCapabilityResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := parseSemanticUUID[domainsemantic.ModelEndpointCapabilityID](req.GetModelEndpointCapabilityId(), "model_endpoint_capability_id")
	if err != nil {
		return nil, err
	}
	refs, err := s.capabilityReferences(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(refs) > 0 {
		return nil, referencedPrecondition("model endpoint capability", refs)
	}
	if err := s.semantic.GlobalManager().DeleteModelEndpointCapability(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model endpoint capability")
	}
	return &adminv1.AdminInferenceServiceDeleteModelEndpointCapabilityResponse{ModelEndpointCapabilityId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteCredential(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteCredentialRequest) (*adminv1.AdminInferenceServiceDeleteCredentialResponse, error) {
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
		secretDeleted = true
	}
	if err := s.semantic.GlobalManager().DeleteCredential(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete credential")
	}
	return &adminv1.AdminInferenceServiceDeleteCredentialResponse{CredentialId: id.String(), CredentialGrantsDeleted: deletedGrants, SecretDeleted: secretDeleted}, nil
}

func (s *AdminInferenceService) DeleteCredentialGrant(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteCredentialGrantRequest) (*adminv1.AdminInferenceServiceDeleteCredentialGrantResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
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
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminInferenceError(err, "open semantic space manager")
	}
	if err := spaceMgr.DeleteCredentialGrant(ctx, grantID); err != nil {
		return nil, mapAdminInferenceError(err, "delete credential grant")
	}
	return &adminv1.AdminInferenceServiceDeleteCredentialGrantResponse{CredentialGrantId: grantID.String()}, nil
}

func (s *AdminInferenceService) DeleteInferencePolicy(ctx context.Context, req *adminv1.AdminInferenceServiceDeleteInferencePolicyRequest) (*adminv1.AdminInferenceServiceDeleteInferencePolicyResponse, error) {
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
	return &adminv1.AdminInferenceServiceDeleteInferencePolicyResponse{InferencePolicyId: policyID.String()}, nil
}

func (s *AdminInferenceService) modelEndpointReferences(ctx context.Context, id domainsemantic.ModelEndpointID) ([]string, error) {
	refs := []string{}
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
	}
	return refs, nil
}

func (s *AdminInferenceService) modelReferences(ctx context.Context, id domainsemantic.InferenceModelID) ([]string, error) {
	refs := []string{}
	caps, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, cap := range caps {
		if cap.ModelID == id {
			refs = append(refs, "capability:"+cap.ID.String())
		}
	}
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
	}
	return refs, nil
}

func (s *AdminInferenceService) credentialVectorReferences(ctx context.Context, id domainsemantic.InferenceCredentialID) ([]string, error) {
	return s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialID == id })
}

func (s *AdminInferenceService) credentialGrantVectorReferences(ctx context.Context, id domainsemantic.CredentialGrantID) ([]string, error) {
	return s.vectorRecordReferences(ctx, func(rec domainsemantic.AdvancedEmbeddingRecord) bool { return rec.CredentialGrantID == id })
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
	return refs, nil
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

func (s *AdminInferenceService) requireInferenceManage(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required inference capability")
	}
	return principal, nil
}

func modelEndpointFromProto(in *adminv1.ModelEndpoint) (domainsemantic.ModelEndpoint, error) {
	if in == nil {
		return domainsemantic.ModelEndpoint{}, status.Error(codes.InvalidArgument, "model endpoint is required")
	}
	id, err := optionalSemanticUUID[domainsemantic.ModelEndpointID](in.GetModelEndpointId(), "model_endpoint_id")
	if err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	return domainsemantic.ModelEndpoint{ID: id, Key: in.GetKey(), Name: in.GetName(), ConnectorType: domainsemantic.ConnectorType(in.GetConnectorType()), EndpointURL: in.GetEndpointUrl(), NetworkClass: domainsemantic.NetworkClass(in.GetNetworkClass()), PrivacyClass: domainsemantic.PrivacyClass(in.GetPrivacyClass()), AuthModes: authModesFromStrings(in.GetAuthModes()), Operations: operationsFromStringsAdmin(in.GetOperations()), Enabled: in.GetEnabled(), Metadata: structToMap(in.GetMetadata())}, nil
}

func inferenceModelFromProto(in *adminv1.InferenceModel) (domainsemantic.InferenceModel, error) {
	if in == nil {
		return domainsemantic.InferenceModel{}, status.Error(codes.InvalidArgument, "model is required")
	}
	id, err := optionalSemanticUUID[domainsemantic.InferenceModelID](in.GetModelId(), "model_id")
	if err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	return domainsemantic.InferenceModel{ID: id, Key: in.GetKey(), Operation: domainsemantic.Operation(in.GetOperation()), ModelName: in.GetModelName(), ConnectorTypes: connectorTypesFromStrings(in.GetConnectorTypes()), Dimensions: int(in.GetDimensions()), Modality: in.GetModality(), VectorSpaceKey: in.GetVectorSpaceKey(), Metadata: structToMap(in.GetMetadata())}, nil
}

func vectorStoreFromProto(in *adminv1.VectorStore) (domainsemantic.VectorStoreBackend, error) {
	if in == nil {
		return domainsemantic.VectorStoreBackend{}, status.Error(codes.InvalidArgument, "vector store is required")
	}
	id, err := optionalSemanticUUID[domainsemantic.VectorStoreID](in.GetVectorStoreId(), "vector_store_id")
	if err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	return domainsemantic.VectorStoreBackend{ID: id, Key: in.GetKey(), Name: in.GetName(), Type: domainsemantic.VectorStoreType(in.GetType()), PrivacyClass: domainsemantic.PrivacyClass(in.GetPrivacyClass()), Enabled: in.GetEnabled(), Config: structToMap(in.GetConfig())}, nil
}

func mapInferencePackage(in domainsemantic.InferencePackage) *adminv1.InferencePackage {
	counts := map[string]int32{}
	for k, v := range in.DefinitionCounts {
		counts[k] = int32(v)
	}
	return &adminv1.InferencePackage{InferencePackageId: in.ID.String(), Name: in.Name, Version: in.Version, Source: in.Source, Checksum: in.Checksum, DefinitionCounts: counts, InstalledAt: timestamppb.New(in.InstalledAt), InstalledBy: in.InstalledBy}
}
func mapInferencePackages(items []domainsemantic.InferencePackage) []*adminv1.InferencePackage {
	out := make([]*adminv1.InferencePackage, 0, len(items))
	for _, item := range items {
		out = append(out, mapInferencePackage(item))
	}
	return out
}
func mapModelEndpoint(in domainsemantic.ModelEndpoint) *adminv1.ModelEndpoint {
	return &adminv1.ModelEndpoint{ModelEndpointId: in.ID.String(), Key: in.Key, Name: in.Name, ConnectorType: string(in.ConnectorType), EndpointUrl: in.EndpointURL, NetworkClass: string(in.NetworkClass), PrivacyClass: string(in.PrivacyClass), AuthModes: stringsFromAuthModes(in.AuthModes), Operations: stringsFromOperations(in.Operations), Enabled: in.Enabled, Metadata: protoStructAdmin(in.Metadata)}
}
func mapModelEndpoints(items []domainsemantic.ModelEndpoint) []*adminv1.ModelEndpoint {
	out := make([]*adminv1.ModelEndpoint, 0, len(items))
	for _, item := range items {
		out = append(out, mapModelEndpoint(item))
	}
	return out
}
func mapInferenceModel(in domainsemantic.InferenceModel) *adminv1.InferenceModel {
	return &adminv1.InferenceModel{ModelId: in.ID.String(), Key: in.Key, Operation: string(in.Operation), ModelName: in.ModelName, ConnectorTypes: stringsFromConnectorTypes(in.ConnectorTypes), Dimensions: int32(in.Dimensions), Modality: in.Modality, VectorSpaceKey: in.VectorSpaceKey, Metadata: protoStructAdmin(in.Metadata)}
}
func mapInferenceModels(items []domainsemantic.InferenceModel) []*adminv1.InferenceModel {
	out := make([]*adminv1.InferenceModel, 0, len(items))
	for _, item := range items {
		out = append(out, mapInferenceModel(item))
	}
	return out
}
func mapVectorStore(in domainsemantic.VectorStoreBackend) *adminv1.VectorStore {
	return &adminv1.VectorStore{VectorStoreId: in.ID.String(), Key: in.Key, Name: in.Name, Type: string(in.Type), PrivacyClass: string(in.PrivacyClass), Enabled: in.Enabled, Config: protoStructAdmin(in.Config)}
}
func mapVectorStores(items []domainsemantic.VectorStoreBackend) []*adminv1.VectorStore {
	out := make([]*adminv1.VectorStore, 0, len(items))
	for _, item := range items {
		out = append(out, mapVectorStore(item))
	}
	return out
}
func mapModelEndpointCapability(in domainsemantic.ModelEndpointCapability) *adminv1.ModelEndpointCapability {
	return &adminv1.ModelEndpointCapability{ModelEndpointCapabilityId: in.ID.String(), ModelEndpointId: in.ModelEndpointID.String(), ModelId: in.ModelID.String(), Operation: string(in.Operation), Enabled: in.Enabled, ModelNameOverride: in.ModelNameOverride, Metadata: protoStructAdmin(in.Metadata)}
}
func mapModelEndpointCapabilities(items []domainsemantic.ModelEndpointCapability) []*adminv1.ModelEndpointCapability {
	out := make([]*adminv1.ModelEndpointCapability, 0, len(items))
	for _, item := range items {
		out = append(out, mapModelEndpointCapability(item))
	}
	return out
}

func mapSecret(in domainsemantic.Secret) *adminv1.Secret {
	var inline *adminv1.InlineSecret
	if in.Ciphertext != nil {
		inline = &adminv1.InlineSecret{Algorithm: in.Ciphertext.Algorithm, NonceB64: in.Ciphertext.NonceB64, CipherB64: in.Ciphertext.CipherB64}
	}
	return &adminv1.Secret{SecretId: in.ID.String(), OwnerType: string(in.OwnerType), OwnerId: in.OwnerID, Kind: string(in.Kind), InlineSecret: inline, ExternalRef: in.ExternalRef, CreateTime: timestamppb.New(in.CreatedAt), UpdateTime: timestamppb.New(in.UpdatedAt)}
}

func mapCredential(in domainsemantic.InferenceCredential) *adminv1.InferenceCredential {
	out := &adminv1.InferenceCredential{CredentialId: in.ID.String(), Key: in.Key, DisplayName: in.Name, ModelEndpointId: in.ModelEndpointID.String(), OwnerType: string(in.OwnerType), OwnerId: in.OwnerID, AuthType: string(in.AuthType), SecretId: in.SecretRef.String(), Status: string(in.Status), IsDefault: in.IsDefault, CreateTime: timestamppb.New(in.CreatedAt), UpdateTime: timestamppb.New(in.UpdatedAt)}
	if in.LastUsedAt != nil {
		out.LastUsedTime = timestamppb.New(*in.LastUsedAt)
	}
	return out
}
func mapCredentials(items []domainsemantic.InferenceCredential) []*adminv1.InferenceCredential {
	out := make([]*adminv1.InferenceCredential, 0, len(items))
	for _, item := range items {
		out = append(out, mapCredential(item))
	}
	return out
}

func mapProcessingScope(in domainsemantic.ProcessingScope) *adminv1.ProcessingScope {
	return &adminv1.ProcessingScope{SpaceId: in.SpaceID.String(), DomainId: uuidOrEmptyAdmin(in.DomainID), SemanticIndexId: uuidOrEmptyAdmin(in.SemanticIndexID), NodeId: uuidOrEmptyAdmin(in.NodeID), IncludeDescendants: in.IncludeDescendants}
}

func mapCredentialGrant(in domainsemantic.CredentialGrant) *adminv1.CredentialGrant {
	out := &adminv1.CredentialGrant{CredentialGrantId: in.ID.String(), CredentialId: in.CredentialID.String(), Scope: mapProcessingScope(in.Scope), Operations: stringsFromOperations(in.Operations), Priority: int32(in.Priority), IsDefault: in.IsDefault, AllowBackgroundUse: in.AllowBackgroundUse, GrantedBy: in.GrantedBy, CreateTime: timestamppb.New(in.CreatedAt)}
	if in.ModelEndpointID != nil {
		out.ModelEndpointId = in.ModelEndpointID.String()
	}
	if in.ModelID != nil {
		out.ModelId = in.ModelID.String()
	}
	if in.ExpiresAt != nil {
		out.ExpireTime = timestamppb.New(*in.ExpiresAt)
	}
	return out
}
func mapCredentialGrants(items []domainsemantic.CredentialGrant) []*adminv1.CredentialGrant {
	out := make([]*adminv1.CredentialGrant, 0, len(items))
	for _, item := range items {
		out = append(out, mapCredentialGrant(item))
	}
	return out
}

func mapInferencePolicy(in domainsemantic.InferencePolicy) *adminv1.InferencePolicy {
	out := &adminv1.InferencePolicy{InferencePolicyId: in.ID.String(), Scope: mapProcessingScope(in.Scope), Effect: string(in.Effect), Operations: stringsFromOperations(in.Operations), NoInference: in.NoInference, AllowedPrivacyClasses: stringsFromPrivacyClasses(in.AllowedPrivacyClasses), DisallowThirdParty: in.DisallowThirdParty, RequireLocalEndpoint: in.RequireLocalEndpoint, Reason: in.Reason, CreatedBy: in.CreatedBy, CreateTime: timestamppb.New(in.CreatedAt)}
	if in.ExpiresAt != nil {
		out.ExpireTime = timestamppb.New(*in.ExpiresAt)
	}
	return out
}
func mapInferencePolicies(items []domainsemantic.InferencePolicy) []*adminv1.InferencePolicy {
	out := make([]*adminv1.InferencePolicy, 0, len(items))
	for _, item := range items {
		out = append(out, mapInferencePolicy(item))
	}
	return out
}

func processingScopeFromProto(in *adminv1.ProcessingScope, defaultSpaceID domainspace.SpaceID) (domainsemantic.ProcessingScope, error) {
	if in == nil {
		return domainsemantic.ProcessingScope{SpaceID: defaultSpaceID}, nil
	}
	spaceID := defaultSpaceID
	if strings.TrimSpace(in.GetSpaceId()) != "" {
		id, err := parseSemanticUUID[domainspace.SpaceID](in.GetSpaceId(), "scope.space_id")
		if err != nil {
			return domainsemantic.ProcessingScope{}, err
		}
		spaceID = id
	}
	domainID, err := optionalSemanticUUID[graph.DomainID](in.GetDomainId(), "scope.domain_id")
	if err != nil {
		return domainsemantic.ProcessingScope{}, err
	}
	indexID, err := optionalSemanticUUID[domainsemantic.SemanticIndexID](in.GetSemanticIndexId(), "scope.semantic_index_id")
	if err != nil {
		return domainsemantic.ProcessingScope{}, err
	}
	nodeID, err := optionalSemanticUUID[graph.NodeID](in.GetNodeId(), "scope.node_id")
	if err != nil {
		return domainsemantic.ProcessingScope{}, err
	}
	return domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: nodeID, IncludeDescendants: in.GetIncludeDescendants()}, nil
}

func uuidOrEmptyAdmin[T ~[16]byte](id T) string {
	if uuid.UUID(id) == uuid.Nil {
		return ""
	}
	return uuid.UUID(id).String()
}

func timeFromProto(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	value := ts.AsTime()
	return &value
}

func paginateAdminInference[T any](items []T, pageSize int, pageToken string) ([]T, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > adminInferenceMaxPageSize {
		pageSize = adminInferenceMaxPageSize
	}
	if start >= len(items) {
		return []T{}, "", nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

func structToMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return value.AsMap()
}
func protoStructAdmin(value map[string]any) *structpb.Struct {
	if value == nil {
		value = map[string]any{}
	}
	out, err := structpb.NewStruct(value)
	if err != nil {
		return &structpb.Struct{}
	}
	return out
}

func authModesFromStrings(values []string) []domainsemantic.AuthMode {
	out := make([]domainsemantic.AuthMode, 0, len(values))
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, domainsemantic.AuthMode(strings.TrimSpace(v)))
		}
	}
	return out
}
func connectorTypesFromStrings(values []string) []domainsemantic.ConnectorType {
	out := make([]domainsemantic.ConnectorType, 0, len(values))
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, domainsemantic.ConnectorType(strings.TrimSpace(v)))
		}
	}
	return out
}
func operationsFromStringsAdmin(values []string) []domainsemantic.Operation {
	out := make([]domainsemantic.Operation, 0, len(values))
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, domainsemantic.Operation(strings.TrimSpace(v)))
		}
	}
	return out
}
func stringsFromAuthModes(values []domainsemantic.AuthMode) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, string(v))
	}
	return out
}
func stringsFromConnectorTypes(values []domainsemantic.ConnectorType) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, string(v))
	}
	return out
}
func stringsFromOperations(values []domainsemantic.Operation) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, string(v))
	}
	return out
}
func privacyClassesFromStringsAdmin(values []string) []domainsemantic.PrivacyClass {
	out := make([]domainsemantic.PrivacyClass, 0, len(values))
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, domainsemantic.PrivacyClass(strings.TrimSpace(v)))
		}
	}
	return out
}
func stringsFromPrivacyClasses(values []domainsemantic.PrivacyClass) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, string(v))
	}
	return out
}

func filterEndpoints(items []domainsemantic.ModelEndpoint, includeDisabled bool) []domainsemantic.ModelEndpoint {
	if includeDisabled {
		return items
	}
	out := items[:0]
	for _, item := range items {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}
func filterVectorStores(items []domainsemantic.VectorStoreBackend, includeDisabled bool) []domainsemantic.VectorStoreBackend {
	if includeDisabled {
		return items
	}
	out := items[:0]
	for _, item := range items {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}
func filterCapabilitiesByEndpoint(items []domainsemantic.ModelEndpointCapability, id domainsemantic.ModelEndpointID) []domainsemantic.ModelEndpointCapability {
	out := items[:0]
	for _, item := range items {
		if item.ModelEndpointID == id {
			out = append(out, item)
		}
	}
	return out
}
func filterCapabilitiesByModel(items []domainsemantic.ModelEndpointCapability, id domainsemantic.InferenceModelID) []domainsemantic.ModelEndpointCapability {
	out := items[:0]
	for _, item := range items {
		if item.ModelID == id {
			out = append(out, item)
		}
	}
	return out
}
func filterCapabilitiesByOperation(items []domainsemantic.ModelEndpointCapability, op domainsemantic.Operation) []domainsemantic.ModelEndpointCapability {
	out := items[:0]
	for _, item := range items {
		if item.Operation == op {
			out = append(out, item)
		}
	}
	return out
}
func filterCapabilitiesEnabled(items []domainsemantic.ModelEndpointCapability) []domainsemantic.ModelEndpointCapability {
	out := items[:0]
	for _, item := range items {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}
func filterCredentialsByOwnerType(items []domainsemantic.InferenceCredential, ownerType domainsemantic.CredentialOwnerType) []domainsemantic.InferenceCredential {
	out := items[:0]
	for _, item := range items {
		if item.OwnerType == ownerType {
			out = append(out, item)
		}
	}
	return out
}
func filterCredentialsByOwnerID(items []domainsemantic.InferenceCredential, ownerID string) []domainsemantic.InferenceCredential {
	out := items[:0]
	for _, item := range items {
		if item.OwnerID == ownerID {
			out = append(out, item)
		}
	}
	return out
}
func filterCredentialsByEndpoint(items []domainsemantic.InferenceCredential, id domainsemantic.ModelEndpointID) []domainsemantic.InferenceCredential {
	out := items[:0]
	for _, item := range items {
		if item.ModelEndpointID == id {
			out = append(out, item)
		}
	}
	return out
}
func filterCredentialsActive(items []domainsemantic.InferenceCredential) []domainsemantic.InferenceCredential {
	out := items[:0]
	for _, item := range items {
		if item.Status == "" || item.Status == domainsemantic.CredentialStatusActive {
			out = append(out, item)
		}
	}
	return out
}
func filterGrantsByCredential(items []domainsemantic.CredentialGrant, id domainsemantic.InferenceCredentialID) []domainsemantic.CredentialGrant {
	out := items[:0]
	for _, item := range items {
		if item.CredentialID == id {
			out = append(out, item)
		}
	}
	return out
}
func filterGrantsUnexpired(items []domainsemantic.CredentialGrant, now time.Time) []domainsemantic.CredentialGrant {
	out := items[:0]
	for _, item := range items {
		if item.ExpiresAt == nil || item.ExpiresAt.After(now) {
			out = append(out, item)
		}
	}
	return out
}
func filterPoliciesByEffect(items []domainsemantic.InferencePolicy, effect domainsemantic.PolicyEffect) []domainsemantic.InferencePolicy {
	out := items[:0]
	for _, item := range items {
		if item.Effect == effect {
			out = append(out, item)
		}
	}
	return out
}
func filterPoliciesUnexpired(items []domainsemantic.InferencePolicy, now time.Time) []domainsemantic.InferencePolicy {
	out := items[:0]
	for _, item := range items {
		if item.ExpiresAt == nil || item.ExpiresAt.After(now) {
			out = append(out, item)
		}
	}
	return out
}

func mapAdminInferenceError(err error, action string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(msg, "required") || strings.Contains(msg, "invalid") {
		return status.Error(codes.InvalidArgument, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
