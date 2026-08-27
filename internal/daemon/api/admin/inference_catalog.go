package admin

import (
	"context"
	"sort"
	"strings"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Catalog and package RPC handlers for AdminInferenceService.

func (s *AdminInferenceService) ApplyInferencePackage(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
	principal, err := s.requireInferenceManage(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetName()) == "" || strings.TrimSpace(req.GetVersion()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name and version are required")
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
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
		if err := s.syncInferenceEndpoint(ctx, stored); err != nil {
			return nil, mapAdminInferenceError(err, "sync inference endpoint")
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
		if err := s.syncInferenceModel(ctx, stored); err != nil {
			return nil, mapAdminInferenceError(err, "sync inference model")
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
		if err := s.syncInferenceVectorStore(ctx, stored); err != nil {
			return nil, mapAdminInferenceError(err, "sync inference vector store")
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
		if err := s.syncInferenceCapability(ctx, capability); err != nil {
			return nil, mapAdminInferenceError(err, "sync inference capability")
		}
		capabilities = append(capabilities, capability)
	}
	counts := map[string]int{"model_endpoints": len(req.GetModelEndpoints()), "models": len(req.GetModels()), "model_endpoint_capabilities": len(req.GetModelEndpointCapabilities()), "vector_stores": len(req.GetVectorStores())}
	pkg, err := mgr.UpsertPackage(ctx, domainsemantic.InferencePackage{Name: req.GetName(), Version: req.GetVersion(), Source: req.GetSource(), Checksum: req.GetChecksum(), InstalledBy: principal.PrincipalID, DefinitionCounts: counts})
	if err != nil {
		return nil, mapAdminInferenceError(err, "upsert inference package")
	}
	if err := s.syncInferencePackage(ctx, pkg); err != nil {
		return nil, mapAdminInferenceError(err, "sync inference package")
	}
	return &adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse{Package: mapInferencePackage(pkg), ModelEndpoints: mapModelEndpoints(endpoints), Models: mapInferenceModels(models), VectorStores: mapVectorStores(stores), ModelEndpointCapabilities: mapModelEndpointCapabilities(capabilities)}, nil
}

func (s *AdminInferenceService) ListInferencePackages(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceListInferencePackagesRequest) (*adminv1.AdminInferenceCatalogServiceListInferencePackagesResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceCatalogRead, inferenceScope("", "")); err != nil {
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
	return &adminv1.AdminInferenceCatalogServiceListInferencePackagesResponse{Packages: mapInferencePackages(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModelEndpoints(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceListModelEndpointsRequest) (*adminv1.AdminInferenceCatalogServiceListModelEndpointsResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceCatalogRead, inferenceScope("", "")); err != nil {
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
	return &adminv1.AdminInferenceCatalogServiceListModelEndpointsResponse{ModelEndpoints: mapModelEndpoints(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModels(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceListModelsRequest) (*adminv1.AdminInferenceCatalogServiceListModelsResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceCatalogRead, inferenceScope("", "")); err != nil {
		return nil, err
	}
	items, err := s.semantic.GlobalManager().ListModels(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list models")
	}
	kind := strings.TrimSpace(req.GetKind())
	if kind != "" {
		filtered := items[:0]
		for _, item := range items {
			if string(item.Kind) == kind {
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
	return &adminv1.AdminInferenceCatalogServiceListModelsResponse{Models: mapInferenceModels(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListVectorStores(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceListVectorStoresRequest) (*adminv1.AdminInferenceCatalogServiceListVectorStoresResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceCatalogRead, inferenceScope("", "")); err != nil {
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
	return &adminv1.AdminInferenceCatalogServiceListVectorStoresResponse{VectorStores: mapVectorStores(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) ListModelEndpointCapabilities(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceListModelEndpointCapabilitiesRequest) (*adminv1.AdminInferenceCatalogServiceListModelEndpointCapabilitiesResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceCatalogRead, inferenceScope("", "")); err != nil {
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
	return &adminv1.AdminInferenceCatalogServiceListModelEndpointCapabilitiesResponse{ModelEndpointCapabilities: mapModelEndpointCapabilities(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) SetModelEndpointEnabled(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceSetModelEndpointEnabledRequest) (*adminv1.AdminInferenceCatalogServiceSetModelEndpointEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveModelEndpointID(ctx, firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()))
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
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
			if err := s.syncInferenceEndpoint(ctx, stored); err != nil {
				return nil, mapAdminInferenceError(err, "sync inference endpoint")
			}
			return &adminv1.AdminInferenceCatalogServiceSetModelEndpointEnabledResponse{ModelEndpoint: mapModelEndpoint(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "model endpoint not found")
}

func (s *AdminInferenceService) SetVectorStoreEnabled(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceSetVectorStoreEnabledRequest) (*adminv1.AdminInferenceCatalogServiceSetVectorStoreEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	id, err := s.resolveVectorStoreID(ctx, firstNonEmptyAdmin(req.GetVectorStoreId(), req.GetVectorStore()))
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
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
			if err := s.syncInferenceVectorStore(ctx, stored); err != nil {
				return nil, mapAdminInferenceError(err, "sync inference vector store")
			}
			return &adminv1.AdminInferenceCatalogServiceSetVectorStoreEnabledResponse{VectorStore: mapVectorStore(stored)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "vector store not found")
}

func (s *AdminInferenceService) SetModelEndpointCapabilityEnabled(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceSetModelEndpointCapabilityEnabledRequest) (*adminv1.AdminInferenceCatalogServiceSetModelEndpointCapabilityEnabledResponse, error) {
	if _, err := s.requireInferenceManage(ctx); err != nil {
		return nil, err
	}
	cap, err := s.resolveCapability(ctx, req.GetModelEndpointCapabilityId(), firstNonEmptyAdmin(req.GetModelEndpointId(), req.GetModelEndpoint()), firstNonEmptyAdmin(req.GetModelId(), req.GetModel()), req.GetOperation())
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	cap.Enabled = req.GetEnabled()
	stored, err := s.semantic.GlobalManager().UpsertModelEndpointCapability(ctx, cap)
	if err != nil {
		return nil, mapAdminInferenceError(err, "update model endpoint capability")
	}
	if err := s.syncInferenceCapability(ctx, stored); err != nil {
		return nil, mapAdminInferenceError(err, "sync inference capability")
	}
	return &adminv1.AdminInferenceCatalogServiceSetModelEndpointCapabilityEnabledResponse{ModelEndpointCapability: mapModelEndpointCapability(stored)}, nil
}

func (s *AdminInferenceService) DeleteModelEndpoint(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceDeleteModelEndpointRequest) (*adminv1.AdminInferenceCatalogServiceDeleteModelEndpointResponse, error) {
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
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.semantic.GlobalManager().DeleteModelEndpoint(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model endpoint")
	}
	if err := s.deleteSyncedInferenceEndpoint(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference endpoint")
	}
	return &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointResponse{ModelEndpointId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteModel(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceDeleteModelRequest) (*adminv1.AdminInferenceCatalogServiceDeleteModelResponse, error) {
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
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.semantic.GlobalManager().DeleteModel(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model")
	}
	if err := s.deleteSyncedInferenceModel(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference model")
	}
	return &adminv1.AdminInferenceCatalogServiceDeleteModelResponse{ModelId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteVectorStore(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceDeleteVectorStoreRequest) (*adminv1.AdminInferenceCatalogServiceDeleteVectorStoreResponse, error) {
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
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.semantic.GlobalManager().DeleteVectorStore(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete vector store")
	}
	if err := s.deleteSyncedInferenceVectorStore(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference vector store")
	}
	return &adminv1.AdminInferenceCatalogServiceDeleteVectorStoreResponse{VectorStoreId: id.String()}, nil
}

func (s *AdminInferenceService) DeleteModelEndpointCapability(ctx context.Context, req *adminv1.AdminInferenceCatalogServiceDeleteModelEndpointCapabilityRequest) (*adminv1.AdminInferenceCatalogServiceDeleteModelEndpointCapabilityResponse, error) {
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
	ctx, release, err := s.beginSemanticMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.semantic.GlobalManager().DeleteModelEndpointCapability(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete model endpoint capability")
	}
	if err := s.deleteSyncedInferenceCapability(ctx, id); err != nil {
		return nil, mapAdminInferenceError(err, "delete inference capability")
	}
	return &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointCapabilityResponse{ModelEndpointCapabilityId: id.String()}, nil
}
