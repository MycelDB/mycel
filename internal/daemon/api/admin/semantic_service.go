package admin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const adminSemanticMaxPageSize = 500

type AdminSemanticService struct {
	adminv1.UnimplementedAdminSemanticServiceServer
	semantic   daemonsemantic.Manager
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

func NewAdminSemanticService(semantic daemonsemantic.Manager, spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminSemanticService {
	return &AdminSemanticService{semantic: semantic, spaces: spaces, authorizer: authorizer}
}

func (s *AdminSemanticService) ListSemanticIndexes(ctx context.Context, req *adminv1.AdminSemanticServiceListSemanticIndexesRequest) (*adminv1.AdminSemanticServiceListSemanticIndexesResponse, error) {
	if _, err := s.requireSemanticManage(ctx); err != nil {
		return nil, err
	}
	spaceID, domainID, err := s.validateSpaceDomain(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	indexes, err := s.semantic.ListIndexes(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list semantic indexes")
	}
	if req.GetIncludeDisabled() {
		spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
		if err != nil {
			return nil, mapAdminSemanticError(err, "open semantic space manager")
		}
		all, err := spaceMgr.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, mapAdminSemanticError(err, "list semantic indexes")
		}
		indexes = indexes[:0]
		for _, index := range all {
			if index.SpaceID == spaceID && index.DomainID == domainID && domainsemantic.IsSearchSemanticIndexPurpose(index.Purpose) {
				indexes = append(indexes, index)
			}
		}
	}
	states, endpoints, models, stores, err := s.semanticDisplayMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic metadata")
	}
	items := make([]*clientv1.SemanticIndex, 0, len(indexes))
	for _, index := range indexes {
		items = append(items, clientapi.MapSemanticIndexProto(index, states[index.ID], endpoints, models, stores))
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].GetKey() < items[j].GetKey() })
	page, next, err := paginateAdminSemantic(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminSemanticServiceListSemanticIndexesResponse{Indexes: page, NextPageToken: next}, nil
}

func (s *AdminSemanticService) UpsertSemanticIndex(ctx context.Context, req *adminv1.UpsertSemanticIndexRequest) (*adminv1.UpsertSemanticIndexResponse, error) {
	if _, err := s.requireSemanticManage(ctx); err != nil {
		return nil, err
	}
	spaceID, domainID, err := s.validateSpaceDomain(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	key := strings.TrimSpace(req.GetKey())
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	endpointID, err := parseSemanticUUID[domainsemantic.ModelEndpointID](req.GetModelEndpointId(), "model_endpoint_id")
	if err != nil {
		return nil, err
	}
	modelID, err := parseSemanticUUID[domainsemantic.InferenceModelID](req.GetModelId(), "model_id")
	if err != nil {
		return nil, err
	}
	vectorStoreID, err := parseSemanticUUID[domainsemantic.VectorStoreID](req.GetVectorStoreId(), "vector_store_id")
	if err != nil {
		return nil, err
	}
	indexID, err := optionalSemanticUUID[domainsemantic.SemanticIndexID](req.GetSemanticIndexId(), "semantic_index_id")
	if err != nil {
		return nil, err
	}
	if err := s.validateSemanticBinding(ctx, endpointID, modelID, vectorStoreID); err != nil {
		return nil, err
	}
	capabilityID, err := s.capabilityFor(ctx, endpointID, modelID)
	if err != nil {
		return nil, err
	}
	policy, err := sourcePolicyFromProto(req.GetSourcePolicy())
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	purpose := domainsemantic.NormalizeSemanticIndexPurpose(domainsemantic.SemanticIndexPurpose(strings.TrimSpace(req.GetPurpose())))
	index, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{ID: indexID, SpaceID: spaceID, DomainID: domainID, Key: key, Name: firstNonEmptyAdmin(req.GetDisplayName(), key), Purpose: purpose, SourcePolicy: policy, ModelEndpointID: endpointID, ModelID: modelID, ModelEndpointCapabilityID: capabilityID, VectorStoreID: vectorStoreID, Enabled: req.GetEnabled()})
	if err != nil {
		return nil, mapAdminSemanticError(err, "upsert semantic index")
	}
	states, endpoints, models, stores, err := s.semanticDisplayMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "load semantic metadata")
	}
	return &adminv1.UpsertSemanticIndexResponse{Index: clientapi.MapSemanticIndexProto(index, states[index.ID], endpoints, models, stores)}, nil
}

func (s *AdminSemanticService) DeleteSemanticIndex(ctx context.Context, req *adminv1.DeleteSemanticIndexRequest) (*adminv1.DeleteSemanticIndexResponse, error) {
	if _, err := s.requireSemanticManage(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	indexID, err := parseSemanticUUID[domainsemantic.SemanticIndexID](req.GetSemanticIndexId(), "semantic_index_id")
	if err != nil {
		return nil, err
	}
	if _, err := s.spaces.GetSpace(ctx, spaceID.String()); err != nil {
		return nil, mapSpaceError(err, "get space")
	}
	ctx, release, err := s.semantic.BeginMutation(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "begin semantic mutation")
	}
	defer release()
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, mapAdminSemanticError(err, "open semantic space manager")
	}
	indexes, err := spaceMgr.ListSemanticIndexes(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list semantic indexes")
	}
	found := false
	for _, index := range indexes {
		if index.ID == indexID {
			found = true
			break
		}
	}
	if !found {
		return nil, status.Error(codes.NotFound, "semantic index not found")
	}
	grants, err := spaceMgr.ListCredentialGrants(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list credential grants")
	}
	policies, err := spaceMgr.ListInferencePolicies(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list inference policies")
	}
	grantRefs := 0
	for _, grant := range grants {
		if grant.Scope.SemanticIndexID == indexID {
			grantRefs++
		}
	}
	policyRefs := 0
	for _, policy := range policies {
		if policy.Scope.SemanticIndexID == indexID {
			policyRefs++
		}
	}
	decisions, err := spaceMgr.ListPolicyDecisions(ctx)
	if err != nil {
		return nil, mapAdminSemanticError(err, "list policy decisions")
	}
	decisionRefs := 0
	for _, decision := range decisions {
		if decision.Scope.SemanticIndexID == indexID {
			decisionRefs++
		}
	}
	if (grantRefs > 0 || policyRefs > 0 || decisionRefs > 0) && !req.GetPurgeReferences() {
		return nil, status.Errorf(codes.FailedPrecondition, "semantic index has references: credential_grants=%d inference_policies=%d policy_decisions=%d; pass purge_references to delete them", grantRefs, policyRefs, decisionRefs)
	}
	if err := spaceMgr.DeleteSemanticIndex(ctx, indexID, req.GetPurgeReferences()); err != nil {
		return nil, mapAdminSemanticError(err, "delete semantic index")
	}
	vectorsPurged := false
	if req.GetPurgeVectors() {
		if err := s.semantic.PurgeVectorIndex(ctx, spaceID, indexID); err != nil {
			return nil, mapAdminSemanticError(err, "purge semantic vectors")
		}
		vectorsPurged = true
	}
	return &adminv1.DeleteSemanticIndexResponse{SemanticIndexId: indexID.String(), CredentialGrantsDeleted: int32(grantRefs), InferencePoliciesDeleted: int32(policyRefs), PolicyDecisionsDeleted: int32(decisionRefs), VectorsPurged: vectorsPurged}, nil
}

func (s *AdminSemanticService) validateSpaceDomain(ctx context.Context, spaceIDText string, domainIDText string) (domainspace.SpaceID, graph.DomainID, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](spaceIDText, "space_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	domainID, err := parseSemanticUUID[graph.DomainID](domainIDText, "domain_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if _, err := s.spaces.GetSpace(ctx, spaceID.String()); err != nil {
		return uuid.Nil, uuid.Nil, mapSpaceError(err, "get space")
	}
	return spaceID, domainID, nil
}

func (s *AdminSemanticService) validateSemanticBinding(ctx context.Context, endpointID domainsemantic.ModelEndpointID, modelID domainsemantic.InferenceModelID, vectorStoreID domainsemantic.VectorStoreID) error {
	global := s.semantic.GlobalManager()
	endpoints, err := global.ListModelEndpoints(ctx)
	if err != nil {
		return mapAdminSemanticError(err, "list model endpoints")
	}
	endpointFound := false
	for _, endpoint := range endpoints {
		if endpoint.ID == endpointID && endpoint.Enabled {
			endpointFound = true
			break
		}
	}
	if !endpointFound {
		return status.Error(codes.NotFound, "enabled model endpoint not found")
	}
	models, err := global.ListModels(ctx)
	if err != nil {
		return mapAdminSemanticError(err, "list models")
	}
	modelFound := false
	for _, model := range models {
		if model.ID == modelID && model.Operation == domainsemantic.OperationEmbeddings {
			modelFound = true
			break
		}
	}
	if !modelFound {
		return status.Error(codes.NotFound, "embedding model not found")
	}
	stores, err := global.ListVectorStores(ctx)
	if err != nil {
		return mapAdminSemanticError(err, "list vector stores")
	}
	storeFound := false
	for _, store := range stores {
		if store.ID == vectorStoreID && store.Enabled {
			storeFound = true
			break
		}
	}
	if !storeFound {
		return status.Error(codes.NotFound, "enabled vector store not found")
	}
	return nil
}

func (s *AdminSemanticService) capabilityFor(ctx context.Context, endpointID domainsemantic.ModelEndpointID, modelID domainsemantic.InferenceModelID) (domainsemantic.ModelEndpointCapabilityID, error) {
	caps, err := s.semantic.GlobalManager().ListModelEndpointCapabilities(ctx)
	if err != nil {
		return uuid.Nil, mapAdminSemanticError(err, "list model endpoint capabilities")
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == endpointID && cap.ModelID == modelID && cap.Operation == domainsemantic.OperationEmbeddings && cap.Enabled {
			return cap.ID, nil
		}
	}
	return uuid.Nil, status.Error(codes.NotFound, "enabled embedding capability not found for endpoint and model")
}

func (s *AdminSemanticService) semanticDisplayMetadata(ctx context.Context, spaceID domainspace.SpaceID) (map[domainsemantic.SemanticIndexID]domainsemantic.SemanticIndexState, map[domainsemantic.ModelEndpointID]domainsemantic.ModelEndpoint, map[domainsemantic.InferenceModelID]domainsemantic.InferenceModel, map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend, error) {
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	stateRows, err := spaceMgr.ListIndexStates(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	states := map[domainsemantic.SemanticIndexID]domainsemantic.SemanticIndexState{}
	for _, st := range stateRows {
		states[st.SemanticIndexID] = st
	}
	global := s.semantic.GlobalManager()
	endpointRows, err := global.ListModelEndpoints(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	modelRows, err := global.ListModels(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	storeRows, err := global.ListVectorStores(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	endpoints := map[domainsemantic.ModelEndpointID]domainsemantic.ModelEndpoint{}
	for _, endpoint := range endpointRows {
		endpoints[endpoint.ID] = endpoint
	}
	models := map[domainsemantic.InferenceModelID]domainsemantic.InferenceModel{}
	for _, model := range modelRows {
		models[model.ID] = model
	}
	stores := map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend{}
	for _, store := range storeRows {
		stores[store.ID] = store
	}
	return states, endpoints, models, stores, nil
}

func (s *AdminSemanticService) requireSemanticManage(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required semantic capability")
	}
	return principal, nil
}

func sourcePolicyFromProto(in *adminv1.SemanticSourcePolicy) (domainsemantic.SemanticSourcePolicy, error) {
	if in == nil {
		return domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree}, nil
	}
	extraction := domainsemantic.SourceExtraction(strings.TrimSpace(in.GetExtraction()))
	if extraction == "" {
		extraction = domainsemantic.SourceExtractionSubtree
	}
	if extraction != domainsemantic.SourceExtractionSelf && extraction != domainsemantic.SourceExtractionSubtree {
		return domainsemantic.SemanticSourcePolicy{}, status.Error(codes.InvalidArgument, "source_policy.extraction must be self or subtree")
	}
	out := domainsemantic.SemanticSourcePolicy{Extraction: extraction, TemplateKeys: append([]string(nil), in.GetTemplateKeys()...), IncludeProps: append([]string(nil), in.GetIncludeProps()...), MinimumTextLength: int(in.GetMinimumTextLength())}
	if in.MaxDepth != nil {
		v := int(in.GetMaxDepth())
		out.MaxDepth = &v
	}
	return out, nil
}

func parseSemanticUUID[T ~[16]byte](raw string, name string) (T, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		var zero T
		return zero, status.Errorf(codes.InvalidArgument, "%s must be a UUID", name)
	}
	return T(id), nil
}

func optionalSemanticUUID[T ~[16]byte](raw string, name string) (T, error) {
	if strings.TrimSpace(raw) == "" {
		var zero T
		return zero, nil
	}
	return parseSemanticUUID[T](raw, name)
}

func paginateAdminSemantic[T any](items []T, pageSize int, pageToken string) ([]T, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > adminSemanticMaxPageSize {
		pageSize = adminSemanticMaxPageSize
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

func mapAdminSemanticError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
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

func firstNonEmptyAdmin(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
