package admin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proto/domain mapping, filtering, pagination, and error helpers for AdminInferenceService.

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
	return domainsemantic.InferenceModel{ID: id, Key: in.GetKey(), Kind: semanticModelKindFromProto(in), ModelName: in.GetModelName(), ConnectorTypes: connectorTypesFromStrings(in.GetConnectorTypes()), Dimensions: int(in.GetDimensions()), InputModalities: append([]string(nil), in.GetInputModalities()...), OutputModalities: append([]string(nil), in.GetOutputModalities()...), VectorSpaceKey: in.GetVectorSpaceKey(), Metadata: structToMap(in.GetMetadata())}, nil
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
	return &adminv1.InferenceModel{ModelId: in.ID.String(), Key: in.Key, Kind: string(in.Kind), KindValue: semanticModelKindToProto(in.Kind), ModelName: in.ModelName, ConnectorTypes: stringsFromConnectorTypes(in.ConnectorTypes), Dimensions: int32(in.Dimensions), InputModalities: append([]string(nil), in.InputModalities...), OutputModalities: append([]string(nil), in.OutputModalities...), VectorSpaceKey: in.VectorSpaceKey, Metadata: protoStructAdmin(in.Metadata)}
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
	return &adminv1.Secret{SecretId: in.ID.String(), OwnerType: string(in.OwnerType), OwnerId: in.OwnerID, Kind: string(in.Kind), CreateTime: timestamppb.New(in.CreatedAt), UpdateTime: timestamppb.New(in.UpdatedAt), SecretSuffix: in.SecretSuffix}
}

func mapCredential(in domainsemantic.InferenceCredential) *adminv1.IntelligenceCredential {
	out := &adminv1.IntelligenceCredential{CredentialId: in.ID.String(), Key: in.Key, DisplayName: in.Name, ModelEndpointId: in.ModelEndpointID.String(), OwnerType: string(in.OwnerType), OwnerId: in.OwnerID, AuthType: string(in.AuthType), SecretId: in.SecretRef.String(), SecretSuffix: in.SecretSuffix, Status: string(in.Status), IsDefault: in.IsDefault, CreateTime: timestamppb.New(in.CreatedAt), UpdateTime: timestamppb.New(in.UpdatedAt)}
	if in.LastUsedAt != nil {
		out.LastUsedTime = timestamppb.New(*in.LastUsedAt)
	}
	return out
}

func mapCredentials(items []domainsemantic.InferenceCredential) []*adminv1.IntelligenceCredential {
	out := make([]*adminv1.IntelligenceCredential, 0, len(items))
	for _, item := range items {
		out = append(out, mapCredential(item))
	}
	return out
}

func mapProcessingScope(in domainsemantic.ProcessingScope) *adminv1.ProcessingScope {
	return &adminv1.ProcessingScope{SpaceId: in.SpaceID.String(), DomainId: uuidOrEmptyAdmin(in.DomainID), SemanticRuleId: firstNonEmptyAdmin(uuidOrEmptyAdmin(in.SemanticRuleID), uuidOrEmptyAdmin(in.SemanticIndexID)), NodeId: uuidOrEmptyAdmin(in.NodeID), IncludeDescendants: in.IncludeDescendants}
}

func mapCredentialGrant(in domainsemantic.CredentialGrant) *adminv1.CredentialGrant {
	out := &adminv1.CredentialGrant{CredentialGrantId: in.ID.String(), CredentialId: in.CredentialID.String(), Scope: mapProcessingScope(in.Scope), Operations: stringsFromOperations(in.Operations), Priority: int32(in.Priority), IsDefault: in.IsDefault, AllowBackgroundUse: in.AllowBackgroundUse, GranteePrincipalIds: append([]string(nil), in.GranteePrincipalIDs...), AllowOnBehalfOfPrincipalIds: append([]string(nil), in.AllowOnBehalfOfPrincipalIDs...), GrantedBy: in.GrantedBy, CreateTime: timestamppb.New(in.CreatedAt)}
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

func mapAccessPolicy(in domainsemantic.InferencePolicy) *adminv1.AccessPolicy {
	out := &adminv1.AccessPolicy{AccessPolicyId: in.ID.String(), Scope: mapProcessingScope(in.Scope), Effect: string(in.Effect), Operations: stringsFromOperations(in.Operations), NoIntelligence: in.NoInference, AllowedPrivacyClasses: stringsFromPrivacyClasses(in.AllowedPrivacyClasses), DisallowThirdParty: in.DisallowThirdParty, RequireLocalEndpoint: in.RequireLocalEndpoint, Reason: in.Reason, CreatedBy: in.CreatedBy, CreateTime: timestamppb.New(in.CreatedAt)}
	if in.ExpiresAt != nil {
		out.ExpireTime = timestamppb.New(*in.ExpiresAt)
	}
	return out
}

func mapAccessPolicies(items []domainsemantic.InferencePolicy) []*adminv1.AccessPolicy {
	out := make([]*adminv1.AccessPolicy, 0, len(items))
	for _, item := range items {
		out = append(out, mapAccessPolicy(item))
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
	ruleID, err := optionalSemanticUUID[domainsemantic.SemanticRuleID](in.GetSemanticRuleId(), "scope.semantic_rule_id")
	if err != nil {
		return domainsemantic.ProcessingScope{}, err
	}
	nodeID, err := optionalSemanticUUID[graph.NodeID](in.GetNodeId(), "scope.node_id")
	if err != nil {
		return domainsemantic.ProcessingScope{}, err
	}
	return domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, IncludeDescendants: in.GetIncludeDescendants()}, nil
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

func cleanStringListAdmin(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
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

func inferenceProfileFromProto(in *adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest, principalID string) (domaininference.Profile, error) {
	if in == nil {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "inference profile is required")
	}
	spaceID := strings.TrimSpace(in.GetSpaceId())
	if spaceID == "" {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "space_id is required")
	}
	if _, err := uuid.Parse(spaceID); err != nil {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "space_id must be a UUID")
	}
	if strings.TrimSpace(in.GetKey()) == "" {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "key is required")
	}
	op := inferenceOperationFromProto(in.GetOperation())
	if op == "" {
		return domaininference.Profile{}, status.Error(codes.InvalidArgument, "operation is required")
	}
	return domaininference.Profile{SpaceID: spaceID, Key: in.GetKey(), DisplayName: firstNonEmptyAdmin(in.GetDisplayName(), in.GetKey()), Description: in.GetDescription(), Operation: op, Purpose: in.GetPurpose(), DomainIDs: append([]string(nil), in.GetDomainIds()...), CapabilityRefs: append([]string(nil), in.GetCapabilityRefs()...), EndpointRefs: append([]string(nil), in.GetEndpointRefs()...), ModelRefs: append([]string(nil), in.GetModelRefs()...), RequiredFeatures: append([]string(nil), in.GetRequiredFeatures()...), PrivacyRequirement: privacyRequirementFromProto(in.GetPrivacyRequirement()), DefaultParameters: parametersFromProto(in.GetDefaultParameters()), Enabled: in.GetEnabled(), CreatedBy: principalID, Metadata: structToMap(in.GetMetadata())}, nil
}

func mapIntelligenceProfile(in domaininference.Profile) *adminv1.IntelligenceProfile {
	return &adminv1.IntelligenceProfile{IntelligenceProfileId: in.ID.String(), SpaceId: in.SpaceID, Key: in.Key, DisplayName: in.DisplayName, Description: in.Description, Operation: inferenceOperationToProto(in.Operation), Purpose: in.Purpose, DomainIds: append([]string(nil), in.DomainIDs...), CapabilityRefs: append([]string(nil), in.CapabilityRefs...), EndpointRefs: append([]string(nil), in.EndpointRefs...), ModelRefs: append([]string(nil), in.ModelRefs...), RequiredFeatures: append([]string(nil), in.RequiredFeatures...), PrivacyRequirement: privacyRequirementToProto(in.PrivacyRequirement), DefaultParameters: parametersToProto(in.DefaultParameters), Enabled: in.Enabled, CreatedBy: in.CreatedBy, CreateTime: timestamppb.New(in.CreatedAt), UpdateTime: timestamppb.New(in.UpdatedAt), Metadata: protoStructAdmin(in.Metadata)}
}

func mapIntelligenceProfiles(items []domaininference.Profile) []*adminv1.IntelligenceProfile {
	out := make([]*adminv1.IntelligenceProfile, 0, len(items))
	for _, item := range items {
		out = append(out, mapIntelligenceProfile(item))
	}
	return out
}

func semanticProfileFromProto(in *adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest, principalID string) (domainsemantic.IntelligenceProfile, error) {
	if in == nil {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "inference profile is required")
	}
	spaceIDText := strings.TrimSpace(in.GetSpaceId())
	if spaceIDText == "" {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "space_id is required")
	}
	spaceID, err := uuid.Parse(spaceIDText)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "space_id must be a UUID")
	}
	if strings.TrimSpace(in.GetKey()) == "" {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "key is required")
	}
	op := semanticOperationFromProto(in.GetOperation())
	if op == "" {
		return domainsemantic.IntelligenceProfile{}, status.Error(codes.InvalidArgument, "operation is required")
	}
	profile := domainsemantic.IntelligenceProfile{SpaceID: domainspace.SpaceID(spaceID), Key: in.GetKey(), DisplayName: firstNonEmptyAdmin(in.GetDisplayName(), in.GetKey()), Description: in.GetDescription(), Operation: op, Purpose: in.GetPurpose(), DomainIDs: append([]string(nil), in.GetDomainIds()...), CapabilityRefs: append([]string(nil), in.GetCapabilityRefs()...), EndpointRefs: append([]string(nil), in.GetEndpointRefs()...), ModelRefs: append([]string(nil), in.GetModelRefs()...), RequiredFeatures: append([]string(nil), in.GetRequiredFeatures()...), PrivacyRequirement: semanticPrivacyRequirementFromProto(in.GetPrivacyRequirement()), DefaultParameters: semanticParametersFromProto(in.GetDefaultParameters()), Enabled: in.GetEnabled(), CreatedBy: principalID, Metadata: structToMap(in.GetMetadata())}
	return domainsemantic.NormalizeIntelligenceProfile(profile), nil
}

func mapSemanticIntelligenceProfile(in domainsemantic.IntelligenceProfile) *adminv1.IntelligenceProfile {
	return mapIntelligenceProfile(semanticProfileToInference(in))
}

func mapSemanticIntelligenceProfiles(items []domainsemantic.IntelligenceProfile) []*adminv1.IntelligenceProfile {
	out := make([]*adminv1.IntelligenceProfile, 0, len(items))
	for _, item := range items {
		out = append(out, mapSemanticIntelligenceProfile(item))
	}
	return out
}

func semanticProfileToInference(in domainsemantic.IntelligenceProfile) domaininference.Profile {
	return domaininference.Profile{ID: domaininference.ProfileID(in.ID), SpaceID: in.SpaceID.String(), Key: in.Key, DisplayName: in.DisplayName, Description: in.Description, Operation: domaininference.Operation(in.Operation), Purpose: in.Purpose, DomainIDs: append([]string(nil), in.DomainIDs...), CapabilityRefs: append([]string(nil), in.CapabilityRefs...), EndpointRefs: append([]string(nil), in.EndpointRefs...), ModelRefs: append([]string(nil), in.ModelRefs...), RequiredFeatures: append([]string(nil), in.RequiredFeatures...), PrivacyRequirement: semanticPrivacyRequirementToInference(in.PrivacyRequirement), DefaultParameters: semanticParametersToInference(in.DefaultParameters), Enabled: in.Enabled, CreatedBy: in.CreatedBy, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticProfileFromInference(in domaininference.Profile) domainsemantic.IntelligenceProfile {
	spaceID, _ := uuid.Parse(strings.TrimSpace(in.SpaceID))
	return domainsemantic.NormalizeIntelligenceProfile(domainsemantic.IntelligenceProfile{ID: domainsemantic.IntelligenceProfileID(in.ID), SpaceID: domainspace.SpaceID(spaceID), Key: in.Key, DisplayName: in.DisplayName, Description: in.Description, Operation: domainsemantic.Operation(in.Operation), Purpose: in.Purpose, DomainIDs: append([]string(nil), in.DomainIDs...), CapabilityRefs: append([]string(nil), in.CapabilityRefs...), EndpointRefs: append([]string(nil), in.EndpointRefs...), ModelRefs: append([]string(nil), in.ModelRefs...), RequiredFeatures: append([]string(nil), in.RequiredFeatures...), PrivacyRequirement: semanticPrivacyRequirementFromInference(in.PrivacyRequirement), DefaultParameters: semanticParametersFromInference(in.DefaultParameters), Enabled: in.Enabled, CreatedBy: in.CreatedBy, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata})
}

func semanticPrivacyRequirementFromProto(in *commonv1.InferencePrivacyRequirement) domainsemantic.IntelligencePrivacyRequirement {
	if in == nil {
		return domainsemantic.IntelligencePrivacyRequirement{}
	}
	classes := make([]domainsemantic.PrivacyClass, 0, len(in.GetAllowedPrivacyClasses()))
	for _, value := range in.GetAllowedPrivacyClasses() {
		if cls := semanticPrivacyClassFromProto(value); cls != "" {
			classes = append(classes, cls)
		}
	}
	return domainsemantic.IntelligencePrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.GetRequireLocalEndpoint(), DisallowThirdParty: in.GetDisallowThirdParty()}
}

func semanticPrivacyRequirementToInference(in domainsemantic.IntelligencePrivacyRequirement) domaininference.PrivacyRequirement {
	classes := make([]domaininference.PrivacyClass, 0, len(in.AllowedPrivacyClasses))
	for _, value := range in.AllowedPrivacyClasses {
		classes = append(classes, inferencePrivacyClassFromSemantic(value))
	}
	return domaininference.PrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty}
}

func semanticPrivacyRequirementFromInference(in domaininference.PrivacyRequirement) domainsemantic.IntelligencePrivacyRequirement {
	classes := make([]domainsemantic.PrivacyClass, 0, len(in.AllowedPrivacyClasses))
	for _, value := range in.AllowedPrivacyClasses {
		switch value {
		case domaininference.PrivacyClassLocalOnly:
			classes = append(classes, domainsemantic.PrivacyClassLocalOnly)
		case domaininference.PrivacyClassPrivate:
			classes = append(classes, domainsemantic.PrivacyClassEnterprisePrivate)
		case domaininference.PrivacyClassThirdParty:
			classes = append(classes, domainsemantic.PrivacyClassThirdParty)
		}
	}
	return domainsemantic.IntelligencePrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty}
}

func semanticParametersFromProto(in *commonv1.InferenceParameters) domainsemantic.IntelligenceParameters {
	if in == nil {
		return domainsemantic.IntelligenceParameters{}
	}
	var temp *float64
	if in.Temperature != nil {
		v := in.GetTemperature()
		temp = &v
	}
	return domainsemantic.IntelligenceParameters{Temperature: temp, MaxInputTokens: int(in.GetMaxInputTokens()), MaxOutputTokens: int(in.GetMaxOutputTokens()), ResponseFormat: in.GetResponseFormat(), Metadata: structToMap(in.GetMetadata())}
}

func semanticParametersToInference(in domainsemantic.IntelligenceParameters) domaininference.Parameters {
	return domaininference.Parameters{Temperature: in.Temperature, MaxInputTokens: in.MaxInputTokens, MaxOutputTokens: in.MaxOutputTokens, ResponseFormat: in.ResponseFormat, Metadata: in.Metadata}
}

func semanticParametersFromInference(in domaininference.Parameters) domainsemantic.IntelligenceParameters {
	return domainsemantic.IntelligenceParameters{Temperature: in.Temperature, MaxInputTokens: in.MaxInputTokens, MaxOutputTokens: in.MaxOutputTokens, ResponseFormat: in.ResponseFormat, Metadata: in.Metadata}
}

func semanticOperationFromProto(value commonv1.InferenceOperation) domainsemantic.Operation {
	switch value {
	case commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS:
		return domainsemantic.OperationEmbeddings
	case commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT:
		return domainsemantic.OperationChat
	case commonv1.InferenceOperation_INFERENCE_OPERATION_RERANK:
		return domainsemantic.OperationRerank
	case commonv1.InferenceOperation_INFERENCE_OPERATION_SUMMARIZE:
		return domainsemantic.OperationSummarize
	case commonv1.InferenceOperation_INFERENCE_OPERATION_CLASSIFY:
		return domainsemantic.OperationClassify
	case commonv1.InferenceOperation_INFERENCE_OPERATION_IMAGE_ANALYSIS:
		return domainsemantic.OperationImageAnalysis
	default:
		return ""
	}
}

func semanticPrivacyClassFromProto(value commonv1.InferencePrivacyClass) domainsemantic.PrivacyClass {
	switch value {
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY:
		return domainsemantic.PrivacyClassLocalOnly
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_PRIVATE:
		return domainsemantic.PrivacyClassEnterprisePrivate
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_THIRD_PARTY:
		return domainsemantic.PrivacyClassThirdParty
	default:
		return ""
	}
}

func privacyRequirementFromProto(in *commonv1.InferencePrivacyRequirement) domaininference.PrivacyRequirement {
	if in == nil {
		return domaininference.PrivacyRequirement{}
	}
	classes := make([]domaininference.PrivacyClass, 0, len(in.GetAllowedPrivacyClasses()))
	for _, value := range in.GetAllowedPrivacyClasses() {
		if cls := inferencePrivacyClassFromProto(value); cls != "" {
			classes = append(classes, cls)
		}
	}
	return domaininference.PrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.GetRequireLocalEndpoint(), DisallowThirdParty: in.GetDisallowThirdParty()}
}

func privacyRequirementToProto(in domaininference.PrivacyRequirement) *commonv1.InferencePrivacyRequirement {
	classes := make([]commonv1.InferencePrivacyClass, 0, len(in.AllowedPrivacyClasses))
	for _, value := range in.AllowedPrivacyClasses {
		classes = append(classes, inferencePrivacyClassToProto(value))
	}
	return &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty}
}

func parametersFromProto(in *commonv1.InferenceParameters) domaininference.Parameters {
	if in == nil {
		return domaininference.Parameters{}
	}
	var temp *float64
	if in.Temperature != nil {
		v := in.GetTemperature()
		temp = &v
	}
	return domaininference.Parameters{Temperature: temp, MaxInputTokens: int(in.GetMaxInputTokens()), MaxOutputTokens: int(in.GetMaxOutputTokens()), ResponseFormat: in.GetResponseFormat(), Metadata: structToMap(in.GetMetadata())}
}

func parametersToProto(in domaininference.Parameters) *commonv1.InferenceParameters {
	out := &commonv1.InferenceParameters{MaxInputTokens: int32(in.MaxInputTokens), MaxOutputTokens: int32(in.MaxOutputTokens), ResponseFormat: in.ResponseFormat, Metadata: protoStructAdmin(in.Metadata)}
	if in.Temperature != nil {
		out.Temperature = in.Temperature
	}
	return out
}

func inferenceOperationFromProto(value commonv1.InferenceOperation) domaininference.Operation {
	switch value {
	case commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS:
		return domaininference.OperationEmbeddings
	case commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT:
		return domaininference.OperationChat
	case commonv1.InferenceOperation_INFERENCE_OPERATION_RERANK:
		return domaininference.OperationRerank
	case commonv1.InferenceOperation_INFERENCE_OPERATION_SUMMARIZE:
		return domaininference.OperationSummarize
	case commonv1.InferenceOperation_INFERENCE_OPERATION_CLASSIFY:
		return domaininference.OperationClassify
	case commonv1.InferenceOperation_INFERENCE_OPERATION_IMAGE_ANALYSIS:
		return domaininference.OperationImageAnalysis
	default:
		return ""
	}
}

func inferenceOperationToProto(value domaininference.Operation) commonv1.InferenceOperation {
	switch value {
	case domaininference.OperationEmbeddings:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS
	case domaininference.OperationChat:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT
	case domaininference.OperationRerank:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_RERANK
	case domaininference.OperationSummarize:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_SUMMARIZE
	case domaininference.OperationClassify:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CLASSIFY
	case domaininference.OperationImageAnalysis:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_IMAGE_ANALYSIS
	default:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED
	}
}

func semanticModelKindFromProto(in *adminv1.InferenceModel) domainsemantic.ModelKind {
	if kind := strings.TrimSpace(in.GetKind()); kind != "" {
		return domainsemantic.ModelKind(kind)
	}
	switch in.GetKindValue() {
	case commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_GENERATIVE:
		return domainsemantic.ModelKindGenerative
	case commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_EMBEDDING:
		return domainsemantic.ModelKindEmbedding
	case commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_RERANKER:
		return domainsemantic.ModelKindReranker
	default:
		return ""
	}
}

func semanticModelKindToProto(value domainsemantic.ModelKind) commonv1.InferenceModelKind {
	switch value {
	case domainsemantic.ModelKindGenerative:
		return commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_GENERATIVE
	case domainsemantic.ModelKindEmbedding:
		return commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_EMBEDDING
	case domainsemantic.ModelKindReranker:
		return commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_RERANKER
	default:
		return commonv1.InferenceModelKind_INFERENCE_MODEL_KIND_UNSPECIFIED
	}
}

func inferenceModelKindFromSemantic(value domainsemantic.ModelKind) domaininference.ModelKind {
	switch value {
	case domainsemantic.ModelKindGenerative:
		return domaininference.ModelKindGenerative
	case domainsemantic.ModelKindEmbedding:
		return domaininference.ModelKindEmbedding
	case domainsemantic.ModelKindReranker:
		return domaininference.ModelKindReranker
	default:
		return ""
	}
}

func inferencePrivacyClassFromProto(value commonv1.InferencePrivacyClass) domaininference.PrivacyClass {
	switch value {
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY:
		return domaininference.PrivacyClassLocalOnly
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_PRIVATE:
		return domaininference.PrivacyClassPrivate
	case commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_THIRD_PARTY:
		return domaininference.PrivacyClassThirdParty
	default:
		return ""
	}
}

func inferencePrivacyClassToProto(value domaininference.PrivacyClass) commonv1.InferencePrivacyClass {
	switch value {
	case domaininference.PrivacyClassLocalOnly:
		return commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY
	case domaininference.PrivacyClassPrivate:
		return commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_PRIVATE
	case domaininference.PrivacyClassThirdParty:
		return commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_THIRD_PARTY
	default:
		return commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_UNSPECIFIED
	}
}

func semanticEndpointToInference(in domainsemantic.ModelEndpoint) domaininference.Endpoint {
	return domaininference.Endpoint{ID: domaininference.EndpointID(in.ID), Key: in.Key, DisplayName: in.Name, ConnectorType: domaininference.ConnectorType(in.ConnectorType), BaseURL: in.EndpointURL, NetworkClass: inferenceNetworkClassFromSemantic(in.NetworkClass), PrivacyClass: inferencePrivacyClassFromSemantic(in.PrivacyClass), AuthTypes: inferenceAuthTypesFromSemantic(in.AuthModes), Operations: inferenceOperationsFromSemantic(in.Operations), Enabled: in.Enabled, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticModelToInference(in domainsemantic.InferenceModel) domaininference.Model {
	return domaininference.Model{ID: domaininference.ModelID(in.ID), Key: in.Key, Kind: inferenceModelKindFromSemantic(in.Kind), ProviderModelName: in.ModelName, ConnectorTypes: inferenceConnectorTypesFromSemantic(in.ConnectorTypes), InputModalities: append([]string(nil), in.InputModalities...), OutputModalities: append([]string(nil), in.OutputModalities...), EmbeddingDims: in.Dimensions, VectorSpace: in.VectorSpaceKey, Enabled: true, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticCapabilityToInference(in domainsemantic.ModelEndpointCapability) domaininference.Capability {
	return domaininference.Capability{ID: domaininference.CapabilityID(in.ID), EndpointID: domaininference.EndpointID(in.ModelEndpointID), ModelID: domaininference.ModelID(in.ModelID), Operation: domaininference.Operation(in.Operation), ProviderModelOverride: in.ModelNameOverride, Enabled: in.Enabled, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticVectorStoreToInference(in domainsemantic.VectorStoreBackend) domaininference.VectorStore {
	return domaininference.VectorStore{ID: domaininference.VectorStoreID(in.ID), Key: in.Key, DisplayName: in.Name, Type: string(in.Type), PrivacyClass: inferencePrivacyClassFromSemantic(in.PrivacyClass), Enabled: in.Enabled, Config: in.Config, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func semanticSecretToInference(in domainsemantic.Secret) domaininference.Secret {
	var ciphertext *domaininference.EncryptedSecretPayload
	if in.Ciphertext != nil {
		ciphertext = &domaininference.EncryptedSecretPayload{Algorithm: in.Ciphertext.Algorithm, NonceB64: in.Ciphertext.NonceB64, CipherB64: in.Ciphertext.CipherB64}
	}
	return domaininference.Secret{ID: domaininference.SecretID(in.ID), OwnerType: inferenceOwnerTypeFromSemantic(in.OwnerType), OwnerID: in.OwnerID, Kind: string(in.Kind), Ciphertext: ciphertext, SecretSuffix: in.SecretSuffix, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func semanticCredentialToInference(in domainsemantic.InferenceCredential) domaininference.Credential {
	return domaininference.Credential{ID: domaininference.CredentialID(in.ID), Key: in.Key, DisplayName: in.Name, EndpointID: domaininference.EndpointID(in.ModelEndpointID), OwnerType: inferenceOwnerTypeFromSemantic(in.OwnerType), OwnerID: in.OwnerID, AuthType: inferenceAuthTypeFromSemantic(in.AuthType), SecretID: domaininference.SecretID(in.SecretRef), SecretSuffix: in.SecretSuffix, Status: inferenceCredentialStatusFromSemantic(in.Status), CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func inferenceNetworkClassFromSemantic(value domainsemantic.NetworkClass) domaininference.NetworkClass {
	switch value {
	case domainsemantic.NetworkClassLocal:
		return domaininference.NetworkClassLocal
	case domainsemantic.NetworkClassPrivateNetwork:
		return domaininference.NetworkClassPrivateNetwork
	default:
		return domaininference.NetworkClassPublicInternet
	}
}

func inferencePrivacyClassFromSemantic(value domainsemantic.PrivacyClass) domaininference.PrivacyClass {
	switch value {
	case domainsemantic.PrivacyClassLocalOnly:
		return domaininference.PrivacyClassLocalOnly
	case domainsemantic.PrivacyClassEnterprisePrivate:
		return domaininference.PrivacyClassPrivate
	default:
		return domaininference.PrivacyClassThirdParty
	}
}

func inferenceAuthTypeFromSemantic(value domainsemantic.AuthMode) domaininference.CredentialAuthType {
	switch value {
	case domainsemantic.AuthModeNone:
		return domaininference.CredentialAuthNone
	case domainsemantic.AuthModeBearerToken:
		return domaininference.CredentialAuthBearer
	default:
		return domaininference.CredentialAuthAPIKey
	}
}

func inferenceAuthTypesFromSemantic(values []domainsemantic.AuthMode) []domaininference.CredentialAuthType {
	out := make([]domaininference.CredentialAuthType, 0, len(values))
	for _, value := range values {
		out = append(out, inferenceAuthTypeFromSemantic(value))
	}
	return out
}

func inferenceOperationsFromSemantic(values []domainsemantic.Operation) []domaininference.Operation {
	out := make([]domaininference.Operation, 0, len(values))
	for _, value := range values {
		out = append(out, domaininference.Operation(value))
	}
	return out
}

func inferenceConnectorTypesFromSemantic(values []domainsemantic.ConnectorType) []domaininference.ConnectorType {
	out := make([]domaininference.ConnectorType, 0, len(values))
	for _, value := range values {
		out = append(out, domaininference.ConnectorType(value))
	}
	return out
}

func inferenceOwnerTypeFromSemantic(value domainsemantic.CredentialOwnerType) domaininference.CredentialOwnerType {
	switch value {
	case domainsemantic.CredentialOwnerSpace:
		return domaininference.CredentialOwnerSpace
	case domainsemantic.CredentialOwnerSystem:
		return domaininference.CredentialOwnerSystem
	default:
		return domaininference.CredentialOwnerPrincipal
	}
}

func inferenceCredentialStatusFromSemantic(value domainsemantic.CredentialStatus) domaininference.CredentialStatus {
	switch value {
	case domainsemantic.CredentialStatusDisabled:
		return domaininference.CredentialStatusDisabled
	case domainsemantic.CredentialStatusRevoked, domainsemantic.CredentialStatusExpired:
		return domaininference.CredentialStatusRevoked
	default:
		return domaininference.CredentialStatusActive
	}
}

func semanticPackageToInference(in domainsemantic.InferencePackage) domaininference.InferencePackage {
	return domaininference.InferencePackage{ID: domaininference.InferencePackageID(in.ID), Name: in.Name, Version: in.Version, Source: in.Source, Checksum: in.Checksum, InstalledAt: in.InstalledAt, InstalledBy: in.InstalledBy, DefinitionCounts: in.DefinitionCounts}
}

func semanticScopeToInference(in domainsemantic.ProcessingScope) domaininference.Scope {
	return domaininference.Scope{SpaceID: uuidOrEmptyAdmin(in.SpaceID), DomainID: uuidOrEmptyAdmin(in.DomainID), SemanticRuleID: uuidOrEmptyAdmin(in.SemanticRuleID), EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: uuidOrEmptyAdmin(in.SemanticIndexID), NodeID: uuidOrEmptyAdmin(in.NodeID), IncludeDescendants: in.IncludeDescendants}
}

func semanticGrantToInference(spaceID string, in domainsemantic.CredentialGrant) domaininference.CredentialGrant {
	out := domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(in.ID), SpaceID: spaceID, CredentialID: domaininference.CredentialID(in.CredentialID), Scope: semanticScopeToInference(in.Scope), Operations: inferenceOperationsFromSemantic(in.Operations), Priority: in.Priority, GranteePrincipals: append([]string(nil), in.GranteePrincipalIDs...), AllowOnBehalfOfPrincipals: append([]string(nil), in.AllowOnBehalfOfPrincipalIDs...), State: domaininference.GrantStateActive, CreatedBy: in.GrantedBy, CreatedAt: in.CreatedAt, Reason: "semantic admin grant"}
	if in.ModelEndpointID != nil {
		out.EndpointRefs = []string{in.ModelEndpointID.String()}
	}
	if in.ModelID != nil {
		out.ModelRefs = []string{in.ModelID.String()}
	}
	if in.AllowBackgroundUse {
		out.UsageModes = []domaininference.UsageMode{domaininference.UsageModeInteractive, domaininference.UsageModeAutomation, domaininference.UsageModeBackground, domaininference.UsageModeSemantic}
	} else {
		out.UsageModes = []domaininference.UsageMode{domaininference.UsageModeInteractive, domaininference.UsageModeAutomation, domaininference.UsageModeSemantic}
	}
	if in.ExpiresAt != nil {
		out.ExpiresAt = *in.ExpiresAt
		if !in.ExpiresAt.After(time.Now()) {
			out.State = domaininference.GrantStateExpired
		}
	}
	return out
}

func semanticPolicyToInference(spaceID string, in domainsemantic.InferencePolicy) domaininference.Policy {
	out := domaininference.Policy{ID: domaininference.PolicyID(in.ID), SpaceID: spaceID, Scope: semanticScopeToInference(in.Scope), Operations: inferenceOperationsFromSemantic(in.Operations), Action: inferencePolicyActionFromSemantic(in.Effect), NoInference: in.NoInference, AllowedPrivacyClasses: inferencePrivacyClassesFromSemantic(in.AllowedPrivacyClasses), RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty, State: domaininference.PolicyStateActive, CreatedBy: in.CreatedBy, CreatedAt: in.CreatedAt, Reason: in.Reason}
	if in.ExpiresAt != nil {
		out.ExpiresAt = *in.ExpiresAt
		if !in.ExpiresAt.After(time.Now()) {
			out.State = domaininference.PolicyStateExpired
		}
	}
	return out
}

func inferencePolicyActionFromSemantic(value domainsemantic.PolicyEffect) domaininference.PolicyAction {
	switch value {
	case domainsemantic.PolicyEffectDeny:
		return domaininference.PolicyActionDeny
	case domainsemantic.PolicyEffectRestrict:
		return domaininference.PolicyActionRestrict
	default:
		return domaininference.PolicyActionAllow
	}
}

func inferencePrivacyClassesFromSemantic(values []domainsemantic.PrivacyClass) []domaininference.PrivacyClass {
	out := make([]domaininference.PrivacyClass, 0, len(values))
	for _, value := range values {
		out = append(out, inferencePrivacyClassFromSemantic(value))
	}
	return out
}

func mapStandalonePolicyDecision(in domaininference.PolicyDecision) *adminv1.PolicyDecision {
	return &adminv1.PolicyDecision{PolicyDecisionId: in.ID.String(), SpaceId: in.SpaceID, DomainId: in.DomainID, NodeId: in.NodeID, Operation: inferenceOperationToProto(in.Operation), UsageMode: inferenceUsageModeToProto(in.UsageMode), IntelligenceProfileId: uuidOrEmptyAdmin(in.ProfileID), ModelEndpointCapabilityId: uuidOrEmptyAdmin(in.CapabilityID), ModelEndpointId: uuidOrEmptyAdmin(in.EndpointID), ModelId: uuidOrEmptyAdmin(in.ModelID), CredentialId: uuidOrEmptyAdmin(in.CredentialID), CredentialGrantId: uuidOrEmptyAdmin(in.CredentialGrantID), ActorPrincipalId: in.ActorPrincipalID, OnBehalfOfPrincipalId: in.OnBehalfOfPrincipalID, Action: inferencePolicyDecisionActionToProto(in.Action), MatchedAccessPolicyIds: append([]string(nil), in.MatchedPolicyIDs...), Reason: in.Reason, DecidedAt: timestamppb.New(in.DecidedAt), Metadata: protoStructAdmin(in.Metadata)}
}

func inferenceUsageModeToProto(value domaininference.UsageMode) commonv1.InferenceUsageMode {
	switch value {
	case domaininference.UsageModeInteractive:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_INTERACTIVE
	case domaininference.UsageModeAutomation:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_AUTOMATION
	case domaininference.UsageModeBackground:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_BACKGROUND
	case domaininference.UsageModeSemantic:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_SEMANTIC
	default:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_UNSPECIFIED
	}
}

func inferencePolicyDecisionActionToProto(value domaininference.PolicyDecisionAction) commonv1.AccessPolicyDecisionAction {
	switch value {
	case domaininference.PolicyDecisionAllowed:
		return commonv1.AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_ALLOWED
	case domaininference.PolicyDecisionDenied:
		return commonv1.AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_DENIED
	default:
		return commonv1.AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_UNSPECIFIED
	}
}

func mapStandaloneUsageEvent(in domaininference.UsageEvent) *adminv1.IntelligenceAccessUsageEvent {
	return &adminv1.IntelligenceAccessUsageEvent{UsageEventId: in.ID.String(), RequestId: in.RequestID, Operation: inferenceOperationToProto(in.Operation), UsageMode: inferenceUsageModeToProto(in.UsageMode), Status: inferenceUsageStatusToProto(in.Status), SpaceId: in.SpaceID, DomainId: in.DomainID, NodeId: in.NodeID, AutomationId: in.AutomationID, AutomationRunId: in.AutomationRunID, SemanticRuleId: firstNonEmptyAdmin(in.SemanticRuleID, in.SemanticIndexID), EmbeddingBindingKey: in.EmbeddingBindingKey, ActorPrincipalId: in.ActorPrincipalID, OnBehalfOfPrincipalId: in.OnBehalfOfPrincipalID, IntelligenceProfileId: uuidOrEmptyAdmin(in.ProfileID), ModelEndpointId: uuidOrEmptyAdmin(in.EndpointID), ModelId: uuidOrEmptyAdmin(in.ModelID), ModelEndpointCapabilityId: uuidOrEmptyAdmin(in.CapabilityID), CredentialId: uuidOrEmptyAdmin(in.CredentialID), CredentialGrantId: uuidOrEmptyAdmin(in.CredentialGrantID), PolicyDecisionId: uuidOrEmptyAdmin(in.PolicyDecisionID), ProviderRequestId: in.ProviderRequestID, InputTokens: in.InputTokens, OutputTokens: in.OutputTokens, TotalTokens: in.TotalTokens, LatencyMillis: in.LatencyMillis, ErrorCode: in.ErrorCode, ErrorMessage: in.ErrorMessage, StartedAt: timestamppb.New(in.StartedAt), CompletedAt: timestamppb.New(in.CompletedAt), Metadata: protoStructAdmin(in.Metadata)}
}

func mapStandaloneUsageEvents(items []domaininference.UsageEvent) []*adminv1.IntelligenceAccessUsageEvent {
	out := make([]*adminv1.IntelligenceAccessUsageEvent, 0, len(items))
	for _, item := range items {
		out = append(out, mapStandaloneUsageEvent(item))
	}
	return out
}

func inferenceUsageStatusToProto(value domaininference.UsageStatus) commonv1.InferenceUsageStatus {
	switch value {
	case domaininference.UsageStatusSucceeded:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_SUCCEEDED
	case domaininference.UsageStatusFailed:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_FAILED
	case domaininference.UsageStatusDenied:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_DENIED
	case domaininference.UsageStatusCanceled:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_CANCELED
	default:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_UNSPECIFIED
	}
}

func inferenceUsageModeFromProto(value commonv1.InferenceUsageMode) domaininference.UsageMode {
	switch value {
	case commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_INTERACTIVE:
		return domaininference.UsageModeInteractive
	case commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_AUTOMATION:
		return domaininference.UsageModeAutomation
	case commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_BACKGROUND:
		return domaininference.UsageModeBackground
	case commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_SEMANTIC:
		return domaininference.UsageModeSemantic
	default:
		return ""
	}
}

func inferenceUsageStatusFromProto(value commonv1.InferenceUsageStatus) domaininference.UsageStatus {
	switch value {
	case commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_SUCCEEDED:
		return domaininference.UsageStatusSucceeded
	case commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_FAILED:
		return domaininference.UsageStatusFailed
	case commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_DENIED:
		return domaininference.UsageStatusDenied
	case commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_CANCELED:
		return domaininference.UsageStatusCanceled
	default:
		return ""
	}
}
