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
	graph "github.com/myceldb/mycel/internal/graph/model"
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
