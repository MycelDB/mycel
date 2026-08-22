#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat >"${repo_root}/internal/gen/mycel/common/v1/intelligence_compat.go" <<'GOEOF'
package commonv1

// Transitional aliases for code moving from Inference API names to
// Intelligence Access API names. These files are generated after protobuf
// generation and should be removed once callers are fully renamed.
type InferenceScope = IntelligenceAccessScope

type InferenceUsageMode = IntelligenceAccessUsageMode

const (
	InferenceUsageMode_INFERENCE_USAGE_MODE_UNSPECIFIED = IntelligenceAccessUsageMode_INTELLIGENCE_ACCESS_USAGE_MODE_UNSPECIFIED
	InferenceUsageMode_INFERENCE_USAGE_MODE_INTERACTIVE = IntelligenceAccessUsageMode_INTELLIGENCE_ACCESS_USAGE_MODE_INTERACTIVE
	InferenceUsageMode_INFERENCE_USAGE_MODE_AUTOMATION  = IntelligenceAccessUsageMode_INTELLIGENCE_ACCESS_USAGE_MODE_AUTOMATION
	InferenceUsageMode_INFERENCE_USAGE_MODE_BACKGROUND  = IntelligenceAccessUsageMode_INTELLIGENCE_ACCESS_USAGE_MODE_BACKGROUND
	InferenceUsageMode_INFERENCE_USAGE_MODE_SEMANTIC    = IntelligenceAccessUsageMode_INTELLIGENCE_ACCESS_USAGE_MODE_SEMANTIC
)

type InferenceUsageStatus = IntelligenceAccessUsageStatus

const (
	InferenceUsageStatus_INFERENCE_USAGE_STATUS_UNSPECIFIED = IntelligenceAccessUsageStatus_INTELLIGENCE_ACCESS_USAGE_STATUS_UNSPECIFIED
	InferenceUsageStatus_INFERENCE_USAGE_STATUS_SUCCEEDED   = IntelligenceAccessUsageStatus_INTELLIGENCE_ACCESS_USAGE_STATUS_SUCCEEDED
	InferenceUsageStatus_INFERENCE_USAGE_STATUS_FAILED      = IntelligenceAccessUsageStatus_INTELLIGENCE_ACCESS_USAGE_STATUS_FAILED
	InferenceUsageStatus_INFERENCE_USAGE_STATUS_DENIED      = IntelligenceAccessUsageStatus_INTELLIGENCE_ACCESS_USAGE_STATUS_DENIED
	InferenceUsageStatus_INFERENCE_USAGE_STATUS_CANCELED    = IntelligenceAccessUsageStatus_INTELLIGENCE_ACCESS_USAGE_STATUS_CANCELED
)

type AccessPolicyDecisionAction = IntelligenceAccessPolicyDecisionAction

const (
	AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_UNSPECIFIED = IntelligenceAccessPolicyDecisionAction_INTELLIGENCE_ACCESS_POLICY_DECISION_ACTION_UNSPECIFIED
	AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_ALLOWED     = IntelligenceAccessPolicyDecisionAction_INTELLIGENCE_ACCESS_POLICY_DECISION_ACTION_ALLOWED
	AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_DENIED      = IntelligenceAccessPolicyDecisionAction_INTELLIGENCE_ACCESS_POLICY_DECISION_ACTION_DENIED
)
GOEOF

cat >"${repo_root}/internal/gen/mycel/client/v1/semantic_compat.go" <<'GOEOF'
package clientv1

// Transitional aliases for callers migrating from SemanticIndex APIs to
// semantic generation rules.
type SemanticIndex = SemanticGenerationRuleSummary

type ListSemanticIndexesResponse = ListSemanticRulesResponse

func (x *SemanticGenerationRuleSummary) GetSemanticIndexId() string {
	if x != nil {
		return x.SemanticRuleId
	}
	return ""
}

func (x *SemanticGenerationRuleSummary) GetModelLabel() string {
	if x != nil && len(x.Bindings) > 0 {
		return x.Bindings[0].GetIntelligenceProfileKey()
	}
	return ""
}

func (x *ListSemanticRulesResponse) GetIndexes() []*SemanticGenerationRuleSummary { return x.GetRules() }
GOEOF

cat >"${repo_root}/internal/gen/mycel/admin/v1/intelligence_compat.go" <<'GOEOF'
package adminv1

import (
	"context"

	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"google.golang.org/grpc"
)

// Transitional Go aliases while daemon adapters move from Inference API names
// to Intelligence Access API names. Source protobuf services are already
// renamed.
type UnimplementedAdminInferenceProfileServiceServer = UnimplementedAdminIntelligenceAccessProfileServiceServer
type UnimplementedAdminInferenceCredentialServiceServer = UnimplementedAdminIntelligenceAccessCredentialServiceServer
type UnimplementedAdminInferenceGrantServiceServer = UnimplementedAdminIntelligenceAccessGrantServiceServer
type UnimplementedAdminInferencePolicyServiceServer = UnimplementedAdminIntelligenceAccessPolicyServiceServer
type UnimplementedAdminInferenceUsageServiceServer = UnimplementedAdminIntelligenceAccessUsageServiceServer

type AdminInferenceProfileServiceCreateInferenceProfileRequest = AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest
type AdminInferenceProfileServiceCreateInferenceProfileResponse = AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse
type AdminInferenceProfileServiceListInferenceProfilesRequest = AdminIntelligenceAccessProfileServiceListIntelligenceProfilesRequest
type AdminInferenceProfileServiceListInferenceProfilesResponse = AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse
type AdminInferenceProfileServiceGetInferenceProfileRequest = AdminIntelligenceAccessProfileServiceGetIntelligenceProfileRequest
type AdminInferenceProfileServiceGetInferenceProfileResponse = AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse
type AdminInferenceProfileServiceSetInferenceProfileEnabledRequest = AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledRequest
type AdminInferenceProfileServiceSetInferenceProfileEnabledResponse = AdminIntelligenceAccessProfileServiceSetIntelligenceProfileEnabledResponse
type AdminInferenceProfileServiceDeleteInferenceProfileRequest = AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest
type AdminInferenceProfileServiceDeleteInferenceProfileResponse = AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse

type AdminInferenceCredentialServiceCreateCredentialRequest = AdminIntelligenceAccessCredentialServiceCreateCredentialRequest
type AdminInferenceCredentialServiceCreateCredentialResponse = AdminIntelligenceAccessCredentialServiceCreateCredentialResponse
type AdminInferenceCredentialServiceListCredentialsRequest = AdminIntelligenceAccessCredentialServiceListCredentialsRequest
type AdminInferenceCredentialServiceListCredentialsResponse = AdminIntelligenceAccessCredentialServiceListCredentialsResponse
type AdminInferenceCredentialServiceSetCredentialStatusRequest = AdminIntelligenceAccessCredentialServiceSetCredentialStatusRequest
type AdminInferenceCredentialServiceSetCredentialStatusResponse = AdminIntelligenceAccessCredentialServiceSetCredentialStatusResponse
type AdminInferenceCredentialServiceRotateCredentialRequest = AdminIntelligenceAccessCredentialServiceRotateCredentialRequest
type AdminInferenceCredentialServiceRotateCredentialResponse = AdminIntelligenceAccessCredentialServiceRotateCredentialResponse
type AdminInferenceCredentialServiceDeleteCredentialRequest = AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest
type AdminInferenceCredentialServiceDeleteCredentialResponse = AdminIntelligenceAccessCredentialServiceDeleteCredentialResponse

type AdminInferenceGrantServiceCreateCredentialGrantRequest = AdminIntelligenceAccessGrantServiceCreateCredentialGrantRequest
type AdminInferenceGrantServiceCreateCredentialGrantResponse = AdminIntelligenceAccessGrantServiceCreateCredentialGrantResponse
type AdminInferenceGrantServiceListCredentialGrantsRequest = AdminIntelligenceAccessGrantServiceListCredentialGrantsRequest
type AdminInferenceGrantServiceListCredentialGrantsResponse = AdminIntelligenceAccessGrantServiceListCredentialGrantsResponse
type AdminInferenceGrantServiceExpireCredentialGrantRequest = AdminIntelligenceAccessGrantServiceExpireCredentialGrantRequest
type AdminInferenceGrantServiceExpireCredentialGrantResponse = AdminIntelligenceAccessGrantServiceExpireCredentialGrantResponse
type AdminInferenceGrantServiceDeleteCredentialGrantRequest = AdminIntelligenceAccessGrantServiceDeleteCredentialGrantRequest
type AdminInferenceGrantServiceDeleteCredentialGrantResponse = AdminIntelligenceAccessGrantServiceDeleteCredentialGrantResponse

type AdminInferencePolicyServiceCreateInferencePolicyRequest = AdminIntelligenceAccessPolicyServiceCreateAccessPolicyRequest
type AdminInferencePolicyServiceCreateInferencePolicyResponse = AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse
type AdminInferencePolicyServiceListInferencePoliciesRequest = AdminIntelligenceAccessPolicyServiceListAccessPoliciesRequest
type AdminInferencePolicyServiceListInferencePoliciesResponse = AdminIntelligenceAccessPolicyServiceListAccessPoliciesResponse
type AdminInferencePolicyServiceExpireInferencePolicyRequest = AdminIntelligenceAccessPolicyServiceExpireAccessPolicyRequest
type AdminInferencePolicyServiceExpireInferencePolicyResponse = AdminIntelligenceAccessPolicyServiceExpireAccessPolicyResponse
type AdminInferencePolicyServiceDeleteInferencePolicyRequest = AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyRequest
type AdminInferencePolicyServiceDeleteInferencePolicyResponse = AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyResponse
type AdminInferencePolicyServiceGetPolicyDecisionRequest = AdminIntelligenceAccessPolicyServiceGetPolicyDecisionRequest
type AdminInferencePolicyServiceGetPolicyDecisionResponse = AdminIntelligenceAccessPolicyServiceGetPolicyDecisionResponse

type AdminInferenceUsageServiceListUsageEventsRequest = AdminIntelligenceAccessUsageServiceListUsageEventsRequest
type AdminInferenceUsageServiceListUsageEventsResponse = AdminIntelligenceAccessUsageServiceListUsageEventsResponse
type AdminInferenceUsageServiceSummarizeUsageRequest = AdminIntelligenceAccessUsageServiceSummarizeUsageRequest
type AdminInferenceUsageServiceSummarizeUsageResponse = AdminIntelligenceAccessUsageServiceSummarizeUsageResponse

type Secret = IntelligenceSecret
type InferenceCredential = IntelligenceCredential
type InferenceProfile = IntelligenceProfile
type CredentialGrant = IntelligenceCredentialGrant
type InferencePolicy = IntelligenceAccessPolicy
type AccessPolicy = IntelligenceAccessPolicy
type AccessPolicies = IntelligenceAccessPolicy
type PolicyDecision = IntelligenceAccessPolicyDecision
type InferenceUsageEvent = IntelligenceAccessUsageEvent
type InferenceUsageSummary = IntelligenceAccessUsageSummary
type ProcessingScope = commonv1.IntelligenceAccessScope

type AdminInferenceProfileServiceClient interface {
	AdminIntelligenceAccessProfileServiceClient
	CreateInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceCreateInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceCreateInferenceProfileResponse, error)
	ListInferenceProfiles(ctx context.Context, in *AdminInferenceProfileServiceListInferenceProfilesRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceListInferenceProfilesResponse, error)
	GetInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceGetInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceGetInferenceProfileResponse, error)
	SetInferenceProfileEnabled(ctx context.Context, in *AdminInferenceProfileServiceSetInferenceProfileEnabledRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceSetInferenceProfileEnabledResponse, error)
	DeleteInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceDeleteInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceDeleteInferenceProfileResponse, error)
}

type adminInferenceProfileServiceCompatClient struct{ AdminIntelligenceAccessProfileServiceClient }

func NewAdminInferenceProfileServiceClient(cc grpc.ClientConnInterface) AdminInferenceProfileServiceClient {
	return adminInferenceProfileServiceCompatClient{NewAdminIntelligenceAccessProfileServiceClient(cc)}
}
func (c adminInferenceProfileServiceCompatClient) CreateInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceCreateInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceCreateInferenceProfileResponse, error) {
	return c.CreateIntelligenceProfile(ctx, in, opts...)
}
func (c adminInferenceProfileServiceCompatClient) ListInferenceProfiles(ctx context.Context, in *AdminInferenceProfileServiceListInferenceProfilesRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceListInferenceProfilesResponse, error) {
	return c.ListIntelligenceProfiles(ctx, in, opts...)
}
func (c adminInferenceProfileServiceCompatClient) GetInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceGetInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceGetInferenceProfileResponse, error) {
	return c.GetIntelligenceProfile(ctx, in, opts...)
}
func (c adminInferenceProfileServiceCompatClient) SetInferenceProfileEnabled(ctx context.Context, in *AdminInferenceProfileServiceSetInferenceProfileEnabledRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceSetInferenceProfileEnabledResponse, error) {
	return c.SetIntelligenceProfileEnabled(ctx, in, opts...)
}
func (c adminInferenceProfileServiceCompatClient) DeleteInferenceProfile(ctx context.Context, in *AdminInferenceProfileServiceDeleteInferenceProfileRequest, opts ...grpc.CallOption) (*AdminInferenceProfileServiceDeleteInferenceProfileResponse, error) {
	return c.DeleteIntelligenceProfile(ctx, in, opts...)
}

func NewAdminInferenceCredentialServiceClient(cc grpc.ClientConnInterface) AdminIntelligenceAccessCredentialServiceClient {
	return NewAdminIntelligenceAccessCredentialServiceClient(cc)
}
func NewAdminInferenceGrantServiceClient(cc grpc.ClientConnInterface) AdminIntelligenceAccessGrantServiceClient {
	return NewAdminIntelligenceAccessGrantServiceClient(cc)
}

type AdminInferencePolicyServiceClient interface {
	AdminIntelligenceAccessPolicyServiceClient
	CreateInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceCreateInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceCreateInferencePolicyResponse, error)
	ListInferencePolicies(ctx context.Context, in *AdminInferencePolicyServiceListInferencePoliciesRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceListInferencePoliciesResponse, error)
	ExpireInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceExpireInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceExpireInferencePolicyResponse, error)
	DeleteInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceDeleteInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceDeleteInferencePolicyResponse, error)
	GetPolicyDecision(ctx context.Context, in *AdminInferencePolicyServiceGetPolicyDecisionRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceGetPolicyDecisionResponse, error)
}

type adminInferencePolicyServiceCompatClient struct{ AdminIntelligenceAccessPolicyServiceClient }

func NewAdminInferencePolicyServiceClient(cc grpc.ClientConnInterface) AdminInferencePolicyServiceClient {
	return adminInferencePolicyServiceCompatClient{NewAdminIntelligenceAccessPolicyServiceClient(cc)}
}
func (c adminInferencePolicyServiceCompatClient) CreateInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceCreateInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceCreateInferencePolicyResponse, error) {
	return c.CreateAccessPolicy(ctx, in, opts...)
}
func (c adminInferencePolicyServiceCompatClient) ListInferencePolicies(ctx context.Context, in *AdminInferencePolicyServiceListInferencePoliciesRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceListInferencePoliciesResponse, error) {
	return c.ListAccessPolicies(ctx, in, opts...)
}
func (c adminInferencePolicyServiceCompatClient) ExpireInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceExpireInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceExpireInferencePolicyResponse, error) {
	return c.ExpireAccessPolicy(ctx, in, opts...)
}
func (c adminInferencePolicyServiceCompatClient) DeleteInferencePolicy(ctx context.Context, in *AdminInferencePolicyServiceDeleteInferencePolicyRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceDeleteInferencePolicyResponse, error) {
	return c.DeleteAccessPolicy(ctx, in, opts...)
}
func (c adminInferencePolicyServiceCompatClient) GetPolicyDecision(ctx context.Context, in *AdminInferencePolicyServiceGetPolicyDecisionRequest, opts ...grpc.CallOption) (*AdminInferencePolicyServiceGetPolicyDecisionResponse, error) {
	return c.AdminIntelligenceAccessPolicyServiceClient.GetPolicyDecision(ctx, in, opts...)
}

func NewAdminInferenceUsageServiceClient(cc grpc.ClientConnInterface) AdminIntelligenceAccessUsageServiceClient {
	return NewAdminIntelligenceAccessUsageServiceClient(cc)
}

func (x *IntelligenceProfile) GetInferenceProfileId() string { return x.GetIntelligenceProfileId() }
func (x *AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse) GetInferenceProfile() *IntelligenceProfile {
	return x.GetIntelligenceProfile()
}
func (x *AdminIntelligenceAccessProfileServiceListIntelligenceProfilesResponse) GetInferenceProfiles() []*IntelligenceProfile {
	return x.GetIntelligenceProfiles()
}
func (x *AdminIntelligenceAccessProfileServiceGetIntelligenceProfileResponse) GetInferenceProfile() *IntelligenceProfile {
	return x.GetIntelligenceProfile()
}
func (x *AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse) GetInferenceProfileId() string {
	return x.GetIntelligenceProfileId()
}

func (x *IntelligenceAccessPolicy) GetInferencePolicyId() string { return x.GetAccessPolicyId() }
func (x *AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse) GetInferencePolicy() *IntelligenceAccessPolicy {
	return x.GetAccessPolicy()
}
func (x *AdminIntelligenceAccessPolicyServiceListAccessPoliciesResponse) GetInferencePolicies() []*IntelligenceAccessPolicy {
	return x.GetAccessPolicies()
}
func (x *AdminIntelligenceAccessPolicyServiceExpireAccessPolicyResponse) GetInferencePolicy() *IntelligenceAccessPolicy {
	return x.GetAccessPolicy()
}
func (x *AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyResponse) GetInferencePolicyId() string {
	return x.GetAccessPolicyId()
}

type DeleteSemanticIndexResponse = DeleteSemanticRuleResponse

func (x *DeleteSemanticRuleResponse) GetCredentialGrantsDeleted() int32 { return 0 }
func (x *DeleteSemanticRuleResponse) GetInferencePoliciesDeleted() int32 { return 0 }
GOEOF

gofmt -w \
  "${repo_root}/internal/gen/mycel/common/v1/intelligence_compat.go" \
  "${repo_root}/internal/gen/mycel/client/v1/semantic_compat.go" \
  "${repo_root}/internal/gen/mycel/admin/v1/intelligence_compat.go"
