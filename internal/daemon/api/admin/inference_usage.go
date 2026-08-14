package admin

import (
	"context"
	"sort"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *AdminInferenceService) ListUsageEvents(ctx context.Context, req *adminv1.AdminInferenceUsageServiceListUsageEventsRequest) (*adminv1.AdminInferenceUsageServiceListUsageEventsResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceAuditRead, inferenceScope(req.GetSpaceId(), req.GetScope().GetDomainId())); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	items, err := s.inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list inference usage events")
	}
	items = filterStandaloneUsageEvents(items, usageFilterFromListRequest(req))
	sort.SliceStable(items, func(i, j int) bool { return items[i].StartedAt.After(items[j].StartedAt) })
	page, next, err := paginateAdminInference(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &adminv1.AdminInferenceUsageServiceListUsageEventsResponse{UsageEvents: mapStandaloneUsageEvents(page), NextPageToken: next}, nil
}

func (s *AdminInferenceService) SummarizeUsage(ctx context.Context, req *adminv1.AdminInferenceUsageServiceSummarizeUsageRequest) (*adminv1.AdminInferenceUsageServiceSummarizeUsageResponse, error) {
	if _, err := s.requireInferenceCapability(ctx, capInferenceAuditRead, inferenceScope(req.GetSpaceId(), req.GetScope().GetDomainId())); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference subsystem is not configured")
	}
	items, err := s.inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil {
		return nil, mapAdminInferenceError(err, "list inference usage events")
	}
	items = filterStandaloneUsageEvents(items, usageFilter{SpaceID: req.GetSpaceId(), Scope: inferenceScopeFromProto(req.GetScope()), Since: timeValueFromProto(req.GetSince()), Until: timeValueFromProto(req.GetUntil())})
	groups := summarizeStandaloneUsage(items, req.GetGroupBy())
	return &adminv1.AdminInferenceUsageServiceSummarizeUsageResponse{Summaries: groups}, nil
}

type usageFilter struct {
	SpaceID               string
	Scope                 domaininference.Scope
	Operation             domaininference.Operation
	UsageMode             domaininference.UsageMode
	Status                domaininference.UsageStatus
	ProfileID             string
	EndpointID            string
	ModelID               string
	CredentialGrantID     string
	AutomationID          string
	AutomationRunID       string
	SemanticIndexID       string
	ActorPrincipalID      string
	OnBehalfOfPrincipalID string
	Since                 time.Time
	Until                 time.Time
}

func usageFilterFromListRequest(req *adminv1.AdminInferenceUsageServiceListUsageEventsRequest) usageFilter {
	return usageFilter{SpaceID: req.GetSpaceId(), Scope: inferenceScopeFromProto(req.GetScope()), Operation: inferenceOperationFromProto(req.GetOperation()), UsageMode: inferenceUsageModeFromProto(req.GetUsageMode()), Status: inferenceUsageStatusFromProto(req.GetStatus()), ProfileID: req.GetInferenceProfileId(), EndpointID: req.GetModelEndpointId(), ModelID: req.GetModelId(), CredentialGrantID: req.GetCredentialGrantId(), AutomationID: req.GetAutomationId(), AutomationRunID: req.GetAutomationRunId(), SemanticIndexID: req.GetSemanticIndexId(), ActorPrincipalID: req.GetActorPrincipalId(), OnBehalfOfPrincipalID: req.GetOnBehalfOfPrincipalId(), Since: timeValueFromProto(req.GetSince()), Until: timeValueFromProto(req.GetUntil())}
}

func filterStandaloneUsageEvents(items []domaininference.UsageEvent, filter usageFilter) []domaininference.UsageEvent {
	out := items[:0]
	for _, item := range items {
		if filter.SpaceID != "" && item.SpaceID != filter.SpaceID {
			continue
		}
		if filter.Scope.SpaceID != "" && item.SpaceID != filter.Scope.SpaceID {
			continue
		}
		if filter.Scope.DomainID != "" && item.DomainID != filter.Scope.DomainID {
			continue
		}
		if filter.Scope.SemanticIndexID != "" && item.SemanticIndexID != filter.Scope.SemanticIndexID {
			continue
		}
		if filter.Scope.NodeID != "" && item.NodeID != filter.Scope.NodeID {
			continue
		}
		if filter.Operation != "" && item.Operation != filter.Operation {
			continue
		}
		if filter.UsageMode != "" && item.UsageMode != filter.UsageMode {
			continue
		}
		if filter.Status != "" && item.Status != filter.Status {
			continue
		}
		if filter.ProfileID != "" && item.ProfileID.String() != filter.ProfileID {
			continue
		}
		if filter.EndpointID != "" && item.EndpointID.String() != filter.EndpointID {
			continue
		}
		if filter.ModelID != "" && item.ModelID.String() != filter.ModelID {
			continue
		}
		if filter.CredentialGrantID != "" && item.CredentialGrantID.String() != filter.CredentialGrantID {
			continue
		}
		if filter.AutomationID != "" && item.AutomationID != filter.AutomationID {
			continue
		}
		if filter.AutomationRunID != "" && item.AutomationRunID != filter.AutomationRunID {
			continue
		}
		if filter.SemanticIndexID != "" && item.SemanticIndexID != filter.SemanticIndexID {
			continue
		}
		if filter.ActorPrincipalID != "" && item.ActorPrincipalID != filter.ActorPrincipalID {
			continue
		}
		if filter.OnBehalfOfPrincipalID != "" && item.OnBehalfOfPrincipalID != filter.OnBehalfOfPrincipalID {
			continue
		}
		if !filter.Since.IsZero() && item.StartedAt.Before(filter.Since) {
			continue
		}
		if !filter.Until.IsZero() && item.StartedAt.After(filter.Until) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func summarizeStandaloneUsage(items []domaininference.UsageEvent, groupBy []string) []*adminv1.InferenceUsageSummary {
	if len(groupBy) == 0 {
		groupBy = []string{"space_id", "operation", "usage_mode", "status"}
	}
	byKey := map[string]*adminv1.InferenceUsageSummary{}
	for _, item := range items {
		group := usageGroup(item, groupBy)
		key := usageGroupKey(groupBy, group)
		summary := byKey[key]
		if summary == nil {
			summary = &adminv1.InferenceUsageSummary{Group: group}
			byKey[key] = summary
		}
		summary.RequestCount++
		switch item.Status {
		case domaininference.UsageStatusSucceeded:
			summary.SucceededCount++
		case domaininference.UsageStatusFailed, domaininference.UsageStatusCanceled:
			summary.FailedCount++
		case domaininference.UsageStatusDenied:
			summary.DeniedCount++
		}
		summary.InputTokens += item.InputTokens
		summary.OutputTokens += item.OutputTokens
		summary.TotalTokens += item.TotalTokens
		summary.TotalLatencyMillis += item.LatencyMillis
	}
	out := make([]*adminv1.InferenceUsageSummary, 0, len(byKey))
	for _, summary := range byKey {
		out = append(out, summary)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return usageGroupKey(groupBy, out[i].GetGroup()) < usageGroupKey(groupBy, out[j].GetGroup())
	})
	return out
}

func usageGroup(item domaininference.UsageEvent, groupBy []string) map[string]string {
	out := map[string]string{}
	for _, field := range groupBy {
		field = strings.TrimSpace(field)
		switch field {
		case "space", "space_id":
			out[field] = item.SpaceID
		case "domain", "domain_id":
			out[field] = item.DomainID
		case "operation":
			out[field] = string(item.Operation)
		case "usage_mode":
			out[field] = string(item.UsageMode)
		case "status":
			out[field] = string(item.Status)
		case "profile", "profile_id", "inference_profile_id":
			out[field] = uuidOrEmptyAdmin(item.ProfileID)
		case "endpoint", "model_endpoint_id":
			out[field] = uuidOrEmptyAdmin(item.EndpointID)
		case "model", "model_id":
			out[field] = uuidOrEmptyAdmin(item.ModelID)
		case "credential_grant", "credential_grant_id":
			out[field] = uuidOrEmptyAdmin(item.CredentialGrantID)
		case "automation", "automation_id":
			out[field] = item.AutomationID
		case "semantic_index", "semantic_index_id":
			out[field] = item.SemanticIndexID
		case "actor", "actor_principal_id":
			out[field] = item.ActorPrincipalID
		}
	}
	return out
}

func usageGroupKey(groupBy []string, group map[string]string) string {
	parts := make([]string, 0, len(groupBy))
	for _, field := range groupBy {
		parts = append(parts, strings.TrimSpace(field)+"="+group[strings.TrimSpace(field)])
	}
	return strings.Join(parts, "|")
}

func inferenceScopeFromProto(in interface {
	GetSpaceId() string
	GetDomainId() string
	GetSemanticIndexId() string
	GetNodeId() string
	GetIncludeDescendants() bool
}) domaininference.Scope {
	if in == nil {
		return domaininference.Scope{}
	}
	return domaininference.Scope{SpaceID: in.GetSpaceId(), DomainID: in.GetDomainId(), SemanticIndexID: in.GetSemanticIndexId(), NodeID: in.GetNodeId(), IncludeDescendants: in.GetIncludeDescendants()}
}

func timeValueFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
