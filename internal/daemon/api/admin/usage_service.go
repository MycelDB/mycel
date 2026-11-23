package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	activitymodel "github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	usageservice "github.com/myceldb/mycel/internal/usage/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	usageReadCapability           = "usage.read"
	usageDiagnosticReadCapability = "usage.diagnostic.read"
	usageReconcileCapability      = "usage.reconcile"
	usageLimitManageCapability    = "usage.limit.manage"
)

type UsageAccountingManager interface {
	Snapshot(context.Context, usageservice.SnapshotRequest) (usageservice.Snapshot, error)
	List(context.Context, usageservice.ListRequest) (usageservice.ListResponse, error)
	Health(context.Context, usageservice.HealthRequest) usageservice.Health
}

type UsageAdmissionManager interface {
	UpsertUsageLimit(context.Context, usageservice.LimitMutationRequest) (usageservice.UsageLimit, error)
	DeleteUsageLimit(context.Context, usageservice.LimitLookupRequest) (usageservice.UsageLimit, error)
	GetUsageLimit(context.Context, usageservice.LimitLookupRequest) (usageservice.UsageLimit, error)
	ListUsageLimits(context.Context, usageservice.LimitListRequest) (usageservice.LimitListResponse, error)
}

type AdminUsageAccountingService struct {
	adminv1.UnimplementedAdminUsageAccountingServiceServer
	usage      UsageAccountingManager
	authorizer OperatorAuthorizer
}

func NewAdminUsageAccountingService(usage UsageAccountingManager, authorizer OperatorAuthorizer) *AdminUsageAccountingService {
	return &AdminUsageAccountingService{usage: usage, authorizer: authorizer}
}

func (s *AdminUsageAccountingService) GetUsage(ctx context.Context, req *adminv1.GetUsageRequest) (*adminv1.GetUsageResponse, error) {
	if err := s.requireUsageRead(ctx, req.GetIncludeDiagnosticPhysical()); err != nil {
		return nil, err
	}
	snapshot, err := s.usage.Snapshot(ctx, snapshotRequestFromProto(req))
	if err != nil {
		return nil, mapUsageError(err, "get usage")
	}
	if req.GetRequireQuotaSafe() && !snapshot.QuotaSafeComplete {
		return nil, quotaUnsafeError(snapshot)
	}
	return &adminv1.GetUsageResponse{Snapshot: mapUsageSnapshot(snapshot)}, nil
}

func (s *AdminUsageAccountingService) BatchGetUsage(ctx context.Context, req *adminv1.BatchGetUsageRequest) (*adminv1.BatchGetUsageResponse, error) {
	includeDiagnostic := false
	for _, item := range req.GetRequests() {
		includeDiagnostic = includeDiagnostic || item.GetIncludeDiagnosticPhysical()
	}
	if err := s.requireUsageRead(ctx, includeDiagnostic); err != nil {
		return nil, err
	}
	response := &adminv1.BatchGetUsageResponse{Results: make([]*adminv1.BatchUsageResult, 0, len(req.GetRequests()))}
	for _, item := range req.GetRequests() {
		result := &adminv1.BatchUsageResult{Scope: item.GetScope()}
		snapshot, err := s.usage.Snapshot(ctx, snapshotRequestFromProto(item))
		if err != nil {
			result.Result = &adminv1.BatchUsageResult_Status{Status: usageErrorStatus(item.GetScope(), err)}
			response.Results = append(response.Results, result)
			continue
		}
		if item.GetRequireQuotaSafe() && !snapshot.QuotaSafeComplete {
			result.Result = &adminv1.BatchUsageResult_Status{Status: quotaUnsafeStatus(snapshot)}
			response.Results = append(response.Results, result)
			continue
		}
		result.Scope = mapUsageScope(snapshot.Scope)
		result.Result = &adminv1.BatchUsageResult_Snapshot{Snapshot: mapUsageSnapshot(snapshot)}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func (s *AdminUsageAccountingService) ListUsage(ctx context.Context, req *adminv1.ListUsageRequest) (*adminv1.ListUsageResponse, error) {
	if err := s.requireUsageRead(ctx, req.GetIncludeDiagnosticPhysical()); err != nil {
		return nil, err
	}
	listed, err := s.usage.List(ctx, listUsageRequestFromProto(req))
	if err != nil {
		return nil, mapUsageError(err, "list usage")
	}
	response := &adminv1.ListUsageResponse{NextPageToken: listed.NextPageToken}
	for _, snapshot := range listed.Snapshots {
		if req.GetRequireQuotaSafe() && !snapshot.QuotaSafeComplete {
			return nil, quotaUnsafeError(snapshot)
		}
		response.Snapshots = append(response.Snapshots, mapUsageSnapshot(snapshot))
	}
	return response, nil
}

func (s *AdminUsageAccountingService) GetUsageAccountingHealth(ctx context.Context, req *adminv1.GetUsageAccountingHealthRequest) (*adminv1.GetUsageAccountingHealthResponse, error) {
	if err := s.requireUsageRead(ctx, req.GetIncludeDiagnosticCollectors()); err != nil {
		return nil, err
	}
	if req.GetIncludeReconciliation() {
		if err := s.requireUsageReconcile(ctx); err != nil {
			return nil, err
		}
	}
	scope := usageScopeFromProto(req.GetScope())
	if err := scope.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "get usage accounting health: %v", err)
	}
	health := s.usage.Health(ctx, usageservice.HealthRequest{Scope: scope, IncludeReconciliation: req.GetIncludeReconciliation(), IncludeDiagnosticCollectors: req.GetIncludeDiagnosticCollectors()})
	return &adminv1.GetUsageAccountingHealthResponse{Health: mapUsageHealth(health)}, nil
}

type AdminUsageAdmissionService struct {
	adminv1.UnimplementedAdminUsageAdmissionServiceServer
	usage      UsageAdmissionManager
	authorizer OperatorAuthorizer
	activity   activityservice.Manager
}

func NewAdminUsageAdmissionService(usage UsageAdmissionManager, authorizer OperatorAuthorizer, activity ...activityservice.Manager) *AdminUsageAdmissionService {
	svc := &AdminUsageAdmissionService{usage: usage, authorizer: authorizer}
	if len(activity) > 0 {
		svc.activity = activity[0]
	}
	return svc
}

func (s *AdminUsageAdmissionService) UpsertUsageLimit(ctx context.Context, req *adminv1.UpsertUsageLimitRequest) (*adminv1.UpsertUsageLimitResponse, error) {
	if err := s.requireUsageLimitManage(ctx); err != nil {
		return nil, err
	}
	limit, err := s.usage.UpsertUsageLimit(ctx, usageservice.LimitMutationRequest{Limit: usageLimitFromProto(req.GetLimit())})
	if err != nil {
		return nil, mapUsageAdmissionError(err, "upsert usage limit")
	}
	s.emitLimitActivity(ctx, "usage.limit.upserted", "Usage limit upserted", limit)
	return &adminv1.UpsertUsageLimitResponse{Limit: mapUsageLimit(limit)}, nil
}

func (s *AdminUsageAdmissionService) DeleteUsageLimit(ctx context.Context, req *adminv1.DeleteUsageLimitRequest) (*adminv1.DeleteUsageLimitResponse, error) {
	if err := s.requireUsageLimitManage(ctx); err != nil {
		return nil, err
	}
	limit, err := s.usage.DeleteUsageLimit(ctx, usageLimitLookupFromDeleteProto(req))
	if err != nil {
		return nil, mapUsageAdmissionError(err, "delete usage limit")
	}
	s.emitLimitActivity(ctx, "usage.limit.deleted", "Usage limit deleted", limit)
	return &adminv1.DeleteUsageLimitResponse{Limit: mapUsageLimit(limit)}, nil
}

func (s *AdminUsageAdmissionService) GetUsageLimit(ctx context.Context, req *adminv1.GetUsageLimitRequest) (*adminv1.GetUsageLimitResponse, error) {
	if err := s.requireUsageLimitManage(ctx); err != nil {
		return nil, err
	}
	limit, err := s.usage.GetUsageLimit(ctx, usageLimitLookupFromGetProto(req))
	if err != nil {
		return nil, mapUsageAdmissionError(err, "get usage limit")
	}
	return &adminv1.GetUsageLimitResponse{Limit: mapUsageLimit(limit)}, nil
}

func (s *AdminUsageAdmissionService) ListUsageLimits(ctx context.Context, req *adminv1.ListUsageLimitsRequest) (*adminv1.ListUsageLimitsResponse, error) {
	if err := s.requireUsageLimitManage(ctx); err != nil {
		return nil, err
	}
	listed, err := s.usage.ListUsageLimits(ctx, usageservice.LimitListRequest{Parent: usageScopeFromProto(req.GetParent()), PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken(), MetricName: strings.TrimSpace(req.GetMetricName())})
	if err != nil {
		return nil, mapUsageAdmissionError(err, "list usage limits")
	}
	resp := &adminv1.ListUsageLimitsResponse{NextPageToken: listed.NextPageToken}
	for _, limit := range listed.Limits {
		resp.Limits = append(resp.Limits, mapUsageLimit(limit))
	}
	return resp, nil
}

func (s *AdminUsageAdmissionService) requireUsageLimitManage(ctx context.Context) error {
	if s.usage == nil {
		return status.Error(codes.Unavailable, "usage admission service is not configured")
	}
	return requireUsageCapability(ctx, s.authorizer, usageLimitManageCapability, "usage limit management")
}

func (s *AdminUsageAdmissionService) emitLimitActivity(ctx context.Context, eventType string, message string, limit usageservice.UsageLimit) {
	if s.activity == nil {
		return
	}
	principal, _ := principalFromContext(ctx)
	_ = s.activity.Emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryAccess, eventType, message, func(event *activitymodel.Event) {
		event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		event.Resource = activitymodel.Resource{Kind: "usage_limit", ID: limit.LimitID, Name: limit.MetricName}
	})
}

func (s *AdminUsageAccountingService) requireUsageRead(ctx context.Context, includeDiagnostic bool) error {
	if s.usage == nil {
		return status.Error(codes.Unavailable, "usage accounting service is not configured")
	}
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if s.authorizer == nil {
		return status.Error(codes.PermissionDenied, "usage accounting authorization is not configured")
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, usageReadCapability)
	if err != nil {
		return status.Errorf(codes.Internal, "authorize usage read: %v", err)
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required usage.read capability")
	}
	if includeDiagnostic {
		ok, err = s.authorizer.HasCapability(ctx, principal.PrincipalID, usageDiagnosticReadCapability)
		if err != nil {
			return status.Errorf(codes.Internal, "authorize usage diagnostic read: %v", err)
		}
		if !ok {
			return status.Error(codes.PermissionDenied, "operator lacks required usage.diagnostic.read capability")
		}
	}
	return nil
}

func (s *AdminUsageAccountingService) requireUsageReconcile(ctx context.Context) error {
	if s.usage == nil {
		return status.Error(codes.Unavailable, "usage accounting service is not configured")
	}
	return requireUsageCapability(ctx, s.authorizer, usageReconcileCapability, "usage reconciliation")
}

func requireUsageCapability(ctx context.Context, authorizer OperatorAuthorizer, capability string, action string) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if authorizer == nil {
		return status.Error(codes.PermissionDenied, "usage accounting authorization is not configured")
	}
	ok, err := authorizer.HasCapability(ctx, principal.PrincipalID, capability)
	if err != nil {
		return status.Errorf(codes.Internal, "authorize %s: %v", action, err)
	}
	if !ok {
		return status.Errorf(codes.PermissionDenied, "operator lacks required %s capability", capability)
	}
	return nil
}

func snapshotRequestFromProto(req *adminv1.GetUsageRequest) usageservice.SnapshotRequest {
	if req == nil {
		return usageservice.SnapshotRequest{}
	}
	return usageservice.SnapshotRequest{Scope: usageScopeFromProto(req.GetScope()), DomainInclusion: usageDomainInclusionFromProto(req.GetDomainInclusion()), MetricNames: trimUsageMetricNames(req.GetMetricNames()), RequireQuotaSafe: req.GetRequireQuotaSafe(), IncludeDiagnosticPhysical: req.GetIncludeDiagnosticPhysical(), MaxStaleness: durationFromUsageSeconds(req.GetMaxStalenessSeconds()), MinimumRevision: usageRevisionFromProto(req.GetMinimumRevision())}
}

func listUsageRequestFromProto(req *adminv1.ListUsageRequest) usageservice.ListRequest {
	if req == nil {
		return usageservice.ListRequest{}
	}
	return usageservice.ListRequest{Parent: usageScopeFromProto(req.GetParent()), Mode: usageListModeFromProto(req.GetMode()), DomainInclusion: usageDomainInclusionFromProto(req.GetDomainInclusion()), MetricNames: trimUsageMetricNames(req.GetMetricNames()), RequireQuotaSafe: req.GetRequireQuotaSafe(), IncludeDiagnosticPhysical: req.GetIncludeDiagnosticPhysical(), MaxStaleness: durationFromUsageSeconds(req.GetMaxStalenessSeconds()), MinimumRevision: usageRevisionFromProto(req.GetMinimumRevision()), PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken()}
}

func usageScopeFromProto(scope *adminv1.UsageScope) usageservice.Scope {
	if scope == nil {
		return usageservice.Scope{}
	}
	if space := scope.GetSpace(); space != nil {
		return usageservice.SpaceScope(space.GetSpaceId())
	}
	if domain := scope.GetDomain(); domain != nil {
		return usageservice.DomainScope(domain.GetSpaceId(), domain.GetDomainId())
	}
	return usageservice.Scope{}
}

func mapUsageScope(scope usageservice.Scope) *adminv1.UsageScope {
	spaceID := strings.TrimSpace(scope.SpaceID)
	domainID := strings.TrimSpace(scope.DomainID)
	switch scope.Type {
	case usageservice.ScopeTypeSpace:
		return &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: spaceID}}}
	case usageservice.ScopeTypeDomain:
		return &adminv1.UsageScope{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: spaceID, DomainId: domainID}}}
	default:
		return nil
	}
}

func usageDomainInclusionFromProto(mode adminv1.DomainInclusionMode) usageservice.DomainInclusionMode {
	switch mode {
	case adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_NON_SYSTEM:
		return usageservice.DomainInclusionRetainedNonSystem
	case adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_ALL:
		return usageservice.DomainInclusionRetainedAll
	case adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_SYSTEM_ONLY:
		return usageservice.DomainInclusionSystemOnly
	default:
		return usageservice.DomainInclusionUnspecified
	}
}

func mapUsageDomainInclusion(mode usageservice.DomainInclusionMode) adminv1.DomainInclusionMode {
	switch mode {
	case usageservice.DomainInclusionRetainedNonSystem:
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_NON_SYSTEM
	case usageservice.DomainInclusionRetainedAll:
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_ALL
	case usageservice.DomainInclusionSystemOnly:
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_SYSTEM_ONLY
	default:
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_UNSPECIFIED
	}
}

func usageListModeFromProto(mode adminv1.UsageListMode) usageservice.ListMode {
	switch mode {
	case adminv1.UsageListMode_USAGE_LIST_MODE_DOMAINS_IN_SPACE:
		return usageservice.ListModeDomainsInSpace
	case adminv1.UsageListMode_USAGE_LIST_MODE_SPACE_WITH_DOMAIN_BREAKDOWN:
		return usageservice.ListModeSpaceWithDomainBreakdown
	default:
		return usageservice.ListModeSpaces
	}
}

func usageLimitFromProto(limit *adminv1.UsageLimit) usageservice.UsageLimit {
	if limit == nil {
		return usageservice.UsageLimit{}
	}
	createdAt := time.Time{}
	if value := timeFromProto(limit.GetCreatedAt()); value != nil {
		createdAt = *value
	}
	updatedAt := time.Time{}
	if value := timeFromProto(limit.GetUpdatedAt()); value != nil {
		updatedAt = *value
	}
	return usageservice.UsageLimit{LimitID: strings.TrimSpace(limit.GetLimitId()), Scope: usageScopeFromProto(limit.GetScope()), DomainInclusion: usageDomainInclusionFromProto(limit.GetDomainInclusion()), MetricName: strings.TrimSpace(limit.GetMetricName()), MetricVersion: limit.GetMetricVersion(), HardLimit: limit.GetHardLimit(), Enabled: limit.GetEnabled(), PolicyRef: strings.TrimSpace(limit.GetPolicyRef()), Description: strings.TrimSpace(limit.GetDescription()), CreatedAt: createdAt, UpdatedAt: updatedAt}
}

func usageLimitLookupFromGetProto(req *adminv1.GetUsageLimitRequest) usageservice.LimitLookupRequest {
	if req == nil {
		return usageservice.LimitLookupRequest{}
	}
	return usageservice.LimitLookupRequest{LimitID: strings.TrimSpace(req.GetLimitId()), Scope: usageScopeFromProto(req.GetScope()), DomainInclusion: usageDomainInclusionFromProto(req.GetDomainInclusion()), MetricName: strings.TrimSpace(req.GetMetricName()), MetricVersion: req.GetMetricVersion()}
}

func usageLimitLookupFromDeleteProto(req *adminv1.DeleteUsageLimitRequest) usageservice.LimitLookupRequest {
	if req == nil {
		return usageservice.LimitLookupRequest{}
	}
	return usageservice.LimitLookupRequest{LimitID: strings.TrimSpace(req.GetLimitId()), Scope: usageScopeFromProto(req.GetScope()), DomainInclusion: usageDomainInclusionFromProto(req.GetDomainInclusion()), MetricName: strings.TrimSpace(req.GetMetricName()), MetricVersion: req.GetMetricVersion()}
}

func mapUsageLimit(limit usageservice.UsageLimit) *adminv1.UsageLimit {
	return &adminv1.UsageLimit{LimitId: limit.LimitID, Scope: mapUsageScope(limit.Scope), DomainInclusion: mapUsageDomainInclusion(limit.DomainInclusion), MetricName: limit.MetricName, MetricVersion: limit.MetricVersion, HardLimit: limit.HardLimit, Enabled: limit.Enabled, PolicyRef: limit.PolicyRef, Description: limit.Description, CreatedAt: timestampOrNil(limit.CreatedAt), UpdatedAt: timestampOrNil(limit.UpdatedAt)}
}

func mapUsageSnapshot(snapshot usageservice.Snapshot) *adminv1.UsageSnapshot {
	out := &adminv1.UsageSnapshot{Scope: mapUsageScope(snapshot.Scope), DomainInclusion: mapUsageDomainInclusion(snapshot.DomainInclusion), ObservedAt: timestampOrNil(snapshot.ObservedAt), AsOfRevision: mapUsageRevision(snapshot.Revision), AccountingState: mapUsageAccountingState(snapshot.State), QuotaSafeComplete: snapshot.QuotaSafeComplete}
	for _, metric := range snapshot.Metrics {
		out.Metrics = append(out.Metrics, mapUsageMetric(metric))
	}
	out.Blockers = mapUsageStatuses(snapshot.Blockers)
	out.Warnings = mapUsageStatuses(snapshot.Warnings)
	return out
}

func mapUsageHealth(health usageservice.Health) *adminv1.UsageAccountingHealth {
	return &adminv1.UsageAccountingHealth{Scope: mapUsageScope(health.Scope), ObservedAt: timestampOrNil(health.ObservedAt), State: mapUsageAccountingState(health.State), AsOfRevision: mapUsageRevision(health.Revision), QuotaSafeReady: health.QuotaSafeReady, Blockers: mapUsageStatuses(health.Blockers), Warnings: mapUsageStatuses(health.Warnings), RebuildingScopeCount: health.RebuildingScopeCount, StaleScopeCount: health.StaleScopeCount, DriftScopeCount: health.DriftScopeCount, DiagnosticCollectorUnavailableCount: health.DiagnosticCollectorUnavailableCount}
}

func mapUsageMetric(metric usageservice.Metric) *adminv1.UsageMetric {
	return &adminv1.UsageMetric{Name: metric.Name, MetricVersion: metric.Version, Value: metric.Value, Unit: mapUsageUnit(metric.Unit), Authority: mapUsageAuthority(metric.Authority), QuotaSafe: metric.QuotaSafe, HardLimitEligible: metric.HardLimitEligible, ObservedAt: timestampOrNil(metric.ObservedAt), AsOfRevision: mapUsageRevision(metric.Revision)}
}

func mapUsageStatuses(statuses []usageservice.Status) []*adminv1.UsageStatus {
	out := make([]*adminv1.UsageStatus, 0, len(statuses))
	for _, item := range statuses {
		out = append(out, mapUsageStatus(item))
	}
	return out
}

func mapUsageStatus(item usageservice.Status) *adminv1.UsageStatus {
	return &adminv1.UsageStatus{Reason: mapUsageStatusReason(item.Reason), Severity: mapUsageStatusSeverity(item.Severity), Message: item.Message, Scope: mapUsageScope(item.Scope), MetricName: item.Metric, Evidence: cloneStringMap(item.Evidence)}
}

func mapUsageRevision(revision usageservice.Revision) *adminv1.UsageRevision {
	out := &adminv1.UsageRevision{RevisionToken: revision.Token}
	for _, source := range revision.Sources {
		out.SourceRevisions = append(out.SourceRevisions, &adminv1.UsageSourceRevision{Source: source.Source, Revision: source.Revision})
	}
	return out
}

func usageRevisionFromProto(revision *adminv1.UsageRevision) usageservice.Revision {
	if revision == nil {
		return usageservice.Revision{}
	}
	out := usageservice.Revision{Token: strings.TrimSpace(revision.GetRevisionToken())}
	for _, source := range revision.GetSourceRevisions() {
		out.Sources = append(out.Sources, usageservice.SourceRevision{Source: strings.TrimSpace(source.GetSource()), Revision: strings.TrimSpace(source.GetRevision())})
	}
	return out
}

func mapUsageUnit(unit usageservice.Unit) adminv1.UsageUnit {
	switch unit {
	case usageservice.UnitBytes:
		return adminv1.UsageUnit_USAGE_UNIT_BYTES
	case usageservice.UnitCount:
		return adminv1.UsageUnit_USAGE_UNIT_COUNT
	default:
		return adminv1.UsageUnit_USAGE_UNIT_UNSPECIFIED
	}
}

func mapUsageAuthority(authority usageservice.Authority) adminv1.UsageAuthority {
	switch authority {
	case usageservice.AuthorityRaftCommittedExact:
		return adminv1.UsageAuthority_USAGE_AUTHORITY_RAFT_COMMITTED_EXACT
	case usageservice.AuthorityStateMachineExact:
		return adminv1.UsageAuthority_USAGE_AUTHORITY_STATE_MACHINE_EXACT
	case usageservice.AuthorityObservedApproximate:
		return adminv1.UsageAuthority_USAGE_AUTHORITY_OBSERVED_APPROXIMATE
	case usageservice.AuthorityRebuildableOperation:
		return adminv1.UsageAuthority_USAGE_AUTHORITY_REBUILDABLE_OPERATIONAL
	default:
		return adminv1.UsageAuthority_USAGE_AUTHORITY_UNSPECIFIED
	}
}

func mapUsageAccountingState(state usageservice.AccountingState) adminv1.UsageAccountingState {
	switch state {
	case usageservice.AccountingStateReady:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_READY
	case usageservice.AccountingStateRebuilding:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_REBUILDING
	case usageservice.AccountingStateReconciling:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_RECONCILING
	case usageservice.AccountingStateStale:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_STALE
	case usageservice.AccountingStateDriftDetected:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_DRIFT_DETECTED
	case usageservice.AccountingStateUnavailable:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_UNAVAILABLE
	default:
		return adminv1.UsageAccountingState_USAGE_ACCOUNTING_STATE_UNSPECIFIED
	}
}

func mapUsageStatusReason(reason usageservice.StatusReason) adminv1.UsageStatusReason {
	switch reason {
	case usageservice.StatusReasonCountersRebuilding:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_COUNTERS_REBUILDING
	case usageservice.StatusReasonCounterStale:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_COUNTER_STALE
	case usageservice.StatusReasonReconciliationDrift:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_RECONCILIATION_DRIFT
	case usageservice.StatusReasonMetricUnavailable:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_UNAVAILABLE
	case usageservice.StatusReasonMetricNotQuotaSafe:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_NOT_QUOTA_SAFE
	case usageservice.StatusReasonUnsupportedScope:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_UNSUPPORTED_SCOPE
	case usageservice.StatusReasonUnsupportedRevision:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_UNSUPPORTED_REVISION
	case usageservice.StatusReasonDiagnosticCollectorUnavailable:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_DIAGNOSTIC_COLLECTOR_UNAVAILABLE
	default:
		return adminv1.UsageStatusReason_USAGE_STATUS_REASON_UNSPECIFIED
	}
}

func mapUsageStatusSeverity(severity usageservice.StatusSeverity) adminv1.UsageStatusSeverity {
	switch severity {
	case usageservice.StatusSeverityInfo:
		return adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_INFO
	case usageservice.StatusSeverityWarning:
		return adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_WARNING
	case usageservice.StatusSeverityBlocker:
		return adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_BLOCKER
	default:
		return adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_UNSPECIFIED
	}
}

func durationFromUsageSeconds(seconds uint64) time.Duration {
	const maxDurationSeconds = uint64(1<<63-1) / uint64(time.Second)
	if seconds > maxDurationSeconds {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(seconds) * time.Second
}

func trimUsageMetricNames(names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func quotaUnsafeError(snapshot usageservice.Snapshot) error {
	msg := "usage snapshot is not quota-safe complete"
	if len(snapshot.Blockers) > 0 && strings.TrimSpace(snapshot.Blockers[0].Message) != "" {
		msg = snapshot.Blockers[0].Message
	}
	return status.Error(codes.FailedPrecondition, msg)
}

func quotaUnsafeStatus(snapshot usageservice.Snapshot) *adminv1.UsageStatus {
	if len(snapshot.Blockers) > 0 {
		return mapUsageStatus(snapshot.Blockers[0])
	}
	return &adminv1.UsageStatus{Reason: adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_UNAVAILABLE, Severity: adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_BLOCKER, Message: "usage snapshot is not quota-safe complete", Scope: mapUsageScope(snapshot.Scope)}
}

func usageErrorStatus(scope *adminv1.UsageScope, err error) *adminv1.UsageStatus {
	reason := adminv1.UsageStatusReason_USAGE_STATUS_REASON_METRIC_UNAVAILABLE
	if status.Code(mapUsageError(err, "usage")) == codes.InvalidArgument {
		reason = adminv1.UsageStatusReason_USAGE_STATUS_REASON_UNSUPPORTED_SCOPE
	}
	return &adminv1.UsageStatus{Reason: reason, Severity: adminv1.UsageStatusSeverity_USAGE_STATUS_SEVERITY_BLOCKER, Message: err.Error(), Scope: scope}
}

func mapUsageError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, usageservice.ErrCountersRebuilding) {
		return status.Errorf(codes.FailedPrecondition, "%s: %v", action, err)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "scope") || strings.Contains(message, "page_token") || strings.Contains(message, "unsupported") || strings.Contains(message, "requires") || strings.Contains(message, "registered") {
		return status.Errorf(codes.InvalidArgument, "%s: %v", action, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

func mapUsageAdmissionError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, usageservice.ErrUsageLimitNotFound) {
		return status.Errorf(codes.NotFound, "%s: usage limit not found", action)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "scope") || strings.Contains(message, "page token") || strings.Contains(message, "page_token") || strings.Contains(message, "unsupported") || strings.Contains(message, "requires") || strings.Contains(message, "registered") || strings.Contains(message, "hard-limit eligible") || strings.Contains(message, "does not match") {
		return status.Errorf(codes.InvalidArgument, "%s: %v", action, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

var _ adminv1.AdminUsageAccountingServiceServer = (*AdminUsageAccountingService)(nil)
var _ adminv1.AdminUsageAdmissionServiceServer = (*AdminUsageAdmissionService)(nil)
