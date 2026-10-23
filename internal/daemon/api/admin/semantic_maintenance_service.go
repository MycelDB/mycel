package admin

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminSemanticMaintenanceService struct {
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer
	semantic   daemonsemantic.Manager
	authorizer OperatorAuthorizer
}

func NewAdminSemanticMaintenanceService(semantic daemonsemantic.Manager, authorizer OperatorAuthorizer) *AdminSemanticMaintenanceService {
	return &AdminSemanticMaintenanceService{semantic: semantic, authorizer: authorizer}
}

func (s *AdminSemanticMaintenanceService) GetSemanticMaintenanceStatus(ctx context.Context, req *adminv1.GetSemanticMaintenanceStatusRequest) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	res, err := s.semantic.GetMaintenanceStatus(ctx, daemonsemantic.MaintenanceStatusInput{SpaceID: spaceID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "get semantic maintenance status")
	}
	return &adminv1.GetSemanticMaintenanceStatusResponse{Enabled: res.Enabled, Degraded: res.Degraded, DegradedReason: res.DegradedReason, QueueDepthPending: int32(res.QueueDepthPending), QueueDepthRunning: int32(res.QueueDepthRunning), QueueDepthFailedRetryable: int32(res.QueueDepthFailedRetryable), QueueDepthFailedPermanent: int32(res.QueueDepthFailedPermanent), OldestPendingAgeSeconds: int64(res.OldestPendingAge.Seconds()), LastDirtyEventAt: formatMaintenanceTime(res.LastDirtyEventAt), LastAnalyzedAt: formatMaintenanceTime(res.LastAnalyzedAt), LastWorkerSuccessAt: formatMaintenanceTime(res.LastWorkerSuccessAt), LastWorkerErrorAt: formatMaintenanceTime(res.LastWorkerErrorAt), ThrottleState: res.ThrottleState, AnalyzerRuns: int32(res.AnalyzerRuns), WorkerRuns: int32(res.WorkerRuns)}, nil
}

func (s *AdminSemanticMaintenanceService) GetSemanticReliabilityStatus(ctx context.Context, req *adminv1.GetSemanticReliabilityStatusRequest) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	result, err := s.semantic.GetReliabilityStatus(ctx, daemonsemantic.ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: req.GetEmbeddingBindingKey()})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "get semantic reliability status")
	}
	return mapReliabilityStatus(result), nil
}

func (s *AdminSemanticMaintenanceService) RebuildSemanticRule(ctx context.Context, req *adminv1.RebuildSemanticRuleRequest) (*adminv1.RebuildSemanticRuleResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	result, err := s.semantic.RebuildSemanticRule(ctx, daemonsemantic.RebuildInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: req.GetEmbeddingBindingKey(), BackfillLimit: int(req.GetBackfillLimit()), MaintenanceBatchLimit: int(req.GetMaintenanceBatchLimit()), MaxCatchupIterations: int(req.GetMaxCatchupIterations())})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "rebuild semantic rule")
	}
	return &adminv1.RebuildSemanticRuleResponse{Rebuild: mapSemanticRebuildStatus(result.Reliability.Rebuild), Reliability: mapReliabilityStatus(result.Reliability), SelectedCount: int32(result.Backfill.SelectedCount), GeneratedCount: int32(result.Backfill.GeneratedCount), SkippedCount: int32(result.Backfill.SkippedCount), FailedCount: int32(result.Backfill.FailedCount)}, nil
}

func (s *AdminSemanticMaintenanceService) ListSemanticMaintenanceWork(ctx context.Context, req *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := optionalSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	items, err := s.semantic.ListMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkListInput{SpaceID: spaceID, Status: req.GetStatus(), Limit: int(req.GetLimit()), SemanticRuleID: ruleID, EmbeddingBindingKey: req.GetEmbeddingBindingKey()})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "list semantic maintenance work")
	}
	out := &adminv1.ListSemanticMaintenanceWorkResponse{}
	for _, item := range items {
		out.Items = append(out.Items, mapMaintenanceWorkItem(item))
	}
	return out, nil
}

func (s *AdminSemanticMaintenanceService) RetrySemanticMaintenanceWork(ctx context.Context, req *adminv1.RetrySemanticMaintenanceWorkRequest) (*adminv1.RetrySemanticMaintenanceWorkResponse, error) {
	spaceID, workID, err := parseMaintenanceWorkControl(req.GetSpaceId(), req.GetWorkItemId())
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	item, err := s.semantic.RetryMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkControlInput{SpaceID: spaceID, WorkItemID: workID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "retry semantic maintenance work")
	}
	return &adminv1.RetrySemanticMaintenanceWorkResponse{Item: mapMaintenanceWorkItem(item)}, nil
}

func (s *AdminSemanticMaintenanceService) CancelSemanticMaintenanceWork(ctx context.Context, req *adminv1.CancelSemanticMaintenanceWorkRequest) (*adminv1.CancelSemanticMaintenanceWorkResponse, error) {
	spaceID, workID, err := parseMaintenanceWorkControl(req.GetSpaceId(), req.GetWorkItemId())
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	item, err := s.semantic.CancelMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkControlInput{SpaceID: spaceID, WorkItemID: workID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "cancel semantic maintenance work")
	}
	return &adminv1.CancelSemanticMaintenanceWorkResponse{Item: mapMaintenanceWorkItem(item)}, nil
}

func (s *AdminSemanticMaintenanceService) AnalyzeSemanticDirtyWork(ctx context.Context, req *adminv1.AnalyzeSemanticDirtyWorkRequest) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := optionalSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	res, err := s.semantic.AnalyzeDirtyWork(ctx, daemonsemantic.AnalyzeInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: req.GetEmbeddingBindingKey(), Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "analyze semantic dirty work")
	}
	return &adminv1.AnalyzeSemanticDirtyWorkResponse{ProcessedEvents: int32(res.ProcessedEvents), EnqueuedItems: int32(res.EnqueuedItems)}, nil
}

func (s *AdminSemanticMaintenanceService) ProcessSemanticDirtyWork(ctx context.Context, req *adminv1.ProcessSemanticDirtyWorkRequest) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	res, err := s.semantic.ProcessDirtyWork(ctx, daemonsemantic.ProcessInput{SpaceID: spaceID, Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "process semantic dirty work")
	}
	return &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: int32(res.Processed), CompletedItems: int32(res.Completed), FailedItems: int32(res.Failed)}, nil
}

func (s *AdminSemanticMaintenanceService) BackfillSemanticRule(ctx context.Context, req *adminv1.BackfillSemanticRuleRequest) (*adminv1.BackfillSemanticRuleResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	ruleID, err := parseSemanticUUID[domainsemantic.SemanticRuleID](req.GetSemanticRuleId(), "semantic_rule_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	nodeIDs := make([]graph.NodeID, 0, len(req.GetNodeIds()))
	for _, raw := range req.GetNodeIds() {
		id, err := parseSemanticUUID[graph.NodeID](raw, "node_id")
		if err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, id)
	}
	res, err := s.semantic.BackfillIndex(ctx, semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: req.GetEmbeddingBindingKey(), NodeIDs: nodeIDs, Force: req.GetForce(), Reconcile: req.GetForce() && len(nodeIDs) == 0, Limit: int(req.GetLimit()), ContinueOnError: req.GetContinueOnError()})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "backfill semantic rule")
	}
	return mapBackfillResponse(res), nil
}

func (s *AdminSemanticMaintenanceService) requireMaintenance(ctx context.Context, scope principalservice.AccessScope) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
		return scoped.Authorize(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String(), scope)
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE.String())
	if err != nil {
		return status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required semantic maintenance capability")
	}
	return nil
}

func parseMaintenanceWorkControl(spaceIDText string, workIDText string) (domainspace.SpaceID, uuid.UUID, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](spaceIDText, "space_id")
	if err != nil {
		return domainspace.SpaceID(uuid.Nil), uuid.Nil, err
	}
	workID, err := uuid.Parse(workIDText)
	if err != nil || workID == uuid.Nil {
		return domainspace.SpaceID(uuid.Nil), uuid.Nil, status.Error(codes.InvalidArgument, "work_item_id must be a UUID")
	}
	return spaceID, workID, nil
}

func mapMaintenanceWorkItem(item daemonsemantic.MaintenanceWorkItem) *adminv1.SemanticMaintenanceWorkItem {
	return &adminv1.SemanticMaintenanceWorkItem{WorkItemId: item.ID.String(), SpaceId: item.SpaceID.String(), DomainId: item.DomainID.String(), SemanticRuleId: item.SemanticRuleID.String(), EmbeddingBindingKey: item.EmbeddingBindingKey, TargetNodeId: item.TargetNodeID.String(), Action: item.Action, Status: item.Status, AttemptCount: int32(item.AttemptCount), NotBefore: formatMaintenanceTime(item.NotBefore), ClaimedUntil: formatMaintenanceTime(item.ClaimedUntil), LastErrorCategory: item.LastErrorCategory, LastErrorMessageSanitized: item.LastErrorMessageSanitized, CreatedAt: formatMaintenanceTime(item.CreatedAt), UpdatedAt: formatMaintenanceTime(item.UpdatedAt), FirstGraphRevision: item.FirstGraphRevision, LastGraphRevision: item.LastGraphRevision, WorkGeneration: item.Generation}
}

func mapReliabilityStatus(result daemonsemantic.ReliabilityStatus) *adminv1.GetSemanticReliabilityStatusResponse {
	return &adminv1.GetSemanticReliabilityStatusResponse{SpaceId: result.SpaceID.String(), SemanticRuleId: result.SemanticRuleID.String(), EmbeddingBindingKey: result.EmbeddingBindingKey, LatestObservedGraphRevision: result.LatestObservedGraphRevision, AnalyzedThroughGraphRevision: result.AnalyzedThroughGraphRevision, FreshThroughGraphRevision: result.FreshThroughGraphRevision, UnresolvedGapCount: result.UnresolvedGapCount, OldestUnresolvedGraphRevision: result.OldestUnresolvedGraphRevision, ConfiguredFrontierLimit: result.ConfiguredFrontierLimit, ConfiguredGapIntervalLimit: result.ConfiguredGapIntervalLimit, ConfiguredRevisionSpanLimit: result.ConfiguredRevisionSpanLimit, ConfiguredSerializedByteLimit: result.ConfiguredSerializedByteLimit, RetainedWorkItemCount: result.RetainedWorkItemCount, RetainedGapIntervalCount: result.RetainedGapIntervalCount, RetainedRevisionSpan: result.RetainedRevisionSpan, RetainedSerializedBytes: result.RetainedSerializedBytes, SemanticGeneration: result.SemanticGeneration, CapacityState: result.CapacityState, Degraded: result.Degraded, DegradedReason: result.DegradedReason, RebuildRequired: result.RebuildRequired, Rebuild: mapSemanticRebuildStatus(result.Rebuild)}
}

func mapSemanticRebuildStatus(result domainsemantic.SemanticReliabilityState) *adminv1.SemanticRebuildStatus {
	out := &adminv1.SemanticRebuildStatus{RebuildEpoch: result.RebuildEpoch, State: result.RebuildState, PinnedTargetGraphRevision: result.PinnedTargetRevision, DeltaCatchupGraphRevision: result.DeltaCatchupRevision, SemanticGeneration: result.SemanticGeneration, ErrorMessageSanitized: result.RebuildErrorSanitized}
	if result.RebuildStartedAt != nil {
		out.StartedAt = formatMaintenanceTime(*result.RebuildStartedAt)
	}
	if result.RebuildCompletedAt != nil {
		out.CompletedAt = formatMaintenanceTime(*result.RebuildCompletedAt)
	}
	return out
}

func formatMaintenanceTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func mapBackfillResponse(res semanticbackfill.Result) *adminv1.BackfillSemanticRuleResponse {
	out := &adminv1.BackfillSemanticRuleResponse{SemanticRuleId: uuid.UUID(res.SemanticRuleID).String(), EmbeddingBindingKey: res.EmbeddingBindingKey, SelectedCount: int32(res.SelectedCount), GeneratedCount: int32(res.GeneratedCount), SkippedCount: int32(res.SkippedCount), FailedCount: int32(res.FailedCount)}
	for _, skipped := range res.Skipped {
		out.Skipped = append(out.Skipped, &adminv1.BackfillSkipped{NodeId: uuid.UUID(skipped.NodeID).String(), Reason: skipped.Reason})
	}
	for _, failure := range res.Failures {
		out.Failures = append(out.Failures, &adminv1.BackfillFailure{NodeId: uuid.UUID(failure.NodeID).String(), Error: sanitizeSemanticBackfillError(failure.Error)})
	}
	return out
}

func mapAdminSemanticMaintenanceError(err error, action string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, "semantic maintenance request did not complete")
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return status.Error(st.Code(), "semantic maintenance request failed")
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "not found"):
		return status.Error(codes.NotFound, "semantic maintenance resource not found")
	case strings.Contains(lower, "required") || strings.Contains(lower, "invalid"):
		return status.Error(codes.InvalidArgument, "semantic maintenance request is invalid")
	case strings.Contains(lower, "already running") || strings.Contains(lower, "stale semantic"):
		return status.Error(codes.FailedPrecondition, "semantic maintenance state changed")
	case strings.Contains(lower, "capacity") || strings.Contains(lower, "exhausted"):
		return status.Error(codes.ResourceExhausted, "semantic maintenance capacity exhausted")
	default:
		return status.Errorf(codes.Internal, "%s failed", action)
	}
}

func sanitizeSemanticBackfillError(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	switch {
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline"):
		return "semantic provider timeout"
	case strings.Contains(lower, "rate limit") || strings.Contains(lower, "rate_limited") || strings.Contains(lower, "429"):
		return "semantic provider rate limited"
	case strings.Contains(lower, "credential") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "permission") || strings.Contains(lower, "policy"):
		return "semantic provider authorization failed"
	case strings.Contains(lower, "configuration") || strings.Contains(lower, "not found") || strings.Contains(lower, "disabled") || strings.Contains(lower, "invalid"):
		return "semantic configuration failed"
	case strings.Contains(lower, "vector"):
		return "semantic vector store failed"
	default:
		return "semantic backfill failed"
	}
}
