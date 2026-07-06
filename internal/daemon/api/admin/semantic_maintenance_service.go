package admin

import (
	"context"
	"time"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	"github.com/myceldb/mycel/internal/graph/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
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
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	res, err := s.semantic.GetMaintenanceStatus(ctx, daemonsemantic.MaintenanceStatusInput{SpaceID: spaceID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "get semantic maintenance status")
	}
	return &adminv1.GetSemanticMaintenanceStatusResponse{Enabled: res.Enabled, Degraded: res.Degraded, DegradedReason: res.DegradedReason, QueueDepthPending: int32(res.QueueDepthPending), QueueDepthRunning: int32(res.QueueDepthRunning), QueueDepthFailedRetryable: int32(res.QueueDepthFailedRetryable), QueueDepthFailedPermanent: int32(res.QueueDepthFailedPermanent), OldestPendingAgeSeconds: int64(res.OldestPendingAge.Seconds()), LastDirtyEventAt: formatMaintenanceTime(res.LastDirtyEventAt), LastAnalyzedAt: formatMaintenanceTime(res.LastAnalyzedAt), LastWorkerSuccessAt: formatMaintenanceTime(res.LastWorkerSuccessAt), LastWorkerErrorAt: formatMaintenanceTime(res.LastWorkerErrorAt), ThrottleState: res.ThrottleState, AnalyzerRuns: int32(res.AnalyzerRuns), WorkerRuns: int32(res.WorkerRuns)}, nil
}

func (s *AdminSemanticMaintenanceService) ListSemanticMaintenanceWork(ctx context.Context, req *adminv1.ListSemanticMaintenanceWorkRequest) (*adminv1.ListSemanticMaintenanceWorkResponse, error) {
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	items, err := s.semantic.ListMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkListInput{SpaceID: spaceID, Status: req.GetStatus(), Limit: int(req.GetLimit())})
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
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, workID, err := parseMaintenanceWorkControl(req.GetSpaceId(), req.GetWorkItemId())
	if err != nil {
		return nil, err
	}
	item, err := s.semantic.RetryMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkControlInput{SpaceID: spaceID, WorkItemID: workID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "retry semantic maintenance work")
	}
	return &adminv1.RetrySemanticMaintenanceWorkResponse{Item: mapMaintenanceWorkItem(item)}, nil
}

func (s *AdminSemanticMaintenanceService) CancelSemanticMaintenanceWork(ctx context.Context, req *adminv1.CancelSemanticMaintenanceWorkRequest) (*adminv1.CancelSemanticMaintenanceWorkResponse, error) {
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, workID, err := parseMaintenanceWorkControl(req.GetSpaceId(), req.GetWorkItemId())
	if err != nil {
		return nil, err
	}
	item, err := s.semantic.CancelMaintenanceWork(ctx, daemonsemantic.MaintenanceWorkControlInput{SpaceID: spaceID, WorkItemID: workID})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "cancel semantic maintenance work")
	}
	return &adminv1.CancelSemanticMaintenanceWorkResponse{Item: mapMaintenanceWorkItem(item)}, nil
}

func (s *AdminSemanticMaintenanceService) AnalyzeSemanticDirtyWork(ctx context.Context, req *adminv1.AnalyzeSemanticDirtyWorkRequest) (*adminv1.AnalyzeSemanticDirtyWorkResponse, error) {
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	indexID, err := optionalSemanticUUID[domainsemantic.SemanticIndexID](req.GetSemanticIndexId(), "semantic_index_id")
	if err != nil {
		return nil, err
	}
	res, err := s.semantic.AnalyzeDirtyWork(ctx, daemonsemantic.AnalyzeInput{SpaceID: spaceID, SemanticIndexID: indexID, Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "analyze semantic dirty work")
	}
	return &adminv1.AnalyzeSemanticDirtyWorkResponse{ProcessedEvents: int32(res.ProcessedEvents), EnqueuedItems: int32(res.EnqueuedItems)}, nil
}

func (s *AdminSemanticMaintenanceService) ProcessSemanticDirtyWork(ctx context.Context, req *adminv1.ProcessSemanticDirtyWorkRequest) (*adminv1.ProcessSemanticDirtyWorkResponse, error) {
	if err := s.requireMaintenance(ctx); err != nil {
		return nil, err
	}
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	res, err := s.semantic.ProcessDirtyWork(ctx, daemonsemantic.ProcessInput{SpaceID: spaceID, Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "process semantic dirty work")
	}
	return &adminv1.ProcessSemanticDirtyWorkResponse{ProcessedItems: int32(res.Processed), CompletedItems: int32(res.Completed), FailedItems: int32(res.Failed)}, nil
}

func (s *AdminSemanticMaintenanceService) BackfillSemanticIndex(ctx context.Context, req *adminv1.BackfillSemanticIndexRequest) (*adminv1.BackfillSemanticIndexResponse, error) {
	if err := s.requireMaintenance(ctx); err != nil {
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
	nodeIDs := make([]graph.NodeID, 0, len(req.GetNodeIds()))
	for _, raw := range req.GetNodeIds() {
		id, err := parseSemanticUUID[graph.NodeID](raw, "node_id")
		if err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, id)
	}
	res, err := s.semantic.BackfillIndex(ctx, semanticbackfill.Input{SpaceID: spaceID, SemanticIndexID: indexID, NodeIDs: nodeIDs, Force: req.GetForce(), Limit: int(req.GetLimit()), ContinueOnError: req.GetContinueOnError()})
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "backfill semantic index")
	}
	return mapBackfillResponse(res), nil
}

func (s *AdminSemanticMaintenanceService) requireMaintenance(ctx context.Context) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
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
	return &adminv1.SemanticMaintenanceWorkItem{WorkItemId: item.ID.String(), SpaceId: item.SpaceID.String(), DomainId: item.DomainID.String(), SemanticIndexId: item.SemanticIndexID.String(), TargetNodeId: item.TargetNodeID.String(), Action: item.Action, Status: item.Status, AttemptCount: int32(item.AttemptCount), NotBefore: formatMaintenanceTime(item.NotBefore), ClaimedUntil: formatMaintenanceTime(item.ClaimedUntil), LastErrorCategory: item.LastErrorCategory, LastErrorMessageSanitized: item.LastErrorMessageSanitized, CreatedAt: formatMaintenanceTime(item.CreatedAt), UpdatedAt: formatMaintenanceTime(item.UpdatedAt)}
}

func formatMaintenanceTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func mapBackfillResponse(res semanticbackfill.Result) *adminv1.BackfillSemanticIndexResponse {
	out := &adminv1.BackfillSemanticIndexResponse{SemanticIndexId: uuid.UUID(res.SemanticIndexID).String(), SelectedCount: int32(res.SelectedCount), GeneratedCount: int32(res.GeneratedCount), SkippedCount: int32(res.SkippedCount), FailedCount: int32(res.FailedCount)}
	for _, skipped := range res.Skipped {
		out.Skipped = append(out.Skipped, &adminv1.BackfillSkipped{NodeId: uuid.UUID(skipped.NodeID).String(), Reason: skipped.Reason})
	}
	for _, failure := range res.Failures {
		out.Failures = append(out.Failures, &adminv1.BackfillFailure{NodeId: uuid.UUID(failure.NodeID).String(), Error: failure.Error})
	}
	return out
}

func mapAdminSemanticMaintenanceError(err error, action string) error {
	if err == nil {
		return nil
	}
	return mapAdminInferenceError(err, action)
}
