package admin

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	semanticbudgetview "github.com/myceldb/mycel/internal/daemon/api/semanticbudgetview"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	daemoninference "github.com/myceldb/mycel/internal/inference/service"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AdminSemanticMaintenanceService struct {
	adminv1.UnimplementedAdminSemanticMaintenanceServiceServer
	semantic   daemonsemantic.Manager
	inference  daemoninference.Manager
	authorizer OperatorAuthorizer
	budgets    *semanticbudgetview.Codec
}

func NewAdminSemanticMaintenanceService(semantic daemonsemantic.Manager, authorizer OperatorAuthorizer, inference ...daemoninference.Manager) *AdminSemanticMaintenanceService {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic("generate semantic provider budget cursor key: " + err.Error())
	}
	codec, err := semanticbudgetview.New(key)
	if err != nil {
		panic("create semantic provider budget cursor codec: " + err.Error())
	}
	service := &AdminSemanticMaintenanceService{semantic: semantic, authorizer: authorizer, budgets: codec}
	if len(inference) > 0 {
		service.inference = inference[0]
	}
	return service
}

func (s *AdminSemanticMaintenanceService) WithCursorKey(key []byte) *AdminSemanticMaintenanceService {
	codec, err := semanticbudgetview.New(key)
	if err != nil {
		panic(err)
	}
	s.budgets = codec
	return s
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

func (s *AdminSemanticMaintenanceService) GetSemanticProviderBudgetCapabilities(ctx context.Context, req *adminv1.GetSemanticProviderBudgetCapabilitiesRequest) (*adminv1.GetSemanticProviderBudgetCapabilitiesResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	available := s.inference != nil && s.inference.RequireBudgetWriteAllowed() == nil
	raftReplicated := available && s.inference.RequireLocalWriteAllowed() != nil
	return &adminv1.GetSemanticProviderBudgetCapabilitiesResponse{ContractVersion: "semantic-provider-budget/v1", DaemonOwnedScopes: available, PreProviderAttemptReservation: available, DurableRestartRecovery: available, RaftReplicated: raftReplicated, RequestLimit: available, DerivedAttemptLimit: available, AggregateRetryLimit: available, PerRequestRetryLimit: available, ProviderConcurrencyLimit: available, SingleItemBatching: available, MultiItemBatching: false, PerRequestInputTokenLimit: available, AggregateInputTokenLimit: available, RequestTimeout: available, TotalTimeout: available, VersionedCostLimit: available, ConservativeAmbiguousReservations: available, AppendOnlyAttemptEvents: available, SupportedCurrencies: []string{"USD"}, MaximumAttemptEventPageSize: semanticbudgetview.MaxPageSize, MaximumScopePageSize: semanticbudgetview.MaxPageSize, OverageExhaustion: available}, nil
}

func (s *AdminSemanticMaintenanceService) GetSemanticProviderBudgetStatus(ctx context.Context, req *adminv1.GetSemanticProviderBudgetStatusRequest) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic provider budget enforcement is unavailable")
	}
	if err := s.inference.EnsureLinearizableBudgetRead(ctx); err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "establish semantic provider budget read barrier")
	}
	var scope domaininference.BudgetScope
	switch selector := req.GetSelector().(type) {
	case *adminv1.GetSemanticProviderBudgetStatusRequest_BudgetScopeId:
		if strings.TrimSpace(selector.BudgetScopeId) == "" {
			return nil, status.Error(codes.InvalidArgument, "budget_scope_id is required")
		}
		scope, err = s.inference.GetBudgetScope(ctx, domaininference.BudgetScopeID(selector.BudgetScopeId))
	case *adminv1.GetSemanticProviderBudgetStatusRequest_CurrentBinding:
		if selector.CurrentBinding == nil || strings.TrimSpace(selector.CurrentBinding.GetSemanticRuleId()) == "" || strings.TrimSpace(selector.CurrentBinding.GetEmbeddingBindingKey()) == "" {
			return nil, status.Error(codes.InvalidArgument, "current_binding requires semantic_rule_id and embedding_binding_key")
		}
		scope, err = s.inference.CurrentSemanticBudgetScope(ctx, spaceID.String(), selector.CurrentBinding.GetSemanticRuleId(), selector.CurrentBinding.GetEmbeddingBindingKey())
	default:
		return nil, status.Error(codes.InvalidArgument, "budget scope selector is required")
	}
	if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) || err == nil && scope.SpaceID != spaceID.String() {
		return nil, status.Error(codes.NotFound, "semantic provider budget scope not found")
	}
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "get semantic provider budget status")
	}
	return &adminv1.GetSemanticProviderBudgetStatusResponse{Status: semanticProviderBudgetStatusToProto(scope)}, nil
}

func (s *AdminSemanticMaintenanceService) ListSemanticProviderBudgetScopes(ctx context.Context, req *adminv1.ListSemanticProviderBudgetScopesRequest) (*adminv1.ListSemanticProviderBudgetScopesResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic provider budget enforcement is unavailable")
	}
	if err := s.inference.EnsureLinearizableBudgetRead(ctx); err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "establish semantic provider budget read barrier")
	}
	if strings.TrimSpace(req.GetCorrelationId()) == "" && strings.TrimSpace(req.GetSemanticRuleId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "correlation_id or semantic_rule_id is required")
	}
	if strings.TrimSpace(req.GetEmbeddingBindingKey()) != "" && strings.TrimSpace(req.GetSemanticRuleId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "embedding_binding_key requires semantic_rule_id")
	}
	pageSize, err := semanticbudgetview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	scopes, err := s.inference.ListBudgetScopes(ctx)
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "list semantic provider budget scopes")
	}
	principal, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	page, err := s.budgets.ListScopes(scopes, semanticbudgetview.ScopeFilter{SpaceID: spaceID.String(), CorrelationID: strings.TrimSpace(req.GetCorrelationId()), SemanticRuleID: strings.TrimSpace(req.GetSemanticRuleId()), EmbeddingBindingKey: strings.ToLower(strings.TrimSpace(req.GetEmbeddingBindingKey()))}, pageSize, req.GetPageToken(), principal.PrincipalID)
	if err != nil {
		if errors.Is(err, semanticbudgetview.ErrRetentionRace) {
			return nil, status.Error(codes.FailedPrecondition, "semantic provider budget scope retention changed during pagination")
		}
		return nil, status.Error(codes.InvalidArgument, "invalid semantic provider budget scope cursor")
	}
	out := &adminv1.ListSemanticProviderBudgetScopesResponse{NextPageToken: page.NextPageToken}
	for _, scope := range page.Scopes {
		out.Scopes = append(out.Scopes, &adminv1.SemanticProviderBudgetScopeReference{ScopeSequence: scope.ScopeSequence, BudgetScopeId: string(scope.ID), SemanticRuleId: scope.SemanticRuleID, EmbeddingBindingKey: scope.EmbeddingBindingKey, CorrelationId: scope.CorrelationID, CreatedAt: timestampOrNil(scope.CreatedAt), RetentionUntil: timestampOrNil(scope.RetentionUntil)})
	}
	return out, nil
}

func (s *AdminSemanticMaintenanceService) ListSemanticProviderBudgetAttemptEvents(ctx context.Context, req *adminv1.ListSemanticProviderBudgetAttemptEventsRequest) (*adminv1.ListSemanticProviderBudgetAttemptEventsResponse, error) {
	spaceID, err := parseSemanticUUID[domainspace.SpaceID](req.GetSpaceId(), "space_id")
	if err != nil {
		return nil, err
	}
	if err := s.requireMaintenance(ctx, semanticScope(spaceID, uuid.Nil)); err != nil {
		return nil, err
	}
	if s.inference == nil {
		return nil, status.Error(codes.FailedPrecondition, "semantic provider budget enforcement is unavailable")
	}
	if err := s.inference.EnsureLinearizableBudgetRead(ctx); err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "establish semantic provider budget read barrier")
	}
	if strings.TrimSpace(req.GetBudgetScopeId()) == "" || req.GetAfterSequence() == math.MaxUint64 || req.GetPageToken() != "" && req.GetAfterSequence() != 0 {
		return nil, status.Error(codes.InvalidArgument, "budget scope and valid mutually exclusive sequence/cursor inputs are required")
	}
	pageSize, err := semanticbudgetview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	principal, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	continuation := strings.TrimSpace(req.GetPageToken()) != ""
	if continuation {
		if err := s.budgets.ValidateEventContinuation(req.GetPageToken(), spaceID.String(), req.GetBudgetScopeId(), principal.PrincipalID); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid semantic provider budget event cursor")
		}
	}
	scope, err := s.inference.GetBudgetScope(ctx, domaininference.BudgetScopeID(req.GetBudgetScopeId()))
	if errors.Is(err, inferencestorage.ErrBudgetScopeNotFound) || err == nil && scope.SpaceID != spaceID.String() {
		if continuation {
			return nil, status.Error(codes.FailedPrecondition, "semantic provider budget event retention changed during pagination")
		}
		return nil, status.Error(codes.NotFound, "semantic provider budget scope not found")
	}
	if err != nil {
		return nil, mapAdminSemanticMaintenanceError(err, "get semantic provider budget events")
	}
	page, err := s.budgets.ListEvents(scope, req.GetAfterSequence(), pageSize, req.GetPageToken(), principal.PrincipalID)
	if err != nil {
		if errors.Is(err, semanticbudgetview.ErrRetentionRace) {
			return nil, status.Error(codes.FailedPrecondition, "semantic provider budget event retention changed during pagination")
		}
		return nil, status.Error(codes.InvalidArgument, "invalid semantic provider budget event cursor")
	}
	out := &adminv1.ListSemanticProviderBudgetAttemptEventsResponse{NextPageToken: page.NextPageToken, EarliestRetainedEventSequence: page.EarliestRetainedSequence, SnapshotHighWaterSequence: page.SnapshotHighWater, CurrentHighWaterSequence: page.CurrentHighWater, HistoryGap: page.HistoryGap}
	for _, event := range page.Events {
		out.Events = append(out.Events, semanticProviderBudgetEventToProto(scope.ID, event))
	}
	return out, nil
}

func semanticProviderBudgetStatusToProto(scope domaininference.BudgetScope) *adminv1.SemanticProviderBudgetStatus {
	earliest, highWater := uint64(0), uint64(0)
	for _, event := range scope.AttemptEvents {
		if earliest == 0 || event.Sequence < earliest {
			earliest = event.Sequence
		}
		if event.Sequence > highWater {
			highWater = event.Sequence
		}
	}
	return &adminv1.SemanticProviderBudgetStatus{SpaceId: scope.SpaceID, SemanticRuleId: scope.SemanticRuleID, EmbeddingBindingKey: scope.EmbeddingBindingKey, BudgetScopeId: string(scope.ID), CorrelationId: scope.CorrelationID, ModelEndpointId: scope.Route.EndpointID.String(), ModelId: scope.Route.ModelID.String(), ModelEndpointCapabilityId: scope.Route.CapabilityID.String(), State: semanticProviderBudgetScopeState(scope.State), Limits: inferenceBudgetLimitsToProto(scope), Usage: &adminv1.SemanticProviderBudgetUsage{Requests: scope.Usage.Requests, Attempts: scope.Usage.Attempts, RetryAttempts: scope.Usage.RetryAttempts, BatchItems: scope.Usage.BatchItems, ActiveProviderCalls: scope.Usage.ActiveProviderCalls, InputTokens: scope.Usage.InputTokens, ProviderMillis: scope.Usage.ProviderMillis, EstimatedCostMicros: scope.Usage.EstimatedCostMicros, AmbiguousAttempts: scope.Usage.AmbiguousAttempts, AccountingOverflow: scope.Usage.AccountingOverflow}, EarliestRetainedEventSequence: earliest, HighWaterEventSequence: highWater, CreatedAt: timestampOrNil(scope.CreatedAt), UpdatedAt: timestampOrNil(scope.UpdatedAt), ExpiresAt: timestampOrNil(scope.ExpiresAt), RetentionUntil: timestampOrNil(scope.RetentionUntil), DenialReason: semanticProviderBudgetDenial(scope.DenialReason), ScopeSequence: scope.ScopeSequence}
}

func inferenceBudgetLimitsToProto(scope domaininference.BudgetScope) *adminv1.SemanticProviderBudgetPolicy {
	mode := adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_UNSPECIFIED
	if scope.Limits.BatchingMode == domaininference.BatchingModeSingle {
		mode = adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_SINGLE
	} else if scope.Limits.BatchingMode == domaininference.BatchingModeBatch {
		mode = adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_BATCH
	}
	total := scope.Limits.Deadline.Sub(scope.CreatedAt)
	return &adminv1.SemanticProviderBudgetPolicy{CorrelationId: scope.CorrelationID, MaxRequests: int64Pointer(scope.Limits.MaxRequests), MaxRetryAttempts: int64Pointer(scope.Limits.MaxRetryAttempts), MaxRetriesPerRequest: int64Pointer(scope.Limits.MaxRetriesPerRequest), MaxConcurrentProviderCalls: int64Pointer(scope.Limits.MaxConcurrentProviderCalls), BatchingMode: semanticBatchingPointer(mode), MaxBatchItems: int64Pointer(scope.Limits.MaxBatchItems), MaxInputTokensPerRequest: int64Pointer(scope.Limits.MaxInputTokensPerRequest), MaxInputTokens: int64Pointer(scope.Limits.MaxInputTokens), RequestTimeout: durationpb.New(time.Duration(scope.Limits.MaxProviderMillisPerRequest) * time.Millisecond), TotalTimeout: durationpb.New(total), Cost: &adminv1.SemanticProviderCostLimit{Currency: scope.Limits.Currency, PriceDefinitionVersion: scope.Limits.PriceDefinitionVersion, InputMicrosPerMillionTokens: int64Pointer(scope.Limits.InputMicrosPerMillionTokens), MaxEstimatedCostMicros: int64Pointer(scope.Limits.MaxEstimatedCostMicros)}}
}

func semanticProviderBudgetEventToProto(scopeID domaininference.BudgetScopeID, event domaininference.BudgetAttemptEvent) *adminv1.SemanticProviderBudgetAttemptEvent {
	attempt := event.Attempt
	return &adminv1.SemanticProviderBudgetAttemptEvent{EventSequence: event.Sequence, BudgetScopeId: string(scopeID), AttemptId: string(attempt.ID), AttemptVersion: event.AttemptVersion, RequestId: attempt.RequestID, RetryIndex: attempt.RetryIndex, State: semanticProviderBudgetAttemptState(attempt.State), BatchItems: attempt.BatchItems, ReservedInputTokens: attempt.ReservedInputTokens, ChargedInputTokens: attempt.ChargedInputTokens, ReservedProviderMillis: attempt.ReservedProviderMillis, ChargedProviderMillis: attempt.ChargedProviderMillis, ReservedCostMicros: attempt.ReservedCostMicros, ChargedCostMicros: attempt.ChargedCostMicros, UsageKnown: attempt.UsageKnown, OutcomeKnown: attempt.OutcomeKnown, ProviderDurationKnown: attempt.ProviderDurationKnown, OutcomeCategory: semanticProviderBudgetOutcome(attempt), RecordedAt: timestampOrNil(event.RecordedAt), ReservedUntil: timestampOrNil(attempt.ReservedUntil), ChargedCostSaturated: attempt.ChargedCostSaturated}
}

func semanticProviderBudgetScopeState(value domaininference.BudgetScopeState) adminv1.SemanticProviderBudgetScopeState {
	switch value {
	case domaininference.BudgetScopeActive:
		return adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_ACTIVE
	case domaininference.BudgetScopeCompleted:
		return adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_COMPLETED
	case domaininference.BudgetScopeExpired:
		return adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXPIRED
	case domaininference.BudgetScopeExhausted:
		return adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_EXHAUSTED
	default:
		return adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_UNSPECIFIED
	}
}

func semanticProviderBudgetAttemptState(value domaininference.BudgetAttemptState) adminv1.SemanticProviderBudgetAttemptState {
	switch value {
	case domaininference.BudgetAttemptReserved:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_RESERVED
	case domaininference.BudgetAttemptSucceeded:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_SUCCEEDED
	case domaininference.BudgetAttemptFailed:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_FAILED
	case domaininference.BudgetAttemptCanceled:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_CANCELED
	case domaininference.BudgetAttemptAmbiguous:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_AMBIGUOUS
	default:
		return adminv1.SemanticProviderBudgetAttemptState_SEMANTIC_PROVIDER_BUDGET_ATTEMPT_STATE_UNSPECIFIED
	}
}

func semanticProviderBudgetOutcome(attempt domaininference.BudgetAttempt) adminv1.SemanticProviderBudgetOutcomeCategory {
	if attempt.State == domaininference.BudgetAttemptReserved {
		return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_NONE
	}
	if attempt.State == domaininference.BudgetAttemptSucceeded {
		return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_SUCCESS
	}
	if attempt.State == domaininference.BudgetAttemptAmbiguous || !attempt.OutcomeKnown {
		return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_AMBIGUOUS
	}
	if attempt.State == domaininference.BudgetAttemptCanceled {
		return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_CANCELED_BEFORE_EFFECT
	}
	if strings.Contains(attempt.Outcome, "rate") || strings.Contains(attempt.Outcome, "transport") || strings.Contains(attempt.Outcome, "unavailable") || strings.Contains(attempt.Outcome, "deadline") {
		return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_TRANSIENT_FAILURE
	}
	return adminv1.SemanticProviderBudgetOutcomeCategory_SEMANTIC_PROVIDER_BUDGET_OUTCOME_CATEGORY_PERMANENT_FAILURE
}

func semanticProviderBudgetDenial(value string) adminv1.SemanticProviderBudgetDenialReason {
	switch value {
	case "":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE
	case "request_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_REQUEST_LIMIT
	case "attempt_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_ATTEMPT_LIMIT
	case "retry_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_RETRY_LIMIT
	case "per_request_retry_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_PER_REQUEST_RETRY_LIMIT
	case "concurrency_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_CONCURRENCY_LIMIT
	case "batch_limit_exceeded", "connector_batching_unsupported":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_BATCH_LIMIT
	case "input_token_limit_exhausted", "input_tokens_per_request_exceeded":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_INPUT_TOKEN_LIMIT
	case "provider_time_limit_exhausted", "provider_time_per_request_exceeded":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_PROVIDER_TIME_LIMIT
	case "deadline_exhausted", "scope_expired":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_DEADLINE
	case "cost_limit_exhausted":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_COST_LIMIT
	case "route_mismatch":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_ROUTE_MISMATCH
	case "reservation_exceeded":
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_ACCOUNTING
	default:
		return adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_CAPACITY
	}
}

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value.UTC())
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
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "semantic maintenance request was canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "semantic maintenance request exceeded its deadline")
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
