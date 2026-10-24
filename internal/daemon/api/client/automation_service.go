package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/daemon/api/automationview"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type AutomationService struct {
	clientv1.UnimplementedAutomationServiceServer
	automations automationservice.Manager
	spaces      daemonspace.Manager
	cursors     *automationview.CursorCodec
}

func NewAutomationService(automations automationservice.Manager, cursorKey ...[]byte) *AutomationService {
	key := make([]byte, 32)
	if len(cursorKey) > 0 && len(cursorKey[0]) >= 16 {
		key = append(key[:0], cursorKey[0]...)
	} else if _, err := rand.Read(key); err != nil {
		panic("generate procedure inference cursor key: " + err.Error())
	}
	codec, err := automationview.NewCursorCodec(key)
	if err != nil {
		panic(err)
	}
	return &AutomationService{automations: automations, cursors: codec}
}

func (s *AutomationService) WithSpaceManager(spaces daemonspace.Manager) *AutomationService {
	s.spaces = spaces
	return s
}

func (s *AutomationService) ValidateAutomation(ctx context.Context, req *clientv1.ValidateAutomationRequest) (*clientv1.ValidateAutomationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.ValidateAutomation(ctx, domainID, req.GetDefinitionJson())
	if err != nil {
		return &clientv1.ValidateAutomationResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.ValidateAutomationResponse{Valid: true, NormalizedDefinitionJson: jsonText}, nil
}

func (s *AutomationService) CreateAutomation(ctx context.Context, req *clientv1.CreateAutomationRequest) (*clientv1.CreateAutomationResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.CreateAutomationAs(ctx, domainID, req.GetDefinitionJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.CreateAutomationResponse{DefinitionJson: jsonText}, nil
}

func (s *AutomationService) UpdateAutomation(ctx context.Context, req *clientv1.UpdateAutomationRequest) (*clientv1.UpdateAutomationResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.UpdateAutomationAs(ctx, domainID, req.GetAutomationId(), req.GetDefinitionJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.UpdateAutomationResponse{DefinitionJson: jsonText}, nil
}

func (s *AutomationService) DeleteAutomation(ctx context.Context, req *clientv1.DeleteAutomationRequest) (*clientv1.DeleteAutomationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.automations.DeleteAutomation(ctx, domainID, req.GetAutomationId()); err != nil {
		return nil, mapAutomationError(err)
	}
	return &clientv1.DeleteAutomationResponse{}, nil
}

func (s *AutomationService) GetAutomation(ctx context.Context, req *clientv1.GetAutomationRequest) (*clientv1.GetAutomationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.GetAutomation(ctx, domainID, req.GetAutomationId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.GetAutomationResponse{DefinitionJson: jsonText}, nil
}

func (s *AutomationService) ListAutomations(ctx context.Context, req *clientv1.ListAutomationsRequest) (*clientv1.ListAutomationsResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	defs, err := s.automations.ListAutomations(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	out := &clientv1.ListAutomationsResponse{Automations: make([]*clientv1.AutomationDefinitionSummary, 0, len(defs))}
	for _, def := range defs {
		out.Automations = append(out.Automations, automationSummary(def))
	}
	return out, nil
}

func (s *AutomationService) EnableAutomation(ctx context.Context, req *clientv1.EnableAutomationRequest) (*clientv1.EnableAutomationResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatusAs(ctx, domainID, req.GetAutomationId(), automationmodel.StatusEnabled, principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.EnableAutomationResponse{DefinitionJson: jsonText}, nil
}

func (s *AutomationService) DisableAutomation(ctx context.Context, req *clientv1.DisableAutomationRequest) (*clientv1.DisableAutomationResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatusAs(ctx, domainID, req.GetAutomationId(), automationmodel.StatusDisabled, principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &clientv1.DisableAutomationResponse{DefinitionJson: jsonText}, nil
}

func (s *AutomationService) ListAutomationInvocations(ctx context.Context, req *clientv1.ListAutomationInvocationsRequest) (*clientv1.ListAutomationInvocationsResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	items, err := s.automations.ListInvocations(ctx, domainID, storage.InvocationFilter{AutomationID: strings.TrimSpace(req.GetAutomationId()), Status: strings.TrimSpace(req.GetStatus()), Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAutomationError(err)
	}
	out := &clientv1.ListAutomationInvocationsResponse{Invocations: make([]*clientv1.AutomationInvocationSummary, 0, len(items))}
	for _, inv := range items {
		out.Invocations = append(out.Invocations, invocationSummary(inv))
	}
	return out, nil
}

func (s *AutomationService) GetAutomationRun(ctx context.Context, req *clientv1.GetAutomationRunRequest) (*clientv1.GetAutomationRunResponse, error) {
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if s.spaces == nil {
		return nil, status.Error(codes.FailedPrecondition, "procedure inference authorization is unavailable")
	}
	domain, err := s.spaces.GetDomain(ctx, domainID.String())
	if err != nil {
		return nil, status.Error(codes.NotFound, "automation run not found")
	}
	if _, err := s.requireProcedureInferenceRead(ctx, domain.SpaceID.String()); err != nil {
		return nil, status.Error(codes.NotFound, "automation run not found")
	}
	run, err := s.automations.GetRun(ctx, domainID, req.GetRunId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	typedRun := automationview.RunToProto(run)
	data, err := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(typedRun)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &clientv1.GetAutomationRunResponse{RunJson: string(data), Run: typedRun, InferenceUsageEventIds: append([]string(nil), run.InferenceUsageEventIDs...)}, nil
}

func (s *AutomationService) GetProcedureInferenceAttempt(ctx context.Context, req *clientv1.GetProcedureInferenceAttemptRequest) (*clientv1.GetProcedureInferenceAttemptResponse, error) {
	if _, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId()); err != nil {
		return nil, err
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAutomationError(err)
	}
	attempt, ok := automationview.GetAttempt(ledger, req.GetAttemptId())
	if !ok {
		return nil, status.Error(codes.NotFound, "procedure inference attempt not found")
	}
	return &clientv1.GetProcedureInferenceAttemptResponse{Attempt: automationview.AttemptToProto(attempt)}, nil
}

func (s *AutomationService) ListProcedureInferenceAttempts(ctx context.Context, req *clientv1.ListProcedureInferenceAttemptsRequest) (*clientv1.ListProcedureInferenceAttemptsResponse, error) {
	principal, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId())
	if err != nil {
		return nil, err
	}
	pageSize, err := automationview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAutomationError(err)
	}
	page, err := s.cursors.ListCurrent(ledger, req.GetFilter(), pageSize, req.GetPageToken(), principal.PrincipalID)
	if errors.Is(err, automationview.ErrInvalidCursor) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &clientv1.ListProcedureInferenceAttemptsResponse{NextPageToken: page.NextPageToken, SnapshotCursor: page.SnapshotCursor, SnapshotHighWaterSequence: page.SnapshotHighWaterSequence, EarliestRetainedStateSequence: page.EarliestRetainedStateSequence, Attempts: make([]*commonv1.ProcedureInferenceAttempt, 0, len(page.Attempts))}
	for _, attempt := range page.Attempts {
		out.Attempts = append(out.Attempts, automationview.AttemptToProto(attempt))
	}
	return out, nil
}

func (s *AutomationService) ListProcedureInferenceAttemptChanges(ctx context.Context, req *clientv1.ListProcedureInferenceAttemptChangesRequest) (*clientv1.ListProcedureInferenceAttemptChangesResponse, error) {
	principal, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId())
	if err != nil {
		return nil, err
	}
	pageSize, err := automationview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAutomationError(err)
	}
	page, err := s.cursors.ListChanges(ledger, req.GetFilter(), pageSize, req.GetAfterCursor(), principal.PrincipalID)
	if errors.Is(err, automationview.ErrInvalidCursor) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &clientv1.ListProcedureInferenceAttemptChangesResponse{NextCursor: page.NextCursor, HighWaterSequence: page.HighWaterSequence, EarliestRetainedSequence: page.EarliestRetainedSequence, EarliestRetainedStateSequence: page.EarliestRetainedStateSequence, HistoryGap: page.HistoryGap, Changes: make([]*commonv1.ProcedureInferenceAttemptChange, 0, len(page.Changes))}
	for _, change := range page.Changes {
		out.Changes = append(out.Changes, automationview.ChangeToProto(change))
	}
	return out, nil
}

func (s *AutomationService) requireProcedureInferenceRead(ctx context.Context, spaceID string) (principal daemonauth.Principal, err error) {
	principal, err = spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return principal, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return principal, status.Error(codes.InvalidArgument, "space_id is required")
	}
	if s.spaces == nil {
		return principal, status.Error(codes.FailedPrecondition, "procedure inference authorization is unavailable")
	}
	sp, err := s.spaces.GetVisibleSpace(ctx, principal.PrincipalID, spaceID)
	if err != nil {
		return principal, status.Error(codes.NotFound, "space not found")
	}
	access, err := s.spaces.EffectiveAccess(ctx, principal.PrincipalID, sp)
	if err != nil {
		return principal, status.Error(codes.PermissionDenied, "procedure inference evidence is not accessible")
	}
	if !containsCapability(access.Capabilities, "automation.read") || !containsCapability(access.Capabilities, "inference.audit.read") {
		return principal, status.Error(codes.PermissionDenied, "procedure inference evidence requires automation.read and inference.audit.read")
	}
	return principal, nil
}

func containsCapability(values []string, required string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), required) {
			return true
		}
	}
	return false
}

func (s *AutomationService) EnableGraphProcedure(ctx context.Context, req *clientv1.EnableGraphProcedureRequest) (*clientv1.EnableGraphProcedureResponse, error) {
	procedure, barrier, err := s.controlGraphProcedure(ctx, req.GetSpaceId(), req.GetDomainId(), req.GetProcedureId(), automationmodel.StatusEnabled)
	if err != nil {
		return nil, err
	}
	encoded, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.EnableGraphProcedureResponse{ProcedureJson: encoded, Control: automationview.ControlBarrierToProto(barrier)}, nil
}

func (s *AutomationService) DisableGraphProcedure(ctx context.Context, req *clientv1.DisableGraphProcedureRequest) (*clientv1.DisableGraphProcedureResponse, error) {
	procedure, barrier, err := s.controlGraphProcedure(ctx, req.GetSpaceId(), req.GetDomainId(), req.GetProcedureId(), automationmodel.StatusDisabled)
	if err != nil {
		return nil, err
	}
	encoded, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.DisableGraphProcedureResponse{ProcedureJson: encoded, Control: automationview.ControlBarrierToProto(barrier)}, nil
}

func (s *AutomationService) controlGraphProcedure(ctx context.Context, spaceID, domainIDText, procedureID, target string) (automationmodel.Procedure, automationmodel.ProcedureControlBarrier, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" || s.spaces == nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.FailedPrecondition, "procedure control authorization is unavailable")
	}
	sp, err := s.spaces.GetVisibleSpace(ctx, principal.PrincipalID, spaceID)
	if err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.NotFound, "space not found")
	}
	access, err := s.spaces.EffectiveAccess(ctx, principal.PrincipalID, sp)
	if err != nil || !containsCapability(access.Capabilities, "automation.manage") {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.PermissionDenied, "procedure control requires automation.manage")
	}
	domainID, err := parseDomainID(domainIDText)
	if err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, err
	}
	if _, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, spaceID, domainID.String(), ""); err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.NotFound, "domain not found")
	}
	procedure, barrier, err := s.automations.SetProcedureStatusAs(ctx, spaceID, domainID, strings.TrimSpace(procedureID), target, principal.PrincipalID)
	if err != nil {
		return automationmodel.Procedure{}, barrier, mapAutomationError(err)
	}
	return procedure, barrier, nil
}

func (s *AutomationService) RetryAutomationInvocation(ctx context.Context, req *clientv1.RetryAutomationInvocationRequest) (*clientv1.RetryAutomationInvocationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	inv, err := s.automations.RetryInvocation(ctx, domainID, req.GetInvocationId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	return &clientv1.RetryAutomationInvocationResponse{Invocation: invocationSummary(inv)}, nil
}

func (s *AutomationService) CancelAutomationInvocation(ctx context.Context, req *clientv1.CancelAutomationInvocationRequest) (*clientv1.CancelAutomationInvocationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	inv, err := s.automations.CancelInvocation(ctx, domainID, req.GetInvocationId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	return &clientv1.CancelAutomationInvocationResponse{Invocation: invocationSummary(inv)}, nil
}

func (s *AutomationService) ValidateGraphProcedure(ctx context.Context, req *clientv1.ValidateGraphProcedureRequest) (*clientv1.ValidateGraphProcedureResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	procedure, err := s.automations.ValidateProcedure(ctx, domainID, req.GetProcedureJson())
	if err != nil {
		return &clientv1.ValidateGraphProcedureResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.ValidateGraphProcedureResponse{Valid: true, NormalizedProcedureJson: jsonText}, nil
}

func (s *AutomationService) CreateGraphProcedure(ctx context.Context, req *clientv1.CreateGraphProcedureRequest) (*clientv1.CreateGraphProcedureResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	procedure, err := s.automations.CreateProcedureAs(ctx, domainID, req.GetProcedureJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.CreateGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AutomationService) UpdateGraphProcedure(ctx context.Context, req *clientv1.UpdateGraphProcedureRequest) (*clientv1.UpdateGraphProcedureResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	procedure, err := s.automations.UpdateProcedureAs(ctx, domainID, req.GetProcedureId(), req.GetProcedureJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.UpdateGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AutomationService) DeleteGraphProcedure(ctx context.Context, req *clientv1.DeleteGraphProcedureRequest) (*clientv1.DeleteGraphProcedureResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.automations.DeleteProcedure(ctx, domainID, req.GetProcedureId()); err != nil {
		return nil, mapAutomationError(err)
	}
	return &clientv1.DeleteGraphProcedureResponse{}, nil
}

func (s *AutomationService) GetGraphProcedure(ctx context.Context, req *clientv1.GetGraphProcedureRequest) (*clientv1.GetGraphProcedureResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	procedure, err := s.automations.GetProcedure(ctx, domainID, req.GetProcedureId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &clientv1.GetGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AutomationService) ListGraphProcedures(ctx context.Context, req *clientv1.ListGraphProceduresRequest) (*clientv1.ListGraphProceduresResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	procedures, err := s.automations.ListProcedures(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	out := &clientv1.ListGraphProceduresResponse{Procedures: make([]*clientv1.GraphProcedureSummary, 0, len(procedures))}
	for _, procedure := range procedures {
		out.Procedures = append(out.Procedures, procedureSummary(procedure))
	}
	return out, nil
}

func (s *AutomationService) ValidateGraphAutomationBinding(ctx context.Context, req *clientv1.ValidateGraphAutomationBindingRequest) (*clientv1.ValidateGraphAutomationBindingResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.ValidateBinding(ctx, domainID, req.GetBindingJson())
	if err != nil {
		return &clientv1.ValidateGraphAutomationBindingResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.ValidateGraphAutomationBindingResponse{Valid: true, NormalizedBindingJson: jsonText}, nil
}

func (s *AutomationService) CreateGraphAutomationBinding(ctx context.Context, req *clientv1.CreateGraphAutomationBindingRequest) (*clientv1.CreateGraphAutomationBindingResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.CreateBindingAs(ctx, domainID, req.GetBindingJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.CreateGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AutomationService) UpdateGraphAutomationBinding(ctx context.Context, req *clientv1.UpdateGraphAutomationBindingRequest) (*clientv1.UpdateGraphAutomationBindingResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.UpdateBindingAs(ctx, domainID, req.GetBindingId(), req.GetBindingJson(), principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.UpdateGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AutomationService) DeleteGraphAutomationBinding(ctx context.Context, req *clientv1.DeleteGraphAutomationBindingRequest) (*clientv1.DeleteGraphAutomationBindingResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.automations.DeleteBinding(ctx, domainID, req.GetBindingId()); err != nil {
		return nil, mapAutomationError(err)
	}
	return &clientv1.DeleteGraphAutomationBindingResponse{}, nil
}

func (s *AutomationService) GetGraphAutomationBinding(ctx context.Context, req *clientv1.GetGraphAutomationBindingRequest) (*clientv1.GetGraphAutomationBindingResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.GetBinding(ctx, domainID, req.GetBindingId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.GetGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AutomationService) ListGraphAutomationBindings(ctx context.Context, req *clientv1.ListGraphAutomationBindingsRequest) (*clientv1.ListGraphAutomationBindingsResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	bindings, err := s.automations.ListBindings(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	out := &clientv1.ListGraphAutomationBindingsResponse{Bindings: make([]*clientv1.GraphAutomationBindingSummary, 0, len(bindings))}
	for _, binding := range bindings {
		out.Bindings = append(out.Bindings, bindingSummary(binding))
	}
	return out, nil
}

func (s *AutomationService) EnableGraphAutomationBinding(ctx context.Context, req *clientv1.EnableGraphAutomationBindingRequest) (*clientv1.EnableGraphAutomationBindingResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.SetBindingStatusAs(ctx, domainID, req.GetBindingId(), automationmodel.StatusEnabled, principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.EnableGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AutomationService) DisableGraphAutomationBinding(ctx context.Context, req *clientv1.DisableGraphAutomationBindingRequest) (*clientv1.DisableGraphAutomationBindingResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	binding, err := s.automations.SetBindingStatusAs(ctx, domainID, req.GetBindingId(), automationmodel.StatusDisabled, principal.PrincipalID)
	if err != nil {
		return nil, mapAutomationError(err)
	}
	jsonText, err := automationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &clientv1.DisableGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func automationDefinitionJSON(def automationmodel.Definition) (string, error) {
	return marshalAutomationJSON(def)
}
func automationProcedureJSON(procedure automationmodel.Procedure) (string, error) {
	return marshalAutomationJSON(procedure)
}
func automationBindingJSON(binding automationmodel.Binding) (string, error) {
	return marshalAutomationJSON(binding)
}
func marshalAutomationJSON(value any) (string, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return string(data), nil
}

func automationSummary(def automationmodel.Definition) *clientv1.AutomationDefinitionSummary {
	return &clientv1.AutomationDefinitionSummary{Id: def.ID, Name: def.Name, Version: int32(def.Version), Status: def.Status, Events: def.Trigger.Events, Labels: def.Trigger.Labels, UpdatedAt: formatAutomationTime(def.UpdatedAt)}
}

func procedureSummary(procedure automationmodel.Procedure) *clientv1.GraphProcedureSummary {
	procedure = procedure.Normalize()
	return &clientv1.GraphProcedureSummary{Id: procedure.ID, Name: procedure.Name, Version: int32(procedure.Version), Status: procedure.Status, UpdatedAt: formatAutomationTime(procedure.UpdatedAt), Operation: procedure.Inference.Operation, InferenceProfile: procedure.Inference.Profile, InferenceProfileId: procedure.Inference.ProfileID}
}

func bindingSummary(binding automationmodel.Binding) *clientv1.GraphAutomationBindingSummary {
	binding = binding.Normalize()
	return &clientv1.GraphAutomationBindingSummary{Id: binding.ID, Name: binding.Name, Version: int32(binding.Version), Status: binding.Status, ProcedureId: binding.ProcedureID, ProcedureVersion: int32(binding.ProcedureVersion), TriggerType: binding.Trigger.Type, Events: binding.Trigger.Events, Labels: binding.Trigger.Labels, ActorPrincipalId: binding.Runtime.ActorPrincipalID, OwnerPrincipalId: binding.Runtime.OwnerPrincipalID, OnBehalfOfPrincipalId: binding.Runtime.OnBehalfOfPrincipalID, InferenceProfile: binding.Runtime.InferenceProfile, InferenceProfileId: binding.Runtime.InferenceProfileID, ScopeSpaceId: binding.Scope.SpaceID, ScopeDomainId: binding.Scope.DomainID.String(), UpdatedAt: formatAutomationTime(binding.UpdatedAt)}
}

func invocationSummary(inv automationmodel.Invocation) *clientv1.AutomationInvocationSummary {
	return &clientv1.AutomationInvocationSummary{Id: inv.ID, AutomationId: inv.AutomationID, AutomationVersion: int32(inv.AutomationVersion), EventId: inv.EventID, ChangedElementId: inv.ChangedElementID, EventType: inv.EventType, Status: inv.Status, SkipReason: inv.SkipReason, CreatedAt: formatAutomationTime(inv.CreatedAt), UpdatedAt: formatAutomationTime(inv.UpdatedAt), BindingId: inv.BindingID, BindingVersion: int32(inv.BindingVersion), ProcedureId: inv.ProcedureID, ProcedureVersion: int32(inv.ProcedureVersion), ActorPrincipalId: inv.ActorPrincipalID, OwnerPrincipalId: inv.OwnerPrincipalID, OnBehalfOfPrincipalId: inv.OnBehalfOfPrincipalID, EventOriginPrincipalId: inv.EventOriginPrincipalID}
}

func formatAutomationTime(t interface {
	IsZero() bool
	Format(string) string
}) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02T15:04:05Z07:00")
}

func mapAutomationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, automationservice.ErrAutomationNotFound) {
		return status.Error(codes.NotFound, "automation not found")
	}
	return status.Error(codes.InvalidArgument, fmt.Sprint(err))
}

var _ graph.DomainID
