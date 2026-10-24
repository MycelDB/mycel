package admin

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/daemon/api/automationview"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	capAutomationRead   = "automation.read"
	capAutomationManage = "automation.manage"
	capAutomationRun    = "automation.run"
)

type AdminAutomationDomainResolver interface {
	GetDomain(ctx context.Context, domainID string) (graph.Domain, error)
}

type AdminAutomationService struct {
	adminv1.UnimplementedAdminAutomationServiceServer
	automations    automationservice.Manager
	authorizer     OperatorAuthorizer
	domainResolver AdminAutomationDomainResolver
	cursors        *automationview.CursorCodec
}

func NewAdminAutomationService(automations automationservice.Manager, authorizer ...OperatorAuthorizer) *AdminAutomationService {
	var authz OperatorAuthorizer
	if len(authorizer) > 0 {
		authz = authorizer[0]
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic("generate procedure inference cursor key: " + err.Error())
	}
	codec, err := automationview.NewCursorCodec(key)
	if err != nil {
		panic(err)
	}
	return &AdminAutomationService{automations: automations, authorizer: authz, cursors: codec}
}

func (s *AdminAutomationService) WithCursorKey(key []byte) *AdminAutomationService {
	codec, err := automationview.NewCursorCodec(key)
	if err != nil {
		panic(err)
	}
	s.cursors = codec
	return s
}

func (s *AdminAutomationService) WithDomainResolver(resolver AdminAutomationDomainResolver) *AdminAutomationService {
	s.domainResolver = resolver
	return s
}

func (s *AdminAutomationService) ValidateAutomation(ctx context.Context, req *adminv1.ValidateAutomationRequest) (*adminv1.ValidateAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	def, err := s.automations.ValidateAutomation(ctx, domainID, req.GetDefinitionJson())
	if err != nil {
		return &adminv1.ValidateAutomationResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.ValidateAutomationResponse{Valid: true, NormalizedDefinitionJson: jsonText}, nil
}

func (s *AdminAutomationService) CreateAutomation(ctx context.Context, req *adminv1.CreateAutomationRequest) (*adminv1.CreateAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	def, err := s.automations.CreateAutomationAs(ctx, domainID, req.GetDefinitionJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.CreateAutomationResponse{DefinitionJson: jsonText}, nil
}
func (s *AdminAutomationService) UpdateAutomation(ctx context.Context, req *adminv1.UpdateAutomationRequest) (*adminv1.UpdateAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	def, err := s.automations.UpdateAutomationAs(ctx, domainID, req.GetAutomationId(), req.GetDefinitionJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.UpdateAutomationResponse{DefinitionJson: jsonText}, nil
}
func (s *AdminAutomationService) DeleteAutomation(ctx context.Context, req *adminv1.DeleteAutomationRequest) (*adminv1.DeleteAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	if err := s.automations.DeleteAutomation(ctx, domainID, req.GetAutomationId()); err != nil {
		return nil, mapAdminAutomationError(err)
	}
	return &adminv1.DeleteAutomationResponse{}, nil
}
func (s *AdminAutomationService) GetAutomation(ctx context.Context, req *adminv1.GetAutomationRequest) (*adminv1.GetAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	def, err := s.automations.GetAutomation(ctx, domainID, req.GetAutomationId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetAutomationResponse{DefinitionJson: jsonText}, nil
}
func (s *AdminAutomationService) ListAutomations(ctx context.Context, req *adminv1.ListAutomationsRequest) (*adminv1.ListAutomationsResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	defs, err := s.automations.ListAutomations(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	out := &adminv1.ListAutomationsResponse{Automations: make([]*clientv1.AutomationDefinitionSummary, 0, len(defs))}
	for _, def := range defs {
		out.Automations = append(out.Automations, adminAutomationSummary(def))
	}
	return out, nil
}
func (s *AdminAutomationService) EnableAutomation(ctx context.Context, req *adminv1.EnableAutomationRequest) (*adminv1.EnableAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatusAs(ctx, domainID, req.GetAutomationId(), automationmodel.StatusEnabled, automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.EnableAutomationResponse{DefinitionJson: jsonText}, nil
}
func (s *AdminAutomationService) DisableAutomation(ctx context.Context, req *adminv1.DisableAutomationRequest) (*adminv1.DisableAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatusAs(ctx, domainID, req.GetAutomationId(), automationmodel.StatusDisabled, automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationDefinitionJSON(def)
	if err != nil {
		return nil, err
	}
	return &adminv1.DisableAutomationResponse{DefinitionJson: jsonText}, nil
}
func (s *AdminAutomationService) ListAutomationInvocations(ctx context.Context, req *adminv1.ListAutomationInvocationsRequest) (*adminv1.ListAutomationInvocationsResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	items, err := s.automations.ListInvocations(ctx, domainID, storage.InvocationFilter{AutomationID: strings.TrimSpace(req.GetAutomationId()), Status: strings.TrimSpace(req.GetStatus()), Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	out := &adminv1.ListAutomationInvocationsResponse{Invocations: make([]*clientv1.AutomationInvocationSummary, 0, len(items))}
	for _, inv := range items {
		out.Invocations = append(out.Invocations, adminInvocationSummary(inv))
	}
	return out, nil
}
func (s *AdminAutomationService) GetAutomationRun(ctx context.Context, req *adminv1.GetAutomationRunRequest) (*adminv1.GetAutomationRunResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capInferenceAuditRead); err != nil {
		return nil, err
	}
	run, err := s.automations.GetRun(ctx, domainID, req.GetRunId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	typedRun := automationview.RunToProto(run)
	data, err := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(typedRun)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &adminv1.GetAutomationRunResponse{RunJson: string(data), Run: typedRun, InferenceUsageEventIds: append([]string(nil), run.InferenceUsageEventIDs...)}, nil
}

func (s *AdminAutomationService) GetProcedureInferenceAttempt(ctx context.Context, req *adminv1.GetProcedureInferenceAttemptRequest) (*adminv1.GetProcedureInferenceAttemptResponse, error) {
	if _, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId(), ""); err != nil {
		return nil, err
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	attempt, ok := automationview.GetAttempt(ledger, req.GetAttemptId())
	if !ok {
		return nil, status.Error(codes.NotFound, "procedure inference attempt not found")
	}
	return &adminv1.GetProcedureInferenceAttemptResponse{Attempt: automationview.AttemptToProto(attempt)}, nil
}

func (s *AdminAutomationService) ListProcedureInferenceAttempts(ctx context.Context, req *adminv1.ListProcedureInferenceAttemptsRequest) (*adminv1.ListProcedureInferenceAttemptsResponse, error) {
	domainID := ""
	if req.GetFilter() != nil {
		domainID = req.GetFilter().GetDomainId()
	}
	principal, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId(), domainID)
	if err != nil {
		return nil, err
	}
	pageSize, err := automationview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	page, err := s.cursors.ListCurrent(ledger, req.GetFilter(), pageSize, req.GetPageToken(), principal.PrincipalID)
	if errors.Is(err, automationview.ErrInvalidCursor) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &adminv1.ListProcedureInferenceAttemptsResponse{NextPageToken: page.NextPageToken, SnapshotCursor: page.SnapshotCursor, SnapshotHighWaterSequence: page.SnapshotHighWaterSequence, EarliestRetainedStateSequence: page.EarliestRetainedStateSequence, Attempts: make([]*commonv1.ProcedureInferenceAttempt, 0, len(page.Attempts))}
	for _, attempt := range page.Attempts {
		out.Attempts = append(out.Attempts, automationview.AttemptToProto(attempt))
	}
	return out, nil
}

func (s *AdminAutomationService) ListProcedureInferenceAttemptChanges(ctx context.Context, req *adminv1.ListProcedureInferenceAttemptChangesRequest) (*adminv1.ListProcedureInferenceAttemptChangesResponse, error) {
	domainID := ""
	if req.GetFilter() != nil {
		domainID = req.GetFilter().GetDomainId()
	}
	principal, err := s.requireProcedureInferenceRead(ctx, req.GetSpaceId(), domainID)
	if err != nil {
		return nil, err
	}
	pageSize, err := automationview.NormalizePageSize(req.GetPageSize())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ledger, err := s.automations.GetProcedureInferenceLedger(ctx, strings.TrimSpace(req.GetSpaceId()))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	page, err := s.cursors.ListChanges(ledger, req.GetFilter(), pageSize, req.GetAfterCursor(), principal.PrincipalID)
	if errors.Is(err, automationview.ErrInvalidCursor) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &adminv1.ListProcedureInferenceAttemptChangesResponse{NextCursor: page.NextCursor, HighWaterSequence: page.HighWaterSequence, EarliestRetainedSequence: page.EarliestRetainedSequence, EarliestRetainedStateSequence: page.EarliestRetainedStateSequence, HistoryGap: page.HistoryGap, Changes: make([]*commonv1.ProcedureInferenceAttemptChange, 0, len(page.Changes))}
	for _, change := range page.Changes {
		out.Changes = append(out.Changes, automationview.ChangeToProto(change))
	}
	return out, nil
}

func (s *AdminAutomationService) requireProcedureInferenceRead(ctx context.Context, spaceID, domainID string) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return daemonauth.Principal{}, status.Error(codes.InvalidArgument, "space_id is required")
	}
	scope := principalservice.AccessScope{Type: "space", SpaceID: spaceID}
	if strings.TrimSpace(domainID) != "" {
		parsed, err := parseAdminAutomationDomainID(domainID)
		if err != nil {
			return daemonauth.Principal{}, err
		}
		scope.Type = "domain"
		scope.DomainID = parsed.String()
		if s.domainResolver != nil {
			domain, err := s.domainResolver.GetDomain(ctx, parsed.String())
			if err != nil || domain.SpaceID.String() != spaceID {
				return daemonauth.Principal{}, status.Error(codes.NotFound, "procedure inference scope not found")
			}
		}
	}
	for _, capability := range []string{capAutomationRead, capInferenceAuditRead} {
		if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
			if err := scoped.Authorize(ctx, principal.PrincipalID, capability, scope); err != nil {
				return daemonauth.Principal{}, err
			}
			continue
		}
		if s.authorizer == nil {
			continue
		}
		ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability)
		if err != nil {
			return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize procedure inference read: %v", err)
		}
		if !ok {
			return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required procedure inference capability")
		}
	}
	return principal, nil
}

func (s *AdminAutomationService) RetryAutomationInvocation(ctx context.Context, req *adminv1.RetryAutomationInvocationRequest) (*adminv1.RetryAutomationInvocationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRun); err != nil {
		return nil, err
	}
	inv, err := s.automations.RetryInvocation(ctx, domainID, req.GetInvocationId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	return &adminv1.RetryAutomationInvocationResponse{Invocation: adminInvocationSummary(inv)}, nil
}

func (s *AdminAutomationService) CancelAutomationInvocation(ctx context.Context, req *adminv1.CancelAutomationInvocationRequest) (*adminv1.CancelAutomationInvocationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRun); err != nil {
		return nil, err
	}
	inv, err := s.automations.CancelInvocation(ctx, domainID, req.GetInvocationId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	return &adminv1.CancelAutomationInvocationResponse{Invocation: adminInvocationSummary(inv)}, nil
}

func (s *AdminAutomationService) ValidateGraphProcedure(ctx context.Context, req *adminv1.ValidateGraphProcedureRequest) (*adminv1.ValidateGraphProcedureResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	procedure, err := s.automations.ValidateProcedure(ctx, domainID, req.GetProcedureJson())
	if err != nil {
		return &adminv1.ValidateGraphProcedureResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.ValidateGraphProcedureResponse{Valid: true, NormalizedProcedureJson: jsonText}, nil
}

func (s *AdminAutomationService) CreateGraphProcedure(ctx context.Context, req *adminv1.CreateGraphProcedureRequest) (*adminv1.CreateGraphProcedureResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	procedure, err := s.automations.CreateProcedureAs(ctx, domainID, req.GetProcedureJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.CreateGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AdminAutomationService) UpdateGraphProcedure(ctx context.Context, req *adminv1.UpdateGraphProcedureRequest) (*adminv1.UpdateGraphProcedureResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	procedure, err := s.automations.UpdateProcedureAs(ctx, domainID, req.GetProcedureId(), req.GetProcedureJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.UpdateGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AdminAutomationService) DeleteGraphProcedure(ctx context.Context, req *adminv1.DeleteGraphProcedureRequest) (*adminv1.DeleteGraphProcedureResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	if err := s.automations.DeleteProcedure(ctx, domainID, req.GetProcedureId()); err != nil {
		return nil, mapAdminAutomationError(err)
	}
	return &adminv1.DeleteGraphProcedureResponse{}, nil
}

func (s *AdminAutomationService) GetGraphProcedure(ctx context.Context, req *adminv1.GetGraphProcedureRequest) (*adminv1.GetGraphProcedureResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	procedure, err := s.automations.GetProcedure(ctx, domainID, req.GetProcedureId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetGraphProcedureResponse{ProcedureJson: jsonText}, nil
}

func (s *AdminAutomationService) ListGraphProcedures(ctx context.Context, req *adminv1.ListGraphProceduresRequest) (*adminv1.ListGraphProceduresResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	procedures, err := s.automations.ListProcedures(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	out := &adminv1.ListGraphProceduresResponse{Procedures: make([]*clientv1.GraphProcedureSummary, 0, len(procedures))}
	for _, procedure := range procedures {
		out.Procedures = append(out.Procedures, adminProcedureSummary(procedure))
	}
	return out, nil
}

func (s *AdminAutomationService) ValidateGraphAutomationBinding(ctx context.Context, req *adminv1.ValidateGraphAutomationBindingRequest) (*adminv1.ValidateGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	binding, err := s.automations.ValidateBinding(ctx, domainID, req.GetBindingJson())
	if err != nil {
		return &adminv1.ValidateGraphAutomationBindingResponse{Valid: false, Error: err.Error()}, nil
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.ValidateGraphAutomationBindingResponse{Valid: true, NormalizedBindingJson: jsonText}, nil
}

func (s *AdminAutomationService) CreateGraphAutomationBinding(ctx context.Context, req *adminv1.CreateGraphAutomationBindingRequest) (*adminv1.CreateGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	binding, err := s.automations.CreateBindingAs(ctx, domainID, req.GetBindingJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.CreateGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AdminAutomationService) UpdateGraphAutomationBinding(ctx context.Context, req *adminv1.UpdateGraphAutomationBindingRequest) (*adminv1.UpdateGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	binding, err := s.automations.UpdateBindingAs(ctx, domainID, req.GetBindingId(), req.GetBindingJson(), automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.UpdateGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AdminAutomationService) DeleteGraphAutomationBinding(ctx context.Context, req *adminv1.DeleteGraphAutomationBindingRequest) (*adminv1.DeleteGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	if err := s.automations.DeleteBinding(ctx, domainID, req.GetBindingId()); err != nil {
		return nil, mapAdminAutomationError(err)
	}
	return &adminv1.DeleteGraphAutomationBindingResponse{}, nil
}

func (s *AdminAutomationService) GetGraphAutomationBinding(ctx context.Context, req *adminv1.GetGraphAutomationBindingRequest) (*adminv1.GetGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	binding, err := s.automations.GetBinding(ctx, domainID, req.GetBindingId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AdminAutomationService) ListGraphAutomationBindings(ctx context.Context, req *adminv1.ListGraphAutomationBindingsRequest) (*adminv1.ListGraphAutomationBindingsResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationRead); err != nil {
		return nil, err
	}
	bindings, err := s.automations.ListBindings(ctx, domainID, req.GetStatus())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	out := &adminv1.ListGraphAutomationBindingsResponse{Bindings: make([]*clientv1.GraphAutomationBindingSummary, 0, len(bindings))}
	for _, binding := range bindings {
		out.Bindings = append(out.Bindings, adminBindingSummary(binding))
	}
	return out, nil
}

func (s *AdminAutomationService) EnableGraphAutomationBinding(ctx context.Context, req *adminv1.EnableGraphAutomationBindingRequest) (*adminv1.EnableGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	binding, err := s.automations.SetBindingStatusAs(ctx, domainID, req.GetBindingId(), automationmodel.StatusEnabled, automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.EnableGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func (s *AdminAutomationService) DisableGraphAutomationBinding(ctx context.Context, req *adminv1.DisableGraphAutomationBindingRequest) (*adminv1.DisableGraphAutomationBindingResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return nil, err
	}
	binding, err := s.automations.SetBindingStatusAs(ctx, domainID, req.GetBindingId(), automationmodel.StatusDisabled, automationActorFromContext(ctx))
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	jsonText, err := adminAutomationBindingJSON(binding)
	if err != nil {
		return nil, err
	}
	return &adminv1.DisableGraphAutomationBindingResponse{BindingJson: jsonText}, nil
}

func adminAutomationDefinitionJSON(def automationmodel.Definition) (string, error) {
	return adminMarshalAutomationJSON(def)
}
func adminAutomationProcedureJSON(procedure automationmodel.Procedure) (string, error) {
	return adminMarshalAutomationJSON(procedure)
}
func adminAutomationBindingJSON(binding automationmodel.Binding) (string, error) {
	return adminMarshalAutomationJSON(binding)
}
func adminMarshalAutomationJSON(value any) (string, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return string(data), nil
}
func adminAutomationSummary(def automationmodel.Definition) *clientv1.AutomationDefinitionSummary {
	return &clientv1.AutomationDefinitionSummary{Id: def.ID, Name: def.Name, Version: int32(def.Version), Status: def.Status, Events: def.Trigger.Events, Labels: def.Trigger.Labels, UpdatedAt: formatAdminAutomationTime(def.UpdatedAt)}
}
func adminProcedureSummary(procedure automationmodel.Procedure) *clientv1.GraphProcedureSummary {
	procedure = procedure.Normalize()
	return &clientv1.GraphProcedureSummary{Id: procedure.ID, Name: procedure.Name, Version: int32(procedure.Version), Status: procedure.Status, UpdatedAt: formatAdminAutomationTime(procedure.UpdatedAt), Operation: procedure.Inference.Operation, InferenceProfile: procedure.Inference.Profile, InferenceProfileId: procedure.Inference.ProfileID}
}
func adminBindingSummary(binding automationmodel.Binding) *clientv1.GraphAutomationBindingSummary {
	binding = binding.Normalize()
	return &clientv1.GraphAutomationBindingSummary{Id: binding.ID, Name: binding.Name, Version: int32(binding.Version), Status: binding.Status, ProcedureId: binding.ProcedureID, ProcedureVersion: int32(binding.ProcedureVersion), TriggerType: binding.Trigger.Type, Events: binding.Trigger.Events, Labels: binding.Trigger.Labels, ActorPrincipalId: binding.Runtime.ActorPrincipalID, OwnerPrincipalId: binding.Runtime.OwnerPrincipalID, OnBehalfOfPrincipalId: binding.Runtime.OnBehalfOfPrincipalID, InferenceProfile: binding.Runtime.InferenceProfile, InferenceProfileId: binding.Runtime.InferenceProfileID, ScopeSpaceId: binding.Scope.SpaceID, ScopeDomainId: binding.Scope.DomainID.String(), UpdatedAt: formatAdminAutomationTime(binding.UpdatedAt)}
}
func adminInvocationSummary(inv automationmodel.Invocation) *clientv1.AutomationInvocationSummary {
	return &clientv1.AutomationInvocationSummary{Id: inv.ID, AutomationId: inv.AutomationID, AutomationVersion: int32(inv.AutomationVersion), EventId: inv.EventID, ChangedElementId: inv.ChangedElementID, EventType: inv.EventType, Status: inv.Status, SkipReason: inv.SkipReason, CreatedAt: formatAdminAutomationTime(inv.CreatedAt), UpdatedAt: formatAdminAutomationTime(inv.UpdatedAt), BindingId: inv.BindingID, BindingVersion: int32(inv.BindingVersion), ProcedureId: inv.ProcedureID, ProcedureVersion: int32(inv.ProcedureVersion), ActorPrincipalId: inv.ActorPrincipalID, OwnerPrincipalId: inv.OwnerPrincipalID, OnBehalfOfPrincipalId: inv.OnBehalfOfPrincipalID, EventOriginPrincipalId: inv.EventOriginPrincipalID}
}
func formatAdminAutomationTime(t interface {
	IsZero() bool
	Format(string) string
}) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02T15:04:05Z07:00")
}
func (s *AdminAutomationService) EnableGraphProcedure(ctx context.Context, req *adminv1.EnableGraphProcedureRequest) (*adminv1.EnableGraphProcedureResponse, error) {
	procedure, barrier, err := s.controlGraphProcedure(ctx, req.GetSpaceId(), req.GetDomainId(), req.GetProcedureId(), automationmodel.StatusEnabled)
	if err != nil {
		return nil, err
	}
	encoded, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.EnableGraphProcedureResponse{ProcedureJson: encoded, Control: automationview.ControlBarrierToProto(barrier)}, nil
}

func (s *AdminAutomationService) DisableGraphProcedure(ctx context.Context, req *adminv1.DisableGraphProcedureRequest) (*adminv1.DisableGraphProcedureResponse, error) {
	procedure, barrier, err := s.controlGraphProcedure(ctx, req.GetSpaceId(), req.GetDomainId(), req.GetProcedureId(), automationmodel.StatusDisabled)
	if err != nil {
		return nil, err
	}
	encoded, err := adminAutomationProcedureJSON(procedure)
	if err != nil {
		return nil, err
	}
	return &adminv1.DisableGraphProcedureResponse{ProcedureJson: encoded, Control: automationview.ControlBarrierToProto(barrier)}, nil
}

func (s *AdminAutomationService) controlGraphProcedure(ctx context.Context, spaceID, domainIDText, procedureID, target string) (automationmodel.Procedure, automationmodel.ProcedureControlBarrier, error) {
	domainID, err := parseAdminAutomationDomainID(domainIDText)
	if err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, err
	}
	if err := s.requireAutomationCapability(ctx, domainID, capAutomationManage); err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if _, err := uuid.Parse(spaceID); err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.InvalidArgument, "space_id must be a UUID")
	}
	if scope, err := s.automationAccessScope(ctx, domainID); err != nil {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, err
	} else if scope.SpaceID != "" && scope.SpaceID != spaceID {
		return automationmodel.Procedure{}, automationmodel.ProcedureControlBarrier{}, status.Error(codes.NotFound, "domain not found in space")
	}
	procedure, barrier, err := s.automations.SetProcedureStatusAs(ctx, strings.TrimSpace(spaceID), domainID, strings.TrimSpace(procedureID), target, automationActorFromContext(ctx))
	if err != nil {
		return automationmodel.Procedure{}, barrier, mapAdminAutomationError(err)
	}
	return procedure, barrier, nil
}

func parseAdminAutomationDomainID(value string) (graph.DomainID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return graph.DomainID{}, status.Error(codes.InvalidArgument, "domain_id must be a UUID")
	}
	return graph.DomainID(id), nil
}
func mapAdminAutomationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, automationservice.ErrAutomationNotFound) {
		return status.Error(codes.NotFound, "automation not found")
	}
	return status.Error(codes.InvalidArgument, fmt.Sprint(err))
}

func (s *AdminAutomationService) requireAutomationCapability(ctx context.Context, domainID graph.DomainID, capability string) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if s.authorizer == nil {
		return nil
	}
	scope, err := s.automationAccessScope(ctx, domainID)
	if err != nil {
		return err
	}
	if scoped, ok := s.authorizer.(ScopedOperatorAuthorizer); ok {
		return scoped.Authorize(ctx, principal.PrincipalID, capability, scope)
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability)
	if err != nil {
		return status.Errorf(codes.Internal, "authorize automation operator: %v", err)
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required automation capability")
	}
	return nil
}

func (s *AdminAutomationService) automationAccessScope(ctx context.Context, domainID graph.DomainID) (principalservice.AccessScope, error) {
	scope := principalservice.AccessScope{Type: "domain", DomainID: domainID.String()}
	if s.domainResolver == nil {
		return scope, nil
	}
	domain, err := s.domainResolver.GetDomain(ctx, domainID.String())
	if err != nil {
		return principalservice.AccessScope{}, mapAdminAutomationDomainResolveError(err)
	}
	if domain.SpaceID != uuid.Nil {
		scope.SpaceID = domain.SpaceID.String()
	}
	return scope, nil
}

func mapAdminAutomationDomainResolveError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, storedomains.ErrDomainNotFound) {
		return status.Error(codes.NotFound, "domain not found")
	}
	return status.Errorf(codes.Internal, "resolve automation domain scope: %v", err)
}

func automationActorFromContext(ctx context.Context) string {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return ""
	}
	return principal.PrincipalID
}
