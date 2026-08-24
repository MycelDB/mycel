package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	"github.com/myceldb/mycel/internal/automation/storage"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
}

func NewAdminAutomationService(automations automationservice.Manager, authorizer ...OperatorAuthorizer) *AdminAutomationService {
	var authz OperatorAuthorizer
	if len(authorizer) > 0 {
		authz = authorizer[0]
	}
	return &AdminAutomationService{automations: automations, authorizer: authz}
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
	run, err := s.automations.GetRun(ctx, domainID, req.GetRunId())
	if err != nil {
		return nil, mapAdminAutomationError(err)
	}
	data, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &adminv1.GetAutomationRunResponse{RunJson: string(data)}, nil
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
