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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminAutomationService struct {
	adminv1.UnimplementedAdminAutomationServiceServer
	automations automationservice.Manager
}

func NewAdminAutomationService(automations automationservice.Manager) *AdminAutomationService {
	return &AdminAutomationService{automations: automations}
}

func (s *AdminAutomationService) CreateAutomation(ctx context.Context, req *adminv1.CreateAutomationRequest) (*adminv1.CreateAutomationResponse, error) {
	domainID, err := parseAdminAutomationDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.CreateAutomation(ctx, domainID, req.GetDefinitionJson())
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
	def, err := s.automations.UpdateAutomation(ctx, domainID, req.GetAutomationId(), req.GetDefinitionJson())
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
	def, err := s.automations.SetAutomationStatus(ctx, domainID, req.GetAutomationId(), automationmodel.StatusEnabled)
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
	def, err := s.automations.SetAutomationStatus(ctx, domainID, req.GetAutomationId(), automationmodel.StatusDisabled)
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

func adminAutomationDefinitionJSON(def automationmodel.Definition) (string, error) {
	data, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return string(data), nil
}
func adminAutomationSummary(def automationmodel.Definition) *clientv1.AutomationDefinitionSummary {
	return &clientv1.AutomationDefinitionSummary{Id: def.ID, Name: def.Name, Version: int32(def.Version), Status: def.Status, Events: def.Trigger.Events, Labels: def.Trigger.Labels, UpdatedAt: formatAdminAutomationTime(def.UpdatedAt)}
}
func adminInvocationSummary(inv automationmodel.Invocation) *clientv1.AutomationInvocationSummary {
	return &clientv1.AutomationInvocationSummary{Id: inv.ID, AutomationId: inv.AutomationID, AutomationVersion: int32(inv.AutomationVersion), EventId: inv.EventID, ChangedElementId: inv.ChangedElementID, EventType: inv.EventType, Status: inv.Status, SkipReason: inv.SkipReason, CreatedAt: formatAdminAutomationTime(inv.CreatedAt), UpdatedAt: formatAdminAutomationTime(inv.UpdatedAt)}
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
