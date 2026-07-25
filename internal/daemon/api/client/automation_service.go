package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	"github.com/myceldb/mycel/internal/automation/storage"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AutomationService struct {
	clientv1.UnimplementedAutomationServiceServer
	automations automationservice.Manager
}

func NewAutomationService(automations automationservice.Manager) *AutomationService {
	return &AutomationService{automations: automations}
}

func (s *AutomationService) CreateAutomation(ctx context.Context, req *clientv1.CreateAutomationRequest) (*clientv1.CreateAutomationResponse, error) {
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.CreateAutomation(ctx, domainID, req.GetDefinitionJson())
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
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.UpdateAutomation(ctx, domainID, req.GetAutomationId(), req.GetDefinitionJson())
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
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatus(ctx, domainID, req.GetAutomationId(), automationmodel.StatusEnabled)
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
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	def, err := s.automations.SetAutomationStatus(ctx, domainID, req.GetAutomationId(), automationmodel.StatusDisabled)
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
	if _, err := spaceUserPrincipalFromContext(ctx); err != nil {
		return nil, err
	}
	domainID, err := parseDomainID(req.GetDomainId())
	if err != nil {
		return nil, err
	}
	run, err := s.automations.GetRun(ctx, domainID, req.GetRunId())
	if err != nil {
		return nil, mapAutomationError(err)
	}
	data, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &clientv1.GetAutomationRunResponse{RunJson: string(data)}, nil
}

func automationDefinitionJSON(def automationmodel.Definition) (string, error) {
	data, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return string(data), nil
}

func automationSummary(def automationmodel.Definition) *clientv1.AutomationDefinitionSummary {
	return &clientv1.AutomationDefinitionSummary{Id: def.ID, Name: def.Name, Version: int32(def.Version), Status: def.Status, Events: def.Trigger.Events, Labels: def.Trigger.Labels, UpdatedAt: formatAutomationTime(def.UpdatedAt)}
}

func invocationSummary(inv automationmodel.Invocation) *clientv1.AutomationInvocationSummary {
	return &clientv1.AutomationInvocationSummary{Id: inv.ID, AutomationId: inv.AutomationID, AutomationVersion: int32(inv.AutomationVersion), EventId: inv.EventID, ChangedElementId: inv.ChangedElementID, EventType: inv.EventType, Status: inv.Status, SkipReason: inv.SkipReason, CreatedAt: formatAutomationTime(inv.CreatedAt), UpdatedAt: formatAutomationTime(inv.UpdatedAt)}
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
