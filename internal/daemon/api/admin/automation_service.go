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
