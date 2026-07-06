package admin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListPageSize = 100
	maxListPageSize     = 1000
)

type OperatorService struct {
	adminv1.UnimplementedAdminOperatorServiceServer
	manager daemonadmin.OperatorManager
}

func NewOperatorService(manager daemonadmin.OperatorManager) *OperatorService {
	return &OperatorService{manager: manager}
}

func (s *OperatorService) ListOperators(ctx context.Context, req *adminv1.ListOperatorsRequest) (*adminv1.ListOperatorsResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	admins, err := s.manager.ListAdmins(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list admins: %v", err)
	}
	filtered := make([]daemonadmin.AdminSummary, 0, len(admins))
	for _, admin := range admins {
		if admin.State == daemonadmin.AdminStateDeleted && !req.GetIncludeDeleted() {
			continue
		}
		if admin.State == daemonadmin.AdminStateDisabled && !req.GetIncludeDisabled() {
			continue
		}
		filtered = append(filtered, admin)
	}
	if offset > len(filtered) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the operator list")
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	operators := make([]*adminv1.Operator, 0, end-offset)
	for _, admin := range filtered[offset:end] {
		operators = append(operators, mapAdminSummary(admin))
	}
	var nextToken string
	if end < len(filtered) {
		nextToken = strconv.Itoa(end)
	}
	return &adminv1.ListOperatorsResponse{Operators: operators, NextPageToken: nextToken}, nil
}

func (s *OperatorService) GetOperator(ctx context.Context, req *adminv1.GetOperatorRequest) (*adminv1.GetOperatorResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.GetOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "get operator")
	}
	return &adminv1.GetOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) FindOperator(ctx context.Context, req *adminv1.FindOperatorRequest) (*adminv1.FindOperatorResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.FindOperator(ctx, req.GetUsername(), req.GetEmail())
	if err != nil {
		return nil, mapAdminError(err, "find operator")
	}
	return &adminv1.FindOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) CreateOperator(ctx context.Context, req *adminv1.CreateOperatorRequest) (*adminv1.CreateOperatorResponse, error) {
	principal, err := s.requireSystemAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetUsername()) == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and password are required")
	}
	input := daemonadmin.CreateOperatorInput{Username: req.GetUsername(), Email: req.GetEmail(), Password: req.GetPassword(), Disabled: req.GetDisabled(), CreatedBy: principal.OperatorID}
	for _, role := range req.GetRoles() {
		internalRole, err := roleToInternal(role)
		if err != nil {
			return nil, err
		}
		input.Roles = append(input.Roles, daemonadmin.RoleGrant{Role: internalRole, Scope: daemonadmin.AccessScope{Type: "system"}, Reason: "create operator", GrantedByOperatorID: principal.OperatorID})
	}
	for _, grant := range req.GetCapabilityGrants() {
		capability, err := capabilityToInternal(grant.GetCapability())
		if err != nil {
			return nil, err
		}
		input.CapabilityGrants = append(input.CapabilityGrants, daemonadmin.CapabilityGrant{Capability: capability, Scope: scopeToInternal(grant.GetScope()), Reason: grant.GetReason(), GrantedByOperatorID: principal.OperatorID})
	}
	admin, err := s.manager.CreateOperator(ctx, input)
	if err != nil {
		return nil, mapAdminError(err, "create operator")
	}
	return &adminv1.CreateOperatorResponse{Operator: mapAdminSummary(admin), RoleGrants: mapRoleGrants(admin.RoleGrants), CapabilityGrants: mapCapabilityGrants(admin.CapabilityGrants), EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) UpdateOperator(ctx context.Context, req *adminv1.UpdateOperatorRequest) (*adminv1.UpdateOperatorResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	operator := req.GetOperator()
	if operator == nil || operator.GetOperatorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "operator.operator_id is required")
	}
	email := operator.GetEmail()
	admin, err := s.manager.UpdateOperator(ctx, daemonadmin.UpdateOperatorInput{OperatorID: operator.GetOperatorId(), Email: &email})
	if err != nil {
		return nil, mapAdminError(err, "update operator")
	}
	return &adminv1.UpdateOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) DisableOperator(ctx context.Context, req *adminv1.DisableOperatorRequest) (*adminv1.DisableOperatorResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.DisableOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "disable operator")
	}
	return &adminv1.DisableOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) EnableOperator(ctx context.Context, req *adminv1.EnableOperatorRequest) (*adminv1.EnableOperatorResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.EnableOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "enable operator")
	}
	return &adminv1.EnableOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) DeleteOperator(ctx context.Context, req *adminv1.DeleteOperatorRequest) (*adminv1.DeleteOperatorResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.DeleteOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "delete operator")
	}
	return &adminv1.DeleteOperatorResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) SetOperatorPassword(ctx context.Context, req *adminv1.SetOperatorPasswordRequest) (*adminv1.SetOperatorPasswordResponse, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	operatorID := req.GetOperatorId()
	if operatorID == "" {
		operatorID = principal.OperatorID
	}
	if operatorID != principal.OperatorID {
		if _, err := s.requireSystemAdmin(ctx); err != nil {
			return nil, err
		}
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password must not be empty")
	}
	admin, err := s.manager.SetOperatorPassword(ctx, operatorID, req.GetPassword())
	if err != nil {
		return nil, mapAdminError(err, "set operator password")
	}
	return &adminv1.SetOperatorPasswordResponse{Operator: mapAdminSummary(admin)}, nil
}

func (s *OperatorService) ListOperatorRoles(ctx context.Context, req *adminv1.ListOperatorRolesRequest) (*adminv1.ListOperatorRolesResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.GetOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "list operator roles")
	}
	roles := make([]adminv1.OperatorRole, 0, len(admin.RoleGrants))
	for _, grant := range admin.RoleGrants {
		roles = append(roles, roleFromInternal(grant.Role))
	}
	return &adminv1.ListOperatorRolesResponse{Grants: mapRoleGrants(admin.RoleGrants), EffectiveRoles: roles}, nil
}

func (s *OperatorService) GrantOperatorRole(ctx context.Context, req *adminv1.GrantOperatorRoleRequest) (*adminv1.GrantOperatorRoleResponse, error) {
	principal, err := s.requireSystemAdmin(ctx)
	if err != nil {
		return nil, err
	}
	role, err := roleToInternal(req.GetRole())
	if err != nil {
		return nil, err
	}
	grant, admin, err := s.manager.GrantRole(ctx, req.GetOperatorId(), role, scopeToInternal(req.GetScope()), req.GetReason(), principal.OperatorID)
	if err != nil {
		return nil, mapAdminError(err, "grant operator role")
	}
	return &adminv1.GrantOperatorRoleResponse{Grant: mapRoleGrant(grant), EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) RevokeOperatorRole(ctx context.Context, req *adminv1.RevokeOperatorRoleRequest) (*adminv1.RevokeOperatorRoleResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.RevokeRole(ctx, req.GetOperatorId(), req.GetRoleGrantId())
	if err != nil {
		return nil, mapAdminError(err, "revoke operator role")
	}
	return &adminv1.RevokeOperatorRoleResponse{EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) ListOperatorCapabilities(ctx context.Context, req *adminv1.ListOperatorCapabilitiesRequest) (*adminv1.ListOperatorCapabilitiesResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.GetOperator(ctx, req.GetOperatorId())
	if err != nil {
		return nil, mapAdminError(err, "list operator capabilities")
	}
	return &adminv1.ListOperatorCapabilitiesResponse{Grants: mapCapabilityGrants(admin.CapabilityGrants), EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) GrantOperatorCapability(ctx context.Context, req *adminv1.GrantOperatorCapabilityRequest) (*adminv1.GrantOperatorCapabilityResponse, error) {
	principal, err := s.requireSystemAdmin(ctx)
	if err != nil {
		return nil, err
	}
	capability, err := capabilityToInternal(req.GetCapability())
	if err != nil {
		return nil, err
	}
	grant, admin, err := s.manager.GrantCapability(ctx, req.GetOperatorId(), capability, scopeToInternal(req.GetScope()), req.GetReason(), principal.OperatorID)
	if err != nil {
		return nil, mapAdminError(err, "grant operator capability")
	}
	return &adminv1.GrantOperatorCapabilityResponse{Grant: mapCapabilityGrant(grant), EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) RevokeOperatorCapability(ctx context.Context, req *adminv1.RevokeOperatorCapabilityRequest) (*adminv1.RevokeOperatorCapabilityResponse, error) {
	if _, err := s.requireSystemAdmin(ctx); err != nil {
		return nil, err
	}
	admin, err := s.manager.RevokeCapability(ctx, req.GetOperatorId(), req.GetCapabilityGrantId())
	if err != nil {
		return nil, mapAdminError(err, "revoke operator capability")
	}
	return &adminv1.RevokeOperatorCapabilityResponse{EffectiveCapabilities: effectiveCapabilities(admin)}, nil
}

func (s *OperatorService) ListOperatorSessions(ctx context.Context, req *adminv1.ListOperatorSessionsRequest) (*adminv1.ListOperatorSessionsResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	return &adminv1.ListOperatorSessionsResponse{Sessions: []*adminv1.OperatorAuthSessionSummary{}}, nil
}

func (s *OperatorService) RevokeOperatorSession(ctx context.Context, req *adminv1.RevokeOperatorSessionRequest) (*adminv1.RevokeOperatorSessionResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	return &adminv1.RevokeOperatorSessionResponse{}, nil
}

func (s *OperatorService) RevokeOperatorSessions(ctx context.Context, req *adminv1.RevokeOperatorSessionsRequest) (*adminv1.RevokeOperatorSessionsResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	return &adminv1.RevokeOperatorSessionsResponse{RevokedCount: 0}, nil
}

func (s *OperatorService) requireSystemAdmin(ctx context.Context) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.manager.IsSystemAdmin(ctx, principal.OperatorID)
	if err != nil {
		return daemonauth.Principal{}, mapAdminError(err, "authorize operator")
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "system admin role is required")
	}
	return principal, nil
}

func principalFromContext(ctx context.Context) (daemonauth.Principal, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok || principal.OperatorID == "" || (principal.Kind != "" && principal.Kind != daemonauth.PrincipalKindOperator) {
		return daemonauth.Principal{}, status.Error(codes.Unauthenticated, "operator authentication is required")
	}
	return principal, nil
}

func normalizePageSize(size int32) int {
	if size <= 0 {
		return defaultListPageSize
	}
	if size > maxListPageSize {
		return maxListPageSize
	}
	return int(size)
}

func mapAdminError(err error, action string) error {
	if errors.Is(err, daemonadmin.ErrAdminNotFound) {
		return status.Error(codes.NotFound, "operator not found")
	}
	if errors.Is(err, daemonadmin.ErrDuplicateAdmin) {
		return status.Error(codes.AlreadyExists, "operator already exists")
	}
	if errors.Is(err, daemonadmin.ErrGrantNotFound) {
		return status.Error(codes.NotFound, "grant not found")
	}
	if errors.Is(err, daemonadmin.ErrLastSystemAdmin) {
		return status.Error(codes.FailedPrecondition, "cannot remove the last active system admin")
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

func mapAdminSummary(admin daemonadmin.AdminSummary) *adminv1.Operator {
	return &adminv1.Operator{OperatorId: admin.ID, Username: admin.Username, Email: admin.Email, State: stateFromInternal(admin.State), CreateTime: timestamppb.New(admin.CreatedAt), UpdateTime: timestamppb.New(admin.UpdatedAt)}
}

func stateFromInternal(state string) adminv1.OperatorState {
	switch state {
	case daemonadmin.AdminStateDisabled:
		return adminv1.OperatorState_OPERATOR_STATE_DISABLED
	case daemonadmin.AdminStateDeleted:
		return adminv1.OperatorState_OPERATOR_STATE_DELETED
	default:
		return adminv1.OperatorState_OPERATOR_STATE_ACTIVE
	}
}

func roleToInternal(role adminv1.OperatorRole) (string, error) {
	switch role {
	case adminv1.OperatorRole_OPERATOR_ROLE_SYSTEM_ADMIN:
		return daemonadmin.OperatorRoleSystemAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_USER_ADMIN:
		return daemonadmin.OperatorRoleUserAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_SPACE_ADMIN:
		return daemonadmin.OperatorRoleSpaceAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_SEMANTIC_ADMIN:
		return daemonadmin.OperatorRoleSemanticAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_STORAGE_ADMIN:
		return daemonadmin.OperatorRoleStorageAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_MESH_ADMIN:
		return daemonadmin.OperatorRoleMeshAdmin, nil
	case adminv1.OperatorRole_OPERATOR_ROLE_AUDIT_READER:
		return daemonadmin.OperatorRoleAuditReader, nil
	default:
		return "", status.Error(codes.InvalidArgument, "operator role is required")
	}
}

func roleFromInternal(role string) adminv1.OperatorRole {
	switch role {
	case daemonadmin.OperatorRoleSystemAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_SYSTEM_ADMIN
	case daemonadmin.OperatorRoleUserAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_USER_ADMIN
	case daemonadmin.OperatorRoleSpaceAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_SPACE_ADMIN
	case daemonadmin.OperatorRoleSemanticAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_SEMANTIC_ADMIN
	case daemonadmin.OperatorRoleStorageAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_STORAGE_ADMIN
	case daemonadmin.OperatorRoleMeshAdmin:
		return adminv1.OperatorRole_OPERATOR_ROLE_MESH_ADMIN
	case daemonadmin.OperatorRoleAuditReader:
		return adminv1.OperatorRole_OPERATOR_ROLE_AUDIT_READER
	default:
		return adminv1.OperatorRole_OPERATOR_ROLE_UNSPECIFIED
	}
}

func capabilityToInternal(capability commonv1.Capability) (string, error) {
	if capability == commonv1.Capability_CAPABILITY_UNSPECIFIED {
		return "", status.Error(codes.InvalidArgument, "capability is required")
	}
	return capability.String(), nil
}
func capabilityFromInternal(capability string) commonv1.Capability {
	if value, ok := commonv1.Capability_value[capability]; ok {
		return commonv1.Capability(value)
	}
	return commonv1.Capability_CAPABILITY_UNSPECIFIED
}

func scopeToInternal(scope *commonv1.AccessScope) daemonadmin.AccessScope {
	if scope == nil {
		return daemonadmin.AccessScope{Type: "system"}
	}
	return daemonadmin.AccessScope{Type: scope.GetType().String(), SpaceID: scope.GetSpaceId(), DomainID: scope.GetDomainId()}
}
func scopeFromInternal(scope daemonadmin.AccessScope) *commonv1.AccessScope {
	typ := commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SYSTEM
	if value, ok := commonv1.AccessScopeType_value[scope.Type]; ok {
		typ = commonv1.AccessScopeType(value)
	}
	return &commonv1.AccessScope{Type: typ, SpaceId: optionalString(scope.SpaceID), DomainId: optionalString(scope.DomainID)}
}
func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func mapRoleGrant(grant daemonadmin.RoleGrant) *adminv1.OperatorRoleGrant {
	return &adminv1.OperatorRoleGrant{RoleGrantId: grant.ID, OperatorId: grant.OperatorID, Role: roleFromInternal(grant.Role), Scope: scopeFromInternal(grant.Scope), Reason: grant.Reason, GrantedByOperatorId: grant.GrantedByOperatorID, CreateTime: timestamppb.New(grant.CreatedAt)}
}
func mapRoleGrants(grants []daemonadmin.RoleGrant) []*adminv1.OperatorRoleGrant {
	out := make([]*adminv1.OperatorRoleGrant, 0, len(grants))
	for _, grant := range grants {
		out = append(out, mapRoleGrant(grant))
	}
	return out
}
func mapCapabilityGrant(grant daemonadmin.CapabilityGrant) *adminv1.OperatorCapabilityGrant {
	return &adminv1.OperatorCapabilityGrant{CapabilityGrantId: grant.ID, OperatorId: grant.OperatorID, Capability: capabilityFromInternal(grant.Capability), Scope: scopeFromInternal(grant.Scope), Reason: grant.Reason, GrantedByOperatorId: grant.GrantedByOperatorID, CreateTime: timestamppb.New(grant.CreatedAt)}
}
func mapCapabilityGrants(grants []daemonadmin.CapabilityGrant) []*adminv1.OperatorCapabilityGrant {
	out := make([]*adminv1.OperatorCapabilityGrant, 0, len(grants))
	for _, grant := range grants {
		out = append(out, mapCapabilityGrant(grant))
	}
	return out
}

func effectiveCapabilities(admin daemonadmin.AdminSummary) []commonv1.Capability {
	seen := map[commonv1.Capability]bool{}
	var out []commonv1.Capability
	add := func(cap commonv1.Capability) {
		if cap != commonv1.Capability_CAPABILITY_UNSPECIFIED && !seen[cap] {
			seen[cap] = true
			out = append(out, cap)
		}
	}
	for _, grant := range admin.CapabilityGrants {
		add(capabilityFromInternal(grant.Capability))
	}
	for _, grant := range admin.RoleGrants {
		for _, cap := range capabilitiesForRole(grant.Role) {
			add(cap)
		}
	}
	return out
}

func capabilitiesForRole(role string) []commonv1.Capability {
	switch role {
	case daemonadmin.OperatorRoleSystemAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_OPERATOR_CREATE, commonv1.Capability_CAPABILITY_OPERATOR_MANAGE, commonv1.Capability_CAPABILITY_USER_CREATE, commonv1.Capability_CAPABILITY_USER_MANAGE, commonv1.Capability_CAPABILITY_DAEMON_CONFIGURE, commonv1.Capability_CAPABILITY_MESH_MANAGE}
	case daemonadmin.OperatorRoleUserAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_USER_CREATE, commonv1.Capability_CAPABILITY_USER_MANAGE}
	case daemonadmin.OperatorRoleSpaceAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_SPACE_CREATE, commonv1.Capability_CAPABILITY_SPACE_ARCHIVE, commonv1.Capability_CAPABILITY_SPACE_DELETE, commonv1.Capability_CAPABILITY_SPACE_MANAGE_ACCESS}
	case daemonadmin.OperatorRoleSemanticAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH}
	case daemonadmin.OperatorRoleStorageAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_SYSTEM_COMPACT_SPACE, commonv1.Capability_CAPABILITY_SYSTEM_MAINTAIN_SPACE, commonv1.Capability_CAPABILITY_SYSTEM_BACKUP_SPACE}
	case daemonadmin.OperatorRoleMeshAdmin:
		return []commonv1.Capability{commonv1.Capability_CAPABILITY_MESH_MANAGE}
	case daemonadmin.OperatorRoleAuditReader:
		return []commonv1.Capability{}
	default:
		return nil
	}
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("page_token must be a non-negative integer offset")
	}
	return offset, nil
}
