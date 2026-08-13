package admin

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	adminPrincipalRefreshTokenBytes  = 32
	adminPrincipalRefreshIdleTTL     = 30 * 24 * time.Hour
	adminPrincipalRefreshAbsoluteTTL = 90 * 24 * time.Hour
)

type PrincipalManager = principalservice.Manager

type PrincipalService struct {
	adminv1.UnimplementedAdminPrincipalServiceServer
	manager principalservice.Manager
	tokens  *daemonauth.TokenManager
}

func NewPrincipalService(manager principalservice.Manager, tokens *daemonauth.TokenManager) *PrincipalService {
	return &PrincipalService{manager: manager, tokens: tokens}
}

func (s *PrincipalService) ListPrincipals(ctx context.Context, req *adminv1.ListPrincipalsRequest) (*adminv1.ListPrincipalsResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_READ); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	principals, err := s.manager.ListPrincipals(ctx)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "list principals")
	}
	filtered := make([]principalservice.PrincipalSummary, 0, len(principals))
	for _, principal := range principals {
		if principal.State == principalservice.PrincipalStateDeleted && !req.GetIncludeDeleted() {
			continue
		}
		if principal.State == principalservice.PrincipalStateDisabled && !req.GetIncludeDisabled() {
			continue
		}
		filtered = append(filtered, principal)
	}
	if offset > len(filtered) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the principal list")
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	out := make([]*adminv1.Principal, 0, end-offset)
	for _, principal := range filtered[offset:end] {
		out = append(out, mapPrincipalSummary(principal))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListPrincipalsResponse{Principals: out, NextPageToken: next}, nil
}

func (s *PrincipalService) GetPrincipal(ctx context.Context, req *adminv1.GetPrincipalRequest) (*adminv1.GetPrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_READ); err != nil {
		return nil, err
	}
	principal, err := s.manager.GetPrincipal(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "get principal")
	}
	return &adminv1.GetPrincipalResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) FindPrincipal(ctx context.Context, req *adminv1.FindPrincipalRequest) (*adminv1.FindPrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_READ); err != nil {
		return nil, err
	}
	principal, err := s.manager.FindPrincipal(ctx, req.GetUsername(), req.GetEmail())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "find principal")
	}
	return &adminv1.FindPrincipalResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) CreatePrincipal(ctx context.Context, req *adminv1.CreatePrincipalRequest) (*adminv1.CreatePrincipalResponse, error) {
	actor, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_CREATE)
	if err != nil {
		return nil, err
	}
	input := principalservice.CreatePrincipalInput{Username: req.GetUsername(), Email: req.GetEmail(), DisplayName: req.GetDisplayName(), Kind: principalKindFromProto(req.GetType()), Password: req.GetPassword(), LoginEnabled: req.GetLoginEnabled(), Disabled: req.GetDisabled(), CreatedBy: actor.PrincipalID}
	for _, role := range req.GetRoles() {
		input.Roles = append(input.Roles, principalservice.RoleBinding{Role: role, Scope: principalservice.AccessScope{Type: "system"}, Reason: "create principal", CreatedBy: actor.PrincipalID})
	}
	for _, grant := range req.GetCapabilityGrants() {
		capability, err := capabilityToInternal(grant.GetCapability())
		if err != nil {
			return nil, err
		}
		input.Capabilities = append(input.Capabilities, principalservice.CapabilityGrant{Capability: capability, Scope: principalScopeFromProto(grant.GetScope()), Reason: grant.GetReason(), CreatedBy: actor.PrincipalID})
	}
	principal, err := s.manager.CreatePrincipal(ctx, input)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "create principal")
	}
	roles, caps, effective, err := s.principalAccess(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	return &adminv1.CreatePrincipalResponse{Principal: mapPrincipalSummary(principal), RoleGrants: roles, CapabilityGrants: caps, EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) UpdatePrincipal(ctx context.Context, req *adminv1.UpdatePrincipalRequest) (*adminv1.UpdatePrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE); err != nil {
		return nil, err
	}
	principal := req.GetPrincipal()
	if principal == nil || strings.TrimSpace(principal.GetPrincipalId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "principal.principal_id is required")
	}
	input := principalservice.UpdatePrincipalInput{PrincipalID: principal.GetPrincipalId()}
	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"username", "display_name", "email", "type", "login_enabled"}
	}
	for _, path := range paths {
		switch path {
		case "username":
			value := principal.GetUsername()
			input.Username = &value
		case "display_name":
			value := principal.GetDisplayName()
			input.DisplayName = &value
		case "email":
			value := principal.GetEmail()
			input.Email = &value
		case "type":
			value := principalKindFromProto(principal.GetType())
			input.Kind = &value
		case "login_enabled":
			value := principal.GetLoginEnabled()
			input.LoginEnabled = &value
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update_mask path %q", path)
		}
	}
	updated, err := s.manager.UpdatePrincipal(ctx, input)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "update principal")
	}
	return &adminv1.UpdatePrincipalResponse{Principal: mapPrincipalSummary(updated)}, nil
}

func (s *PrincipalService) DisablePrincipal(ctx context.Context, req *adminv1.DisablePrincipalRequest) (*adminv1.DisablePrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE); err != nil {
		return nil, err
	}
	principal, err := s.manager.DisablePrincipal(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "disable principal")
	}
	if req.GetRevokeSessions() {
		if _, err := s.manager.RevokePrincipalSessions(ctx, req.GetPrincipalId()); err != nil {
			return nil, mapPrincipalServiceError(err, "revoke principal sessions")
		}
	}
	return &adminv1.DisablePrincipalResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) EnablePrincipal(ctx context.Context, req *adminv1.EnablePrincipalRequest) (*adminv1.EnablePrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE); err != nil {
		return nil, err
	}
	principal, err := s.manager.EnablePrincipal(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "enable principal")
	}
	return &adminv1.EnablePrincipalResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) DeletePrincipal(ctx context.Context, req *adminv1.DeletePrincipalRequest) (*adminv1.DeletePrincipalResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE); err != nil {
		return nil, err
	}
	principal, err := s.manager.DeletePrincipal(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "delete principal")
	}
	if req.GetRevokeSessions() {
		if _, err := s.manager.RevokePrincipalSessions(ctx, req.GetPrincipalId()); err != nil {
			return nil, mapPrincipalServiceError(err, "revoke principal sessions")
		}
	}
	return &adminv1.DeletePrincipalResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) SetPrincipalPassword(ctx context.Context, req *adminv1.SetPrincipalPasswordRequest) (*adminv1.SetPrincipalPasswordResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_CREDENTIAL_SET); err != nil {
		return nil, err
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password must not be empty")
	}
	principal, err := s.manager.SetPrincipalPassword(ctx, req.GetPrincipalId(), req.GetPassword())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "set principal password")
	}
	if req.GetRevokeSessions() {
		if _, err := s.manager.RevokePrincipalSessions(ctx, req.GetPrincipalId()); err != nil {
			return nil, mapPrincipalServiceError(err, "revoke principal sessions")
		}
	}
	return &adminv1.SetPrincipalPasswordResponse{Principal: mapPrincipalSummary(principal)}, nil
}

func (s *PrincipalService) CreatePrincipalSession(ctx context.Context, req *adminv1.CreatePrincipalSessionRequest) (*adminv1.CreatePrincipalSessionResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_SESSION_DELEGATE); err != nil {
		return nil, err
	}
	if s.tokens == nil {
		return nil, status.Error(codes.FailedPrecondition, "token manager is not initialized")
	}
	principal, err := s.manager.GetPrincipal(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "get principal")
	}
	if principal.State != principalservice.PrincipalStateActive || !principal.LoginEnabled {
		return nil, status.Error(codes.FailedPrecondition, "principal cannot log in")
	}
	refreshToken, rec, err := s.manager.CreateAuthSession(ctx, principal, adminClientMetadata(req.GetClient()), adminPrincipalRefreshTokenBytes, adminPrincipalRefreshIdleTTL, adminPrincipalRefreshAbsoluteTTL)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "create principal session")
	}
	accessToken, expireAt, err := s.tokens.Issue(daemonPrincipal(principal, rec.ID.String()))
	if err != nil {
		return nil, err
	}
	refreshTokenText := string(refreshToken)
	return &adminv1.CreatePrincipalSessionResponse{AccessToken: accessToken, AccessTokenExpireTime: timestamppb.New(expireAt), RefreshToken: &refreshTokenText, Principal: mapPrincipalSummary(principal), AuthSessionId: rec.ID.String()}, nil
}

func (s *PrincipalService) ListPrincipalSessions(ctx context.Context, req *adminv1.ListPrincipalSessionsRequest) (*adminv1.ListPrincipalSessionsResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_SESSION_MANAGE); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	sessions, err := s.manager.ListPrincipalSessions(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "list principal sessions")
	}
	filtered := make([]domainauth.RefreshSession, 0, len(sessions))
	for _, session := range sessions {
		if !req.GetIncludeInactive() && session.Status != domainauth.RefreshSessionStatusActive {
			continue
		}
		filtered = append(filtered, session)
	}
	if offset > len(filtered) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the session list")
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	out := make([]*commonv1.AuthSessionSummary, 0, end-offset)
	for _, session := range filtered[offset:end] {
		out = append(out, mapAuthSession(session))
	}
	var next string
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListPrincipalSessionsResponse{Sessions: out, NextPageToken: next}, nil
}

func (s *PrincipalService) RevokePrincipalSession(ctx context.Context, req *adminv1.RevokePrincipalSessionRequest) (*adminv1.RevokePrincipalSessionResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_SESSION_MANAGE); err != nil {
		return nil, err
	}
	if err := s.manager.RevokePrincipalSession(ctx, req.GetPrincipalId(), req.GetAuthSessionId()); err != nil {
		return nil, mapPrincipalServiceError(err, "revoke principal session")
	}
	return &adminv1.RevokePrincipalSessionResponse{}, nil
}

func (s *PrincipalService) RevokePrincipalSessions(ctx context.Context, req *adminv1.RevokePrincipalSessionsRequest) (*adminv1.RevokePrincipalSessionsResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_SESSION_MANAGE); err != nil {
		return nil, err
	}
	count, err := s.manager.RevokePrincipalSessions(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "revoke principal sessions")
	}
	return &adminv1.RevokePrincipalSessionsResponse{RevokedCount: int32(count)}, nil
}

func (s *PrincipalService) ListPrincipalRoles(ctx context.Context, req *adminv1.ListPrincipalRolesRequest) (*adminv1.ListPrincipalRolesResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE); err != nil {
		return nil, err
	}
	bindings, err := s.manager.ListRoleBindings(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "list principal roles")
	}
	grants := make([]*adminv1.PrincipalRoleGrant, 0, len(bindings))
	for _, binding := range bindings {
		if binding.State == principalservice.GrantStateActive {
			grants = append(grants, mapPrincipalRoleGrant(binding))
		}
	}
	access, err := s.manager.EffectiveAccess(ctx, req.GetPrincipalId(), principalservice.AccessScope{Type: "system"})
	if err != nil {
		return nil, mapPrincipalServiceError(err, "get effective principal roles")
	}
	return &adminv1.ListPrincipalRolesResponse{Grants: grants, EffectiveRoles: access.Roles}, nil
}

func (s *PrincipalService) GrantPrincipalRole(ctx context.Context, req *adminv1.GrantPrincipalRoleRequest) (*adminv1.GrantPrincipalRoleResponse, error) {
	actor, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE)
	if err != nil {
		return nil, err
	}
	grant, _, err := s.manager.GrantRole(ctx, req.GetPrincipalId(), req.GetRole(), principalScopeFromProto(req.GetScope()), req.GetReason(), actor.PrincipalID)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "grant principal role")
	}
	effective, err := s.effectiveCapabilities(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GrantPrincipalRoleResponse{Grant: mapPrincipalRoleGrant(grant), EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) RevokePrincipalRole(ctx context.Context, req *adminv1.RevokePrincipalRoleRequest) (*adminv1.RevokePrincipalRoleResponse, error) {
	actor, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE)
	if err != nil {
		return nil, err
	}
	if _, err := s.manager.RevokeRole(ctx, req.GetPrincipalId(), req.GetRoleGrantId(), actor.PrincipalID); err != nil {
		return nil, mapPrincipalServiceError(err, "revoke principal role")
	}
	effective, err := s.effectiveCapabilities(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, err
	}
	return &adminv1.RevokePrincipalRoleResponse{EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) ListPrincipalCapabilities(ctx context.Context, req *adminv1.ListPrincipalCapabilitiesRequest) (*adminv1.ListPrincipalCapabilitiesResponse, error) {
	if _, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE); err != nil {
		return nil, err
	}
	grants, err := s.manager.ListCapabilityGrants(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, mapPrincipalServiceError(err, "list principal capabilities")
	}
	out := make([]*adminv1.PrincipalCapabilityGrant, 0, len(grants))
	for _, grant := range grants {
		if grant.State == principalservice.GrantStateActive {
			out = append(out, mapPrincipalCapabilityGrant(grant))
		}
	}
	effective, err := s.effectiveCapabilities(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, err
	}
	return &adminv1.ListPrincipalCapabilitiesResponse{Grants: out, EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) GrantPrincipalCapability(ctx context.Context, req *adminv1.GrantPrincipalCapabilityRequest) (*adminv1.GrantPrincipalCapabilityResponse, error) {
	actor, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE)
	if err != nil {
		return nil, err
	}
	capability, err := capabilityToInternal(req.GetCapability())
	if err != nil {
		return nil, err
	}
	grant, _, err := s.manager.GrantCapability(ctx, req.GetPrincipalId(), capability, principalScopeFromProto(req.GetScope()), req.GetReason(), actor.PrincipalID)
	if err != nil {
		return nil, mapPrincipalServiceError(err, "grant principal capability")
	}
	effective, err := s.effectiveCapabilities(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GrantPrincipalCapabilityResponse{Grant: mapPrincipalCapabilityGrant(grant), EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) RevokePrincipalCapability(ctx context.Context, req *adminv1.RevokePrincipalCapabilityRequest) (*adminv1.RevokePrincipalCapabilityResponse, error) {
	actor, err := s.requireCapability(ctx, commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE)
	if err != nil {
		return nil, err
	}
	if _, err := s.manager.RevokeCapability(ctx, req.GetPrincipalId(), req.GetCapabilityGrantId(), actor.PrincipalID); err != nil {
		return nil, mapPrincipalServiceError(err, "revoke principal capability")
	}
	effective, err := s.effectiveCapabilities(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, err
	}
	return &adminv1.RevokePrincipalCapabilityResponse{EffectiveCapabilities: effective}, nil
}

func (s *PrincipalService) principalAccess(ctx context.Context, principalID string) ([]*adminv1.PrincipalRoleGrant, []*adminv1.PrincipalCapabilityGrant, []commonv1.Capability, error) {
	bindings, err := s.manager.ListRoleBindings(ctx, principalID)
	if err != nil {
		return nil, nil, nil, mapPrincipalServiceError(err, "list principal roles")
	}
	roles := make([]*adminv1.PrincipalRoleGrant, 0, len(bindings))
	for _, binding := range bindings {
		if binding.State == principalservice.GrantStateActive {
			roles = append(roles, mapPrincipalRoleGrant(binding))
		}
	}
	grants, err := s.manager.ListCapabilityGrants(ctx, principalID)
	if err != nil {
		return nil, nil, nil, mapPrincipalServiceError(err, "list principal capabilities")
	}
	caps := make([]*adminv1.PrincipalCapabilityGrant, 0, len(grants))
	for _, grant := range grants {
		if grant.State == principalservice.GrantStateActive {
			caps = append(caps, mapPrincipalCapabilityGrant(grant))
		}
	}
	effective, err := s.effectiveCapabilities(ctx, principalID)
	if err != nil {
		return nil, nil, nil, err
	}
	return roles, caps, effective, nil
}

func (s *PrincipalService) effectiveCapabilities(ctx context.Context, principalID string) ([]commonv1.Capability, error) {
	access, err := s.manager.EffectiveAccess(ctx, principalID, principalservice.AccessScope{Type: "system"})
	if err != nil {
		return nil, mapPrincipalServiceError(err, "get effective principal capabilities")
	}
	seen := map[commonv1.Capability]bool{}
	out := make([]commonv1.Capability, 0, len(access.Capabilities))
	for _, capability := range access.Capabilities {
		mapped := capabilityFromInternal(capability)
		if mapped != commonv1.Capability_CAPABILITY_UNSPECIFIED && !seen[mapped] {
			seen[mapped] = true
			out = append(out, mapped)
		}
	}
	return out, nil
}

func (s *PrincipalService) requireCapability(ctx context.Context, capability commonv1.Capability) (daemonauth.Principal, error) {
	if s.manager == nil {
		return daemonauth.Principal{}, status.Error(codes.FailedPrecondition, "principal service is not configured")
	}
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.manager.HasCapability(ctx, principal.PrincipalID, capability.String())
	if err != nil {
		return daemonauth.Principal{}, mapPrincipalServiceError(err, "authorize principal operation")
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "principal management capability is required")
	}
	return principal, nil
}

func mapPrincipalSummary(principal principalservice.PrincipalSummary) *adminv1.Principal {
	return &adminv1.Principal{PrincipalId: principal.ID, Username: principal.Username, DisplayName: principal.DisplayName, Email: principal.Email, Type: principalTypeToProto(principal.Kind), State: principalStateToProto(principal.State), LoginEnabled: principal.LoginEnabled, CreateTime: timestamppb.New(principal.CreatedAt), UpdateTime: timestamppb.New(principal.UpdatedAt)}
}

func principalTypeToProto(kind string) commonv1.PrincipalType {
	switch kind {
	case principalservice.PrincipalKindSystem:
		return commonv1.PrincipalType_PRINCIPAL_TYPE_SYSTEM
	case principalservice.PrincipalKindService:
		return commonv1.PrincipalType_PRINCIPAL_TYPE_SERVICE
	default:
		return commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN
	}
}

func principalKindFromProto(kind commonv1.PrincipalType) string {
	switch kind {
	case commonv1.PrincipalType_PRINCIPAL_TYPE_SYSTEM:
		return principalservice.PrincipalKindSystem
	case commonv1.PrincipalType_PRINCIPAL_TYPE_SERVICE:
		return principalservice.PrincipalKindService
	default:
		return principalservice.PrincipalKindHuman
	}
}

func principalStateToProto(state string) adminv1.PrincipalState {
	switch state {
	case principalservice.PrincipalStateDisabled:
		return adminv1.PrincipalState_PRINCIPAL_STATE_DISABLED
	case principalservice.PrincipalStateDeleted:
		return adminv1.PrincipalState_PRINCIPAL_STATE_DELETED
	default:
		return adminv1.PrincipalState_PRINCIPAL_STATE_ACTIVE
	}
}

func principalScopeFromProto(scope *commonv1.AccessScope) principalservice.AccessScope {
	if scope == nil {
		return principalservice.AccessScope{Type: "system"}
	}
	typ := "system"
	switch scope.GetType() {
	case commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SPACE:
		typ = "space"
	case commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_DOMAIN:
		typ = "domain"
	}
	return principalservice.AccessScope{Type: typ, SpaceID: scope.GetSpaceId(), DomainID: scope.GetDomainId()}
}

func protoScopeFromPrincipal(scope principalservice.AccessScope) *commonv1.AccessScope {
	typ := commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SYSTEM
	switch scope.Type {
	case "space":
		typ = commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SPACE
	case "domain":
		typ = commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_DOMAIN
	}
	return &commonv1.AccessScope{Type: typ, SpaceId: optionalString(scope.SpaceID), DomainId: optionalString(scope.DomainID)}
}

func mapPrincipalRoleGrant(grant principalservice.RoleBinding) *adminv1.PrincipalRoleGrant {
	return &adminv1.PrincipalRoleGrant{RoleGrantId: grant.ID, PrincipalId: grant.PrincipalID, Role: grant.Role, Scope: protoScopeFromPrincipal(grant.Scope), Reason: grant.Reason, GrantedByPrincipalId: grant.CreatedBy, CreateTime: timestamppb.New(grant.CreatedAt)}
}

func mapPrincipalCapabilityGrant(grant principalservice.CapabilityGrant) *adminv1.PrincipalCapabilityGrant {
	return &adminv1.PrincipalCapabilityGrant{CapabilityGrantId: grant.ID, PrincipalId: grant.PrincipalID, Capability: capabilityFromInternal(grant.Capability), Scope: protoScopeFromPrincipal(grant.Scope), Reason: grant.Reason, GrantedByPrincipalId: grant.CreatedBy, CreateTime: timestamppb.New(grant.CreatedAt)}
}

func daemonPrincipal(principal principalservice.PrincipalSummary, sessionID string) daemonauth.Principal {
	kind := daemonauth.PrincipalKindHuman
	switch principal.Kind {
	case principalservice.PrincipalKindService:
		kind = daemonauth.PrincipalKindService
	case principalservice.PrincipalKindSystem:
		kind = daemonauth.PrincipalKindSystem
	}
	return daemonauth.Principal{Kind: kind, PrincipalID: principal.ID, AuthSessionID: sessionID, Username: principal.Username, CreatedAt: principal.CreatedAt}
}

func mapPrincipalServiceError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, principalservice.ErrPrincipalNotFound) || errors.Is(err, storesession.ErrSessionNotFound) {
		return status.Error(codes.NotFound, "principal or session not found")
	}
	if errors.Is(err, principalservice.ErrDuplicatePrincipal) {
		return status.Error(codes.AlreadyExists, "principal already exists")
	}
	if errors.Is(err, principalservice.ErrLastSystemAdmin) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, principalservice.ErrInvalidInput) || errors.Is(err, storesession.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
