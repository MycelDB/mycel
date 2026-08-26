package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	activitymodel "github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AdminSpaceService struct {
	adminv1.UnimplementedAdminSpaceServiceServer
	spaces     daemonspace.Manager
	principals principalservice.Manager
	authorizer OperatorAuthorizer
	activity   activityservice.Manager
}

func NewAdminSpaceService(spaces daemonspace.Manager, principals principalservice.Manager, authorizer OperatorAuthorizer, activity ...activityservice.Manager) *AdminSpaceService {
	svc := &AdminSpaceService{spaces: spaces, principals: principals, authorizer: authorizer}
	if len(activity) > 0 {
		svc.activity = activity[0]
	}
	return svc
}

func (s *AdminSpaceService) ListSpaces(ctx context.Context, req *adminv1.AdminSpaceServiceListSpacesRequest) (*adminv1.AdminSpaceServiceListSpacesResponse, error) {
	if _, err := s.requireSpaceManage(ctx); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	spaces, err := s.spaces.ListSpaces(ctx, req.GetIncludeArchived())
	if err != nil {
		return nil, mapSpaceError(err, "list spaces")
	}
	if offset > len(spaces) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the space list")
	}
	end := offset + pageSize
	if end > len(spaces) {
		end = len(spaces)
	}
	out := make([]*clientv1.Space, 0, end-offset)
	for _, sp := range spaces[offset:end] {
		out = append(out, clientapi.MapSpace(sp, daemonspace.EffectiveAccess{Roles: []string{"admin"}, Capabilities: []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_DELETE"}}))
	}
	var next string
	if end < len(spaces) {
		next = fmt.Sprintf("%d", end)
	}
	return &adminv1.AdminSpaceServiceListSpacesResponse{Spaces: out, NextPageToken: next}, nil
}

func (s *AdminSpaceService) GetSpace(ctx context.Context, req *adminv1.AdminSpaceServiceGetSpaceRequest) (*adminv1.AdminSpaceServiceGetSpaceResponse, error) {
	if _, err := s.requireSpaceManage(ctx); err != nil {
		return nil, err
	}
	sp, err := s.spaces.GetSpace(ctx, req.GetSpaceId())
	if err != nil {
		return nil, mapSpaceError(err, "get space")
	}
	return &adminv1.AdminSpaceServiceGetSpaceResponse{Space: clientapi.MapSpace(sp, daemonspace.EffectiveAccess{Roles: []string{"admin"}, Capabilities: []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_DELETE"}})}, nil
}

func (s *AdminSpaceService) CreateSpace(ctx context.Context, req *adminv1.CreateSpaceRequest) (*adminv1.CreateSpaceResponse, error) {
	if _, err := s.requireSpaceCreate(ctx); err != nil {
		return nil, err
	}
	ownerID, err := s.resolveOwnerID(ctx, req.GetOwnerPrincipalId(), req.GetOwnerUsername())
	if err != nil {
		return nil, err
	}
	sp, domain, err := s.spaces.CreateSpace(ctx, daemonspace.CreateSpaceInput{Name: req.GetName(), OwnerPrincipalID: ownerID, DefaultDomainKey: req.GetDefaultDomainKey(), DefaultDomainName: req.GetDefaultDomainName(), CommandID: idempotencyKeyFromContext(ctx)})
	if err != nil {
		return nil, mapSpaceError(err, "create space")
	}
	mapped := clientapi.MapSpace(sp, daemonspace.EffectiveAccess{Roles: []string{"owner"}, Capabilities: []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_DELETE"}})
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategorySpace, "space.created", "Space created", func(event *activitymodel.Event) {
		event.Resource = activitymodel.Resource{Kind: "space", ID: sp.SpaceID.String(), Name: sp.Name}
		if principal, err := principalFromContext(ctx); err == nil {
			event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		}
	})
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategoryDomain, "domain.created", "Default domain created", func(event *activitymodel.Event) {
		event.Resource = activitymodel.Resource{Kind: "domain", ID: domain.ID.String(), Name: domain.Name}
		if principal, err := principalFromContext(ctx); err == nil {
			event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		}
	})
	return &adminv1.CreateSpaceResponse{Space: mapped, DefaultDomainId: domain.ID.String()}, nil
}

func (s *AdminSpaceService) DeleteSpace(ctx context.Context, req *adminv1.DeleteSpaceRequest) (*adminv1.DeleteSpaceResponse, error) {
	if _, err := s.requireSpaceDelete(ctx); err != nil {
		return nil, err
	}
	if err := s.spaces.DeleteSpace(ctx, req.GetSpaceId()); err != nil {
		return nil, mapSpaceError(err, "delete space")
	}
	s.emit(ctx, activitymodel.SeverityInfo, activitymodel.CategorySpace, "space.deleted", "Space deleted", func(event *activitymodel.Event) {
		event.Resource = activitymodel.Resource{Kind: "space", ID: req.GetSpaceId()}
		if principal, err := principalFromContext(ctx); err == nil {
			event.Actor = activitymodel.Actor{PrincipalID: principal.PrincipalID, Username: principal.Username}
		}
	})
	return &adminv1.DeleteSpaceResponse{}, nil
}

func (s *AdminSpaceService) GrantSpacePrincipal(ctx context.Context, req *adminv1.GrantSpacePrincipalRequest) (*adminv1.GrantSpacePrincipalResponse, error) {
	if _, err := s.requireSpaceManage(ctx); err != nil {
		return nil, err
	}
	principalID, err := s.resolveOwnerID(ctx, req.GetPrincipalId(), req.GetUsername())
	if err != nil {
		return nil, err
	}
	role, err := adminSpaceRoleToInternal(req.GetRole())
	if err != nil {
		return nil, err
	}
	grant, err := s.spaces.GrantSpacePrincipal(ctx, req.GetSpaceId(), string(principalID), role)
	if err != nil {
		return nil, mapSpaceError(err, "grant space principal")
	}
	return &adminv1.GrantSpacePrincipalResponse{Grant: mapAdminSpaceGrant(grant)}, nil
}

func (s *AdminSpaceService) emit(ctx context.Context, severity, category, eventType, message string, mutate func(*activitymodel.Event)) {
	if s.activity == nil {
		return
	}
	_ = s.activity.Emit(ctx, severity, category, eventType, message, mutate)
}

func idempotencyKeyFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, key := range []string{"idempotency-key", "x-idempotency-key"} {
		values := md.Get(key)
		if len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func (s *AdminSpaceService) resolveOwnerID(ctx context.Context, ownerPrincipalID string, ownerUsername string) (identity.PrincipalID, error) {
	ownerPrincipalID = strings.TrimSpace(ownerPrincipalID)
	ownerUsername = strings.TrimSpace(ownerUsername)
	if ownerPrincipalID == "" && ownerUsername == "" {
		return "", status.Error(codes.InvalidArgument, "owner_principal_id or owner_username is required")
	}
	var id identity.PrincipalID
	if ownerPrincipalID != "" {
		principal, err := s.principals.GetPrincipal(ctx, ownerPrincipalID)
		if err != nil {
			return "", mapPrincipalServiceError(err, "get owner principal")
		}
		id = identity.PrincipalID(ownerPrincipalID)
		if ownerUsername != "" && principal.Username != ownerUsername {
			return "", status.Error(codes.InvalidArgument, "owner_principal_id and owner_username refer to different principals")
		}
	}
	if ownerUsername != "" && id == "" {
		principal, err := s.principals.FindPrincipal(ctx, ownerUsername, "")
		if err != nil {
			return "", mapPrincipalServiceError(err, "find owner principal")
		}
		id = identity.PrincipalID(principal.ID)
	}
	return id, nil
}

func adminSpaceRoleToInternal(role commonv1.SpaceRole) (string, error) {
	switch role {
	case commonv1.SpaceRole_SPACE_ROLE_ADMIN:
		return "admin", nil
	case commonv1.SpaceRole_SPACE_ROLE_WRITER:
		return "writer", nil
	case commonv1.SpaceRole_SPACE_ROLE_READER:
		return "reader", nil
	default:
		return "", status.Error(codes.InvalidArgument, "space role must be admin, writer, or reader")
	}
}

func mapAdminSpaceGrant(grant daemonspace.SpaceGrant) *commonv1.AccessGrant {
	role := commonv1.SpaceRole_SPACE_ROLE_UNSPECIFIED
	switch grant.Role {
	case "admin":
		role = commonv1.SpaceRole_SPACE_ROLE_ADMIN
	case "writer":
		role = commonv1.SpaceRole_SPACE_ROLE_WRITER
	case "reader":
		role = commonv1.SpaceRole_SPACE_ROLE_READER
	}
	caps := make([]commonv1.Capability, 0, len(grant.Capabilities))
	for _, cap := range grant.Capabilities {
		mapped := capabilityFromInternal(cap)
		if mapped != commonv1.Capability_CAPABILITY_UNSPECIFIED {
			caps = append(caps, mapped)
		}
	}
	return &commonv1.AccessGrant{AccessGrantId: grant.ID, Principal: &commonv1.Principal{Type: commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN, Id: grant.PrincipalID}, Scope: &commonv1.AccessScope{Type: commonv1.AccessScopeType_ACCESS_SCOPE_TYPE_SPACE, SpaceId: &grant.SpaceID}, Roles: []commonv1.SpaceRole{role}, Capabilities: caps}
}

func (s *AdminSpaceService) requireSpaceCreate(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireCapability(ctx, commonv1.Capability_CAPABILITY_SPACE_CREATE)
}
func (s *AdminSpaceService) requireSpaceDelete(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireCapability(ctx, commonv1.Capability_CAPABILITY_SPACE_DELETE)
}
func (s *AdminSpaceService) requireSpaceManage(ctx context.Context) (daemonauth.Principal, error) {
	return s.requireCapability(ctx, commonv1.Capability_CAPABILITY_SPACE_MANAGE_ACCESS)
}
func (s *AdminSpaceService) requireCapability(ctx context.Context, capability commonv1.Capability) (daemonauth.Principal, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return daemonauth.Principal{}, err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.PrincipalID, capability.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required space capability")
	}
	return principal, nil
}

func mapSpaceError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, daemonspace.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonspace.ErrSpaceNotFound) {
		return status.Error(codes.NotFound, "space not found")
	}
	if errors.Is(err, daemonspace.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "space access denied")
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
