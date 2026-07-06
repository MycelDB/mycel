package client

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	domainspace "github.com/myceldb/mycel/domain/space"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SpaceService struct {
	clientv1.UnimplementedSpaceServiceServer
	spaces daemonspace.Manager
}

func NewSpaceService(spaces daemonspace.Manager) *SpaceService { return &SpaceService{spaces: spaces} }

func (s *SpaceService) ListSpaces(ctx context.Context, req *clientv1.ListSpacesRequest) (*clientv1.ListSpacesResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	spaces, err := s.spaces.ListVisibleSpaces(ctx, principal.UserID, req.GetIncludeArchived())
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
		access, err := s.spaces.EffectiveAccess(ctx, principal.UserID, sp)
		if err != nil {
			return nil, mapSpaceError(err, "resolve effective access")
		}
		out = append(out, MapSpace(sp, access))
	}
	var next string
	if end < len(spaces) {
		next = strconv.Itoa(end)
	}
	return &clientv1.ListSpacesResponse{Spaces: out, NextPageToken: next}, nil
}

func (s *SpaceService) GetSpace(ctx context.Context, req *clientv1.GetSpaceRequest) (*clientv1.GetSpaceResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sp, err := s.spaces.GetVisibleSpace(ctx, principal.UserID, req.GetSpaceId())
	if err != nil {
		return nil, mapSpaceError(err, "get space")
	}
	access, err := s.spaces.EffectiveAccess(ctx, principal.UserID, sp)
	if err != nil {
		return nil, mapSpaceError(err, "resolve effective access")
	}
	return &clientv1.GetSpaceResponse{Space: MapSpace(sp, access)}, nil
}

func spaceUserPrincipalFromContext(ctx context.Context) (daemonauth.Principal, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok || principal.Kind != daemonauth.PrincipalKindUser || principal.UserID == "" {
		return daemonauth.Principal{}, status.Error(codes.Unauthenticated, "user authentication is required")
	}
	return principal, nil
}

func MapSpace(sp domainspace.Space, access daemonspace.EffectiveAccess) *clientv1.Space {
	roles := make([]commonv1.SpaceRole, 0, len(access.Roles))
	for _, role := range access.Roles {
		mapped := roleFromString(role)
		if mapped != commonv1.SpaceRole_SPACE_ROLE_UNSPECIFIED {
			roles = append(roles, mapped)
		}
	}
	capabilities := make([]commonv1.Capability, 0, len(access.Capabilities))
	for _, capability := range access.Capabilities {
		mapped := capabilityFromString(capability)
		if mapped != commonv1.Capability_CAPABILITY_UNSPECIFIED {
			capabilities = append(capabilities, mapped)
		}
	}
	state := clientv1.SpaceState_SPACE_STATE_ACTIVE
	if strings.EqualFold(sp.Status, "archived") {
		state = clientv1.SpaceState_SPACE_STATE_ARCHIVED
	}
	return &clientv1.Space{SpaceId: sp.SpaceID.String(), Name: sp.Name, Owner: &commonv1.Principal{Type: commonv1.PrincipalType_PRINCIPAL_TYPE_USER, Id: sp.OwnerID.String()}, State: state, CreateTime: timestampOrNil(sp.CreatedAt), UpdateTime: timestampOrNil(sp.UpdatedAt), CallerAccess: &commonv1.EffectiveAccess{Roles: roles, Capabilities: capabilities}, TemplateUsage: clientv1.SpaceTemplateUsage_SPACE_TEMPLATE_USAGE_OPTIONAL}
}

func mapSpaceError(err error, action string) error {
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

func roleFromString(role string) commonv1.SpaceRole {
	switch strings.TrimSpace(role) {
	case "owner":
		return commonv1.SpaceRole_SPACE_ROLE_OWNER
	case "admin":
		return commonv1.SpaceRole_SPACE_ROLE_ADMIN
	case "writer":
		return commonv1.SpaceRole_SPACE_ROLE_WRITER
	case "reader":
		return commonv1.SpaceRole_SPACE_ROLE_READER
	default:
		return commonv1.SpaceRole_SPACE_ROLE_UNSPECIFIED
	}
}

func capabilityFromString(capability string) commonv1.Capability {
	if value, ok := commonv1.Capability_value[strings.TrimSpace(capability)]; ok {
		return commonv1.Capability(value)
	}
	return commonv1.Capability_CAPABILITY_UNSPECIFIED
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
