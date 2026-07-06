package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel-api/gen/go/mycel/common/v1"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	"github.com/myceldb/mycel/internal/identity/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminSpaceService struct {
	adminv1.UnimplementedAdminSpaceServiceServer
	spaces     daemonspace.Manager
	users      daemonuser.Manager
	authorizer OperatorAuthorizer
}

func NewAdminSpaceService(spaces daemonspace.Manager, users daemonuser.Manager, authorizer OperatorAuthorizer) *AdminSpaceService {
	return &AdminSpaceService{spaces: spaces, users: users, authorizer: authorizer}
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
	ownerID, err := s.resolveOwnerID(ctx, req.GetOwnerUserId(), req.GetOwnerUsername())
	if err != nil {
		return nil, err
	}
	sp, domain, err := s.spaces.CreateSpace(ctx, daemonspace.CreateSpaceInput{Name: req.GetName(), OwnerUserID: ownerID, DefaultDomainKey: req.GetDefaultDomainKey(), DefaultDomainName: req.GetDefaultDomainName()})
	if err != nil {
		return nil, mapSpaceError(err, "create space")
	}
	mapped := clientapi.MapSpace(sp, daemonspace.EffectiveAccess{Roles: []string{"owner"}, Capabilities: []string{"CAPABILITY_SPACE_READ", "CAPABILITY_SPACE_MANAGE_ACCESS", "CAPABILITY_SPACE_DELETE"}})
	return &adminv1.CreateSpaceResponse{Space: mapped, DefaultDomainId: domain.ID.String()}, nil
}

func (s *AdminSpaceService) DeleteSpace(ctx context.Context, req *adminv1.DeleteSpaceRequest) (*adminv1.DeleteSpaceResponse, error) {
	if _, err := s.requireSpaceDelete(ctx); err != nil {
		return nil, err
	}
	if err := s.spaces.DeleteSpace(ctx, req.GetSpaceId()); err != nil {
		return nil, mapSpaceError(err, "delete space")
	}
	return &adminv1.DeleteSpaceResponse{}, nil
}

func (s *AdminSpaceService) resolveOwnerID(ctx context.Context, ownerUserID string, ownerUsername string) (identity.UserID, error) {
	ownerUserID = strings.TrimSpace(ownerUserID)
	ownerUsername = strings.TrimSpace(ownerUsername)
	if ownerUserID == "" && ownerUsername == "" {
		return uuid.Nil, status.Error(codes.InvalidArgument, "owner_user_id or owner_username is required")
	}
	var id identity.UserID
	if ownerUserID != "" {
		parsed, err := uuid.Parse(ownerUserID)
		if err != nil || parsed == uuid.Nil {
			return uuid.Nil, status.Error(codes.InvalidArgument, "owner_user_id must be a UUID")
		}
		user, err := s.users.GetUser(ctx, parsed.String())
		if err != nil {
			return uuid.Nil, mapUserError(err, "get owner user")
		}
		id = parsed
		if ownerUsername != "" && user.Username != ownerUsername {
			return uuid.Nil, status.Error(codes.InvalidArgument, "owner_user_id and owner_username refer to different users")
		}
	}
	if ownerUsername != "" && id == uuid.Nil {
		user, err := s.users.FindUser(ctx, ownerUsername)
		if err != nil {
			return uuid.Nil, mapUserError(err, "find owner user")
		}
		parsed, err := uuid.Parse(user.ID)
		if err != nil || parsed == uuid.Nil {
			return uuid.Nil, status.Error(codes.Internal, "owner user has invalid id")
		}
		id = parsed
	}
	return id, nil
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
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, capability.String())
	if err != nil {
		return daemonauth.Principal{}, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return daemonauth.Principal{}, status.Error(codes.PermissionDenied, "operator lacks required space capability")
	}
	return principal, nil
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
