package admin

import (
	"context"
	"strconv"

	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminTemplateService struct {
	adminv1.UnimplementedAdminTemplateServiceServer
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

func NewAdminTemplateService(spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminTemplateService {
	return &AdminTemplateService{spaces: spaces, authorizer: authorizer}
}

func (s *AdminTemplateService) ListTemplates(ctx context.Context, req *adminv1.ListTemplatesRequest) (*adminv1.ListTemplatesResponse, error) {
	if _, err := s.requireTemplateRead(ctx); err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	templates, err := s.spaces.ListTemplates(ctx, req.GetSpaceId(), req.GetIncludeSystem(), req.GetIncludeArchived())
	if err != nil {
		return nil, mapSpaceError(err, "list templates")
	}
	if offset > len(templates) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the template list")
	}
	end := offset + pageSize
	if end > len(templates) {
		end = len(templates)
	}
	out := make([]*clientv1.Template, 0, end-offset)
	for _, template := range templates[offset:end] {
		out = append(out, clientapi.MapTemplate(template))
	}
	return &adminv1.ListTemplatesResponse{Templates: out, NextPageToken: templateNextPage(end, len(templates))}, nil
}

func (s *AdminTemplateService) GetTemplate(ctx context.Context, req *adminv1.GetTemplateRequest) (*adminv1.GetTemplateResponse, error) {
	if _, err := s.requireTemplateRead(ctx); err != nil {
		return nil, err
	}
	template, err := s.spaces.GetTemplate(ctx, req.GetSpaceId(), req.GetTemplateId())
	if err != nil {
		return nil, mapSpaceError(err, "get template")
	}
	return &adminv1.GetTemplateResponse{Template: clientapi.MapTemplate(template)}, nil
}

func (s *AdminTemplateService) requireTemplateRead(ctx context.Context) (any, error) {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SPACE_MANAGE_ACCESS.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "operator lacks required template capability")
	}
	return principal, nil
}

func templateNextPage(end, total int) string {
	if end < total {
		return strconv.Itoa(end)
	}
	return ""
}
