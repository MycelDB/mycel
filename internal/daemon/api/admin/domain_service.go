package admin

import (
	"context"
	"errors"
	"strconv"
	"strings"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/gen/go/mycel/common/v1"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminDomainService struct {
	adminv1.UnimplementedAdminDomainServiceServer
	spaces     daemonspace.Manager
	authorizer OperatorAuthorizer
}

func NewAdminDomainService(spaces daemonspace.Manager, authorizer OperatorAuthorizer) *AdminDomainService {
	return &AdminDomainService{spaces: spaces, authorizer: authorizer}
}

func (s *AdminDomainService) ListDomains(ctx context.Context, req *adminv1.AdminDomainServiceListDomainsRequest) (*adminv1.AdminDomainServiceListDomainsResponse, error) {
	if err := s.requireDomainRead(ctx); err != nil {
		return nil, err
	}
	offset, err := parseAdminDomainPageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizeAdminDomainPageSize(req.GetPageSize())
	domains, err := s.spaces.ListDomains(ctx, req.GetSpaceId(), req.GetIncludeSystem())
	if err != nil {
		return nil, mapAdminDomainError(err, "list domains")
	}
	if offset > len(domains) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the domain list")
	}
	end := offset + pageSize
	if end > len(domains) {
		end = len(domains)
	}
	// Use the client mapper shape but omit effective access for admin lookup helpers.
	response := &adminv1.AdminDomainServiceListDomainsResponse{}
	for _, domain := range domains[offset:end] {
		response.Domains = append(response.Domains, clientapi.MapDomain(domain, daemonspace.EffectiveAccess{}))
	}
	if end < len(domains) {
		response.NextPageToken = strconv.Itoa(end)
	}
	return response, nil
}

func (s *AdminDomainService) GetDomain(ctx context.Context, req *adminv1.AdminDomainServiceGetDomainRequest) (*adminv1.AdminDomainServiceGetDomainResponse, error) {
	if err := s.requireDomainRead(ctx); err != nil {
		return nil, err
	}
	domain, err := s.spaces.GetDomainByRef(ctx, req.GetSpaceId(), req.GetDomainRef())
	if err != nil {
		return nil, mapAdminDomainError(err, "get domain")
	}
	return &adminv1.AdminDomainServiceGetDomainResponse{Domain: clientapi.MapDomain(domain, daemonspace.EffectiveAccess{})}, nil
}

func (s *AdminDomainService) requireDomainRead(ctx context.Context) error {
	principal, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	ok, err := s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_DOMAIN_READ.String())
	if err != nil {
		return status.Errorf(codes.Internal, "authorize operator: %v", err)
	}
	if !ok {
		// Semantic admins need domain lookup for semantic/inference provisioning.
		ok, err = s.authorizer.HasCapability(ctx, principal.OperatorID, commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH.String())
		if err != nil {
			return status.Errorf(codes.Internal, "authorize operator: %v", err)
		}
	}
	if !ok {
		return status.Error(codes.PermissionDenied, "operator lacks required domain lookup capability")
	}
	return nil
}

func parseAdminDomainPageToken(token string) (int, error) {
	if strings.TrimSpace(token) == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, errors.New("invalid page_token")
	}
	return offset, nil
}

func normalizeAdminDomainPageSize(size int32) int {
	if size <= 0 || size > 500 {
		return 500
	}
	return int(size)
}

func mapAdminDomainError(err error, action string) error {
	if errors.Is(err, daemonspace.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, daemonspace.ErrSpaceNotFound) {
		return status.Error(codes.NotFound, "space or domain not found")
	}
	if errors.Is(err, daemonspace.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, "domain access denied")
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
