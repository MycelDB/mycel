package client

import (
	"context"
	"errors"
	"strconv"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DomainService struct {
	clientv1.UnimplementedDomainServiceServer
	spaces daemonspace.Manager
}

func NewDomainService(spaces daemonspace.Manager) *DomainService {
	return &DomainService{spaces: spaces}
}

func (s *DomainService) ListDomains(ctx context.Context, req *clientv1.ListDomainsRequest) (*clientv1.ListDomainsResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	domains, err := s.spaces.ListVisibleDomains(ctx, principal.UserID, req.GetSpaceId(), req.GetIncludeSystem())
	if err != nil {
		return nil, mapDomainError(err, "list domains")
	}
	if offset > len(domains) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the domain list")
	}
	end := offset + pageSize
	if end > len(domains) {
		end = len(domains)
	}
	access, err := s.spaces.DomainEffectiveAccess(ctx, principal.UserID, req.GetSpaceId())
	if err != nil {
		return nil, mapDomainError(err, "resolve effective access")
	}
	out := make([]*clientv1.Domain, 0, end-offset)
	for _, domain := range domains[offset:end] {
		out = append(out, MapDomain(domain, access))
	}
	var next string
	if end < len(domains) {
		next = strconv.Itoa(end)
	}
	return &clientv1.ListDomainsResponse{Domains: out, NextPageToken: next}, nil
}

func (s *DomainService) GetDomain(ctx context.Context, req *clientv1.GetDomainRequest) (*clientv1.GetDomainResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.UserID, req.GetSpaceId(), req.GetDomainId(), req.GetKey())
	if err != nil {
		return nil, mapDomainError(err, "get domain")
	}
	access, err := s.spaces.DomainEffectiveAccess(ctx, principal.UserID, req.GetSpaceId())
	if err != nil {
		return nil, mapDomainError(err, "resolve effective access")
	}
	return &clientv1.GetDomainResponse{Domain: MapDomain(domain, access)}, nil
}

func (s *DomainService) CreateDomain(ctx context.Context, req *clientv1.CreateDomainRequest) (*clientv1.CreateDomainResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	domain, err := s.spaces.CreateDomain(ctx, principal.UserID, daemonspace.CreateDomainInput{SpaceID: req.GetSpaceId(), Key: req.GetKey(), Name: req.GetName(), Description: req.GetDescription(), DiscoveryMode: discoveryModeFromProto(req.GetDiscoveryMode()), SearchMode: searchModeFromProto(req.GetSearchMode()), SemanticMode: semanticModeFromProto(req.GetSemanticMode()), ReadOnly: req.GetReadOnly()})
	if err != nil {
		return nil, mapDomainError(err, "create domain")
	}
	access, err := s.spaces.DomainEffectiveAccess(ctx, principal.UserID, req.GetSpaceId())
	if err != nil {
		return nil, mapDomainError(err, "resolve effective access")
	}
	return &clientv1.CreateDomainResponse{Domain: MapDomain(domain, access)}, nil
}

func (s *DomainService) UpdateDomain(ctx context.Context, req *clientv1.UpdateDomainRequest) (*clientv1.UpdateDomainResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var name *string
	var description *string
	var discoveryMode *graph.DomainDiscoveryMode
	var searchMode *graph.DomainSearchMode
	var semanticMode *graph.DomainSemanticMode
	var readOnly *bool
	for _, path := range req.GetUpdateMask().GetPaths() {
		switch strings.TrimSpace(path) {
		case "name":
			v := req.GetDomain().GetName()
			name = &v
		case "description":
			v := req.GetDomain().GetDescription()
			description = &v
		case "discovery_mode":
			v := discoveryModeFromProto(req.GetDomain().GetDiscoveryMode())
			discoveryMode = &v
		case "search_mode":
			v := searchModeFromProto(req.GetDomain().GetSearchMode())
			searchMode = &v
		case "semantic_mode":
			v := semanticModeFromProto(req.GetDomain().GetSemanticMode())
			semanticMode = &v
		case "read_only":
			v := req.GetDomain().GetReadOnly()
			readOnly = &v
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update_mask path %q", path)
		}
	}
	if req.GetUpdateMask() == nil || len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_mask is required")
	}
	domain, err := s.spaces.UpdateDomain(ctx, principal.UserID, daemonspace.UpdateDomainInput{SpaceID: req.GetSpaceId(), DomainID: req.GetDomainId(), Name: name, Description: description, DiscoveryMode: discoveryMode, SearchMode: searchMode, SemanticMode: semanticMode, ReadOnly: readOnly})
	if err != nil {
		return nil, mapDomainError(err, "update domain")
	}
	access, err := s.spaces.DomainEffectiveAccess(ctx, principal.UserID, req.GetSpaceId())
	if err != nil {
		return nil, mapDomainError(err, "resolve effective access")
	}
	return &clientv1.UpdateDomainResponse{Domain: MapDomain(domain, access)}, nil
}

func (s *DomainService) DeleteDomain(ctx context.Context, req *clientv1.DeleteDomainRequest) (*clientv1.DeleteDomainResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.spaces.DeleteDomain(ctx, principal.UserID, req.GetSpaceId(), req.GetDomainId()); err != nil {
		return nil, mapDomainError(err, "delete domain")
	}
	return &clientv1.DeleteDomainResponse{}, nil
}

func MapDomain(domain graph.Domain, access daemonspace.EffectiveAccess) *clientv1.Domain {
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
	return &clientv1.Domain{SpaceId: domain.SpaceID.String(), DomainId: domain.ID.String(), Name: domain.Name, Description: domain.Description, State: clientv1.DomainState_DOMAIN_STATE_ACTIVE, Default: domain.Default, System: false, CreateTime: timestampOrNil(domain.CreatedAt), UpdateTime: timestampOrNil(domain.UpdatedAt), CallerAccess: &commonv1.EffectiveAccess{Roles: roles, Capabilities: capabilities}, Key: domain.Key, DiscoveryMode: discoveryModeToProto(domain.DiscoveryMode), SearchMode: searchModeToProto(domain.SearchMode), SemanticMode: semanticModeToProto(domain.SemanticMode), ReadOnly: domain.ReadOnly}
}

func discoveryModeFromProto(mode clientv1.DomainDiscoveryMode) graph.DomainDiscoveryMode {
	switch mode {
	case clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_EXPLICIT_ONLY:
		return graph.DomainDiscoveryModeExplicitOnly
	case clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_HIDDEN:
		return graph.DomainDiscoveryModeHidden
	case clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_NORMAL, clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_UNSPECIFIED:
		return graph.DomainDiscoveryModeNormal
	default:
		return graph.DomainDiscoveryMode(mode.String())
	}
}

func discoveryModeToProto(mode graph.DomainDiscoveryMode) clientv1.DomainDiscoveryMode {
	switch graph.NormalizeDomainDiscoveryMode(mode) {
	case graph.DomainDiscoveryModeExplicitOnly:
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_EXPLICIT_ONLY
	case graph.DomainDiscoveryModeHidden:
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_HIDDEN
	default:
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_NORMAL
	}
}

func searchModeFromProto(mode clientv1.DomainSearchMode) graph.DomainSearchMode {
	switch mode {
	case clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_EXPLICIT_ONLY:
		return graph.DomainSearchModeExplicitOnly
	case clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_DISABLED:
		return graph.DomainSearchModeDisabled
	case clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_NORMAL, clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_UNSPECIFIED:
		return graph.DomainSearchModeNormal
	default:
		return graph.DomainSearchMode(mode.String())
	}
}

func searchModeToProto(mode graph.DomainSearchMode) clientv1.DomainSearchMode {
	switch graph.NormalizeDomainSearchMode(mode) {
	case graph.DomainSearchModeExplicitOnly:
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_EXPLICIT_ONLY
	case graph.DomainSearchModeDisabled:
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_DISABLED
	default:
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_NORMAL
	}
}

func semanticModeFromProto(mode clientv1.DomainSemanticMode) graph.DomainSemanticMode {
	switch mode {
	case clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_EXPLICIT_ONLY:
		return graph.DomainSemanticModeExplicitOnly
	case clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_DISABLED:
		return graph.DomainSemanticModeDisabled
	case clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_NORMAL, clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_UNSPECIFIED:
		return graph.DomainSemanticModeNormal
	default:
		return graph.DomainSemanticMode(mode.String())
	}
}

func semanticModeToProto(mode graph.DomainSemanticMode) clientv1.DomainSemanticMode {
	switch graph.NormalizeDomainSemanticMode(mode) {
	case graph.DomainSemanticModeExplicitOnly:
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_EXPLICIT_ONLY
	case graph.DomainSemanticModeDisabled:
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_DISABLED
	default:
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_NORMAL
	}
}

func mapDomainError(err error, action string) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
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
