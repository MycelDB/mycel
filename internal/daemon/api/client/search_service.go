package client

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalparser "github.com/myceldb/mycel/internal/search/lexical/parser"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const lexicalMaxPageSize = 500

type SearchService struct {
	clientv1.UnimplementedSearchServiceServer
	lexical          lexicalservice.Manager
	spaces           daemonspace.Manager
	graphs           daegraph.Manager
	graphWriteRouter GraphWriteRouteProvider
	router           ClientRequestRouter
}

func NewSearchService(lexical lexicalservice.Manager, spaces daemonspace.Manager, graphs daegraph.Manager) *SearchService {
	return &SearchService{lexical: lexical, spaces: spaces, graphs: graphs}
}

func (s *SearchService) WithClientRequestRouter(router ClientRequestRouter) *SearchService {
	s.router = router
	return s
}

func (s *SearchService) WithGraphWriteRouteProvider(provider GraphWriteRouteProvider) *SearchService {
	s.graphWriteRouter = provider
	return s
}

func (s *SearchService) Search(ctx context.Context, req *clientv1.SearchRequest) (*clientv1.SearchResponse, error) {
	principal, spaceID, domainID, err := s.authorizeLexicalSearch(ctx, req.GetSpaceId(), req.GetDomainId())
	_ = principal
	if err != nil {
		return nil, err
	}
	if err := s.maybeForward(ctx, clientv1.SearchService_Search_FullMethodName, req.GetSpaceId(), req, &clientv1.SearchResponse{}); err != nil {
		if forwarded, ok := err.(forwardedSearchResponse); ok {
			return forwarded.res, nil
		}
		return nil, err
	}
	if s.lexical == nil {
		return nil, status.Error(codes.FailedPrecondition, "lexical search service is not configured")
	}
	if mode := req.GetMode(); mode != clientv1.SearchMode_SEARCH_MODE_UNSPECIFIED && mode != clientv1.SearchMode_SEARCH_MODE_LEXICAL {
		return nil, status.Error(codes.InvalidArgument, "only SEARCH_MODE_LEXICAL is supported")
	}
	query := strings.TrimSpace(req.GetQuery())
	if query == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > lexicalMaxPageSize {
		return nil, status.Errorf(codes.ResourceExhausted, "page_size must be <= %d", lexicalMaxPageSize)
	}
	latest := s.latestGraphRevision(ctx, spaceID)
	freshness := s.lexical.Status(ctx, spaceID.String(), domainID.String(), latest)
	if err := validateFreshnessPolicy(freshness, req.GetAllowStale(), req.GetMaxRevisionLag()); err != nil {
		return nil, err
	}
	result, err := s.lexical.Search(ctx, spaceID.String(), domainID.String(), query, lexicalindex.SearchOptions{PageSize: pageSize, PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapLexicalError(err, "lexical search")
	}
	freshness = s.lexical.Status(ctx, spaceID.String(), domainID.String(), latest)
	return mapSearchResponse(result, freshness, req.GetIncludeDiagnostics()), nil
}

func (s *SearchService) GetLexicalIndexStatus(ctx context.Context, req *clientv1.GetLexicalIndexStatusRequest) (*clientv1.GetLexicalIndexStatusResponse, error) {
	_, spaceID, domainID, err := s.authorizeLexicalSearch(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := s.maybeForward(ctx, clientv1.SearchService_GetLexicalIndexStatus_FullMethodName, req.GetSpaceId(), req, &clientv1.GetLexicalIndexStatusResponse{}); err != nil {
		if forwarded, ok := err.(forwardedStatusResponse); ok {
			return forwarded.res, nil
		}
		return nil, err
	}
	if s.lexical == nil {
		return nil, status.Error(codes.FailedPrecondition, "lexical search service is not configured")
	}
	latest := s.latestGraphRevision(ctx, spaceID)
	return &clientv1.GetLexicalIndexStatusResponse{Status: mapLexicalIndexStatus(s.lexical.Status(ctx, spaceID.String(), domainID.String(), latest))}, nil
}

func (s *SearchService) authorizeLexicalSearch(ctx context.Context, spaceIDText, domainIDText string) (principalUser, domainspace.SpaceID, graph.DomainID, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, err
	}
	spaceID, err := parseDomainSpaceID(spaceIDText)
	if err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, err
	}
	domainID, err := parseGraphDomainID(domainIDText)
	if err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, err
	}
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, spaceID.String(), domainID.String(), "")
	if err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, mapDomainError(err, "lexical authorize domain")
	}
	if !graph.DomainExplicitSearchable(domain) {
		return principalUser{}, uuid.Nil, uuid.Nil, status.Error(codes.FailedPrecondition, "domain is excluded from lexical search")
	}
	return principalUser{PrincipalID: principal.PrincipalID}, spaceID, domainID, nil
}

func (s *SearchService) maybeForward(ctx context.Context, method string, spaceID string, req proto.Message, res proto.Message) error {
	if s.router == nil || s.graphWriteRouter == nil {
		return nil
	}
	leader, local, err := s.graphWriteRouter.GraphWriteRoute(ctx, spaceID)
	if err != nil {
		return mapGraphError(err, "lexical search graph route")
	}
	if leader == 0 || local == 0 || leader == local {
		return nil
	}
	forwarded, err := s.router.ForwardUnaryToNode(ctx, method, leader, "", "", req, res)
	if forwarded || err != nil {
		if err != nil {
			return err
		}
		switch typed := res.(type) {
		case *clientv1.SearchResponse:
			return forwardedSearchResponse{res: typed}
		case *clientv1.GetLexicalIndexStatusResponse:
			return forwardedStatusResponse{res: typed}
		default:
			return status.Error(codes.Internal, "unexpected lexical forwarded response type")
		}
	}
	return nil
}

type forwardedSearchResponse struct{ res *clientv1.SearchResponse }

func (e forwardedSearchResponse) Error() string { return "forwarded lexical search" }

type forwardedStatusResponse struct {
	res *clientv1.GetLexicalIndexStatusResponse
}

func (e forwardedStatusResponse) Error() string { return "forwarded lexical status" }

func (s *SearchService) latestGraphRevision(ctx context.Context, spaceID domainspace.SpaceID) uint64 {
	if s.graphs == nil {
		return 0
	}
	rev, err := s.graphs.CurrentRevision(ctx, spaceID.String())
	if err != nil || rev < 0 {
		return 0
	}
	return uint64(rev)
}

func validateFreshnessPolicy(freshness lexicalservice.Status, allowStale bool, maxLag int64) error {
	if freshness.State == lexicalservice.StateFresh {
		return nil
	}
	if freshness.State == lexicalservice.StateUnavailable || freshness.State == lexicalservice.StateError || freshness.State == lexicalservice.StateRebuilding {
		return status.Error(codes.FailedPrecondition, "lexical index is not available")
	}
	if !allowStale {
		return status.Error(codes.FailedPrecondition, "lexical index is stale; retry with allow_stale to accept stale candidates")
	}
	if maxLag > 0 && freshness.RevisionLag > uint64(maxLag) {
		return status.Errorf(codes.FailedPrecondition, "lexical index revision lag %d exceeds max_revision_lag %d", freshness.RevisionLag, maxLag)
	}
	return nil
}

func mapSearchResponse(in lexicalindex.SearchResponse, freshness lexicalservice.Status, includeDiagnostics bool) *clientv1.SearchResponse {
	out := &clientv1.SearchResponse{NextPageToken: in.NextPageToken, Freshness: mapSearchFreshness(freshness)}
	for _, item := range in.Results {
		result := &clientv1.SearchResult{SpaceId: freshness.SpaceID, DomainId: freshness.DomainID, NodeId: item.NodeID, Score: item.Score, ScoreKind: clientv1.SearchScoreKind_SEARCH_SCORE_KIND_BM25, IndexedGraphRevision: int64(item.IndexedGraphRevision)}
		if includeDiagnostics {
			result.MatchedTerms = append([]string(nil), item.MatchedTerms...)
			result.MatchedFieldPaths = append([]string(nil), item.MatchedFieldPaths...)
			for _, component := range item.ScoreComponents {
				result.ScoreComponents = append(result.ScoreComponents, &clientv1.SearchScoreComponent{Name: component.Name, Value: component.Value, Description: component.Description})
			}
		}
		out.Results = append(out.Results, result)
	}
	if freshness.State == lexicalservice.StateStale {
		out.Warnings = append(out.Warnings, "lexical index is stale")
	}
	if includeDiagnostics {
		out.Diagnostics = &clientv1.SearchDiagnostics{AnalyzerVersion: freshness.AnalyzerVersion, IndexFormatVersion: freshness.IndexFormatVersion, QueryPlan: "LexicalBM25", SegmentsSearched: int32(in.Diagnostics.SegmentsSearched), PostingsListsScanned: int32(in.Diagnostics.PostingsListsScanned), CandidateCount: int32(in.Diagnostics.CandidateCount), Truncated: in.Diagnostics.Truncated}
	}
	return out
}

func mapLexicalIndexStatus(in lexicalservice.Status) *clientv1.LexicalIndexStatus {
	return &clientv1.LexicalIndexStatus{SpaceId: in.SpaceID, DomainId: in.DomainID, State: mapFreshnessState(in.State), IndexedGraphRevision: int64(in.IndexedGraphRevision), LatestKnownGraphRevision: int64(in.LatestKnownGraphRevision), RevisionLag: int64(in.RevisionLag), LiveDocumentCount: int64(in.LiveDocumentCount), DeletedDocumentCount: int64(in.DeletedDocumentCount), SegmentCount: int32(in.SegmentCount), AnalyzerVersion: in.AnalyzerVersion, IndexFormatVersion: in.IndexFormatVersion, LastError: in.LastError}
}

func mapSearchFreshness(in lexicalservice.Status) *clientv1.SearchFreshness {
	return &clientv1.SearchFreshness{State: mapFreshnessState(in.State), IndexedGraphRevision: int64(in.IndexedGraphRevision), LatestKnownGraphRevision: int64(in.LatestKnownGraphRevision), RevisionLag: int64(in.RevisionLag), LastError: in.LastError}
}

func mapFreshnessState(state lexicalservice.State) clientv1.SearchFreshnessState {
	switch state {
	case lexicalservice.StateFresh:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_FRESH
	case lexicalservice.StateStale:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_STALE
	case lexicalservice.StateRebuilding:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_REBUILDING
	case lexicalservice.StateUnavailable:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE
	case lexicalservice.StateError:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_ERROR
	default:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNSPECIFIED
	}
}

func mapLexicalError(err error, action string) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.OK {
		return err
	}
	var parseErr *lexicalparser.ParseError
	if errors.As(err, &parseErr) {
		return status.Errorf(codes.InvalidArgument, "%s: %v", action, err)
	}
	if errors.Is(err, lexicalservice.ErrIndexUnavailable) {
		return status.Errorf(codes.FailedPrecondition, "%s: %v", action, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
