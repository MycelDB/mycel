package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	identity "github.com/myceldb/mycel/internal/identity/model"
	hybridsearch "github.com/myceldb/mycel/internal/search/hybrid"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalparser "github.com/myceldb/mycel/internal/search/lexical/parser"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	lexicalMaxPageSize      = 500
	hybridMaxCandidateCount = 1000
)

type SearchService struct {
	clientv1.UnimplementedSearchServiceServer
	lexical          lexicalservice.Manager
	semantic         daemonsemantic.Manager
	spaces           daemonspace.Manager
	graphs           daegraph.Manager
	graphWriteRouter GraphWriteRouteProvider
	router           ClientRequestRouter
}

func NewSearchService(lexical lexicalservice.Manager, spaces daemonspace.Manager, graphs daegraph.Manager) *SearchService {
	return &SearchService{lexical: lexical, spaces: spaces, graphs: graphs}
}

func (s *SearchService) WithSemanticManager(semantic daemonsemantic.Manager) *SearchService {
	s.semantic = semantic
	return s
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
	mode := req.GetMode()
	if mode != clientv1.SearchMode_SEARCH_MODE_UNSPECIFIED && mode != clientv1.SearchMode_SEARCH_MODE_LEXICAL && mode != clientv1.SearchMode_SEARCH_MODE_HYBRID {
		return nil, status.Error(codes.InvalidArgument, "unsupported search mode")
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
	if mode == clientv1.SearchMode_SEARCH_MODE_HYBRID {
		return s.hybridSearch(ctx, principal, spaceID, domainID, query, pageSize, freshness, req)
	}
	result, err := s.lexical.Search(ctx, spaceID.String(), domainID.String(), query, lexicalindex.SearchOptions{PageSize: pageSize, PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapLexicalError(err, "lexical search")
	}
	freshness = s.lexical.Status(ctx, spaceID.String(), domainID.String(), latest)
	return mapSearchResponse(result, freshness, req.GetIncludeDiagnostics()), nil
}

func (s *SearchService) hybridSearch(ctx context.Context, principal principalUser, spaceID domainspace.SpaceID, domainID graph.DomainID, query string, pageSize int, freshness lexicalservice.Status, req *clientv1.SearchRequest) (*clientv1.SearchResponse, error) {
	if strings.TrimSpace(req.GetPageToken()) != "" {
		return nil, status.Error(codes.InvalidArgument, "page_token is not supported for hybrid search")
	}
	opts, err := hybridOptionsFromProto(req.GetHybrid())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	filters, err := searchFiltersFromProto(req.GetFilters())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lexicalCandidates := hybridsearch.CandidateCount(pageSize, int(req.GetLexical().GetCandidateCount()), hybridMaxCandidateCount)
	semanticCandidates := hybridsearch.CandidateCount(pageSize, int(req.GetSemantic().GetCandidateCount()), hybridMaxCandidateCount)
	lexicalResult, err := s.lexical.Search(ctx, spaceID.String(), domainID.String(), query, lexicalindex.SearchOptions{PageSize: lexicalCandidates})
	if err != nil {
		return nil, mapLexicalError(err, "hybrid lexical search")
	}
	candidates := make([]hybridsearch.Candidate, 0, len(lexicalResult.Results))
	lexicalByNode := map[string]lexicalindex.Result{}
	for i, item := range lexicalResult.Results {
		lexicalByNode[item.NodeID] = item
		candidates = append(candidates, hybridsearch.Candidate{NodeID: item.NodeID, Lexical: &hybridsearch.Source{RawScore: item.Score, Rank: i + 1}})
	}
	warnings := []string{}
	if freshness.State == lexicalservice.StateStale {
		warnings = append(warnings, "lexical index is stale")
	}
	if opts.Weights.Semantic > 0 {
		semanticRequired := opts.RequireBoth || opts.Weights.Lexical == 0
		semanticCandidatesOut, semanticWarnings, err := s.semanticHybridCandidates(ctx, principal, spaceID, domainID, query, semanticCandidates, req.GetSemantic(), semanticRequired)
		if err != nil {
			return nil, err
		}
		warnings = append(warnings, semanticWarnings...)
		candidates = append(candidates, semanticCandidatesOut...)
	}
	fused, err := hybridsearch.Fuse(candidates, opts)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	nodesByID, loadWarnings := s.loadHybridNodes(ctx, principal.PrincipalID, spaceID, domainID, fused)
	warnings = append(warnings, loadWarnings...)
	out := &clientv1.SearchResponse{Freshness: mapSearchFreshness(freshness), Warnings: warnings}
	for _, item := range fused {
		if len(out.Results) >= pageSize {
			break
		}
		node, ok := nodesByID[item.NodeID]
		if !ok {
			continue
		}
		matched, err := hybridsearch.MatchesFilters(node, filters)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !matched {
			continue
		}
		result := &clientv1.SearchResult{SpaceId: spaceID.String(), DomainId: domainID.String(), NodeId: item.NodeID, Score: item.Score, ScoreKind: clientv1.SearchScoreKind_SEARCH_SCORE_KIND_HYBRID_FUSED}
		if lexicalItem, ok := lexicalByNode[item.NodeID]; ok {
			result.IndexedGraphRevision = int64(lexicalItem.IndexedGraphRevision)
			if req.GetIncludeDiagnostics() {
				result.MatchedTerms = append([]string(nil), lexicalItem.MatchedTerms...)
				result.MatchedFieldPaths = append([]string(nil), lexicalItem.MatchedFieldPaths...)
			}
		}
		result.Sources = append(result.Sources, resultSourcesFromHybrid(item)...)
		if req.GetIncludeDiagnostics() {
			result.ScoreComponents = scoreComponentsFromHybrid(item, opts)
		}
		out.Results = append(out.Results, result)
	}
	if req.GetIncludeDiagnostics() {
		out.Diagnostics = &clientv1.SearchDiagnostics{AnalyzerVersion: freshness.AnalyzerVersion, IndexFormatVersion: freshness.IndexFormatVersion, QueryPlan: fmt.Sprintf("HybridWRRF lexical_candidates=%d semantic_candidates=%d require_both=%t", lexicalCandidates, semanticCandidates, opts.RequireBoth), SegmentsSearched: int32(lexicalResult.Diagnostics.SegmentsSearched), PostingsListsScanned: int32(lexicalResult.Diagnostics.PostingsListsScanned), CandidateCount: int32(len(candidates)), Truncated: len(out.Results) == pageSize && len(fused) > pageSize}
	}
	return out, nil
}

func (s *SearchService) semanticHybridCandidates(ctx context.Context, principal principalUser, spaceID domainspace.SpaceID, domainID graph.DomainID, query string, limit int, opts *clientv1.SemanticSearchOptions, requireBoth bool) ([]hybridsearch.Candidate, []string, error) {
	if s.semantic == nil {
		if requireBoth {
			return nil, nil, status.Error(codes.FailedPrecondition, "semantic search service is not configured")
		}
		return nil, []string{"semantic search service is not configured; returning lexical-only hybrid results"}, nil
	}
	if ok, err := s.semanticSearchEnabled(ctx, principal, spaceID, domainID); err != nil {
		return nil, nil, err
	} else if !ok {
		if requireBoth {
			return nil, nil, status.Error(codes.FailedPrecondition, "domain is excluded from semantic search and indexing")
		}
		return nil, []string{"domain is excluded from semantic search and indexing; returning lexical-only hybrid results"}, nil
	}
	bindingKey := strings.TrimSpace(opts.GetEmbeddingBindingKey())
	if bindingKey != "" && strings.TrimSpace(opts.GetSemanticRuleId()) == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "embedding_binding_key requires semantic_rule_id")
	}
	resolver := NewSemanticService(s.semantic, s.spaces, s.graphs)
	selectedRuleIDs, err := resolver.resolveSearchRules(ctx, spaceID, domainID, opts.GetSemanticRuleId())
	if err != nil {
		if isNoSemanticSearchAvailable(err) && !requireBoth {
			return nil, []string{"no enabled semantic search rule is available for the domain; returning lexical-only hybrid results"}, nil
		}
		return nil, nil, err
	}
	actorID, err := parseIdentityPrincipalID(principal.PrincipalID)
	if err != nil {
		return nil, nil, err
	}
	minScore := 0.0
	if opts != nil && opts.MinScore != nil {
		minScore = opts.GetMinScore()
	}
	result, err := s.semantic.Search(ctx, daemonsemantic.SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: selectedRuleIDs, EmbeddingBindingKey: bindingKey, Text: query, Limit: limit, MinScore: minScore, ActorPrincipalID: identity.PrincipalID(actorID)})
	if err != nil {
		return nil, nil, mapSemanticError(err, "hybrid semantic search")
	}
	candidates := make([]hybridsearch.Candidate, 0, len(result.Results))
	for i, item := range result.Results {
		if item.NodeID == uuid.Nil {
			continue
		}
		candidates = append(candidates, hybridsearch.Candidate{NodeID: item.NodeID.String(), Semantic: &hybridsearch.Source{RawScore: item.Score, Rank: i + 1}})
	}
	return candidates, append([]string(nil), result.Warnings...), nil
}

func (s *SearchService) semanticSearchEnabled(ctx context.Context, principal principalUser, spaceID domainspace.SpaceID, domainID graph.DomainID) (bool, error) {
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, spaceID.String(), domainID.String(), "")
	if err != nil {
		return false, mapDomainError(err, "hybrid semantic authorize domain")
	}
	return graph.DomainExplicitSemanticSearchable(domain), nil
}

func (s *SearchService) loadHybridNodes(ctx context.Context, principalID string, spaceID domainspace.SpaceID, domainID graph.DomainID, results []hybridsearch.Result) (map[string]graph.Node, []string) {
	out := map[string]graph.Node{}
	if s.graphs == nil || len(results) == 0 {
		return out, nil
	}
	warnings := []string{}
	tx := daemonsession.GraphTransaction{ID: "hybrid-search-" + uuid.NewString(), PrincipalID: principalID, SpaceID: spaceID.String(), DomainID: domainID.String(), Mode: daemonsession.TransactionModeReadOnly, State: daemonsession.TransactionStateActive, CreatedAt: time.Now().UTC(), LastSeen: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute)}
	for _, result := range results {
		if _, ok := out[result.NodeID]; ok {
			continue
		}
		node, err := s.graphs.GetNode(ctx, tx, result.NodeID)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("node %s skipped: %v", result.NodeID, err))
			continue
		}
		out[result.NodeID] = node
	}
	return out, warnings
}

func hybridOptionsFromProto(in *clientv1.HybridSearchOptions) (hybridsearch.Options, error) {
	strategy := hybridsearch.FusionWeightedReciprocalRank
	if in != nil {
		switch in.GetFusionStrategy() {
		case clientv1.HybridFusionStrategy_HYBRID_FUSION_STRATEGY_UNSPECIFIED, clientv1.HybridFusionStrategy_HYBRID_FUSION_STRATEGY_WEIGHTED_RECIPROCAL_RANK:
			strategy = hybridsearch.FusionWeightedReciprocalRank
		default:
			return hybridsearch.Options{}, fmt.Errorf("unsupported hybrid fusion strategy")
		}
	}
	weights := hybridsearch.WeightOptions{}
	requireBoth := false
	if in != nil {
		weights = hybridsearch.WeightOptions{Lexical: in.GetLexicalWeight(), Semantic: in.GetSemanticWeight()}
		requireBoth = in.GetRequireBoth()
	}
	normalized, err := hybridsearch.NormalizeWeights(weights)
	if err != nil {
		return hybridsearch.Options{}, err
	}
	return hybridsearch.Options{Weights: hybridsearch.WeightOptions{Lexical: normalized.Lexical, Semantic: normalized.Semantic}, Strategy: strategy, RequireBoth: requireBoth}, nil
}

func searchFiltersFromProto(in *clientv1.SearchFilters) (hybridsearch.Filters, error) {
	if in == nil {
		return hybridsearch.Filters{}, nil
	}
	out := hybridsearch.Filters{NodeLabels: append([]string(nil), in.GetNodeLabels()...), NodeIDs: append([]string(nil), in.GetNodeIds()...)}
	for _, item := range in.GetProperties() {
		operator, err := filterOperatorFromProto(item.GetOperator())
		if err != nil {
			return hybridsearch.Filters{}, err
		}
		out.Properties = append(out.Properties, hybridsearch.PropertyFilter{Path: item.GetPath(), Operator: operator, Values: append([]string(nil), item.GetValues()...)})
	}
	return out, nil
}

func filterOperatorFromProto(operator clientv1.FilterOperator) (hybridsearch.FilterOperator, error) {
	switch operator {
	case clientv1.FilterOperator_FILTER_OPERATOR_EQUALS:
		return hybridsearch.FilterEquals, nil
	case clientv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:
		return hybridsearch.FilterNotEquals, nil
	case clientv1.FilterOperator_FILTER_OPERATOR_IN:
		return hybridsearch.FilterIn, nil
	case clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS:
		return hybridsearch.FilterContains, nil
	case clientv1.FilterOperator_FILTER_OPERATOR_EXISTS:
		return hybridsearch.FilterExists, nil
	default:
		return "", fmt.Errorf("unsupported property filter operator")
	}
}

func resultSourcesFromHybrid(in hybridsearch.Result) []*clientv1.SearchResultSource {
	out := []*clientv1.SearchResultSource{}
	if in.Lexical != nil {
		out = append(out, &clientv1.SearchResultSource{Kind: clientv1.SearchResultSourceKind_SEARCH_RESULT_SOURCE_KIND_LEXICAL, RawScore: in.Lexical.RawScore, Rank: int32(in.Lexical.Rank), NormalizedScore: in.Lexical.NormalizedScore})
	}
	if in.Semantic != nil {
		out = append(out, &clientv1.SearchResultSource{Kind: clientv1.SearchResultSourceKind_SEARCH_RESULT_SOURCE_KIND_SEMANTIC, RawScore: in.Semantic.RawScore, Rank: int32(in.Semantic.Rank), NormalizedScore: in.Semantic.NormalizedScore})
	}
	return out
}

func scoreComponentsFromHybrid(in hybridsearch.Result, opts hybridsearch.Options) []*clientv1.SearchScoreComponent {
	components := []*clientv1.SearchScoreComponent{{Name: "hybrid.fused_score", Value: in.Score, Description: "weighted reciprocal-rank fused score"}, {Name: "hybrid.lexical_weight", Value: opts.Weights.Lexical, Description: "normalized lexical weight"}, {Name: "hybrid.semantic_weight", Value: opts.Weights.Semantic, Description: "normalized semantic weight"}}
	if in.Lexical != nil {
		components = append(components, &clientv1.SearchScoreComponent{Name: "lexical.rank_score", Value: in.Lexical.NormalizedScore, Description: "lexical reciprocal-rank contribution before weighting"})
	}
	if in.Semantic != nil {
		components = append(components, &clientv1.SearchScoreComponent{Name: "semantic.rank_score", Value: in.Semantic.NormalizedScore, Description: "semantic reciprocal-rank contribution before weighting"})
	}
	return components
}

func isNoSemanticSearchAvailable(err error) bool {
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), "no enabled semantic search rule")
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
