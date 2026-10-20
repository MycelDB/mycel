package client

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	hybridsearch "github.com/myceldb/mycel/internal/search/hybrid"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSearchSelectiveFiltersReachBelowInitialCandidateWindow(t *testing.T) {
	fixture := newSearchPaginationFixture(60)
	selected := fixture.lexical.results[54].NodeID
	node := fixture.graphs.nodes[selected]
	node.Labels = []string{"Note"}
	node.Properties = map[string]any{"tags": []any{"k3s", "search"}, "status": "open"}
	fixture.graphs.nodes[selected] = node
	filters := &clientv1.SearchFilters{
		NodeLabels: []string{"Note"},
		Properties: []*clientv1.PropertyFilter{
			{Path: "tags", Operator: clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS, Values: []string{"k3s"}},
			{Path: "status", Operator: clientv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"open"}},
		},
	}

	for _, mode := range []clientv1.SearchMode{clientv1.SearchMode_SEARCH_MODE_LEXICAL, clientv1.SearchMode_SEARCH_MODE_HYBRID} {
		response, err := fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: mode, Query: "raft", Filters: filters, PageSize: 1, Hybrid: &clientv1.HybridSearchOptions{LexicalWeight: 1, SemanticWeight: 0}})
		if err != nil {
			t.Fatalf("Search(%s) error = %v", mode, err)
		}
		if len(response.GetResults()) != 1 || response.GetResults()[0].GetNodeId() != selected {
			t.Fatalf("Search(%s) results = %+v, want selective node %s", mode, response.GetResults(), selected)
		}
	}
}

func TestHybridSearchSelectiveSemanticFilterReachesBelowInitialWindow(t *testing.T) {
	fixture := newSearchPaginationFixture(60)
	selected := fixture.lexical.results[54].NodeID
	node := fixture.graphs.nodes[selected]
	node.Labels = []string{"Selected"}
	fixture.graphs.nodes[selected] = node
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	semantic := &fakeHybridSemanticManager{rules: []domainsemantic.SemanticGenerationRule{{ID: ruleID, SpaceID: fixture.spaceID, DomainID: fixture.domainID, Key: "notes", Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), Enabled: true}}}}}
	for i, item := range fixture.lexical.results {
		semantic.results = append(semantic.results, semanticsearch.SearchResult{NodeID: graph.NodeID(uuid.MustParse(item.NodeID)), Score: float64(len(fixture.lexical.results) - i)})
	}
	fixture.service.semantic = semantic
	response, err := fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID,
		Query: "raft", PageSize: 1, Filters: &clientv1.SearchFilters{NodeLabels: []string{"Selected"}},
		Hybrid: &clientv1.HybridSearchOptions{LexicalWeight: 0, SemanticWeight: 1},
	})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(response.GetResults()) != 1 || response.GetResults()[0].GetNodeId() != selected {
		t.Fatalf("semantic filtered results = %+v, want %s", response.GetResults(), selected)
	}
	if semantic.lastInput.Limit != hybridMaxCandidateCount {
		t.Fatalf("semantic candidate limit = %d, want filter-aware expansion to %d", semantic.lastInput.Limit, hybridMaxCandidateCount)
	}
}

func TestHybridSearchPaginationIsStableAndBoundToRequestAndRevision(t *testing.T) {
	fixture := newSearchPaginationFixture(6)
	for id, node := range fixture.graphs.nodes {
		node.Labels = []string{"Note"}
		node.Properties = map[string]any{"status": "open"}
		fixture.graphs.nodes[id] = node
	}
	semanticQuery := "how does raft recover"
	semanticOptions := &clientv1.SemanticSearchOptions{}
	setSemanticSearchQueryOverrideForTest(semanticOptions, semanticQuery)
	request := &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID,
		Query: "raft", PageSize: 2, AllowStale: true,
		Filters:  &clientv1.SearchFilters{NodeLabels: []string{"Note"}, Properties: []*clientv1.PropertyFilter{{Path: "status", Operator: clientv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"open"}}}},
		Hybrid:   &clientv1.HybridSearchOptions{LexicalWeight: 1, SemanticWeight: 0},
		Semantic: semanticOptions,
	}
	seen := map[string]bool{}
	var firstToken string
	for page := 0; page < 3; page++ {
		response, err := fixture.service.Search(fixture.ctx, request)
		if err != nil {
			t.Fatalf("Search(page %d) error = %v", page+1, err)
		}
		if len(response.GetResults()) != 2 {
			t.Fatalf("Search(page %d) result count = %d, want 2", page+1, len(response.GetResults()))
		}
		for _, result := range response.GetResults() {
			if seen[result.GetNodeId()] {
				t.Fatalf("duplicate node %s across hybrid pages", result.GetNodeId())
			}
			seen[result.GetNodeId()] = true
		}
		if page == 0 {
			firstToken = response.GetNextPageToken()
		}
		request.PageToken = response.GetNextPageToken()
	}
	if len(seen) != 6 || request.GetPageToken() != "" {
		t.Fatalf("hybrid pages covered %d nodes with final token %q, want 6 and empty token", len(seen), request.GetPageToken())
	}

	mismatch := *request
	mismatch.PageToken = firstToken
	fixture.lexical.results[0], fixture.lexical.results[1] = fixture.lexical.results[1], fixture.lexical.results[0]
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("candidate-changed cursor error = %v, want InvalidArgument", err)
	}
	fixture.lexical.results[0], fixture.lexical.results[1] = fixture.lexical.results[1], fixture.lexical.results[0]
	fixture.lexical.analyzerVersion = "analyzer-v2"
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("analyzer-invalidated cursor error = %v, want InvalidArgument", err)
	}
	fixture.lexical.analyzerVersion = "analyzer-v1"
	mismatch = *request
	mismatch.PageToken = firstToken
	mismatch.Query = "different"
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("query-mismatched cursor error = %v, want InvalidArgument", err)
	}
	mismatch = *request
	mismatch.PageToken = firstToken
	changedSemanticQuery := "how does raft fail"
	mismatch.Semantic = &clientv1.SemanticSearchOptions{}
	setSemanticSearchQueryOverrideForTest(mismatch.Semantic, changedSemanticQuery)
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("semantic-query-mismatched cursor error = %v, want InvalidArgument", err)
	}
	mismatch = *request
	mismatch.PageToken = firstToken
	mismatch.Filters = &clientv1.SearchFilters{NodeLabels: []string{"Other"}}
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("filter-mismatched cursor error = %v, want InvalidArgument", err)
	}
	mismatch = *request
	mismatch.PageToken = firstToken
	mismatch.Hybrid = &clientv1.HybridSearchOptions{LexicalWeight: 0.75, SemanticWeight: 0.25}
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ranking-mismatched cursor error = %v, want InvalidArgument", err)
	}
	mismatch = *request
	mismatch.PageToken = firstToken
	fixture.graphs.revision++
	if _, err := fixture.service.Search(fixture.ctx, &mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("revision-invalidated cursor error = %v, want InvalidArgument", err)
	}
}

func TestFilteredLexicalPaginationResumesAfterExaminedCandidate(t *testing.T) {
	fixture := newSearchPaginationFixture(6)
	want := []string{}
	for i, item := range fixture.lexical.results {
		if i%2 != 0 {
			continue
		}
		node := fixture.graphs.nodes[item.NodeID]
		node.Labels = []string{"Note"}
		fixture.graphs.nodes[item.NodeID] = node
		want = append(want, item.NodeID)
	}
	request := &clientv1.SearchRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Query: "raft", PageSize: 2, Filters: &clientv1.SearchFilters{NodeLabels: []string{"Note"}}}
	first, err := fixture.service.Search(fixture.ctx, request)
	if err != nil {
		t.Fatalf("Search(page 1) error = %v", err)
	}
	if len(first.GetResults()) != 2 || first.GetResults()[0].GetNodeId() != want[0] || first.GetResults()[1].GetNodeId() != want[1] || first.GetNextPageToken() == "" {
		t.Fatalf("Search(page 1) = %+v, want first two filtered nodes and token", first)
	}
	request.PageToken = first.GetNextPageToken()
	second, err := fixture.service.Search(fixture.ctx, request)
	if err != nil {
		t.Fatalf("Search(page 2) error = %v", err)
	}
	if len(second.GetResults()) != 1 || second.GetResults()[0].GetNodeId() != want[2] || second.GetNextPageToken() != "" {
		t.Fatalf("Search(page 2) = %+v, want final filtered node and no token", second)
	}
}

func TestFilteredSearchFailsClosedOnHydrationErrors(t *testing.T) {
	fixture := newSearchPaginationFixture(10)
	fixture.graphs.revisionErrorAt = 2
	fixture.graphs.revisionErr = errors.New("snapshot unavailable")
	_, err := fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL,
		Query: "raft", PageSize: 1, Filters: &clientv1.SearchFilters{NodeLabels: []string{"Note"}},
	})
	if err == nil {
		t.Fatal("filtered lexical search returned nil error for snapshot failure")
	}

	fixture = newSearchPaginationFixture(10)
	fixture.graphs.getNodeErr = errors.New("node read unavailable")
	_, err = fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID,
		Query: "raft", PageSize: 1, Filters: &clientv1.SearchFilters{NodeLabels: []string{"Note"}},
		Hybrid: &clientv1.HybridSearchOptions{LexicalWeight: 1, SemanticWeight: 0},
	})
	if err == nil {
		t.Fatal("hybrid search returned nil error for candidate node read failure")
	}
}

func TestFilteredLexicalSearchBoundsHydrationAndReturnsContinuation(t *testing.T) {
	fixture := newSearchPaginationFixture(1500)
	response, err := fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL,
		Query: "raft", PageSize: 1, Filters: &clientv1.SearchFilters{NodeLabels: []string{"missing"}}, Lexical: &clientv1.LexicalSearchOptions{CandidateCount: 1},
	})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(response.GetResults()) != 0 || response.GetNextPageToken() == "" {
		t.Fatalf("bounded filtered response = %+v, want empty page with continuation", response)
	}
	if fixture.graphs.getNodeCalls != lexicalMaxFilterCandidatesPerPage {
		t.Fatalf("GetNode calls = %d, want bounded maximum %d", fixture.graphs.getNodeCalls, lexicalMaxFilterCandidatesPerPage)
	}
	if want := lexicalMaxFilterCandidatesPerPage / hybridsearch.DefaultMinCandidates; fixture.lexical.searchCalls != want {
		t.Fatalf("lexical Search calls = %d, want bounded count %d for candidate_count=1", fixture.lexical.searchCalls, want)
	}
	fixture.graphs.getNodeCalls = 0
	response, err = fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
		SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID,
		Query: "raft", PageSize: 1, Filters: &clientv1.SearchFilters{NodeLabels: []string{"missing"}},
		Hybrid: &clientv1.HybridSearchOptions{LexicalWeight: 1, SemanticWeight: 0},
	})
	if err != nil {
		t.Fatalf("hybrid Search() error = %v", err)
	}
	if len(response.GetResults()) != 0 || fixture.graphs.getNodeCalls != hybridMaxCandidateCount {
		t.Fatalf("bounded hybrid response results/calls = %d/%d, want 0/%d", len(response.GetResults()), fixture.graphs.getNodeCalls, hybridMaxCandidateCount)
	}
}

type searchPaginationFixture struct {
	ctx      context.Context
	spaceID  domainspace.SpaceID
	domainID graph.DomainID
	lexical  *paginationLexicalManager
	graphs   *paginationGraphManager
	service  *SearchService
}

func newSearchPaginationFixture(count int) searchPaginationFixture {
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	lexical := &paginationLexicalManager{revision: 10, analyzerVersion: "analyzer-v1", indexFormatVersion: "index-v1"}
	graphs := &paginationGraphManager{revision: 10, nodes: map[string]graph.Node{}}
	for i := 0; i < count; i++ {
		nodeID := graph.NodeID(uuid.New())
		lexical.results = append(lexical.results, lexicalindex.Result{NodeID: nodeID.String(), Score: float64(count - i), IndexedGraphRevision: lexical.revision})
		graphs.nodes[nodeID.String()] = graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Other"}, Properties: map[string]any{"rank": fmt.Sprint(i)}}
	}
	spaces := fakeHybridSpaceManager{domain: graph.Domain{ID: domainID, SpaceID: spaceID, SearchMode: graph.DomainSearchModeNormal}}
	service := NewSearchService(lexical, spaces, graphs)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "alice"})
	return searchPaginationFixture{ctx: ctx, spaceID: spaceID, domainID: domainID, lexical: lexical, graphs: graphs, service: service}
}

type paginationLexicalManager struct {
	lexicalservice.Manager
	revision           uint64
	analyzerVersion    string
	indexFormatVersion string
	results            []lexicalindex.Result
	searchCalls        int
}

func (m *paginationLexicalManager) Status(_ context.Context, spaceID, domainID string, latest uint64) lexicalservice.Status {
	state := lexicalservice.StateFresh
	lag := uint64(0)
	if latest > m.revision {
		state = lexicalservice.StateStale
		lag = latest - m.revision
	}
	return lexicalservice.Status{SpaceID: spaceID, DomainID: domainID, State: state, IndexedGraphRevision: m.revision, LatestKnownGraphRevision: latest, RevisionLag: lag, AnalyzerVersion: m.analyzerVersion, IndexFormatVersion: m.indexFormatVersion}
}

func (m *paginationLexicalManager) Search(_ context.Context, _, _, _ string, opts lexicalindex.SearchOptions) (lexicalindex.SearchResponse, error) {
	m.searchCalls++
	start := min(opts.Offset, len(m.results))
	end := min(start+opts.PageSize, len(m.results))
	response := lexicalindex.SearchResponse{Results: append([]lexicalindex.Result(nil), m.results[start:end]...), Diagnostics: lexicalindex.Diagnostics{CandidateCount: len(m.results), Truncated: end < len(m.results)}}
	if end < len(m.results) {
		response.NextPageToken = "more"
	}
	return response, nil
}

type paginationGraphManager struct {
	daegraph.Manager
	revision        int64
	revisionCalls   int
	revisionErrorAt int
	revisionErr     error
	nodes           map[string]graph.Node
	getNodeCalls    int
	getNodeErr      error
}

func (m *paginationGraphManager) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	m.revisionCalls++
	if m.revisionErrorAt > 0 && m.revisionCalls == m.revisionErrorAt {
		return 0, m.revisionErr
	}
	return m.revision, nil
}

func (m *paginationGraphManager) GetNode(_ context.Context, _ sessionmodel.GraphTransaction, nodeID string) (graph.Node, error) {
	m.getNodeCalls++
	if m.getNodeErr != nil {
		return graph.Node{}, m.getNodeErr
	}
	node, ok := m.nodes[nodeID]
	if !ok {
		return graph.Node{}, daegraph.ErrNotFound
	}
	return node, nil
}
