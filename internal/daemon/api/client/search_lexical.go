package client

import (
	"context"
	"fmt"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	hybridsearch "github.com/myceldb/mycel/internal/search/hybrid"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const lexicalMaxFilterCandidatesPerPage = 1000

func (s *SearchService) lexicalSearch(ctx context.Context, principal principalUser, spaceID domainspace.SpaceID, domainID graph.DomainID, query string, pageSize int, freshness lexicalservice.Status, filters hybridsearch.Filters, req *clientv1.SearchRequest) (*clientv1.SearchResponse, error) {
	offset, err := searchPageOffset(req, searchCursorModeLexical, freshness)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var result lexicalindex.SearchResponse
	warnings := []string{}
	nextOffset := offset
	if hybridsearch.HasFilters(filters) {
		result, nextOffset, warnings, err = s.filteredLexicalPage(ctx, principal, spaceID, domainID, query, pageSize, offset, filters, req.GetLexical())
	} else {
		result, err = s.lexical.Search(ctx, spaceID.String(), domainID.String(), query, lexicalindex.SearchOptions{PageSize: pageSize, Offset: offset})
		nextOffset += len(result.Results)
	}
	if err != nil {
		return nil, mapLexicalError(err, "lexical search")
	}
	freshness, err = s.verifySearchSnapshot(ctx, spaceID, domainID, freshness)
	if err != nil {
		return nil, err
	}
	out := mapSearchResponse(result, freshness, req.GetIncludeDiagnostics())
	out.Warnings = append(out.Warnings, warnings...)
	if result.NextPageToken != "" {
		next, err := encodeNextSearchPageToken(req, searchCursorModeLexical, freshness, nextOffset, "")
		if err != nil {
			return nil, status.Error(codes.Internal, "encode lexical search page token")
		}
		out.NextPageToken = next
	}
	return out, nil
}

func (s *SearchService) filteredLexicalPage(ctx context.Context, principal principalUser, spaceID domainspace.SpaceID, domainID graph.DomainID, query string, pageSize, offset int, filters hybridsearch.Filters, opts *clientv1.LexicalSearchOptions) (lexicalindex.SearchResponse, int, []string, error) {
	batchSize := hybridsearch.CandidateCount(pageSize, int(opts.GetCandidateCount()), lexicalMaxFilterCandidatesPerPage)
	if batchSize <= 0 {
		batchSize = hybridsearch.DefaultMinCandidates
	}
	scanOffset := offset
	examined := 0
	response := lexicalindex.SearchResponse{}
	warnings := []string{}
	sourceHasMore := false
	for examined < lexicalMaxFilterCandidatesPerPage && len(response.Results) < pageSize {
		remaining := lexicalMaxFilterCandidatesPerPage - examined
		if batchSize > remaining {
			batchSize = remaining
		}
		batch, err := s.lexical.Search(ctx, spaceID.String(), domainID.String(), query, lexicalindex.SearchOptions{PageSize: batchSize, Offset: scanOffset})
		if err != nil {
			return lexicalindex.SearchResponse{}, scanOffset, warnings, err
		}
		response.Diagnostics.SegmentsSearched = max(response.Diagnostics.SegmentsSearched, batch.Diagnostics.SegmentsSearched)
		response.Diagnostics.PostingsListsScanned += batch.Diagnostics.PostingsListsScanned
		if len(batch.Results) == 0 {
			break
		}
		hydrationInput := make([]hybridsearch.Result, 0, len(batch.Results))
		for _, item := range batch.Results {
			hydrationInput = append(hydrationInput, hybridsearch.Result{NodeID: item.NodeID})
		}
		nodes, loadWarnings, err := s.loadHybridNodes(ctx, principal.PrincipalID, spaceID, domainID, hydrationInput)
		if err != nil {
			return lexicalindex.SearchResponse{}, scanOffset, warnings, mapGraphError(err, "hydrate filtered lexical candidates")
		}
		warnings = append(warnings, loadWarnings...)
		for i, item := range batch.Results {
			scanOffset++
			examined++
			node, ok := nodes[item.NodeID]
			if ok {
				matched, err := hybridsearch.MatchesFilters(node, filters)
				if err != nil {
					return lexicalindex.SearchResponse{}, scanOffset, warnings, err
				}
				if matched {
					response.Results = append(response.Results, item)
				}
			}
			if len(response.Results) == pageSize {
				response.Diagnostics.Truncated = i+1 < len(batch.Results) || batch.Diagnostics.Truncated
				break
			}
		}
		sourceHasMore = batch.Diagnostics.Truncated
		if len(response.Results) == pageSize || !sourceHasMore {
			break
		}
	}
	response.Diagnostics.CandidateCount = examined
	response.Diagnostics.Truncated = response.Diagnostics.Truncated || examined == lexicalMaxFilterCandidatesPerPage && sourceHasMore
	if response.Diagnostics.Truncated {
		response.NextPageToken = "pending"
	}
	if examined == lexicalMaxFilterCandidatesPerPage && sourceHasMore && len(response.Results) < pageSize {
		warnings = append(warnings, fmt.Sprintf("lexical filter scan examined the bounded maximum of %d candidates; continue with next_page_token", lexicalMaxFilterCandidatesPerPage))
	}
	return response, scanOffset, warnings, nil
}

func (s *SearchService) verifySearchSnapshot(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, captured lexicalservice.Status) (lexicalservice.Status, error) {
	current, err := s.lexicalStatus(ctx, spaceID, domainID)
	if err != nil {
		return lexicalservice.Status{}, mapGraphError(err, "read graph revision after search")
	}
	if current.IndexedGraphRevision != captured.IndexedGraphRevision || current.LatestKnownGraphRevision != captured.LatestKnownGraphRevision || current.AnalyzerVersion != captured.AnalyzerVersion || current.IndexFormatVersion != captured.IndexFormatVersion {
		return lexicalservice.Status{}, status.Error(codes.Aborted, "graph revision, search index, analyzer, or index format changed during search; retry")
	}
	return current, nil
}
