package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const semanticMaxPageSize = 500

type SemanticService struct {
	clientv1.UnimplementedSemanticServiceServer
	semantic daemonsemantic.Manager
	spaces   daemonspace.Manager
	graphs   daegraph.Manager
}

func NewSemanticService(semantic daemonsemantic.Manager, spaces daemonspace.Manager, graphs daegraph.Manager) *SemanticService {
	return &SemanticService{semantic: semantic, spaces: spaces, graphs: graphs}
}

func (s *SemanticService) ListSemanticIndexes(ctx context.Context, req *clientv1.ListSemanticIndexesRequest) (*clientv1.ListSemanticIndexesResponse, error) {
	principal, spaceID, domainID, err := s.authorizeDomainRead(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	_ = principal
	indexes, err := s.semantic.ListIndexes(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "list semantic indexes")
	}
	states, endpoints, models, stores, err := s.semanticDisplayMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapSemanticError(err, "load semantic metadata")
	}
	items := make([]*clientv1.SemanticIndex, 0, len(indexes))
	for _, index := range indexes {
		items = append(items, MapSemanticIndexProto(index, states[index.ID], endpoints, models, stores))
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].GetKey() < items[j].GetKey() })
	page, next, err := paginateSemantic(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &clientv1.ListSemanticIndexesResponse{Indexes: page, NextPageToken: next}, nil
}

func (s *SemanticService) SemanticSearch(ctx context.Context, req *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
	principal, spaceID, domainID, err := s.authorizeDomainRead(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	query := strings.TrimSpace(req.GetQuery())
	if query == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 10
	}
	if limit > semanticMaxPageSize {
		return nil, status.Errorf(codes.ResourceExhausted, "limit must be <= %d", semanticMaxPageSize)
	}
	selectedIndexIDs, err := s.resolveSearchIndexes(ctx, spaceID, domainID, req.GetSemanticIndexId())
	if err != nil {
		return nil, err
	}
	actorID, err := parseIdentityUserID(principal.UserID)
	if err != nil {
		return nil, err
	}
	minScore := 0.0
	if req.MinScore != nil {
		minScore = req.GetMinScore()
	}
	result, err := s.semantic.Search(ctx, daemonsemantic.SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexIDs: selectedIndexIDs, Text: query, Limit: limit, MinScore: minScore, ActorPrincipalID: actorID})
	if err != nil {
		return nil, mapSemanticError(err, "semantic search")
	}
	nodeByID, warnings := s.loadSearchNodes(ctx, principal.UserID, spaceID, domainID, result.Results)
	out := make([]*clientv1.SemanticSearchResult, 0, len(result.Results))
	for _, item := range result.Results {
		node := nodeByID[item.NodeID]
		if node == nil {
			continue
		}
		out = append(out, &clientv1.SemanticSearchResult{NodeId: item.NodeID.String(), Score: item.Score, Node: node, MatchedChunkIds: []string{item.RecordID.String()}, Snippet: semanticSnippet(node.GetContent())})
	}
	allWarnings := append([]string{}, result.Warnings...)
	allWarnings = append(allWarnings, warnings...)
	return &clientv1.SemanticSearchResponse{Results: out, Warnings: allWarnings}, nil
}

func (s *SemanticService) authorizeDomainRead(ctx context.Context, spaceIDText, domainIDText string) (principalUser, domainspace.SpaceID, graph.DomainID, error) {
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
	if _, err := s.spaces.GetVisibleDomain(ctx, principal.UserID, spaceID.String(), domainID.String(), ""); err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, mapDomainError(err, "semantic authorize domain")
	}
	return principalUser{UserID: principal.UserID}, spaceID, domainID, nil
}

type principalUser struct{ UserID string }

func (s *SemanticService) semanticDisplayMetadata(ctx context.Context, spaceID domainspace.SpaceID) (map[domainsemantic.SemanticIndexID]domainsemantic.SemanticIndexState, map[domainsemantic.ModelEndpointID]domainsemantic.ModelEndpoint, map[domainsemantic.InferenceModelID]domainsemantic.InferenceModel, map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend, error) {
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	stateRows, err := spaceMgr.ListIndexStates(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	states := map[domainsemantic.SemanticIndexID]domainsemantic.SemanticIndexState{}
	for _, st := range stateRows {
		states[st.SemanticIndexID] = st
	}
	global := s.semantic.GlobalManager()
	endpointRows, err := global.ListModelEndpoints(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	modelRows, err := global.ListModels(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	storeRows, err := global.ListVectorStores(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	endpoints := map[domainsemantic.ModelEndpointID]domainsemantic.ModelEndpoint{}
	for _, endpoint := range endpointRows {
		endpoints[endpoint.ID] = endpoint
	}
	models := map[domainsemantic.InferenceModelID]domainsemantic.InferenceModel{}
	for _, model := range modelRows {
		models[model.ID] = model
	}
	stores := map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend{}
	for _, store := range storeRows {
		stores[store.ID] = store
	}
	return states, endpoints, models, stores, nil
}

func (s *SemanticService) resolveSearchIndexes(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, rawIndexID string) ([]domainsemantic.SemanticIndexID, error) {
	indexes, err := s.semantic.ListIndexes(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "resolve semantic indexes")
	}
	if strings.TrimSpace(rawIndexID) == "" {
		out := []domainsemantic.SemanticIndexID{}
		for _, index := range indexes {
			if index.Enabled {
				out = append(out, index.ID)
			}
		}
		if len(out) == 0 {
			return nil, status.Error(codes.FailedPrecondition, "no enabled semantic index is available for the domain")
		}
		return out, nil
	}
	id, err := parseSemanticIndexID(rawIndexID)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		if index.ID == id {
			if !index.Enabled {
				return nil, status.Error(codes.FailedPrecondition, "semantic index is disabled")
			}
			return []domainsemantic.SemanticIndexID{id}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "semantic index not found")
}

func (s *SemanticService) loadSearchNodes(ctx context.Context, userID string, spaceID domainspace.SpaceID, domainID graph.DomainID, results []semanticsearch.SearchResult) (map[graph.NodeID]*clientv1.Node, []string) {
	out := map[graph.NodeID]*clientv1.Node{}
	warnings := []string{}
	tx := daemonsession.GraphTransaction{ID: "semantic-search-" + uuid.NewString(), UserID: userID, SpaceID: spaceID.String(), DomainID: domainID.String(), Mode: daemonsession.TransactionModeReadOnly, State: daemonsession.TransactionStateActive, CreatedAt: time.Now().UTC(), LastSeen: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Minute)}
	seen := map[graph.NodeID]bool{}
	for _, result := range results {
		if seen[result.NodeID] {
			continue
		}
		seen[result.NodeID] = true
		node, err := s.graphs.GetNode(ctx, tx, result.NodeID.String())
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("node %s skipped: %v", result.NodeID, err))
			continue
		}
		out[result.NodeID] = mapProtoNode(node)
	}
	return out, warnings
}

func MapSemanticIndexProto(index domainsemantic.SemanticIndex, state domainsemantic.SemanticIndexState, endpoints map[domainsemantic.ModelEndpointID]domainsemantic.ModelEndpoint, models map[domainsemantic.InferenceModelID]domainsemantic.InferenceModel, stores map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend) *clientv1.SemanticIndex {
	modelLabel := index.ModelID.String()
	if model, ok := models[index.ModelID]; ok {
		modelLabel = firstNonEmptyString(model.Key, model.ModelName, model.ID.String())
	}
	storeLabel := index.VectorStoreID.String()
	if store, ok := stores[index.VectorStoreID]; ok {
		storeLabel = firstNonEmptyString(store.Name, store.Key, store.ID.String())
	}
	if endpoint, ok := endpoints[index.ModelEndpointID]; ok && endpoint.Name != "" && modelLabel != "" {
		modelLabel = modelLabel + " via " + endpoint.Name
	}
	return &clientv1.SemanticIndex{SemanticIndexId: index.ID.String(), Key: index.Key, DisplayName: index.Name, Description: semanticIndexDescription(index), SpaceId: index.SpaceID.String(), DomainId: index.DomainID.String(), ModelLabel: modelLabel, VectorStoreLabel: storeLabel, State: mapSemanticIndexState(index, state)}
}

func mapSemanticIndexState(index domainsemantic.SemanticIndex, state domainsemantic.SemanticIndexState) clientv1.SemanticIndexState {
	if !index.Enabled {
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_DISABLED
	}
	switch strings.ToLower(strings.TrimSpace(state.State)) {
	case "building", "running", "backfilling":
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_BUILDING
	case "stale", "dirty":
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_STALE
	case "disabled":
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_DISABLED
	case "error", "failed":
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_ERROR
	default:
		return clientv1.SemanticIndexState_SEMANTIC_INDEX_STATE_ACTIVE
	}
}

func semanticIndexDescription(index domainsemantic.SemanticIndex) string {
	parts := []string{}
	if index.Purpose != "" {
		parts = append(parts, string(index.Purpose))
	}
	if index.SourcePolicy.Extraction != "" {
		parts = append(parts, "source="+string(index.SourcePolicy.Extraction))
	}
	if len(index.SourcePolicy.TemplateKeys) > 0 {
		parts = append(parts, "templates="+strings.Join(index.SourcePolicy.TemplateKeys, ","))
	}
	return strings.Join(parts, "; ")
}

func semanticSnippet(content string) string {
	content = strings.TrimSpace(content)
	if len(content) <= 240 {
		return content
	}
	return content[:240]
}

func paginateSemantic[T any](items []T, pageSize int, pageToken string) ([]T, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > semanticMaxPageSize {
		pageSize = semanticMaxPageSize
	}
	if start >= len(items) {
		return []T{}, "", nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

func parseDomainSpaceID(raw string) (domainspace.SpaceID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "space_id must be a UUID")
	}
	return domainspace.SpaceID(id), nil
}

func parseGraphDomainID(raw string) (graph.DomainID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "domain_id must be a UUID")
	}
	return graph.DomainID(id), nil
}

func parseSemanticIndexID(raw string) (domainsemantic.SemanticIndexID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "semantic_index_id must be a UUID")
	}
	return domainsemantic.SemanticIndexID(id), nil
}

func parseIdentityUserID(raw string) (identity.UserID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "invalid user principal")
	}
	return identity.UserID(id), nil
}

func mapSemanticError(err error, action string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(msg, "required") || strings.Contains(msg, "invalid") {
		return status.Error(codes.InvalidArgument, msg)
	}
	if strings.Contains(msg, "denies") || strings.Contains(msg, "policy") {
		return status.Error(codes.PermissionDenied, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
