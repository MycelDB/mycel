package client

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	identity "github.com/myceldb/mycel/internal/identity/model"
	semanticdiagnostics "github.com/myceldb/mycel/internal/semantic/diagnostics"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	semanticMaxPageSize             = 500
	semanticMaxCandidateExamination = 100000
)

type SemanticService struct {
	clientv1.UnimplementedSemanticServiceServer
	semantic     daemonsemantic.Manager
	spaces       daemonspace.Manager
	graphs       daegraph.Manager
	cursorMACKey []byte
	logger       *slog.Logger
}

func NewSemanticService(semantic daemonsemantic.Manager, spaces daemonspace.Manager, graphs daegraph.Manager, cursorMACKey ...[]byte) *SemanticService {
	key := semanticCursorMACKey
	if len(cursorMACKey) > 0 && len(cursorMACKey[0]) >= 32 {
		key = append([]byte(nil), cursorMACKey[0]...)
	}
	return &SemanticService{semantic: semantic, spaces: spaces, graphs: graphs, cursorMACKey: key}
}

func (s *SemanticService) WithLogger(logger *slog.Logger) *SemanticService {
	s.logger = logger
	return s
}

func effectiveSemanticRuleForClientRead(rule domainsemantic.SemanticGenerationRule) domainsemantic.SemanticGenerationRule {
	rule.Enabled = rule.Enabled && rule.ProviderBudgetTransition == ""
	return rule
}

func (s *SemanticService) ListSemanticRules(ctx context.Context, req *clientv1.ListSemanticRulesRequest) (*clientv1.ListSemanticRulesResponse, error) {
	_, spaceID, domainID, err := s.authorizeDomainRead(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	rules, err := s.semantic.ListRules(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "list semantic rules")
	}
	stateByRule, searchByBinding, stores, err := s.semanticRuleSummaryMetadata(ctx, spaceID)
	if err != nil {
		return nil, mapSemanticError(err, "load semantic rule metadata")
	}
	items := make([]*clientv1.SemanticGenerationRuleSummary, 0, len(rules))
	for _, rule := range rules {
		effectiveRule := effectiveSemanticRuleForClientRead(rule)
		if !req.GetIncludeDisabled() && (!effectiveRule.Enabled || !ruleHasSearchBinding(effectiveRule)) {
			continue
		}
		items = append(items, MapSemanticRuleSummaryProto(effectiveRule, stateByRule[rule.ID], searchByBinding, stores))
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].GetKey() < items[j].GetKey() })
	page, next, err := paginateSemantic(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &clientv1.ListSemanticRulesResponse{Rules: page, NextPageToken: next}, nil
}

func (s *SemanticService) SemanticSearch(ctx context.Context, req *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
	trace, stopTrace := semanticdiagnostics.BeginCancellationTrace(ctx, s.logger, "authorize-domain", "semantic search request canceled")
	defer stopTrace()
	principal, spaceID, domainID, err := s.authorizeDomainRead(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(req.GetQuery())
	if query == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	normalizedFilter, err := normalizeSemanticFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Filter = normalizedFilter
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 10
	}
	if limit > semanticMaxPageSize {
		return nil, status.Errorf(codes.ResourceExhausted, "limit must be <= %d", semanticMaxPageSize)
	}
	bindingKey := strings.TrimSpace(req.GetEmbeddingBindingKey())
	if bindingKey != "" && strings.TrimSpace(req.GetSemanticRuleId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "embedding_binding_key requires semantic_rule_id")
	}
	trace.SetStage("resolve-rules")
	selectedRuleIDs, err := s.resolveSearchRules(ctx, spaceID, domainID, req.GetSemanticRuleId())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	actorID, err := parseIdentityPrincipalID(principal.PrincipalID)
	if err != nil {
		return nil, err
	}
	trace.SetStage("read-graph-revision")
	graphRevision, err := s.semanticGraphRevision(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "read semantic graph revision")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	trace.SetStage("read-semantic-view")
	semanticView, err := s.semanticViewFingerprint(ctx, spaceID, domainID, selectedRuleIDs, bindingKey)
	if err != nil {
		return nil, mapSemanticError(err, "read semantic index view")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	offset, cursor, err := semanticPageOffsetWithKey(req, principal.PrincipalID, graphRevision, semanticView, s.cursorIntegrityKey())
	if err != nil {
		if strings.HasPrefix(err.Error(), "invalid semantic page token") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if offset > semanticMaxPaginationRecords {
		return nil, status.Error(codes.ResourceExhausted, "semantic page token position exceeds the bounded result window")
	}
	minScore := 0.0
	if req.MinScore != nil {
		minScore = req.GetMinScore()
	}
	plannerMinScore := minScore
	if normalizedFilter != nil {
		plannerMinScore = -math.MaxFloat64
	}
	trace.SetStage("execute-semantic-search")
	result, err := s.semantic.Search(ctx, daemonsemantic.SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: selectedRuleIDs, EmbeddingBindingKey: bindingKey, Text: query, Limit: semanticMaxCandidateExamination, MinScore: plannerMinScore, ActorPrincipalID: actorID})
	if err != nil {
		return nil, mapSemanticError(err, "semantic search")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	trace.SetStage("hydrate-graph-results")
	nodeByID, warnings := s.loadSearchNodes(ctx, principal.PrincipalID, spaceID, domainID, result.Results)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]*clientv1.SemanticSearchResult, 0, len(result.Results))
	examinedAuthorizedCandidates := 0
	eligibleCandidates := 0
	for _, item := range result.Results {
		node := nodeByID[item.NodeID]
		if node == nil {
			continue
		}
		examinedAuthorizedCandidates++
		if !semanticNodeEligible(node, normalizedFilter) {
			continue
		}
		eligibleCandidates++
		if req.MinScore != nil && item.Score < minScore {
			continue
		}
		matched := recordIDsForResult(item)
		out = append(out, &clientv1.SemanticSearchResult{SemanticRuleId: item.SemanticRuleID.String(), EmbeddingBindingKey: item.EmbeddingBindingKey, RecordId: item.RecordID.String(), NodeId: item.NodeID.String(), Score: item.Score, Node: node, MatchedChunkIds: matched, Snippet: semanticSnippet(nodePayloadText(node)), Provenance: mapSemanticResultProvenance(item)})
	}
	if err := semanticCandidateBoundError(normalizedFilter != nil, result, examinedAuthorizedCandidates); err != nil {
		return nil, err
	}
	trace.SetStage("recheck-graph-revision")
	finalGraphRevision, err := s.semanticGraphRevision(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "recheck semantic graph revision")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if finalGraphRevision != graphRevision {
		return nil, status.Error(codes.Aborted, "semantic graph view changed while building the page")
	}
	candidateFingerprint := semanticCandidateFingerprint(out)
	if strings.TrimSpace(req.GetPageToken()) != "" && subtle.ConstantTimeCompare([]byte(cursor.CandidateFingerprint), []byte(candidateFingerprint)) != 1 {
		return nil, status.Error(codes.FailedPrecondition, "semantic page token was invalidated because the candidate ranking changed")
	}
	if offset > len(out) {
		return nil, status.Error(codes.FailedPrecondition, "semantic page token position is beyond the current candidate set")
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	page := out[offset:end]
	nextPageToken := ""
	if end < len(out) {
		nextPageToken, err = encodeSemanticNextPageTokenWithKey(req, principal.PrincipalID, graphRevision, semanticView, candidateFingerprint, end, s.cursorIntegrityKey())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode semantic page token: %v", err)
		}
	}
	allWarnings := append([]string{}, result.Warnings...)
	allWarnings = append(allWarnings, warnings...)
	filterMode := ""
	if normalizedFilter != nil {
		filterMode = "authorized_candidate_post_filter"
	}
	trace.SetStage("complete")
	return &clientv1.SemanticSearchResponse{Results: page, Warnings: allWarnings, NextPageToken: nextPageToken, FilterExecutionMode: filterMode, ExaminedSemanticCandidates: int32(examinedAuthorizedCandidates), EligibleSemanticCandidates: int32(eligibleCandidates)}, nil
}

func mapSemanticResultProvenance(item semanticsearch.SearchResult) *clientv1.SemanticResultProvenance {
	// Configured catalog route identity is known. Provider-observed route
	// identity is intentionally unknown until a connector returns independent
	// provider response metadata; configured IDs must never be relabeled actual.
	return &clientv1.SemanticResultProvenance{
		SourceGraphRevision: item.SourceGraphRevision, SemanticGeneration: item.SemanticGeneration,
		SemanticConfigurationHash: item.SemanticConfigurationHash, OpaqueSourceVersion: item.RecordID.String(), Dimensions: int32(item.Dimensions), VectorSpaceKey: item.VectorSpaceKey,
		ConfiguredIntelligenceProfileId: semanticUUIDText(uuid.UUID(item.IntelligenceProfileID)), ConfiguredModelEndpointId: semanticUUIDText(uuid.UUID(item.ModelEndpointID)), ConfiguredModelId: semanticUUIDText(uuid.UUID(item.ModelID)), ConfiguredCapabilityId: semanticUUIDText(uuid.UUID(item.ModelEndpointCapabilityID)),
		ActualRouteKnown: false, RebuildEpoch: item.RebuildEpoch,
		GeneratedAt: item.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func semanticUUIDText(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
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
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, spaceID.String(), domainID.String(), "")
	if err != nil {
		return principalUser{}, uuid.Nil, uuid.Nil, mapDomainError(err, "semantic authorize domain")
	}
	if !graph.DomainExplicitSemanticSearchable(domain) {
		return principalUser{}, uuid.Nil, uuid.Nil, status.Error(codes.FailedPrecondition, "domain is excluded from semantic search and indexing")
	}
	return principalUser{PrincipalID: principal.PrincipalID}, spaceID, domainID, nil
}

type principalUser struct{ PrincipalID string }

func (s *SemanticService) semanticRuleSummaryMetadata(ctx context.Context, spaceID domainspace.SpaceID) (map[domainsemantic.SemanticRuleID]domainsemantic.SemanticRuleState, map[string]domainsemantic.SemanticSearchIndexState, map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend, error) {
	spaceMgr, err := s.semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, nil, nil, err
	}
	ruleStates, err := spaceMgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	stateByRule := map[domainsemantic.SemanticRuleID]domainsemantic.SemanticRuleState{}
	for _, state := range ruleStates {
		stateByRule[state.SemanticRuleID] = state
	}
	searchStates, err := spaceMgr.ListSearchIndexStates(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	searchByBinding := map[string]domainsemantic.SemanticSearchIndexState{}
	for _, state := range searchStates {
		searchByBinding[ruleBindingKey(state.SemanticRuleID, state.EmbeddingBindingKey)] = state
	}
	storeRows, err := s.semantic.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	stores := map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend{}
	for _, store := range storeRows {
		stores[store.ID] = store
	}
	return stateByRule, searchByBinding, stores, nil
}

func (s *SemanticService) resolveSearchRules(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, rawRuleID string) ([]domainsemantic.SemanticRuleID, error) {
	rules, err := s.semantic.ListRules(ctx, spaceID, domainID)
	if err != nil {
		return nil, mapSemanticError(err, "resolve semantic rules")
	}
	if strings.TrimSpace(rawRuleID) == "" {
		out := []domainsemantic.SemanticRuleID{}
		for _, rule := range rules {
			if rule.Enabled && ruleHasSearchBinding(rule) {
				out = append(out, rule.ID)
			}
		}
		if len(out) == 0 {
			return nil, status.Error(codes.FailedPrecondition, "no enabled semantic search rule is available for the domain")
		}
		return out, nil
	}
	id, err := parseSemanticRuleID(rawRuleID)
	if err != nil {
		return nil, err
	}
	for _, rule := range rules {
		if rule.ID == id {
			if !rule.Enabled {
				return nil, status.Error(codes.FailedPrecondition, "semantic rule is disabled")
			}
			if !ruleHasSearchBinding(rule) {
				return nil, status.Error(codes.FailedPrecondition, "semantic rule has no enabled search binding")
			}
			return []domainsemantic.SemanticRuleID{id}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "semantic rule not found")
}

func ruleHasSearchBinding(rule domainsemantic.SemanticGenerationRule) bool {
	for _, raw := range rule.Embeddings {
		binding := domainsemantic.NormalizeSemanticEmbeddingBinding(raw)
		if binding.Enabled && domainsemantic.IsSearchSemanticIndexPurpose(domainsemantic.SemanticIndexPurpose(binding.Purpose)) {
			return true
		}
	}
	return false
}

func (s *SemanticService) loadSearchNodes(ctx context.Context, principalID string, spaceID domainspace.SpaceID, domainID graph.DomainID, results []semanticsearch.SearchResult) (map[graph.NodeID]*clientv1.Node, []string) {
	out := map[graph.NodeID]*clientv1.Node{}
	warnings := []string{}
	if len(results) == 0 {
		return out, nil
	}
	if s.graphs == nil {
		return out, []string{"graph service is not configured"}
	}
	tx, err := readOnlySearchGraphTransaction(ctx, s.graphs, "semantic-search-", principalID, spaceID, domainID)
	if err != nil {
		return out, []string{fmt.Sprintf("graph revision snapshot unavailable: %v", err)}
	}
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

func readOnlySearchGraphTransaction(ctx context.Context, graphs daegraph.Manager, prefix string, principalID string, spaceID domainspace.SpaceID, domainID graph.DomainID) (sessionmodel.GraphTransaction, error) {
	var baseRevision int64
	var err error
	if revisions, ok := graphs.(TransactionGraphDomainRevisioner); ok {
		baseRevision, err = revisions.CurrentDomainRevision(ctx, spaceID.String(), domainID.String())
	} else {
		baseRevision, err = graphs.CurrentRevision(ctx, spaceID.String())
	}
	if err != nil {
		return sessionmodel.GraphTransaction{}, err
	}
	now := time.Now().UTC()
	return sessionmodel.GraphTransaction{ID: prefix + uuid.NewString(), PrincipalID: principalID, SpaceID: spaceID.String(), DomainID: domainID.String(), Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Minute)}, nil
}

func MapSemanticRuleSummaryProto(rule domainsemantic.SemanticGenerationRule, state domainsemantic.SemanticRuleState, searchByBinding map[string]domainsemantic.SemanticSearchIndexState, stores map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend) *clientv1.SemanticGenerationRuleSummary {
	bindings := make([]*clientv1.SemanticEmbeddingBindingSummary, 0, len(rule.Embeddings))
	for _, raw := range rule.Embeddings {
		binding := domainsemantic.NormalizeSemanticEmbeddingBinding(raw)
		storeKey := strings.TrimSpace(binding.VectorStore)
		if store, ok := stores[binding.VectorStoreID]; ok {
			storeKey = firstNonEmptyString(store.Key, store.Name, store.ID.String())
		}
		bindings = append(bindings, &clientv1.SemanticEmbeddingBindingSummary{Key: binding.Key, Purpose: binding.Purpose, IntelligenceProfileId: semanticUUIDString(binding.IntelligenceProfileID), IntelligenceProfileKey: binding.IntelligenceProfile, VectorStoreId: semanticUUIDString(binding.VectorStoreID), VectorStoreKey: storeKey, Enabled: binding.Enabled, SearchIndex: mapSearchIndexStatus(searchByBinding[ruleBindingKey(rule.ID, binding.Key)])})
	}
	return &clientv1.SemanticGenerationRuleSummary{SemanticRuleId: rule.ID.String(), Key: rule.Key, DisplayName: rule.DisplayName, Description: rule.Description, SpaceId: rule.SpaceID.String(), DomainId: rule.DomainID.String(), Enabled: rule.Enabled, State: mapSemanticRuleState(rule, state), Bindings: bindings, Status: mapSemanticRuleStatus(state)}
}

func mapSemanticRuleState(rule domainsemantic.SemanticGenerationRule, state domainsemantic.SemanticRuleState) clientv1.SemanticRuleState {
	if !rule.Enabled {
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_DISABLED
	}
	switch strings.ToLower(strings.TrimSpace(state.State)) {
	case "building", "running", "backfilling":
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_BUILDING
	case "stale", "dirty":
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_STALE
	case "disabled":
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_DISABLED
	case "error", "failed":
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_ERROR
	default:
		return clientv1.SemanticRuleState_SEMANTIC_RULE_STATE_ACTIVE
	}
}

func mapSemanticRuleStatus(state domainsemantic.SemanticRuleState) *clientv1.SemanticRuleStatus {
	return &clientv1.SemanticRuleStatus{QueueDepthPending: int32(state.DirtyCount), LastRefreshAt: timeString(state.LastRefreshAt), LastBackfillAt: timeString(state.LastBackfillAt), LastError: state.LastError}
}

func mapSearchIndexStatus(state domainsemantic.SemanticSearchIndexState) *clientv1.SearchIndexStatus {
	return &clientv1.SearchIndexStatus{State: mapSearchIndexState(state.State), LiveRecordCount: state.LiveRecordCount, LastRebuildAt: timeString(state.LastRebuildAt), LastError: state.LastError}
}

func mapSearchIndexState(raw string) clientv1.SearchIndexState {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "ready", "active":
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_READY
	case "building", "rebuilding":
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_BUILDING
	case "degraded", "stale":
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_DEGRADED
	case "missing", "":
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_MISSING
	case "error", "failed":
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_ERROR
	default:
		return clientv1.SearchIndexState_SEARCH_INDEX_STATE_UNSPECIFIED
	}
}

func recordIDsForResult(item semanticsearch.SearchResult) []string {
	ids := []string{}
	seen := map[string]bool{}
	for _, id := range item.MatchedRecordIDs {
		if id == uuid.Nil {
			continue
		}
		value := id.String()
		if !seen[value] {
			ids = append(ids, value)
			seen[value] = true
		}
	}
	if len(ids) == 0 && item.RecordID != uuid.Nil {
		ids = append(ids, item.RecordID.String())
	}
	return ids
}

func ruleBindingKey(ruleID domainsemantic.SemanticRuleID, bindingKey string) string {
	return ruleID.String() + "/" + strings.ToLower(strings.TrimSpace(bindingKey))
}

func timeString(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func semanticUUIDString(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func nodePayloadText(node *clientv1.Node) string {
	if node == nil || node.GetPayload() == nil {
		return ""
	}
	value, _ := node.GetPayload().AsMap()["text"].(string)
	return value
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

func parseSemanticRuleID(raw string) (domainsemantic.SemanticRuleID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "semantic_rule_id must be a UUID")
	}
	return domainsemantic.SemanticRuleID(id), nil
}

func semanticCandidateBoundError(hasHardFilter bool, result semanticsearch.Result, examinedAuthorizedCandidates int) error {
	if (hasHardFilter && result.CandidateScanTruncated) || len(result.Results) >= semanticMaxCandidateExamination || examinedAuthorizedCandidates > semanticMaxPaginationRecords {
		return status.Error(codes.ResourceExhausted, "direct semantic candidate examination exceeded the bounded pagination window")
	}
	return nil
}

func parseIdentityPrincipalID(raw string) (identity.PrincipalID, error) {
	id, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || id == uuid.Nil {
		return "", status.Error(codes.Unauthenticated, "invalid principal")
	}
	return identity.PrincipalID(id.String()), nil
}

func mapSemanticError(err error, action string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(msg, "required") || strings.Contains(msg, "invalid") || strings.Contains(msg, "no enabled") || strings.Contains(msg, "skipped") || strings.Contains(msg, "disabled") {
		return status.Error(codes.FailedPrecondition, msg)
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
