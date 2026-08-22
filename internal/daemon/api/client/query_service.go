package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
	schemacompile "github.com/myceldb/mycel/internal/schema/compile"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	queryMaxPageSize          = 500
	defaultSubtreeMaxNodes    = 10000
	defaultSubtreeMaxEdges    = 50000
	indexedSubtreePlanName    = "OrderedNodePropertyIndexScan+EdgeAdjacencyIndexScan"
	indexedSubtreeCursorKind  = "root_index_key"
	shapedIndexCursorPrefix   = "shapeidx:"
	indexedSubtreeMaxDepthCap = 64
	indexedPathMaxStartNodes  = 1000
	indexedPathMaxRows        = 10000
	indexedPathMaxNodesLoaded = 50000
	indexedPathMaxEdgesLoaded = 50000
)

type semanticQuerySearcher interface {
	Search(ctx context.Context, in daemonsemantic.SearchInput) (semanticsearch.Result, error)
}

type QueryService struct {
	clientv1.UnimplementedQueryServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
	spaces   daemonspace.Manager
	schemas  schemaservice.Manager
	semantic semanticQuerySearcher
	router   ClientRequestRouter
}

func NewQueryService(sessions daemonsession.Manager, graphs daegraph.Manager, spaces daemonspace.Manager) *QueryService {
	return &QueryService{sessions: sessions, graphs: graphs, spaces: spaces}
}

func (s *QueryService) WithSchemaManager(manager schemaservice.Manager) *QueryService {
	s.schemas = manager
	return s
}

func (s *QueryService) WithSemanticManager(manager semanticQuerySearcher) *QueryService {
	s.semantic = manager
	return s
}

func (s *QueryService) WithClientRequestRouter(router ClientRequestRouter) *QueryService {
	s.router = router
	return s
}

func (s *QueryService) ExecuteQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest) (*clientv1.ExecuteQueryResponse, error) {
	executionStart := time.Now()
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteQueryResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteQuery_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute query")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	if req.GetQuery() == nil || req.GetQuery().GetMatch() == nil || req.GetQuery().GetMatch().GetStart() == nil {
		return nil, status.Error(codes.InvalidArgument, "query.match.start is required")
	}
	if err := validateStructuredQuerySafety(req.GetQuery()); err != nil {
		return nil, err
	}
	domain, err := s.visibleTransactionDomain(ctx, principal.PrincipalID, tx)
	if err != nil {
		return nil, err
	}
	if !domaingraph.DomainBroadSearchable(domain) && !isIndexedAdjacencyQuery(req.GetQuery()) && !isIndexedPathQuery(req.GetQuery()) && !isIndexedRootSubtreeQuery(req.GetQuery()) && !isSemanticPredicateNodeQuery(req.GetQuery()) && !isIndexedPredicateNodeQuery(req.GetQuery()) && !isIndexedEqualityNodeQuery(req.GetQuery()) {
		return nil, status.Error(codes.FailedPrecondition, "domain is excluded from broad query execution")
	}
	schemaCtx, err := s.schemaContext(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := validateStructuredGraphQueryWithSchema(req.GetQuery(), schemaCtx); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	if indexed, res, err := s.tryExecuteIndexedQuery(readCtx, req, tx, schemaCtx, recorder); indexed || err != nil {
		return res, err
	}
	if !domaingraph.DomainBroadSearchable(domain) && queryHasSemanticPredicate(req.GetQuery().GetWhere()) {
		return nil, status.Error(codes.FailedPrecondition, "semantic predicate requires an enabled semantic index")
	}
	nodes, err := s.allNodes(readCtx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list nodes")
	}
	edges, err := s.allEdges(readCtx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list edges")
	}
	exec := newQueryExecution(nodes, edges)
	rows, err := exec.match(req.GetQuery().GetMatch(), req.GetQuery().GetWhere(), req.GetQuery().GetPathAlias())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out, next, err := exec.shapeAndProjectRows(rows, req.GetQuery(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result := queryResultFromRows(out, next)
	diagnostics := completeQueryDiagnostics(&clientv1.QueryDiagnostics{Plan: "BroadGraphFallback", PlanKind: "fallback", FullScan: true, NodesLoaded: int32(len(nodes)), EdgesLoaded: int32(len(edges)), RowsReturned: int32(len(out)), RowsProduced: int32(len(out)), RowsScanned: int32(len(rows)), CandidateCount: int32(len(rows)), NextCursorKind: "offset", FallbackMode: "broad_graph_fallback", ResidualPredicates: predicateDiagnostics(req.GetQuery().GetWhere())}, "fallback", executionStart, len(rows), len(out))
	return &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func (s *QueryService) ExecuteGQL(ctx context.Context, req *clientv1.ExecuteGQLRequest) (*clientv1.ExecuteGQLResponse, error) {
	executionStart := time.Now()
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteGQLResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteGQL_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	params := gqlParamsFromProto(req.GetParams())
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute gql")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	domain, err := s.visibleTransactionDomain(ctx, principal.PrincipalID, tx)
	if err != nil {
		return nil, err
	}
	schemaCtx, err := s.schemaContext(ctx, tx)
	if err != nil {
		return nil, err
	}
	plan, err := gql.CompileWithSchemaAndParams(req.GetQuery(), schemaCtx, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if plan.AccessMode == analysis.ReadWrite && tx.Mode != daemonsession.TransactionModeReadWrite {
		return nil, status.Error(codes.FailedPrecondition, "GQL query requires a read-write transaction")
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	if indexed, res, err := s.tryExecuteIndexedGQL(readCtx, tx, schemaCtx, plan, int(req.GetPageSize()), req.GetPageToken(), recorder); indexed || err != nil {
		if err == nil || status.Code(err) != codes.FailedPrecondition || !domaingraph.DomainBroadSearchable(domain) {
			return res, err
		}
	}
	if !domaingraph.DomainBroadSearchable(domain) && gqlPlanRequiresBroadFallback(plan) {
		return nil, status.Error(codes.FailedPrecondition, "domain is excluded from broad GQL fallback execution")
	}
	execResult, err := execution.Execute(readCtx, gqlDaemonGraph{service: s, tx: tx}, plan)
	if err != nil {
		return nil, mapGQLExecutionError(err)
	}
	pageExecResult, next, err := paginateExecResult(execResult, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageRows := gqlRowsToProto(pageExecResult)
	result := queryResultFromRowsWithCounters(pageRows, next, execResult.Counters)
	mergeExecPathGraph(result, pageExecResult)
	diagnostics := completeQueryDiagnostics(&clientv1.QueryDiagnostics{Plan: "BroadGQLFallback", PlanKind: "fallback", FullScan: true, RowsReturned: int32(len(pageRows)), RowsProduced: int32(len(pageRows)), RowsScanned: int32(len(execResult.Rows)), CandidateCount: int32(len(execResult.Rows)), NextCursorKind: "offset", FallbackMode: "broad_gql_fallback"}, "fallback", executionStart, len(execResult.Rows), len(pageRows))
	return &clientv1.ExecuteGQLResponse{Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func (s *QueryService) ExplainQuery(ctx context.Context, req *clientv1.ExplainQueryRequest) (*clientv1.ExplainQueryResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExplainQueryResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExplainQuery_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	start := time.Now()
	tx, domain, schemaCtx, err := s.explainContext(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	if err := validateStructuredGraphQueryWithSchema(req.GetQuery(), schemaCtx); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	diag := s.explainStructuredGraphQuery(req.GetQuery(), schemaCtx, domain)
	diag.ExplainOnly = true
	diag.PlanningMillis = time.Since(start).Milliseconds()
	return &clientv1.ExplainQueryResponse{Diagnostics: diag}, nil
}

func (s *QueryService) ExplainGQL(ctx context.Context, req *clientv1.ExplainGQLRequest) (*clientv1.ExplainGQLResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExplainGQLResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExplainGQL_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	start := time.Now()
	tx, domain, schemaCtx, err := s.explainContext(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	params := gqlParamsFromProto(req.GetParams())
	plan, err := gql.CompileWithSchemaAndParams(req.GetQuery(), schemaCtx, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	diag := s.explainGQLPlan(plan, schemaCtx, domain)
	diag.ExplainOnly = true
	diag.PlanningMillis = time.Since(start).Milliseconds()
	return &clientv1.ExplainGQLResponse{Diagnostics: diag}, nil
}

func (s *QueryService) ExecuteGQLScript(ctx context.Context, req *clientv1.ExecuteGQLScriptRequest) (*clientv1.ExecuteGQLScriptResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteGQLScriptResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteGQLScript_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetScript()) == "" {
		return nil, status.Error(codes.InvalidArgument, "script is required")
	}
	params := gqlParamsFromProto(req.GetParams())
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute gql script")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	domain, err := s.visibleTransactionDomain(ctx, principal.PrincipalID, tx)
	if err != nil {
		return nil, err
	}
	schemaCtx, err := s.schemaContext(ctx, tx)
	if err != nil {
		return nil, err
	}
	scriptPlan, err := gql.CompileScriptWithSchemaAndParams(req.GetScript(), schemaCtx, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if scriptPlan.AccessMode == analysis.ReadWrite && tx.Mode != daemonsession.TransactionModeReadWrite {
		return nil, status.Error(codes.FailedPrecondition, "GQL script requires a read-write transaction")
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	statementResults := []*clientv1.GQLStatementResult{}
	aggregate := &clientv1.QueryResult{Graph: &clientv1.ResultGraph{}, Counters: &clientv1.QueryCounters{}}
	for _, statement := range scriptPlan.Statements {
		if indexed, res, err := s.tryExecuteIndexedGQL(readCtx, tx, schemaCtx, statement.Plan, int(req.GetPageSize()), "", recorder); indexed || err != nil {
			if err != nil {
				statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: err.Error(), ReadMetadata: protoReadMetadata(recorder.Summary())})
				if req.GetStopOnError() {
					break
				}
				continue
			}
			result := res.GetResult()
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: true, Result: result, ReadMetadata: res.GetReadMetadata()})
			mergeQueryResult(aggregate, result)
			continue
		}
		if !domaingraph.DomainBroadSearchable(domain) && gqlPlanRequiresBroadFallback(statement.Plan) {
			err := status.Error(codes.FailedPrecondition, "domain is excluded from broad GQL fallback execution")
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: err.Error(), ReadMetadata: protoReadMetadata(recorder.Summary())})
			if req.GetStopOnError() {
				break
			}
			continue
		}
		execResult, err := execution.Execute(readCtx, gqlDaemonGraph{service: s, tx: tx}, statement.Plan)
		if err != nil {
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: mapGQLExecutionError(err).Error(), ReadMetadata: protoReadMetadata(recorder.Summary())})
			if req.GetStopOnError() {
				break
			}
			continue
		}
		pageExecResult, next, err := paginateExecResult(execResult, int(req.GetPageSize()), "")
		if err != nil {
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: err.Error()})
			if req.GetStopOnError() {
				break
			}
			continue
		}
		pageRows := gqlRowsToProto(pageExecResult)
		result := queryResultFromRowsWithCounters(pageRows, next, execResult.Counters)
		mergeExecPathGraph(result, pageExecResult)
		statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: true, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary())})
		mergeQueryResult(aggregate, result)
	}
	return &clientv1.ExecuteGQLScriptResponse{Statements: statementResults, Result: aggregate, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func gqlParamsFromProto(params map[string]*structpb.Value) map[string]any {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		if value == nil {
			out[key] = nil
			continue
		}
		out[key] = value.AsInterface()
	}
	return out
}

func mapGQLExecutionError(err error) error {
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	return status.Error(codes.InvalidArgument, err.Error())
}

func (s *QueryService) visibleTransactionDomain(ctx context.Context, principalID string, tx daemonsession.GraphTransaction) (domaingraph.Domain, error) {
	domain, err := s.spaces.GetVisibleDomain(ctx, principalID, tx.SpaceID, tx.DomainID, "")
	if err != nil {
		return domaingraph.Domain{}, mapDomainError(err, "query domain")
	}
	return domain, nil
}

func validateStructuredQuerySafety(query *clientv1.GraphQuery) error {
	if query == nil || query.GetMatch() == nil {
		return nil
	}
	for _, step := range query.GetMatch().GetSteps() {
		minDepth, maxDepth, err := traversalDepthBounds(step.GetDepth())
		if err != nil {
			return err
		}
		if maxDepth < 0 {
			return status.Error(codes.FailedPrecondition, "unbounded structured traversal is not supported; set depth.max_depth")
		}
		if maxDepth < minDepth {
			return status.Error(codes.InvalidArgument, "traversal max_depth must be >= min_depth")
		}
		if maxDepth > indexedSubtreeMaxDepthCap {
			return status.Errorf(codes.FailedPrecondition, "traversal max_depth must be <= %d", indexedSubtreeMaxDepthCap)
		}
	}
	return nil
}

func gqlPlanRequiresBroadFallback(plan planmodel.Plan) bool {
	for _, op := range plan.Operations {
		switch op.(type) {
		case planmodel.InsertNodeOperation:
			continue
		default:
			return true
		}
	}
	return false
}

func (s *QueryService) explainContext(ctx context.Context, transactionID string) (daemonsession.GraphTransaction, domaingraph.Domain, analysis.SchemaContext, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return daemonsession.GraphTransaction{}, domaingraph.Domain{}, analysis.SchemaContext{}, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, transactionID)
	if err != nil {
		return daemonsession.GraphTransaction{}, domaingraph.Domain{}, analysis.SchemaContext{}, mapSessionError(err, "explain query")
	}
	domain, err := s.visibleTransactionDomain(ctx, principal.PrincipalID, tx)
	if err != nil {
		return tx, domaingraph.Domain{}, analysis.SchemaContext{}, err
	}
	schemaCtx, err := s.schemaContext(ctx, tx)
	if err != nil {
		return tx, domain, analysis.SchemaContext{}, err
	}
	return tx, domain, schemaCtx, nil
}

func baseQueryDiagnostics() *clientv1.QueryDiagnostics {
	return &clientv1.QueryDiagnostics{Planner: "mycel-query", PlannerVersion: "qpc8"}
}

func completeQueryDiagnostics(diag *clientv1.QueryDiagnostics, planKind string, executionStart time.Time, rowsScanned int, rowsProduced int) *clientv1.QueryDiagnostics {
	if diag == nil {
		diag = baseQueryDiagnostics()
	}
	if diag.Planner == "" {
		diag.Planner = "mycel-query"
	}
	if diag.PlannerVersion == "" {
		diag.PlannerVersion = "qpc8"
	}
	if diag.PlanKind == "" {
		diag.PlanKind = planKind
	}
	if rowsScanned > 0 && diag.RowsScanned == 0 {
		diag.RowsScanned = int32(rowsScanned)
	}
	if rowsProduced > 0 {
		diag.RowsProduced = int32(rowsProduced)
		if diag.RowsReturned == 0 {
			diag.RowsReturned = int32(rowsProduced)
		}
	}
	if diag.CandidateCount == 0 {
		switch {
		case diag.NodesLoaded > 0:
			diag.CandidateCount = diag.NodesLoaded
		case diag.IndexEntriesScanned > 0:
			diag.CandidateCount = diag.IndexEntriesScanned
		case rowsScanned > 0:
			diag.CandidateCount = int32(rowsScanned)
		}
	}
	if !executionStart.IsZero() && diag.ExecutionMillis == 0 {
		diag.ExecutionMillis = time.Since(executionStart).Milliseconds()
	}
	return diag
}

func rejectPlanDiagnostics(reason string) *clientv1.QueryDiagnostics {
	diag := baseQueryDiagnostics()
	diag.Plan = "Rejected"
	diag.PlanKind = "rejected"
	diag.RejectedReason = reason
	diag.FullScan = false
	return diag
}

func (s *QueryService) explainStructuredGraphQuery(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext, domain domaingraph.Domain) *clientv1.QueryDiagnostics {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil {
		return rejectPlanDiagnostics("query match.start is required")
	}
	if err := validateStructuredQuerySafety(query); err != nil {
		return rejectPlanDiagnostics(err.Error())
	}
	if diag, ok := s.explainAcceptedStructuredIndexQuery(query, schemaCtx); ok {
		return diag
	}
	if !domaingraph.DomainBroadSearchable(domain) {
		return rejectPlanDiagnostics("query shape requires broad fallback but domain is not broad-searchable")
	}
	diag := baseQueryDiagnostics()
	diag.Plan = "BroadGraphFallback"
	diag.PlanKind = "fallback"
	diag.FullScan = true
	diag.FallbackMode = "broad_graph_fallback"
	diag.ResidualPredicates = predicateDiagnostics(query.GetWhere())
	return diag
}

func (s *QueryService) explainAcceptedStructuredIndexQuery(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext) (*clientv1.QueryDiagnostics, bool) {
	if isIndexedRootSubtreeQuery(query) || (len(query.GetOrderBy()) > 0 && len(query.GetMatch().GetSteps()) == 0) {
		return s.explainOrderedIndexQuery(query, schemaCtx), true
	}
	if isIndexedPathQuery(query) {
		diag := baseQueryDiagnostics()
		diag.Plan = "IndexedMultiHopAdjacencyPathScan"
		diag.PlanKind = "indexed_path"
		diag.FullScan = false
		diag.NextCursorKind = "offset"
		diag.Indexes = []string{"adjacency"}
		diag.PushedPredicates = predicateDiagnostics(query.GetWhere())
		return diag, true
	}
	if isIndexedAdjacencyQuery(query) {
		step := query.GetMatch().GetSteps()[0]
		direction := "out"
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
			direction = "in"
		}
		diag := baseQueryDiagnostics()
		diag.Plan = "EdgeAdjacencyIndexScan"
		diag.PlanKind = "indexed_adjacency"
		diag.Indexes = []string{direction + ":" + step.GetEdgeKind()}
		diag.FullScan = false
		diag.NextCursorKind = "adjacency_key"
		return diag, true
	}
	if isIndexedEqualityNodeQuery(query) {
		field, _, ok, err := indexedEqualityPredicate(query.GetWhere(), query.GetMatch().GetStart().GetAlias())
		if err != nil || !ok {
			return rejectPlanDiagnostics("indexed equality predicate could not be planned"), false
		}
		return s.explainOrderedFieldIndex(query, schemaCtx, "OrderedNodePropertyEqualityIndexScan", "indexed_equality", field), true
	}
	if isSemanticPredicateNodeQuery(query) {
		diag := baseQueryDiagnostics()
		diag.Plan = "SemanticVectorSearch"
		diag.PlanKind = "semantic_vector"
		diag.FullScan = false
		diag.PushedPredicates = predicateDiagnostics(query.GetWhere())
		return diag, true
	}
	if isIndexedPredicateNodeQuery(query) {
		return s.explainIndexedPredicateQuery(query, schemaCtx), true
	}
	return nil, false
}

func (s *QueryService) explainOrderedIndexQuery(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext) *clientv1.QueryDiagnostics {
	start := query.GetMatch().GetStart()
	if len(query.GetOrderBy()) != 1 || len(start.GetLabels()) != 1 {
		return rejectPlanDiagnostics("ORDER BY requires one ordered single-label node query")
	}
	prop := query.GetOrderBy()[0].GetValue().GetProp()
	if prop == nil || prop.GetAlias() != start.GetAlias() || strings.TrimSpace(prop.GetName()) == "" {
		return rejectPlanDiagnostics("ORDER BY requires an indexed property reference on the start alias")
	}
	if isIndexedRootSubtreeQuery(query) {
		return s.explainOrderedFieldIndex(query, schemaCtx, indexedSubtreePlanName, "indexed_subtree", prop.GetName())
	}
	return s.explainOrderedFieldIndex(query, schemaCtx, "OrderedNodePropertyIndexScan", "indexed_order", prop.GetName())
}

func (s *QueryService) explainOrderedFieldIndex(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext, plan string, kind string, field string) *clientv1.QueryDiagnostics {
	start := query.GetMatch().GetStart()
	diag := baseQueryDiagnostics()
	diag.Plan = plan
	diag.PlanKind = kind
	diag.FullScan = false
	diag.NextCursorKind = "index_key"
	diag.PushedPredicates = predicateDiagnostics(query.GetWhere())
	if schemaCtx.Schema == nil {
		diag.Plan = "Rejected"
		diag.PlanKind = "rejected"
		diag.RejectedReason = "indexed query requires an active schema with an ordered index"
		diag.RequiredIndex = start.GetLabels()[0] + ".properties." + field
		return diag
	}
	idx, ok := findOrderedNodeIndex(*schemaCtx.Schema, start.GetLabels()[0], field)
	if !ok {
		diag.Plan = "Rejected"
		diag.PlanKind = "rejected"
		diag.RejectedReason = fmt.Sprintf("no ordered index for %s.properties.%s", start.GetLabels()[0], field)
		diag.RequiredIndex = start.GetLabels()[0] + ".properties." + field
		return diag
	}
	diag.Indexes = []string{idx.Name}
	if kind == "indexed_subtree" && len(query.GetMatch().GetSteps()) > 0 {
		step := query.GetMatch().GetSteps()[0]
		direction := "out"
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
			direction = "in"
		}
		diag.Indexes = append(diag.Indexes, direction+":"+step.GetEdgeKind())
		diag.NextCursorKind = indexedSubtreeCursorKind
	}
	return diag
}

func (s *QueryService) explainIndexedPredicateQuery(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext) *clientv1.QueryDiagnostics {
	start := query.GetMatch().GetStart()
	diag := baseQueryDiagnostics()
	diag.Plan = "OrderedNodePropertyPredicateIndexScan"
	diag.PlanKind = "indexed_predicate"
	diag.FullScan = false
	diag.NextCursorKind = "offset"
	diag.PushedPredicates = predicateDiagnostics(query.GetWhere())
	branches, _, _ := indexedPredicateBranches(query.GetWhere(), start.GetAlias())
	if schemaCtx.Schema == nil {
		diag.Plan = "Rejected"
		diag.PlanKind = "rejected"
		diag.RejectedReason = "indexed predicate query requires an active schema with ordered indexes"
		return diag
	}
	seen := map[string]struct{}{}
	for _, branch := range branches {
		for _, scan := range branch.scans {
			idx, ok := findOrderedNodeIndex(*schemaCtx.Schema, start.GetLabels()[0], scan.field)
			if !ok {
				diag.Plan = "Rejected"
				diag.PlanKind = "rejected"
				diag.RejectedReason = fmt.Sprintf("no ordered index for %s.properties.%s", start.GetLabels()[0], scan.field)
				diag.RequiredIndex = start.GetLabels()[0] + ".properties." + scan.field
				return diag
			}
			seen[idx.Name] = struct{}{}
		}
	}
	for index := range seen {
		diag.Indexes = append(diag.Indexes, index)
	}
	sort.Strings(diag.Indexes)
	return diag
}

func (s *QueryService) explainGQLPlan(plan planmodel.Plan, schemaCtx analysis.SchemaContext, domain domaingraph.Domain) *clientv1.QueryDiagnostics {
	if len(plan.Operations) == 0 {
		return rejectPlanDiagnostics("GQL plan has no operations")
	}
	if len(plan.Operations) != 1 {
		diag := baseQueryDiagnostics()
		diag.Plan = "GQLScriptPlan"
		diag.PlanKind = "gql_script"
		return diag
	}
	switch op := plan.Operations[0].(type) {
	case planmodel.QueryNodesOperation:
		query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Variable, Labels: append([]string(nil), op.Labels...)}}, Returns: gqlStructuredReturns(op.Returns), AggregateReturns: gqlStructuredAggregates(op.Aggregates), Distinct: op.Distinct, Offset: int32(op.Offset), Limit: int32(op.Limit)}
		if len(op.OrderBy) > 0 {
			order := op.OrderBy[0]
			direction := clientv1.SortDirection_SORT_DIRECTION_ASC
			if order.Direction == planmodel.SortDescending {
				direction = clientv1.SortDirection_SORT_DIRECTION_DESC
			}
			query.OrderBy = []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: order.Variable, Name: order.Property}}}, Direction: direction}}
		}
		if len(op.ComparisonPredicates) > 0 || len(op.NullPredicates) > 0 || len(op.StringPredicates) > 0 || len(op.TextPredicates) > 0 || len(op.SemanticPredicates) > 0 {
			query.Where = gqlPredicatesToStructured(op)
		}
		return s.explainStructuredGraphQuery(query, schemaCtx, domain)
	case planmodel.QueryPathOperation:
		query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...)}, Steps: gqlPathSteps(op.Segments)}, PathAlias: op.PathVariable, Returns: gqlStructuredReturnsWithPath(op.Returns, op.PathVariable), Distinct: op.Distinct, Offset: int32(op.Offset), Limit: int32(op.Limit)}
		if where, ok := gqlPathPredicatesToStructured(op); ok {
			query.Where = where
		}
		return s.explainStructuredGraphQuery(query, schemaCtx, domain)
	case planmodel.InsertNodeOperation, planmodel.MergeNodeOperation, planmodel.MatchSetOperation, planmodel.MatchDeleteOperation, planmodel.MatchCreateRelationshipOperation, planmodel.MatchMergeRelationshipOperation:
		diag := baseQueryDiagnostics()
		diag.Plan = fmt.Sprintf("%T", op)
		diag.PlanKind = "gql_mutation"
		diag.FullScan = false
		return diag
	default:
		if !domaingraph.DomainBroadSearchable(domain) {
			return rejectPlanDiagnostics("GQL plan requires broad fallback but domain is not broad-searchable")
		}
		diag := baseQueryDiagnostics()
		diag.Plan = "BroadGQLFallback"
		diag.PlanKind = "fallback"
		diag.FullScan = true
		diag.FallbackMode = "broad_gql_fallback"
		return diag
	}
}

func predicateDiagnostics(expr *clientv1.Expr) []string {
	if expr == nil {
		return nil
	}
	desc := queryExprDescription(expr)
	if desc == "" {
		return nil
	}
	return []string{desc}
}

func queryExprDescription(expr *clientv1.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.GetExpr().(type) {
	case *clientv1.Expr_HasTag:
		return "has_tag(" + e.HasTag.GetAlias() + ")"
	case *clientv1.Expr_PropertyExists:
		return "exists(" + e.PropertyExists.GetAlias() + "." + e.PropertyExists.GetName() + ")"
	case *clientv1.Expr_PropertyEquals:
		return "equals(" + e.PropertyEquals.GetAlias() + "." + e.PropertyEquals.GetName() + ")"
	case *clientv1.Expr_LessThan:
		return "less_than"
	case *clientv1.Expr_Between:
		return "between"
	case *clientv1.Expr_And:
		return "and"
	case *clientv1.Expr_Or:
		return "or"
	case *clientv1.Expr_Null:
		return "null(" + e.Null.GetAlias() + "." + e.Null.GetName() + ")"
	case *clientv1.Expr_StringPredicate:
		return "string_predicate"
	case *clientv1.Expr_Text:
		return "text(" + e.Text.GetAlias() + "." + e.Text.GetField() + ")"
	case *clientv1.Expr_Semantic:
		return "semantic(" + e.Semantic.GetAlias() + ")"
	default:
		return "predicate"
	}
}

func gqlPredicatesToStructured(op planmodel.QueryNodesOperation) *clientv1.Expr {
	if len(op.SemanticPredicates) > 0 {
		sem := op.SemanticPredicates[0]
		return &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: sem.Variable, Query: sem.Query, Limit: int32(sem.TopK)}}}
	}
	if len(op.TextPredicates) > 0 {
		text := op.TextPredicates[0]
		return &clientv1.Expr{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: text.Variable, Field: text.Property, Query: text.Query}}}
	}
	if len(op.ComparisonPredicates) > 0 {
		cmp := op.ComparisonPredicates[0]
		if cmp.Operator == planmodel.ComparisonEqual {
			return &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: cmp.Variable, Name: cmp.Property, Value: protoValue(cmp.Value)}}}
		}
	}
	return nil
}

func gqlPathSteps(segments []planmodel.PathSegment) []*clientv1.TraversalStep {
	steps := make([]*clientv1.TraversalStep, 0, len(segments))
	for _, segment := range segments {
		direction := clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT
		if segment.Relationship.Direction == planmodel.RelationshipIncoming {
			direction = clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN
		}
		maxDepth := int32(1)
		minDepth := int32(1)
		if segment.Relationship.Quantifier.Min != 0 || segment.Relationship.Quantifier.Max != 0 {
			minDepth = int32(segment.Relationship.Quantifier.Min)
			maxDepth = int32(segment.Relationship.Quantifier.Max)
		}
		steps = append(steps, &clientv1.TraversalStep{Direction: direction, EdgeKind: firstString(segment.Relationship.Labels), Depth: &clientv1.DepthSpec{MinDepth: minDepth, MaxDepth: maxDepth}, Target: &clientv1.NodePattern{Alias: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...)}})
	}
	return steps
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *QueryService) schemaContext(ctx context.Context, tx daemonsession.GraphTransaction) (analysis.SchemaContext, error) {
	if s.schemas == nil || strings.TrimSpace(tx.DomainID) == "" {
		return analysis.SchemaContext{}, nil
	}
	domainID, err := uuid.Parse(tx.DomainID)
	if err != nil {
		return analysis.SchemaContext{}, status.Error(codes.InvalidArgument, "invalid transaction domain id")
	}
	schemaDoc, err := s.schemas.GetDomainSchema(ctx, domaingraph.DomainID(domainID))
	if errors.Is(err, schemaservice.ErrSchemaNotFound) {
		return analysis.SchemaContext{}, nil
	}
	if err != nil {
		return analysis.SchemaContext{}, status.Errorf(codes.Internal, "load domain schema: %v", err)
	}
	return analysis.SchemaContext{Schema: &schemaDoc}, nil
}

func (s *QueryService) allNodes(ctx context.Context, tx daemonsession.GraphTransaction) ([]domaingraph.Node, error) {
	all := []domaingraph.Node{}
	token := ""
	for {
		nodes, next, err := s.graphs.ListNodes(ctx, tx, queryMaxPageSize, token)
		if err != nil {
			return nil, err
		}
		all = append(all, nodes...)
		if next == "" {
			return all, nil
		}
		token = next
	}
}

func (s *QueryService) allEdges(ctx context.Context, tx daemonsession.GraphTransaction) ([]domaingraph.Edge, error) {
	all := []domaingraph.Edge{}
	token := ""
	for {
		edges, next, err := s.graphs.ListEdges(ctx, tx, queryMaxPageSize, token)
		if err != nil {
			return nil, err
		}
		all = append(all, edges...)
		if next == "" {
			return all, nil
		}
		token = next
	}
}

type queryExecution struct {
	nodes        []domaingraph.Node
	edges        []domaingraph.Edge
	nodeByID     map[string]domaingraph.Node
	outEdgesByID map[string][]domaingraph.Edge
	inEdgesByID  map[string][]domaingraph.Edge
}

type queryRowState struct {
	bindings      map[string][]domaingraph.Node
	edgeBindings  map[string][]domaingraph.Edge
	pathBindings  map[string]*clientv1.PathValue
	parentByChild map[string]string
	orderByChild  map[string]any
}

func newQueryExecution(nodes []domaingraph.Node, edges []domaingraph.Edge) *queryExecution {
	exec := &queryExecution{nodes: nodes, edges: edges, nodeByID: map[string]domaingraph.Node{}, outEdgesByID: map[string][]domaingraph.Edge{}, inEdgesByID: map[string][]domaingraph.Edge{}}
	for _, node := range nodes {
		exec.nodeByID[node.ID.String()] = node
	}
	for _, edge := range edges {
		exec.outEdgesByID[edge.FromID.String()] = append(exec.outEdgesByID[edge.FromID.String()], edge)
		exec.inEdgesByID[edge.ToID.String()] = append(exec.inEdgesByID[edge.ToID.String()], edge)
	}
	return exec
}

func (e *queryExecution) match(pattern *clientv1.GraphPattern, where *clientv1.Expr, pathAlias string) ([]*queryRowState, error) {
	start := pattern.GetStart()
	if strings.TrimSpace(start.GetAlias()) == "" {
		return nil, fmt.Errorf("start alias is required")
	}
	if strings.TrimSpace(pathAlias) != "" {
		return e.matchPathRows(pattern, where, pathAlias)
	}
	if patternHasEdgeAlias(pattern) {
		return e.matchEdgeRows(pattern, where)
	}
	rows := []*queryRowState{}
	for _, node := range e.nodes {
		if !e.nodeMatches(node, start) {
			continue
		}
		row := newQueryRowState(start.GetAlias(), node)
		if err := e.applySteps(row, []domaingraph.Node{node}, pattern.GetSteps()); err != nil {
			return nil, err
		}
		if where != nil {
			ok, err := e.evalExpr(row, where)
			if err != nil || !ok {
				if err != nil {
					return nil, err
				}
				continue
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *queryExecution) nodeMatches(node domaingraph.Node, pattern *clientv1.NodePattern) bool {
	if pattern == nil {
		return true
	}
	if len(pattern.GetNodeIds()) > 0 && !stringInSet(node.ID.String(), pattern.GetNodeIds()) {
		return false
	}
	if len(pattern.GetLabels()) > 0 && !nodeHasLabels(node.Labels, pattern.GetLabels()) {
		return false
	}
	return true
}

func patternHasEdgeAlias(pattern *clientv1.GraphPattern) bool {
	for _, step := range pattern.GetSteps() {
		if strings.TrimSpace(step.GetEdgeAlias()) != "" {
			return true
		}
	}
	return false
}

func newQueryRowState(alias string, node domaingraph.Node) *queryRowState {
	return &queryRowState{bindings: map[string][]domaingraph.Node{alias: []domaingraph.Node{node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
}

func cloneQueryRowState(row *queryRowState) *queryRowState {
	out := &queryRowState{bindings: map[string][]domaingraph.Node{}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
	for alias, nodes := range row.bindings {
		out.bindings[alias] = append([]domaingraph.Node(nil), nodes...)
	}
	for alias, edges := range row.edgeBindings {
		out.edgeBindings[alias] = append([]domaingraph.Edge(nil), edges...)
	}
	for alias, path := range row.pathBindings {
		out.pathBindings[alias] = clonePathValue(path)
	}
	for child, parent := range row.parentByChild {
		out.parentByChild[child] = parent
	}
	for child, order := range row.orderByChild {
		out.orderByChild[child] = order
	}
	return out
}

func (e *queryExecution) matchEdgeRows(pattern *clientv1.GraphPattern, where *clientv1.Expr) ([]*queryRowState, error) {
	start := pattern.GetStart()
	rows := []*queryRowState{}
	for _, node := range e.nodes {
		if !e.nodeMatches(node, start) {
			continue
		}
		rows = append(rows, newQueryRowState(start.GetAlias(), node))
	}
	currentAlias := start.GetAlias()
	for _, step := range pattern.GetSteps() {
		if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
			return nil, fmt.Errorf("traversal target alias is required")
		}
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
			return nil, fmt.Errorf("traversal direction is required")
		}
		if strings.TrimSpace(step.GetEdgeKind()) == "" {
			return nil, fmt.Errorf("traversal edge_kind is required")
		}
		if depth := step.GetDepth(); depth != nil && (depth.GetMinDepth() > 1 || depth.GetMaxDepth() != 1) {
			return nil, fmt.Errorf("edge_alias currently supports one-hop traversal only")
		}
		nextRows := []*queryRowState{}
		for _, row := range rows {
			for _, node := range row.bindings[currentAlias] {
				for _, edge := range e.stepEdges(node, step) {
					candidateID := edge.ToID.String()
					if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
						candidateID = edge.FromID.String()
					}
					candidate, ok := e.nodeByID[candidateID]
					if !ok || !e.nodeMatches(candidate, step.GetTarget()) {
						continue
					}
					child := cloneQueryRowState(row)
					child.bindings[step.GetTarget().GetAlias()] = []domaingraph.Node{candidate}
					if alias := strings.TrimSpace(step.GetEdgeAlias()); alias != "" {
						child.edgeBindings[alias] = []domaingraph.Edge{edge}
					}
					if domaingraph.EdgeHasLabels(edge, []string{"contains"}) && step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT {
						child.parentByChild[candidate.ID.String()] = node.ID.String()
						child.orderByChild[candidate.ID.String()] = edge.Properties["order"]
					}
					nextRows = append(nextRows, child)
				}
			}
		}
		rows = nextRows
		currentAlias = step.GetTarget().GetAlias()
	}
	if where == nil {
		return rows, nil
	}
	out := []*queryRowState{}
	for _, row := range rows {
		ok, err := e.evalExpr(row, where)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, row)
		}
	}
	return out, nil
}

func (e *queryExecution) matchPathRows(pattern *clientv1.GraphPattern, where *clientv1.Expr, pathAlias string) ([]*queryRowState, error) {
	pathAlias = strings.TrimSpace(pathAlias)
	if pathAlias == "" {
		return nil, fmt.Errorf("path alias is required")
	}
	start := pattern.GetStart()
	if len(pattern.GetSteps()) == 0 {
		return nil, fmt.Errorf("path query requires at least one traversal step")
	}
	rows := []*queryRowState{}
	for _, node := range e.nodes {
		if !e.nodeMatches(node, start) {
			continue
		}
		seed := newQueryRowState(start.GetAlias(), node)
		seed.pathBindings[pathAlias] = &clientv1.PathValue{Nodes: []*clientv1.Node{mapProtoNode(node)}}
		rows = append(rows, seed)
	}
	currentAlias := start.GetAlias()
	for _, step := range pattern.GetSteps() {
		if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
			return nil, fmt.Errorf("traversal target alias is required")
		}
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
			return nil, fmt.Errorf("traversal direction is required")
		}
		if strings.TrimSpace(step.GetEdgeKind()) == "" {
			return nil, fmt.Errorf("traversal edge_kind is required")
		}
		nextRows := []*queryRowState{}
		for _, row := range rows {
			for _, current := range row.bindings[currentAlias] {
				expanded := e.expandPathStep(row, pathAlias, current, step)
				nextRows = append(nextRows, expanded...)
			}
		}
		rows = nextRows
		currentAlias = step.GetTarget().GetAlias()
	}
	if where == nil {
		return rows, nil
	}
	out := []*queryRowState{}
	for _, row := range rows {
		ok, err := e.evalExpr(row, where)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, row)
		}
	}
	return out, nil
}

func (e *queryExecution) expandPathStep(row *queryRowState, pathAlias string, start domaingraph.Node, step *clientv1.TraversalStep) []*queryRowState {
	minDepth, maxDepth, _ := traversalDepthBounds(step.GetDepth())
	if minDepth < 0 {
		minDepth = 0
	}
	out := []*queryRowState{}
	var visit func(base *queryRowState, node domaingraph.Node, depth int, seen map[string]struct{})
	visit = func(base *queryRowState, node domaingraph.Node, depth int, seen map[string]struct{}) {
		if maxDepth >= 0 && depth >= maxDepth {
			return
		}
		for _, edge := range e.stepEdges(node, step) {
			candidateID := edge.ToID.String()
			if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
				candidateID = edge.FromID.String()
			}
			candidate, ok := e.nodeByID[candidateID]
			if !ok {
				continue
			}
			if _, cycle := seen[candidate.ID.String()]; cycle {
				continue
			}
			childDepth := depth + 1
			child := cloneQueryRowState(base)
			child.bindings[step.GetTarget().GetAlias()] = []domaingraph.Node{candidate}
			if alias := strings.TrimSpace(step.GetEdgeAlias()); alias != "" {
				child.edgeBindings[alias] = []domaingraph.Edge{edge}
			}
			path := clonePathValue(child.pathBindings[pathAlias])
			path.Edges = append(path.Edges, mapProtoEdge(edge))
			path.Nodes = append(path.Nodes, mapProtoNode(candidate))
			child.pathBindings[pathAlias] = path
			if childDepth >= minDepth && e.nodeMatches(candidate, step.GetTarget()) {
				out = append(out, child)
			}
			nextSeen := map[string]struct{}{}
			for key := range seen {
				nextSeen[key] = struct{}{}
			}
			nextSeen[candidate.ID.String()] = struct{}{}
			visit(child, candidate, childDepth, nextSeen)
		}
	}
	visit(row, start, 0, map[string]struct{}{start.ID.String(): {}})
	return out
}

func stringInSet(value string, set []string) bool {
	for _, candidate := range set {
		if candidate == value {
			return true
		}
	}
	return false
}

func nodeHasLabels(labels []string, required []string) bool {
	seen := map[string]struct{}{}
	for _, label := range labels {
		seen[label] = struct{}{}
	}
	for _, label := range required {
		if _, ok := seen[label]; !ok {
			return false
		}
	}
	return true
}

func (e *queryExecution) applySteps(row *queryRowState, current []domaingraph.Node, steps []*clientv1.TraversalStep) error {
	for _, step := range steps {
		if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
			return fmt.Errorf("traversal target alias is required")
		}
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
			return fmt.Errorf("traversal direction is required")
		}
		label := strings.TrimSpace(step.GetEdgeKind())
		if label == "" {
			return fmt.Errorf("traversal edge_kind is required")
		}
		next := []domaingraph.Node{}
		for _, node := range current {
			next = append(next, e.traverse(row, node, step)...)
		}
		next = dedupeQueryNodes(next)
		row.bindings[step.GetTarget().GetAlias()] = next
		current = next
	}
	return nil
}

func (e *queryExecution) traverse(row *queryRowState, start domaingraph.Node, step *clientv1.TraversalStep) []domaingraph.Node {
	depth := step.GetDepth()
	minDepth, maxDepth := int32(1), int32(1)
	if depth != nil {
		minDepth = depth.GetMinDepth()
		maxDepth = depth.GetMaxDepth()
	}
	if minDepth < 0 {
		minDepth = 0
	}
	out := []domaingraph.Node{}
	visited := map[string]bool{}
	var visit func(node domaingraph.Node, currentDepth int32)
	visit = func(node domaingraph.Node, currentDepth int32) {
		if maxDepth >= 0 && currentDepth > maxDepth {
			return
		}
		for _, edge := range e.stepEdges(node, step) {
			var candidateID string
			if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT {
				candidateID = edge.ToID.String()
			} else {
				candidateID = edge.FromID.String()
			}
			candidate, ok := e.nodeByID[candidateID]
			if !ok {
				continue
			}
			childDepth := currentDepth + 1
			visitKey := candidate.ID.String()
			if visited[visitKey] {
				continue
			}
			visited[visitKey] = true
			if domaingraph.EdgeHasLabels(edge, []string{"contains"}) && step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT {
				row.parentByChild[candidate.ID.String()] = node.ID.String()
				row.orderByChild[candidate.ID.String()] = edge.Properties["order"]
			}
			if childDepth >= minDepth && (maxDepth < 0 || childDepth <= maxDepth) && e.nodeMatches(candidate, step.GetTarget()) {
				out = append(out, candidate)
			}
			visit(candidate, childDepth)
		}
	}
	visit(start, 0)
	return out
}

func (e *queryExecution) stepEdges(node domaingraph.Node, step *clientv1.TraversalStep) []domaingraph.Edge {
	var edges []domaingraph.Edge
	if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
		edges = e.inEdgesByID[node.ID.String()]
	} else {
		edges = e.outEdgesByID[node.ID.String()]
	}
	out := []domaingraph.Edge{}
	for _, edge := range edges {
		if domaingraph.EdgeHasLabels(edge, []string{step.GetEdgeKind()}) {
			out = append(out, edge)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return numericOrder(out[i].Properties["order"], i) < numericOrder(out[j].Properties["order"], j)
	})
	return out
}

func (e *queryExecution) evalExpr(row *queryRowState, expr *clientv1.Expr) (bool, error) {
	switch v := expr.GetExpr().(type) {
	case *clientv1.Expr_And:
		for _, child := range v.And.GetExprs() {
			ok, err := e.evalExpr(row, child)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	case *clientv1.Expr_Or:
		for _, child := range v.Or.GetExprs() {
			ok, err := e.evalExpr(row, child)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	case *clientv1.Expr_HasTag:
		return e.hasTag(row, v.HasTag.GetAlias(), v.HasTag.GetTag())
	case *clientv1.Expr_PropertyExists:
		_, ok, err := e.customProperty(row, v.PropertyExists.GetAlias(), v.PropertyExists.GetName())
		return ok, err
	case *clientv1.Expr_PropertyEquals:
		value, ok, err := e.customProperty(row, v.PropertyEquals.GetAlias(), v.PropertyEquals.GetName())
		if err != nil || !ok {
			return false, err
		}
		return queryValuesEqual(value, v.PropertyEquals.GetValue().AsInterface()), nil
	case *clientv1.Expr_Null:
		value, ok, err := e.fieldValue(row, v.Null.GetAlias(), v.Null.GetName())
		if err != nil {
			return false, err
		}
		isNull := !ok || value == nil
		return isNull == v.Null.GetIsNull(), nil
	case *clientv1.Expr_StringPredicate:
		value, err := e.evalValue(row, v.StringPredicate.GetValue())
		if err != nil {
			return false, err
		}
		return matchStringPredicate(value, v.StringPredicate.GetQuery(), v.StringPredicate.GetMode()), nil
	case *clientv1.Expr_Text:
		value, ok, err := e.fieldValue(row, v.Text.GetAlias(), v.Text.GetField())
		if err != nil || !ok {
			return false, err
		}
		return strings.Contains(strings.ToLower(fmt.Sprint(value)), strings.ToLower(v.Text.GetQuery())), nil
	case *clientv1.Expr_Semantic:
		value, ok, err := e.fieldValue(row, v.Semantic.GetAlias(), v.Semantic.GetField())
		if err != nil || !ok {
			return false, err
		}
		return strings.Contains(strings.ToLower(fmt.Sprint(value)), strings.ToLower(v.Semantic.GetQuery())), nil
	case *clientv1.Expr_Between:
		value, err := e.evalValue(row, v.Between.GetValue())
		if err != nil {
			return false, err
		}
		low, err := e.evalValue(row, v.Between.GetLow())
		if err != nil {
			return false, err
		}
		high, err := e.evalValue(row, v.Between.GetHigh())
		if err != nil {
			return false, err
		}
		return compareQueryValues(value, low) >= 0 && compareQueryValues(value, high) <= 0, nil
	case *clientv1.Expr_LessThan:
		left, err := e.evalValue(row, v.LessThan.GetLeft())
		if err != nil {
			return false, err
		}
		right, err := e.evalValue(row, v.LessThan.GetRight())
		if err != nil {
			return false, err
		}
		return compareQueryValues(left, right) < 0, nil
	default:
		return true, nil
	}
}

func queryFieldParts(name string) (string, string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "properties", name
}

func (e *queryExecution) fieldValue(row *queryRowState, alias string, name string) (any, bool, error) {
	namespace, field := queryFieldParts(name)
	if edge, err := firstBoundEdge(row, alias); err == nil {
		value := edgeProjectionField(edge, namespace, field)
		return value, value != nil, nil
	}
	node, err := firstBoundNode(row, alias)
	if err != nil {
		return nil, false, err
	}
	value := nodeProjectionField(node, namespace, field)
	return value, value != nil, nil
}

func (e *queryExecution) evalValue(row *queryRowState, value *clientv1.ValueExpr) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("value expression is required")
	}
	switch v := value.GetExpr().(type) {
	case *clientv1.ValueExpr_Prop:
		namespace, field := queryFieldParts(v.Prop.GetName())
		if edge, err := firstBoundEdge(row, v.Prop.GetAlias()); err == nil {
			return edgeProjectionField(edge, namespace, field), nil
		}
		node, err := firstBoundNode(row, v.Prop.GetAlias())
		if err != nil {
			return nil, err
		}
		return nodeProjectionField(node, namespace, field), nil
	case *clientv1.ValueExpr_Literal:
		return v.Literal.GetValue().AsInterface(), nil
	case *clientv1.ValueExpr_Date:
		t, err := time.Parse("2006-01-02", v.Date.GetValue())
		if err != nil {
			return nil, err
		}
		return t.AddDate(0, 0, int(v.Date.GetOffsetDays())), nil
	case *clientv1.ValueExpr_CurrentDate:
		now := time.Now()
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, int(v.CurrentDate.GetOffsetDays())), nil
	default:
		return nil, fmt.Errorf("unsupported value expression")
	}
}

func (e *queryExecution) hasTag(row *queryRowState, alias string, tag string) (bool, error) {
	node, err := firstBoundNode(row, alias)
	if err != nil {
		return false, err
	}
	want, err := domaingraph.NormalizeTag(tag)
	if err != nil {
		return false, err
	}
	tagValue := any(nil)
	if node.Properties != nil {
		tagValue = node.Properties[domaingraph.NodePropTags]
	}
	if tagValue == nil {
		tagValue = node.Props[domaingraph.NodePropTags]
	}
	tags, err := domaingraph.NormalizeTagsValue(tagValue)
	if err != nil {
		return false, nil
	}
	for _, got := range tags {
		if got == want {
			return true, nil
		}
	}
	return false, nil
}

func (e *queryExecution) customProperty(row *queryRowState, alias string, name string) (any, bool, error) {
	if edge, err := firstBoundEdge(row, alias); err == nil {
		value, ok := edgeCustomProperty(edge, name)
		return value, ok, nil
	}
	node, err := firstBoundNode(row, alias)
	if err != nil {
		return nil, false, err
	}
	want, err := domaingraph.NormalizePropertyName(name)
	if err != nil {
		return nil, false, err
	}
	props := node.Properties
	if props == nil {
		var err error
		props, err = domaingraph.NormalizeCustomPropertiesValue(node.Props[domaingraph.NodePropCustomProperties])
		if err != nil {
			return nil, false, nil
		}
	}
	value, ok := props[name]
	if !ok {
		value, ok = props[want]
	}
	if !ok {
		if nested, err := domaingraph.NormalizeCustomPropertiesValue(props[domaingraph.NodePropCustomProperties]); err == nil {
			value, ok = nested[name]
			if !ok {
				value, ok = nested[want]
			}
		}
	}
	return value, ok, nil
}

func (e *queryExecution) sortRows(rows []*queryRowState, orders []*clientv1.OrderSpec) error {
	for _, row := range rows {
		for _, order := range orders {
			if _, err := e.evalValue(row, order.GetValue()); err != nil {
				return err
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, order := range orders {
			left, _ := e.evalValue(rows[i], order.GetValue())
			right, _ := e.evalValue(rows[j], order.GetValue())
			cmp := compareQueryValues(left, right)
			if cmp == 0 {
				continue
			}
			if order.GetDirection() == clientv1.SortDirection_SORT_DIRECTION_DESC {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return nil
}

func (e *queryExecution) projectRow(row *queryRowState, returns []*clientv1.ReturnProjection) (*clientv1.QueryRow, error) {
	fields := map[string]*clientv1.QueryValue{}
	for _, ret := range returns {
		name := ret.GetOutputName()
		if name == "" {
			name = ret.GetAlias()
		}
		switch ret.GetKind() {
		case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE:
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Tree{Tree: e.projectTree(row, ret.GetAlias())}}
		case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR:
			value, err := scalarProjectionValue(row, ret.GetAlias())
			if err != nil {
				return nil, err
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(value)}}
		case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE:
			edge, err := firstBoundEdge(row, ret.GetAlias())
			if err != nil {
				return nil, err
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Edge{Edge: mapProtoEdge(edge)}}
		case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH:
			path := row.pathBindings[ret.GetAlias()]
			if path == nil {
				return nil, fmt.Errorf("path alias %q is not bound", ret.GetAlias())
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Path{Path: clonePathValue(path)}}
		default:
			node, err := firstBoundNode(row, ret.GetAlias())
			if err != nil {
				return nil, err
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Node{Node: mapProtoNode(node)}}
		}
	}
	return &clientv1.QueryRow{Fields: fields}, nil
}

type shapedProtoRow struct {
	row        *clientv1.QueryRow
	sortValues []any
	sequence   int
}

func (e *queryExecution) shapeAndProjectRows(rows []*queryRowState, query *clientv1.GraphQuery, pageSize int, pageToken string) ([]*clientv1.QueryRow, string, error) {
	if query.GetOffset() < 0 {
		return nil, "", fmt.Errorf("offset must be non-negative")
	}
	returns := query.GetReturns()
	if len(returns) == 0 && len(query.GetAggregateReturns()) == 0 {
		startAlias := query.GetMatch().GetStart().GetAlias()
		returns = []*clientv1.ReturnProjection{{Alias: startAlias, OutputName: startAlias, Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}
	}
	var shaped []shapedProtoRow
	var err error
	if len(query.GetAggregateReturns()) > 0 {
		shaped, err = e.projectAggregateShapedRows(rows, returns, query.GetAggregateReturns(), query.GetOrderBy())
	} else {
		shaped, err = e.projectShapedRows(rows, returns, query.GetOrderBy())
	}
	if err != nil {
		return nil, "", err
	}
	shaped = distinctShapedProtoRowsIf(shaped, query.GetDistinct())
	if err := sortShapedProtoRows(shaped, query.GetOrderBy()); err != nil {
		return nil, "", err
	}
	projected := materializeShapedProtoRows(shaped)
	projected = applyProtoOffsetLimit(projected, int(query.GetOffset()), int(query.GetLimit()))
	return paginateProtoQueryRows(projected, pageSize, pageToken)
}

func (e *queryExecution) projectShapedRows(rows []*queryRowState, returns []*clientv1.ReturnProjection, orders []*clientv1.OrderSpec) ([]shapedProtoRow, error) {
	projected := make([]shapedProtoRow, 0, len(rows))
	for i, row := range rows {
		protoRow, err := e.projectRow(row, returns)
		if err != nil {
			return nil, err
		}
		sortValues, err := e.rowSortValues(row, orders)
		if err != nil {
			return nil, err
		}
		projected = append(projected, shapedProtoRow{row: protoRow, sortValues: sortValues, sequence: i})
	}
	return projected, nil
}

func (e *queryExecution) rowSortValues(row *queryRowState, orders []*clientv1.OrderSpec) ([]any, error) {
	if len(orders) == 0 {
		return nil, nil
	}
	values := make([]any, 0, len(orders))
	for _, order := range orders {
		value, err := e.evalValue(row, order.GetValue())
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (e *queryExecution) projectAggregateShapedRows(rows []*queryRowState, returns []*clientv1.ReturnProjection, aggregates []*clientv1.AggregateProjection, orders []*clientv1.OrderSpec) ([]shapedProtoRow, error) {
	if len(returns) == 0 {
		fields := map[string]*clientv1.QueryValue{}
		for _, agg := range aggregates {
			value, err := e.aggregateRows(rows, agg)
			if err != nil {
				return nil, err
			}
			fields[aggregateOutputName(agg)] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(value)}}
		}
		return []shapedProtoRow{{row: &clientv1.QueryRow{Fields: fields}, sequence: 0}}, nil
	}
	groups := map[string]map[string]*clientv1.QueryValue{}
	groupRows := map[string][]*queryRowState{}
	groupFirstSeq := map[string]int{}
	groupFirstSortValues := map[string][]any{}
	for i, row := range rows {
		protoRow, err := e.projectRow(row, returns)
		if err != nil {
			return nil, err
		}
		key := protoRowKey(protoRow)
		if _, ok := groups[key]; !ok {
			groups[key] = cloneQueryFields(protoRow.GetFields())
			groupFirstSeq[key] = i
			sortValues, err := e.rowSortValues(row, orders)
			if err != nil {
				return nil, err
			}
			groupFirstSortValues[key] = sortValues
		}
		groupRows[key] = append(groupRows[key], row)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]shapedProtoRow, 0, len(keys))
	for _, key := range keys {
		fields := cloneQueryFields(groups[key])
		for _, agg := range aggregates {
			value, err := e.aggregateRows(groupRows[key], agg)
			if err != nil {
				return nil, err
			}
			fields[aggregateOutputName(agg)] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(value)}}
		}
		out = append(out, shapedProtoRow{row: &clientv1.QueryRow{Fields: fields}, sortValues: groupFirstSortValues[key], sequence: groupFirstSeq[key]})
	}
	return out, nil
}

func distinctShapedProtoRowsIf(rows []shapedProtoRow, distinct bool) []shapedProtoRow {
	if !distinct {
		return rows
	}
	seen := map[string]struct{}{}
	out := make([]shapedProtoRow, 0, len(rows))
	for _, row := range rows {
		key := protoRowKey(row.row)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func sortShapedProtoRows(rows []shapedProtoRow, orders []*clientv1.OrderSpec) error {
	if len(orders) == 0 {
		return nil
	}
	for _, row := range rows {
		if len(row.sortValues) != len(orders) {
			return fmt.Errorf("ORDER BY sort key count mismatch")
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for idx, order := range orders {
			cmp := compareQueryValues(rows[i].sortValues[idx], rows[j].sortValues[idx])
			if cmp == 0 {
				continue
			}
			if order.GetDirection() == clientv1.SortDirection_SORT_DIRECTION_DESC {
				return cmp > 0
			}
			return cmp < 0
		}
		return rows[i].sequence < rows[j].sequence
	})
	return nil
}

func materializeShapedProtoRows(rows []shapedProtoRow) []*clientv1.QueryRow {
	out := make([]*clientv1.QueryRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.row)
	}
	return out
}

func aggregateOutputName(agg *clientv1.AggregateProjection) string {
	if name := strings.TrimSpace(agg.GetOutputName()); name != "" {
		return name
	}
	switch agg.GetFunction() {
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM:
		return "sum"
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG:
		return "avg"
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN:
		return "min"
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX:
		return "max"
	default:
		return "count"
	}
}

func (e *queryExecution) aggregateRows(rows []*queryRowState, agg *clientv1.AggregateProjection) (any, error) {
	switch agg.GetFunction() {
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_UNSPECIFIED, clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT:
		return e.aggregateCount(rows, agg)
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM:
		values, err := e.aggregateNumericValues(rows, agg)
		if err != nil {
			return nil, err
		}
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum, nil
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG:
		values, err := e.aggregateNumericValues(rows, agg)
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, nil
		}
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum / float64(len(values)), nil
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN, clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX:
		values, err := e.aggregateComparableValues(rows, agg)
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, nil
		}
		best := values[0]
		for _, value := range values[1:] {
			cmp := compareQueryValues(value, best)
			if (agg.GetFunction() == clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN && cmp < 0) || (agg.GetFunction() == clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX && cmp > 0) {
				best = value
			}
		}
		return best, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function %s", agg.GetFunction())
	}
}

func (e *queryExecution) aggregateCount(rows []*queryRowState, agg *clientv1.AggregateProjection) (int, error) {
	arg := agg.GetArgument()
	if arg == nil || arg.GetStar() {
		return len(rows), nil
	}
	if valueExpr := arg.GetValue(); valueExpr != nil {
		count := 0
		for _, row := range rows {
			value, err := e.evalValue(row, valueExpr)
			if err != nil {
				return 0, err
			}
			if value != nil {
				count++
			}
		}
		return count, nil
	}
	alias := arg.GetAlias()
	if strings.TrimSpace(alias) == "" {
		return len(rows), nil
	}
	count := 0
	for _, row := range rows {
		if len(row.bindings[alias]) > 0 || len(row.edgeBindings[alias]) > 0 || row.pathBindings[alias] != nil {
			count++
		}
	}
	return count, nil
}

func (e *queryExecution) aggregateNumericValues(rows []*queryRowState, agg *clientv1.AggregateProjection) ([]float64, error) {
	arg := agg.GetArgument()
	if arg == nil {
		return nil, fmt.Errorf("%s aggregate requires a value argument", agg.GetFunction())
	}
	valueExpr := arg.GetValue()
	if valueExpr == nil {
		return nil, fmt.Errorf("%s aggregate requires a value argument", agg.GetFunction())
	}
	values := []float64{}
	for _, row := range rows {
		value, err := e.evalValue(row, valueExpr)
		if err != nil {
			return nil, err
		}
		if value == nil {
			continue
		}
		number, ok := queryNumber(value)
		if !ok {
			return nil, fmt.Errorf("%s aggregate requires numeric values", agg.GetFunction())
		}
		values = append(values, number)
	}
	return values, nil
}

func (e *queryExecution) aggregateComparableValues(rows []*queryRowState, agg *clientv1.AggregateProjection) ([]any, error) {
	arg := agg.GetArgument()
	if arg == nil {
		return nil, fmt.Errorf("%s aggregate requires a value argument", agg.GetFunction())
	}
	valueExpr := arg.GetValue()
	if valueExpr == nil {
		return nil, fmt.Errorf("%s aggregate requires a value argument", agg.GetFunction())
	}
	values := []any{}
	for _, row := range rows {
		value, err := e.evalValue(row, valueExpr)
		if err != nil {
			return nil, err
		}
		if value != nil {
			values = append(values, value)
		}
	}
	return values, nil
}

func queryNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		number, err := typed.Float64()
		return number, err == nil
	default:
		return 0, false
	}
}

func applyProtoOffsetLimit(rows []*clientv1.QueryRow, offset int, limit int) []*clientv1.QueryRow {
	if offset > len(rows) {
		return []*clientv1.QueryRow{}
	}
	if offset > 0 {
		rows = rows[offset:]
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func distinctProtoRowsIf(rows []*clientv1.QueryRow, distinct bool) []*clientv1.QueryRow {
	if !distinct {
		return rows
	}
	seen := map[string]struct{}{}
	out := make([]*clientv1.QueryRow, 0, len(rows))
	for _, row := range rows {
		key := protoRowKey(row)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func protoRowKey(row *clientv1.QueryRow) string {
	payload, err := json.Marshal(row.GetFields())
	if err != nil {
		return fmt.Sprint(row.GetFields())
	}
	return string(payload)
}

func cloneQueryFields(in map[string]*clientv1.QueryValue) map[string]*clientv1.QueryValue {
	out := make(map[string]*clientv1.QueryValue, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (e *queryExecution) projectTree(row *queryRowState, alias string) *clientv1.Tree {
	nodes := row.bindings[alias]
	byID := map[string]domaingraph.Node{}
	children := map[string][]domaingraph.Node{}
	for _, node := range nodes {
		byID[node.ID.String()] = node
	}
	roots := []domaingraph.Node{}
	for _, node := range nodes {
		parentID := row.parentByChild[node.ID.String()]
		if _, parentMatched := byID[parentID]; parentMatched {
			children[parentID] = append(children[parentID], node)
			continue
		}
		roots = append(roots, node)
	}
	sortTreeNodes(roots, row.orderByChild)
	for parentID := range children {
		sortTreeNodes(children[parentID], row.orderByChild)
	}
	var build func(domaingraph.Node) *clientv1.TreeNode
	build = func(node domaingraph.Node) *clientv1.TreeNode {
		out := &clientv1.TreeNode{Node: mapProtoNode(node)}
		for _, child := range children[node.ID.String()] {
			out.Children = append(out.Children, build(child))
		}
		return out
	}
	forest := &clientv1.Tree{}
	for _, root := range roots {
		forest.Roots = append(forest.Roots, build(root))
	}
	return forest
}

func scalarProjectionValue(row *queryRowState, projection string) (any, error) {
	parts := strings.Split(projection, ".")
	if len(parts) == 1 {
		node, err := firstBoundNode(row, projection)
		if err != nil {
			return nil, err
		}
		return node.ID.String(), nil
	}
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf("scalar projection %q must be alias, alias.property, alias.payload.field, or alias.meta.field", projection)
	}
	alias := parts[0]
	namespace := "properties"
	field := parts[1]
	if len(parts) == 3 {
		namespace = parts[1]
		field = parts[2]
	}
	if edge, err := firstBoundEdge(row, alias); err == nil {
		return edgeProjectionField(edge, namespace, field), nil
	}
	node, err := firstBoundNode(row, alias)
	if err != nil {
		return nil, err
	}
	return nodeProjectionField(node, namespace, field), nil
}

func nodeProjectionField(node domaingraph.Node, namespace, field string) any {
	switch namespace {
	case "properties", "":
		return propValue(node, field)
	case "payload":
		if node.Payload == nil {
			return nil
		}
		return node.Payload[field]
	case "meta":
		if node.Meta == nil {
			return nil
		}
		return node.Meta[field]
	default:
		return nil
	}
}

func edgeProjectionField(edge domaingraph.Edge, namespace, field string) any {
	switch namespace {
	case "properties", "":
		return edgePropValue(edge, field)
	case "payload":
		if edge.Payload == nil {
			return nil
		}
		return edge.Payload[field]
	case "meta":
		if edge.Meta == nil {
			return nil
		}
		return edge.Meta[field]
	default:
		return nil
	}
}

func firstBoundNode(row *queryRowState, alias string) (domaingraph.Node, error) {
	nodes := row.bindings[alias]
	if len(nodes) == 0 {
		return domaingraph.Node{}, fmt.Errorf("alias %q is not bound", alias)
	}
	return nodes[0], nil
}

func firstBoundEdge(row *queryRowState, alias string) (domaingraph.Edge, error) {
	edges := row.edgeBindings[alias]
	if len(edges) == 0 {
		return domaingraph.Edge{}, fmt.Errorf("edge alias %q is not bound", alias)
	}
	return edges[0], nil
}

func propValue(node domaingraph.Node, name string) any {
	if name == "node_id" {
		return node.ID.String()
	}
	if name == "content" {
		return domaingraph.PayloadText(node)
	}
	if value, ok := domaingraph.Property(node, name); ok {
		return value
	}
	return nil
}

func edgePropValue(edge domaingraph.Edge, name string) any {
	switch name {
	case "edge_id":
		return edge.ID.String()
	case "from_node_id":
		return edge.FromID.String()
	case "to_node_id":
		return edge.ToID.String()
	}
	if value, ok := domaingraph.EdgeProperty(edge, name); ok {
		return value
	}
	return nil
}

func edgeCustomProperty(edge domaingraph.Edge, name string) (any, bool) {
	if value, ok := domaingraph.EdgeProperty(edge, name); ok {
		return value, true
	}
	want, err := domaingraph.NormalizePropertyName(name)
	if err != nil || want == name {
		return nil, false
	}
	return domaingraph.EdgeProperty(edge, want)
}

func matchStringPredicate(value any, query string, mode clientv1.StringPredicateMode) bool {
	left := strings.ToLower(fmt.Sprint(value))
	right := strings.ToLower(query)
	switch mode {
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH:
		return strings.HasPrefix(left, right)
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_ENDS_WITH:
		return strings.HasSuffix(left, right)
	default:
		return strings.Contains(left, right)
	}
}

func clonePathValue(path *clientv1.PathValue) *clientv1.PathValue {
	if path == nil {
		return &clientv1.PathValue{}
	}
	out := &clientv1.PathValue{Nodes: make([]*clientv1.Node, 0, len(path.GetNodes())), Edges: make([]*clientv1.Edge, 0, len(path.GetEdges()))}
	out.Nodes = append(out.Nodes, path.GetNodes()...)
	out.Edges = append(out.Edges, path.GetEdges()...)
	return out
}

type gqlDaemonGraph struct {
	service *QueryService
	tx      daemonsession.GraphTransaction
}

func (g gqlDaemonGraph) InsertNode(ctx context.Context, node execution.InsertNode) (execmodel.NodeRef, error) {
	created, err := g.service.graphs.CreateNode(ctx, g.tx, daegraph.NodeInput{Labels: append([]string(nil), node.Labels...), Properties: copyMapAny(node.Properties)})
	if err != nil {
		return execmodel.NodeRef{}, err
	}
	return execmodel.NodeRef{ID: created.ID.String()}, nil
}

func (g gqlDaemonGraph) CreateEdge(ctx context.Context, edge execution.CreateEdge) (execmodel.Edge, error) {
	created, err := g.service.graphs.CreateEdge(ctx, g.tx, daegraph.EdgeInput{FromNodeID: edge.FromNodeID, ToNodeID: edge.ToNodeID, Labels: append([]string(nil), edge.Labels...), Properties: copyMapAny(edge.Properties), Payload: copyMapAny(edge.Payload), Meta: copyMapAny(edge.Meta)})
	if err != nil {
		return execmodel.Edge{}, err
	}
	return gqlExecEdge(created), nil
}

func (g gqlDaemonGraph) UpdateNode(ctx context.Context, node execution.UpdateNode) (execmodel.Node, error) {
	updated, err := g.service.graphs.UpdateNode(ctx, g.tx, daegraph.UpdateNodeInput{NodeID: node.NodeID, Labels: append([]string(nil), node.Labels...), Properties: copyMapAny(node.Properties), Payload: copyMapAny(node.Payload), Meta: copyMapAny(node.Meta), UpdateMask: []string{"labels", "properties", "payload", "meta"}})
	if err != nil {
		return execmodel.Node{}, err
	}
	return gqlExecNode(updated), nil
}

func (g gqlDaemonGraph) UpdateEdge(ctx context.Context, edge execution.UpdateEdge) (execmodel.Edge, error) {
	updated, err := g.service.graphs.UpdateEdge(ctx, g.tx, daegraph.UpdateEdgeInput{EdgeID: edge.EdgeID, Labels: append([]string(nil), edge.Labels...), Properties: copyMapAny(edge.Properties), Payload: copyMapAny(edge.Payload), Meta: copyMapAny(edge.Meta), UpdateMask: []string{"labels", "properties", "payload", "meta"}})
	if err != nil {
		return execmodel.Edge{}, err
	}
	return gqlExecEdge(updated), nil
}

func (g gqlDaemonGraph) DeleteNode(ctx context.Context, nodeID string) error {
	_, _, err := g.service.graphs.DeleteNode(ctx, g.tx, nodeID, false)
	return err
}

func (g gqlDaemonGraph) DeleteEdge(ctx context.Context, edgeID string) error {
	_, err := g.service.graphs.DeleteEdge(ctx, g.tx, edgeID)
	return err
}

func (g gqlDaemonGraph) QueryNodes(ctx context.Context, query execution.QueryNodes) ([]execmodel.Node, error) {
	nodes, err := g.service.allNodes(ctx, g.tx)
	if err != nil {
		return nil, err
	}
	out := []execmodel.Node{}
	for _, node := range nodes {
		if !nodeMatchesGQLPattern(node, query.Labels, query.Properties) {
			continue
		}
		out = append(out, gqlExecNode(node))
	}
	return out, nil
}

func (g gqlDaemonGraph) QueryPattern(ctx context.Context, query execution.QueryPattern) ([]execution.PatternRow, error) {
	nodes, err := g.service.allNodes(ctx, g.tx)
	if err != nil {
		return nil, err
	}
	edges, err := g.service.allEdges(ctx, g.tx)
	if err != nil {
		return nil, err
	}
	nodeByID := map[string]domaingraph.Node{}
	for _, node := range nodes {
		nodeByID[node.ID.String()] = node
	}
	out := []execution.PatternRow{}
	for _, edge := range edges {
		if !nodeHasLabels(edge.Labels, query.Relationship.Labels) || !nodeHasProperties(edge.Properties, query.Relationship.Properties) {
			continue
		}
		from, fromOK := nodeByID[edge.FromID.String()]
		to, toOK := nodeByID[edge.ToID.String()]
		if !fromOK || !toOK {
			continue
		}
		appendIfMatch := func(start, end domaingraph.Node) {
			if !nodeMatchesGQLPattern(start, query.Start.Labels, query.Start.Properties) || !nodeMatchesGQLPattern(end, query.End.Labels, query.End.Properties) {
				return
			}
			out = append(out, execution.PatternRow{Start: gqlExecNode(start), Edge: gqlExecEdge(edge), End: gqlExecNode(end)})
		}
		switch query.Relationship.Direction {
		case execution.RelationshipIncoming:
			appendIfMatch(to, from)
		case execution.RelationshipUndirected:
			appendIfMatch(from, to)
			appendIfMatch(to, from)
		default:
			appendIfMatch(from, to)
		}
		if query.Limit > 0 && int64(len(out)) >= query.Limit {
			return out[:query.Limit], nil
		}
	}
	return out, nil
}

func nodeMatchesGQLPattern(node domaingraph.Node, labels []string, properties map[string]any) bool {
	if id, ok := properties["__id"].(string); ok && node.ID.String() != id {
		return false
	}
	return nodeHasLabels(node.Labels, labels) && nodeHasProperties(node.Properties, properties)
}

func gqlExecNode(node domaingraph.Node) execmodel.Node {
	return execmodel.Node{ID: node.ID.String(), DomainID: node.DomainID.String(), Labels: append([]string(nil), node.Labels...), Properties: copyMapAny(node.Properties), Payload: copyMapAny(node.Payload), Meta: copyMapAny(node.Meta)}
}

func gqlExecEdge(edge domaingraph.Edge) execmodel.Edge {
	return execmodel.Edge{ID: edge.ID.String(), DomainID: edge.DomainID.String(), FromID: edge.FromID.String(), ToID: edge.ToID.String(), Labels: append([]string(nil), edge.Labels...), Properties: copyMapAny(edge.Properties), Payload: copyMapAny(edge.Payload), Meta: copyMapAny(edge.Meta)}
}

func nodeHasProperties(values map[string]any, required map[string]any) bool {
	for key, value := range required {
		if key == "__id" {
			continue
		}
		if !queryValuesEqual(values[key], value) {
			return false
		}
	}
	return true
}

func gqlRowsToProto(result execmodel.Result) []*clientv1.QueryRow {
	rows := make([]*clientv1.QueryRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		fields := map[string]*clientv1.QueryValue{}
		for name, value := range row {
			if value.Node != nil {
				fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Node{Node: gqlNodeToProto(*value.Node)}}
				continue
			}
			if value.Edge != nil {
				fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Edge{Edge: gqlEdgeToProto(*value.Edge)}}
				continue
			}
			if value.Path != nil {
				fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Path{Path: gqlPathToProto(*value.Path)}}
				continue
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(value.Scalar)}}
		}
		rows = append(rows, &clientv1.QueryRow{Fields: fields})
	}
	return rows
}

func gqlNodeToProto(node execmodel.Node) *clientv1.Node {
	return &clientv1.Node{NodeId: node.ID, DomainId: node.DomainID, Labels: append([]string(nil), node.Labels...), Properties: protoStruct(node.Properties), Payload: protoStruct(node.Payload), Meta: protoStruct(node.Meta)}
}

func gqlEdgeToProto(edge execmodel.Edge) *clientv1.Edge {
	return &clientv1.Edge{EdgeId: edge.ID, DomainId: edge.DomainID, FromNodeId: edge.FromID, ToNodeId: edge.ToID, Labels: append([]string(nil), edge.Labels...), Properties: protoStruct(edge.Properties), Payload: protoStruct(edge.Payload), Meta: protoStruct(edge.Meta)}
}

func gqlPathToProto(path execmodel.Path) *clientv1.PathValue {
	out := &clientv1.PathValue{Nodes: make([]*clientv1.Node, 0, len(path.Nodes)), Edges: make([]*clientv1.Edge, 0, len(path.Edges))}
	for _, node := range path.Nodes {
		out.Nodes = append(out.Nodes, gqlNodeToProto(node))
	}
	for _, edge := range path.Edges {
		out.Edges = append(out.Edges, gqlEdgeToProto(edge))
	}
	return out
}

func queryResultFromRows(rows []*clientv1.QueryRow, next string) *clientv1.QueryResult {
	return queryResultFromRowsWithCounters(rows, next, execmodel.Counters{})
}

func mergeExecPathGraph(result *clientv1.QueryResult, execResult execmodel.Result) {
	if result == nil {
		return
	}
	if result.Graph == nil {
		result.Graph = &clientv1.ResultGraph{}
	}
	nodeSeen := map[string]struct{}{}
	for _, node := range result.Graph.Nodes {
		nodeSeen[node.GetNodeId()] = struct{}{}
	}
	edgeSeen := map[string]struct{}{}
	for _, edge := range result.Graph.Edges {
		edgeSeen[edge.GetEdgeId()] = struct{}{}
	}
	for _, row := range execResult.Rows {
		for _, value := range row {
			if value.Path == nil {
				continue
			}
			for _, node := range value.Path.Nodes {
				if _, ok := nodeSeen[node.ID]; ok {
					continue
				}
				result.Graph.Nodes = append(result.Graph.Nodes, gqlNodeToProto(node))
				nodeSeen[node.ID] = struct{}{}
			}
			for _, edge := range value.Path.Edges {
				if _, ok := edgeSeen[edge.ID]; ok {
					continue
				}
				result.Graph.Edges = append(result.Graph.Edges, gqlEdgeToProto(edge))
				edgeSeen[edge.ID] = struct{}{}
			}
		}
	}
}

func queryResultFromRowsWithCounters(rows []*clientv1.QueryRow, next string, counters execmodel.Counters) *clientv1.QueryResult {
	return &clientv1.QueryResult{Rows: rows, NextPageToken: next, Graph: graphFromRows(rows), Counters: &clientv1.QueryCounters{RowsReturned: int32(len(rows)), NodesInserted: int32(counters.NodesInserted), NodesUpdated: int32(counters.NodesUpdated), NodesDeleted: int32(counters.NodesDeleted), EdgesInserted: int32(counters.EdgesInserted), EdgesDeleted: int32(counters.EdgesDeleted)}}
}

func mergeQueryResult(aggregate *clientv1.QueryResult, result *clientv1.QueryResult) {
	if aggregate == nil || result == nil {
		return
	}
	if len(result.GetRows()) > 0 {
		aggregate.Rows = result.GetRows()
	}
	aggregate.NextPageToken = result.GetNextPageToken()
	if aggregate.Counters == nil {
		aggregate.Counters = &clientv1.QueryCounters{}
	}
	if result.GetCounters() != nil {
		aggregate.Counters.RowsReturned += result.GetCounters().GetRowsReturned()
		aggregate.Counters.NodesInserted += result.GetCounters().GetNodesInserted()
		aggregate.Counters.NodesUpdated += result.GetCounters().GetNodesUpdated()
		aggregate.Counters.NodesDeleted += result.GetCounters().GetNodesDeleted()
		aggregate.Counters.EdgesInserted += result.GetCounters().GetEdgesInserted()
		aggregate.Counters.EdgesDeleted += result.GetCounters().GetEdgesDeleted()
	}
	if aggregate.Graph == nil {
		aggregate.Graph = &clientv1.ResultGraph{}
	}
	seenNodes := map[string]bool{}
	for _, node := range aggregate.Graph.GetNodes() {
		seenNodes[node.GetNodeId()] = true
	}
	for _, node := range result.GetGraph().GetNodes() {
		if seenNodes[node.GetNodeId()] {
			continue
		}
		seenNodes[node.GetNodeId()] = true
		aggregate.Graph.Nodes = append(aggregate.Graph.Nodes, node)
	}
	seenEdges := map[string]bool{}
	for _, edge := range aggregate.Graph.GetEdges() {
		seenEdges[edge.GetEdgeId()] = true
	}
	for _, edge := range result.GetGraph().GetEdges() {
		if seenEdges[edge.GetEdgeId()] {
			continue
		}
		seenEdges[edge.GetEdgeId()] = true
		aggregate.Graph.Edges = append(aggregate.Graph.Edges, edge)
	}
}

func graphFromRows(rows []*clientv1.QueryRow) *clientv1.ResultGraph {
	seenNodes := map[string]bool{}
	seenEdges := map[string]bool{}
	nodes := []*clientv1.Node{}
	edges := []*clientv1.Edge{}
	for _, row := range rows {
		for _, value := range row.GetFields() {
			if node := value.GetNode(); node != nil && !seenNodes[node.GetNodeId()] {
				seenNodes[node.GetNodeId()] = true
				nodes = append(nodes, node)
			}
			if edge := value.GetEdge(); edge != nil && !seenEdges[edge.GetEdgeId()] {
				seenEdges[edge.GetEdgeId()] = true
				edges = append(edges, edge)
			}
			if path := value.GetPath(); path != nil {
				for _, node := range path.GetNodes() {
					if node != nil && !seenNodes[node.GetNodeId()] {
						seenNodes[node.GetNodeId()] = true
						nodes = append(nodes, node)
					}
				}
				for _, edge := range path.GetEdges() {
					if edge != nil && !seenEdges[edge.GetEdgeId()] {
						seenEdges[edge.GetEdgeId()] = true
						edges = append(edges, edge)
					}
				}
			}
		}
	}
	return &clientv1.ResultGraph{Nodes: nodes, Edges: edges}
}

func paginateExecResult(result execmodel.Result, pageSize int, pageToken string) (execmodel.Result, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return execmodel.Result{}, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > queryMaxPageSize {
		pageSize = queryMaxPageSize
	}
	page := execmodel.Result{Counters: result.Counters, Columns: append([]string(nil), result.Columns...)}
	if start >= len(result.Rows) {
		return page, "", nil
	}
	end := start + pageSize
	if end > len(result.Rows) {
		end = len(result.Rows)
	}
	next := ""
	if end < len(result.Rows) {
		next = strconv.Itoa(end)
	}
	page.Rows = append([]execmodel.Row(nil), result.Rows[start:end]...)
	return page, next, nil
}

func paginateProtoQueryRows(rows []*clientv1.QueryRow, pageSize int, pageToken string) ([]*clientv1.QueryRow, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > queryMaxPageSize {
		pageSize = queryMaxPageSize
	}
	if start >= len(rows) {
		return []*clientv1.QueryRow{}, "", nil
	}
	end := start + pageSize
	if end > len(rows) {
		end = len(rows)
	}
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	return rows[start:end], next, nil
}

func copyMapAny(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func paginateQueryRows(rows []*queryRowState, pageSize int, pageToken string) ([]*queryRowState, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > queryMaxPageSize {
		pageSize = queryMaxPageSize
	}
	if start >= len(rows) {
		return []*queryRowState{}, "", nil
	}
	end := start + pageSize
	if end > len(rows) {
		end = len(rows)
	}
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	return rows[start:end], next, nil
}

func dedupeQueryNodes(nodes []domaingraph.Node) []domaingraph.Node {
	seen := map[string]bool{}
	out := []domaingraph.Node{}
	for _, node := range nodes {
		if seen[node.ID.String()] {
			continue
		}
		seen[node.ID.String()] = true
		out = append(out, node)
	}
	return out
}

func sortTreeNodes(nodes []domaingraph.Node, order map[string]any) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return numericOrder(order[nodes[i].ID.String()], i) < numericOrder(order[nodes[j].ID.String()], j)
	})
}

func numericOrder(value any, fallback int) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	default:
		return fallback * 1000
	}
}

func queryValuesEqual(left any, right any) bool { return compareQueryValues(left, right) == 0 }

func compareQueryValues(left any, right any) int {
	if lt, ok := asQueryTime(left); ok {
		if rt, ok := asQueryTime(right); ok {
			if lt.Before(rt) {
				return -1
			}
			if lt.After(rt) {
				return 1
			}
			return 0
		}
	}
	if lf, ok := asQueryFloat(left); ok {
		if rf, ok := asQueryFloat(right); ok {
			if lf < rf {
				return -1
			}
			if lf > rf {
				return 1
			}
			return 0
		}
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func asQueryFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func asQueryTime(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case string:
		t, err := time.Parse("2006-01-02", v)
		return t, err == nil
	default:
		return time.Time{}, false
	}
}

func protoValue(value any) *structpb.Value {
	out, err := structpb.NewValue(value)
	if err != nil {
		return structpb.NewStringValue(fmt.Sprint(value))
	}
	return out
}

func (s *QueryService) tryExecuteIndexedQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil {
		return false, nil, nil
	}
	if indexed, res, err := s.tryExecuteIndexedPathQuery(ctx, req, tx, schemaCtx, recorder); indexed || err != nil {
		return indexed, res, err
	}
	if indexed, res, err := s.tryExecuteIndexedAdjacencyQuery(ctx, req, tx, recorder); indexed || err != nil {
		return indexed, res, err
	}
	if len(query.GetOrderBy()) == 0 {
		if indexed, res, err := s.tryExecuteIndexedEqualityNodeQuery(ctx, req, tx, schemaCtx, recorder); indexed || err != nil {
			return indexed, res, err
		}
		if indexed, res, err := s.tryExecuteSemanticPredicateQuery(ctx, req, tx, recorder); indexed || err != nil {
			return indexed, res, err
		}
		if indexed, res, err := s.tryExecuteIndexedPredicateNodeQuery(ctx, req, tx, schemaCtx, recorder); indexed || err != nil {
			return indexed, res, err
		}
		return false, nil, nil
	}
	match := query.GetMatch()
	start := match.GetStart()
	if len(query.GetOrderBy()) != 1 || len(start.GetLabels()) != 1 {
		return true, nil, status.Error(codes.FailedPrecondition, "ORDER BY requires an indexed single-label node query")
	}
	order := query.GetOrderBy()[0]
	prop := order.GetValue().GetProp()
	if prop == nil || prop.GetAlias() != start.GetAlias() || strings.TrimSpace(prop.GetName()) == "" {
		return true, nil, status.Error(codes.FailedPrecondition, "ORDER BY requires an indexed property reference on the start alias")
	}
	bounds, err := indexedQueryBounds(query.GetWhere(), start.GetAlias(), prop.GetName())
	if err != nil {
		return true, nil, err
	}
	if schemaCtx.Schema == nil {
		return true, nil, status.Error(codes.FailedPrecondition, "indexed query requires an active schema with an ordered index")
	}
	label := start.GetLabels()[0]
	field := prop.GetName()
	idx, ok := findOrderedNodeIndex(*schemaCtx.Schema, label, field)
	if !ok {
		return true, nil, status.Errorf(codes.FailedPrecondition, "no ordered index for %s.properties.%s", label, field)
	}
	if s.graphs == nil {
		return true, nil, status.Error(codes.Internal, "graph manager is not configured")
	}
	if err := s.graphs.ConfigureIndexes(ctx, tx, schemacompile.Hash(*schemaCtx.Schema), schemaCtx.Schema.Indexes); err != nil {
		return true, nil, mapGraphError(err, "configure query indexes")
	}
	if len(match.GetSteps()) == 1 {
		return s.executeIndexedRootSubtreeQuery(ctx, req, tx, recorder, idx, bounds)
	}
	if len(match.GetSteps()) != 0 {
		return true, nil, status.Error(codes.FailedPrecondition, "ORDER BY traversal requires an indexed bounded subtree query")
	}
	cursor, hasShapedCursor, err := decodeShapedIndexCursor(req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}
	pageLimit := indexedShapingPageLimit(req.GetPageSize(), query.GetLimit(), cursor.RowsReturned)
	if pageLimit == 0 {
		result := queryResultFromRows(nil, "")
		return true, &clientv1.ExecuteQueryResponse{Rows: nil, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: &clientv1.QueryDiagnostics{Plan: "OrderedNodePropertyIndexScan", Indexes: []string{idx.Name}, FullScan: false, NextCursorKind: "index_key"}}, nil
	}
	firstPageOffset := indexedFirstPageOffset(query, hasShapedCursor)
	scanLimit := pageLimit + int(firstPageOffset)
	direction := order.GetDirection()
	indexDirection := schemamodel.IndexSortDirectionAsc
	if direction == clientv1.SortDirection_SORT_DIRECTION_DESC {
		indexDirection = schemamodel.IndexSortDirectionDesc
	}
	nodes, rawNext, stats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: indexDirection, Limit: scanLimit, Cursor: cursor.IndexCursor, HasLow: bounds.hasLow, Low: bounds.low, LowExclusive: bounds.lowExclusive, HasHigh: bounds.hasHigh, High: bounds.high, HighExclusive: bounds.highExclusive})
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed query")
	}
	exec := newQueryExecution(nil, nil)
	rowStates := make([]*queryRowState, 0, len(nodes))
	for _, node := range nodes {
		rowStates = append(rowStates, &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): []domaingraph.Node{node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}})
	}
	shapingQuery := indexedShapingQuery(query, firstPageOffset, pageLimit)
	out, _, err := exec.shapeAndProjectRows(rowStates, shapingQuery, pageLimit, "")
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	next := nextShapedIndexCursor(rawNext, len(out), cursor.RowsReturned, query.GetLimit())
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: stats.Plan, Indexes: []string{idx.Name}, FullScan: stats.FullScan, IndexEntriesScanned: int32(stats.IndexEntriesScanned), NodesLoaded: int32(stats.NodesLoaded), EdgesLoaded: int32(stats.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: stats.NextCursorKind}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

type semanticPredicateTerm struct {
	alias               string
	query               string
	ruleRef             string
	embeddingBindingKey string
	limit               int32
}

func isSemanticPredicateNodeQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || len(query.GetMatch().GetSteps()) != 0 || len(query.GetMatch().GetStart().GetLabels()) != 1 {
		return false
	}
	terms, ok, err := semanticPredicateTerms(query.GetWhere(), query.GetMatch().GetStart().GetAlias())
	return err == nil && ok && len(terms) == 1
}

func (s *QueryService) tryExecuteSemanticPredicateQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || len(query.GetMatch().GetSteps()) != 0 || len(query.GetMatch().GetStart().GetLabels()) != 1 {
		return false, nil, nil
	}
	start := query.GetMatch().GetStart()
	terms, ok, err := semanticPredicateTerms(query.GetWhere(), start.GetAlias())
	if err != nil || !ok || len(terms) == 0 {
		return ok, nil, err
	}
	if len(terms) != 1 {
		return true, nil, status.Error(codes.FailedPrecondition, "semantic predicate query supports one semantic predicate per node query")
	}
	if s.semantic == nil {
		return false, nil, nil
	}
	domain, err := s.visibleTransactionDomain(ctx, tx.PrincipalID, tx)
	if err != nil {
		return true, nil, err
	}
	if !domaingraph.DomainExplicitSemanticSearchable(domain) {
		return true, nil, status.Error(codes.FailedPrecondition, "domain is excluded from semantic search and indexing")
	}
	spaceID, err := parseDomainSpaceID(tx.SpaceID)
	if err != nil {
		return true, nil, err
	}
	domainID, err := parseGraphDomainID(tx.DomainID)
	if err != nil {
		return true, nil, err
	}
	actorID, err := parseIdentityPrincipalID(tx.PrincipalID)
	if err != nil {
		return true, nil, err
	}
	term := terms[0]
	semanticRuleIDs := []domainsemantic.SemanticRuleID{}
	if strings.TrimSpace(term.ruleRef) != "" {
		id, err := parseSemanticRuleID(term.ruleRef)
		if err != nil {
			return true, nil, err
		}
		semanticRuleIDs = append(semanticRuleIDs, id)
	}
	limit := int(term.limit)
	if limit <= 0 {
		limit = effectiveIndexedLimit(req.GetPageSize(), query.GetLimit())
	}
	result, err := s.semantic.Search(ctx, daemonsemantic.SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: semanticRuleIDs, EmbeddingBindingKey: term.embeddingBindingKey, Text: term.query, Limit: limit, ActorPrincipalID: actorID})
	if err != nil {
		return true, nil, mapSemanticError(err, "semantic predicate search")
	}
	residual := removeSemanticPredicates(query.GetWhere())
	exec := newQueryExecution(nil, nil)
	rowStates := make([]*queryRowState, 0, len(result.Results))
	seen := map[string]struct{}{}
	indexNames := map[string]struct{}{}
	for _, item := range result.Results {
		if item.SemanticRuleID != uuid.Nil {
			indexNames[item.SemanticRuleID.String()] = struct{}{}
		} else if item.SemanticIndexID != uuid.Nil {
			indexNames[item.SemanticIndexID.String()] = struct{}{}
		}
		id := item.NodeID.String()
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		node, err := s.graphs.GetNode(ctx, tx, id)
		if err != nil {
			continue
		}
		if !exec.nodeMatches(node, start) {
			continue
		}
		row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
		if residual != nil {
			matched, err := exec.evalExpr(row, residual)
			if err != nil {
				return true, nil, status.Error(codes.InvalidArgument, err.Error())
			}
			if !matched {
				continue
			}
		}
		rowStates = append(rowStates, row)
	}
	out, next, err := exec.shapeAndProjectRows(rowStates, query, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	indexes := make([]string, 0, len(indexNames))
	for id := range indexNames {
		indexes = append(indexes, id)
	}
	sort.Strings(indexes)
	queryResult := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: "SemanticVectorSearch", PlanKind: "semantic_vector", Indexes: indexes, FullScan: false, NodesLoaded: int32(len(rowStates)), RowsReturned: int32(len(out)), NextCursorKind: "offset", PushedPredicates: predicateDiagnostics(query.GetWhere())}
	diagnostics = completeQueryDiagnostics(diagnostics, "semantic_vector", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: queryResult, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func semanticPredicateTerms(expr *clientv1.Expr, alias string) ([]semanticPredicateTerm, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if sem := expr.GetSemantic(); sem != nil {
		if sem.GetAlias() != alias || strings.TrimSpace(sem.GetQuery()) == "" {
			return nil, true, status.Error(codes.FailedPrecondition, "semantic predicate must target the start alias and include query text")
		}
		return []semanticPredicateTerm{{alias: sem.GetAlias(), query: sem.GetQuery(), ruleRef: sem.GetRuleRef(), embeddingBindingKey: sem.GetEmbeddingBindingKey(), limit: sem.GetLimit()}}, true, nil
	}
	if and := expr.GetAnd(); and != nil {
		terms := []semanticPredicateTerm{}
		for _, child := range and.GetExprs() {
			childTerms, _, err := semanticPredicateTerms(child, alias)
			if err != nil {
				return nil, true, err
			}
			terms = append(terms, childTerms...)
		}
		return terms, len(terms) > 0, nil
	}
	if expr.GetOr() != nil {
		if queryHasSemanticPredicate(expr) {
			return nil, true, status.Error(codes.FailedPrecondition, "semantic predicates inside OR are not indexed yet")
		}
	}
	return nil, false, nil
}

func queryHasSemanticPredicate(expr *clientv1.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetSemantic() != nil {
		return true
	}
	if and := expr.GetAnd(); and != nil {
		for _, child := range and.GetExprs() {
			if queryHasSemanticPredicate(child) {
				return true
			}
		}
	}
	if or := expr.GetOr(); or != nil {
		for _, child := range or.GetExprs() {
			if queryHasSemanticPredicate(child) {
				return true
			}
		}
	}
	return false
}

func removeSemanticPredicates(expr *clientv1.Expr) *clientv1.Expr {
	if expr == nil || expr.GetSemantic() != nil {
		return nil
	}
	if and := expr.GetAnd(); and != nil {
		children := []*clientv1.Expr{}
		for _, child := range and.GetExprs() {
			if stripped := removeSemanticPredicates(child); stripped != nil {
				children = append(children, stripped)
			}
		}
		if len(children) == 0 {
			return nil
		}
		if len(children) == 1 {
			return children[0]
		}
		return &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: children}}}
	}
	return expr
}

type indexedPredicateScan struct {
	field         string
	hasLow        bool
	low           any
	lowExclusive  bool
	hasHigh       bool
	high          any
	highExclusive bool
}

type indexedPredicateBranch struct {
	scans []indexedPredicateScan
}

func isIndexedPredicateNodeQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || len(query.GetMatch().GetSteps()) != 0 || len(query.GetMatch().GetStart().GetLabels()) != 1 {
		return false
	}
	branches, ok, err := indexedPredicateBranches(query.GetWhere(), query.GetMatch().GetStart().GetAlias())
	return err == nil && ok && len(branches) > 0
}

func (s *QueryService) tryExecuteIndexedPredicateNodeQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	match := query.GetMatch()
	start := match.GetStart()
	if len(match.GetSteps()) != 0 || len(start.GetLabels()) != 1 {
		return false, nil, nil
	}
	branches, ok, err := indexedPredicateBranches(query.GetWhere(), start.GetAlias())
	if err != nil || !ok {
		return ok, nil, err
	}
	if schemaCtx.Schema == nil {
		return true, nil, status.Error(codes.FailedPrecondition, "indexed predicate query requires an active schema with ordered indexes")
	}
	if s.graphs == nil {
		return true, nil, status.Error(codes.Internal, "graph manager is not configured")
	}
	if err := s.graphs.ConfigureIndexes(ctx, tx, schemacompile.Hash(*schemaCtx.Schema), schemaCtx.Schema.Indexes); err != nil {
		return true, nil, mapGraphError(err, "configure predicate query indexes")
	}
	exec := newQueryExecution(nil, nil)
	label := start.GetLabels()[0]
	union := map[string]domaingraph.Node{}
	indexNames := map[string]struct{}{}
	combined := daegraph.IndexedReadStats{Plan: "OrderedNodePropertyPredicateIndexScan", FullScan: false, NextCursorKind: "offset"}
	for _, branch := range branches {
		branchNodes, branchIndexes, branchStats, err := s.executeIndexedPredicateBranch(ctx, tx, schemaCtx, label, branch)
		if err != nil {
			return true, nil, err
		}
		for _, index := range branchIndexes {
			indexNames[index] = struct{}{}
		}
		combined.IndexEntriesScanned += branchStats.IndexEntriesScanned
		combined.NodesLoaded += branchStats.NodesLoaded
		branchSet := map[string]domaingraph.Node{}
		for _, node := range branchNodes {
			if !exec.nodeMatches(node, start) {
				continue
			}
			row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
			if query.GetWhere() != nil {
				matched, err := exec.evalExpr(row, query.GetWhere())
				if err != nil {
					return true, nil, status.Error(codes.InvalidArgument, err.Error())
				}
				if !matched {
					continue
				}
			}
			branchSet[node.ID.String()] = node
		}
		for id, node := range branchSet {
			union[id] = node
		}
	}
	ids := make([]string, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rowStates := make([]*queryRowState, 0, len(ids))
	for _, id := range ids {
		node := union[id]
		rowStates = append(rowStates, &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}})
	}
	out, next, err := exec.shapeAndProjectRows(rowStates, query, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	idx := make([]string, 0, len(indexNames))
	for index := range indexNames {
		idx = append(idx, index)
	}
	sort.Strings(idx)
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: combined.Plan, Indexes: idx, FullScan: false, IndexEntriesScanned: int32(combined.IndexEntriesScanned), NodesLoaded: int32(combined.NodesLoaded), EdgesLoaded: 0, RowsReturned: int32(len(out)), NextCursorKind: combined.NextCursorKind}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func (s *QueryService) executeIndexedPredicateBranch(ctx context.Context, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, label string, branch indexedPredicateBranch) ([]domaingraph.Node, []string, daegraph.IndexedReadStats, error) {
	var current map[string]domaingraph.Node
	indexes := []string{}
	combined := daegraph.IndexedReadStats{}
	for _, scan := range branch.scans {
		idx, ok := findOrderedNodeIndex(*schemaCtx.Schema, label, scan.field)
		if !ok {
			return nil, nil, combined, status.Errorf(codes.FailedPrecondition, "no ordered index for %s.properties.%s", label, scan.field)
		}
		nodes, _, stats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: schemamodel.IndexSortDirectionAsc, Limit: 0, HasLow: scan.hasLow, Low: scan.low, LowExclusive: scan.lowExclusive, HasHigh: scan.hasHigh, High: scan.high, HighExclusive: scan.highExclusive})
		if err != nil {
			return nil, nil, combined, mapGraphError(err, "execute indexed predicate query")
		}
		indexes = append(indexes, idx.Name)
		combined.IndexEntriesScanned += stats.IndexEntriesScanned
		combined.NodesLoaded += stats.NodesLoaded
		next := map[string]domaingraph.Node{}
		for _, node := range nodes {
			next[node.ID.String()] = node
		}
		if current == nil {
			current = next
			continue
		}
		for id := range current {
			if _, ok := next[id]; !ok {
				delete(current, id)
			}
		}
	}
	out := make([]domaingraph.Node, 0, len(current))
	for _, node := range current {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, indexes, combined, nil
}

func indexedPredicateBranches(expr *clientv1.Expr, alias string) ([]indexedPredicateBranch, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if or := expr.GetOr(); or != nil {
		branches := []indexedPredicateBranch{}
		for _, child := range or.GetExprs() {
			childBranches, ok, err := indexedPredicateBranches(child, alias)
			if err != nil || !ok || len(childBranches) == 0 {
				return nil, ok, err
			}
			branches = append(branches, childBranches...)
		}
		return branches, len(branches) > 0, nil
	}
	branch, ok, err := indexedPredicateBranchForAnd(expr, alias)
	if err != nil || !ok {
		return nil, ok, err
	}
	return []indexedPredicateBranch{branch}, true, nil
}

func indexedPredicateBranchForAnd(expr *clientv1.Expr, alias string) (indexedPredicateBranch, bool, error) {
	if and := expr.GetAnd(); and != nil {
		branch := indexedPredicateBranch{}
		for _, child := range and.GetExprs() {
			childBranch, ok, err := indexedPredicateBranchForAnd(child, alias)
			if err != nil {
				return indexedPredicateBranch{}, true, err
			}
			if ok {
				branch.scans = append(branch.scans, childBranch.scans...)
			}
		}
		return branch, len(branch.scans) > 0, nil
	}
	scan, ok, err := indexedPredicateScanForLeaf(expr, alias)
	if err != nil || !ok {
		return indexedPredicateBranch{}, ok, err
	}
	return indexedPredicateBranch{scans: []indexedPredicateScan{scan}}, true, nil
}

func indexedPredicateScanForLeaf(expr *clientv1.Expr, alias string) (indexedPredicateScan, bool, error) {
	if eq := expr.GetPropertyEquals(); eq != nil {
		if eq.GetAlias() != alias || strings.TrimSpace(eq.GetName()) == "" {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed predicate must target the start alias")
		}
		return indexedPredicateScan{field: eq.GetName(), hasLow: true, low: eq.GetValue().AsInterface(), hasHigh: true, high: eq.GetValue().AsInterface()}, true, nil
	}
	if exists := expr.GetPropertyExists(); exists != nil {
		if exists.GetAlias() != alias || strings.TrimSpace(exists.GetName()) == "" {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed property-exists predicate must target the start alias")
		}
		return indexedPredicateScan{field: exists.GetName()}, true, nil
	}
	if less := expr.GetLessThan(); less != nil {
		prop := less.GetLeft().GetProp()
		if prop == nil || prop.GetAlias() != alias || strings.TrimSpace(prop.GetName()) == "" {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed less-than predicate must compare the start alias property")
		}
		value, err := staticIndexedValue(less.GetRight())
		if err != nil {
			return indexedPredicateScan{}, true, err
		}
		return indexedPredicateScan{field: prop.GetName(), hasHigh: true, high: value, highExclusive: true}, true, nil
	}
	if between := expr.GetBetween(); between != nil {
		prop := between.GetValue().GetProp()
		if prop == nil || prop.GetAlias() != alias || strings.TrimSpace(prop.GetName()) == "" {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed between predicate must compare the start alias property")
		}
		scan := indexedPredicateScan{field: prop.GetName()}
		if between.GetLow() != nil {
			value, err := staticIndexedValue(between.GetLow())
			if err != nil {
				return indexedPredicateScan{}, true, err
			}
			scan.hasLow = true
			scan.low = value
		}
		if between.GetHigh() != nil {
			value, err := staticIndexedValue(between.GetHigh())
			if err != nil {
				return indexedPredicateScan{}, true, err
			}
			scan.hasHigh = true
			scan.high = value
		}
		return scan, true, nil
	}
	if str := expr.GetStringPredicate(); str != nil {
		prop := str.GetValue().GetProp()
		if prop == nil || prop.GetAlias() != alias || strings.TrimSpace(prop.GetName()) == "" {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed string predicate must target the start alias property")
		}
		return indexedPredicateScan{field: prop.GetName()}, true, nil
	}
	if text := expr.GetText(); text != nil {
		if text.GetAlias() != alias {
			return indexedPredicateScan{}, true, status.Error(codes.FailedPrecondition, "indexed text predicate must target the start alias")
		}
		namespace, field := queryFieldParts(text.GetField())
		if namespace != "properties" || strings.TrimSpace(field) == "" {
			return indexedPredicateScan{}, false, nil
		}
		return indexedPredicateScan{field: field}, true, nil
	}
	return indexedPredicateScan{}, false, nil
}

func isIndexedEqualityNodeQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || len(query.GetMatch().GetSteps()) != 0 || len(query.GetMatch().GetStart().GetLabels()) != 1 {
		return false
	}
	field, _, ok, err := indexedEqualityPredicate(query.GetWhere(), query.GetMatch().GetStart().GetAlias())
	return err == nil && ok && strings.TrimSpace(field) != ""
}

func (s *QueryService) tryExecuteIndexedEqualityNodeQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	match := query.GetMatch()
	start := match.GetStart()
	if len(match.GetSteps()) != 0 || len(start.GetLabels()) != 1 {
		return false, nil, nil
	}
	field, value, ok, err := indexedEqualityPredicate(query.GetWhere(), start.GetAlias())
	if err != nil || !ok {
		return ok, nil, err
	}
	if schemaCtx.Schema == nil {
		return true, nil, status.Error(codes.FailedPrecondition, "indexed equality query requires an active schema with an ordered index")
	}
	idx, ok := findOrderedNodeIndex(*schemaCtx.Schema, start.GetLabels()[0], field)
	if !ok {
		return true, nil, status.Errorf(codes.FailedPrecondition, "no ordered index for %s.properties.%s", start.GetLabels()[0], field)
	}
	if s.graphs == nil {
		return true, nil, status.Error(codes.Internal, "graph manager is not configured")
	}
	if err := s.graphs.ConfigureIndexes(ctx, tx, schemacompile.Hash(*schemaCtx.Schema), schemaCtx.Schema.Indexes); err != nil {
		return true, nil, mapGraphError(err, "configure equality query indexes")
	}
	cursor, hasShapedCursor, err := decodeShapedIndexCursor(req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}
	pageLimit := indexedShapingPageLimit(req.GetPageSize(), query.GetLimit(), cursor.RowsReturned)
	if pageLimit == 0 {
		result := queryResultFromRows(nil, "")
		return true, &clientv1.ExecuteQueryResponse{Rows: nil, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: &clientv1.QueryDiagnostics{Plan: "OrderedNodePropertyEqualityIndexScan", Indexes: []string{idx.Name}, FullScan: false, NextCursorKind: "index_key"}}, nil
	}
	firstPageOffset := indexedFirstPageOffset(query, hasShapedCursor)
	scanLimit := pageLimit + int(firstPageOffset)
	nodes, rawNext, stats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: schemamodel.IndexSortDirectionAsc, Limit: scanLimit, Cursor: cursor.IndexCursor, HasLow: true, Low: value, HasHigh: true, High: value})
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed equality query")
	}
	exec := newQueryExecution(nil, nil)
	rowStates := make([]*queryRowState, 0, len(nodes))
	for _, node := range nodes {
		rowStates = append(rowStates, &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {node}}, edgeBindings: map[string][]domaingraph.Edge{}, pathBindings: map[string]*clientv1.PathValue{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}})
	}
	shapingQuery := indexedShapingQuery(query, firstPageOffset, pageLimit)
	out, _, err := exec.shapeAndProjectRows(rowStates, shapingQuery, pageLimit, "")
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	next := nextShapedIndexCursor(rawNext, len(out), cursor.RowsReturned, query.GetLimit())
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: "OrderedNodePropertyEqualityIndexScan", Indexes: []string{idx.Name}, FullScan: stats.FullScan, IndexEntriesScanned: int32(stats.IndexEntriesScanned), NodesLoaded: int32(stats.NodesLoaded), EdgesLoaded: int32(stats.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: stats.NextCursorKind}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func indexedEqualityPredicate(expr *clientv1.Expr, alias string) (string, any, bool, error) {
	if expr == nil {
		return "", nil, false, nil
	}
	if eq := expr.GetPropertyEquals(); eq != nil {
		if eq.GetAlias() != alias || strings.TrimSpace(eq.GetName()) == "" {
			return "", nil, true, status.Error(codes.FailedPrecondition, "indexed equality query predicate must target the start alias")
		}
		return eq.GetName(), eq.GetValue().AsInterface(), true, nil
	}
	return "", nil, false, nil
}

func isIndexedRootSubtreeQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 1 || len(query.GetMatch().GetSteps()) != 1 {
		return false
	}
	start := query.GetMatch().GetStart()
	step := query.GetMatch().GetSteps()[0]
	return len(start.GetLabels()) == 1 && strings.TrimSpace(start.GetAlias()) != "" && step.GetTarget() != nil && strings.TrimSpace(step.GetTarget().GetAlias()) != "" && strings.TrimSpace(step.GetEdgeKind()) != ""
}

type indexedSubtreeExpansion struct {
	rows      []*clientv1.QueryRow
	graph     *clientv1.ResultGraph
	stats     daegraph.IndexedReadStats
	truncated bool
	reason    string
}

func (s *QueryService) executeIndexedRootSubtreeQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, recorder *daegraph.ReadMetadataRecorder, idx schemamodel.IndexDefinition, bounds indexedBounds) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	if err := validateIndexedRootSubtreeShape(query); err != nil {
		return true, nil, err
	}
	cursor, hasShapedCursor, err := decodeShapedIndexCursor(req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}
	pageLimit := effectiveIndexedLimit(req.GetPageSize(), query.GetLimit())
	if pageLimit == 0 {
		result := queryResultFromRows(nil, "")
		return true, &clientv1.ExecuteQueryResponse{Rows: nil, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: &clientv1.QueryDiagnostics{Plan: indexedSubtreePlanName, Indexes: []string{idx.Name}, FullScan: false, NextCursorKind: indexedSubtreeCursorKind}}, nil
	}
	firstPageOffset := indexedFirstPageOffset(query, hasShapedCursor)
	scanLimit := pageLimit + int(firstPageOffset)
	indexDirection := schemamodel.IndexSortDirectionAsc
	if query.GetOrderBy()[0].GetDirection() == clientv1.SortDirection_SORT_DIRECTION_DESC {
		indexDirection = schemamodel.IndexSortDirectionDesc
	}
	rootScanStart := time.Now()
	roots, rawNext, rootStats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: indexDirection, Limit: scanLimit, Cursor: cursor.IndexCursor, HasLow: bounds.hasLow, Low: bounds.low, LowExclusive: bounds.lowExclusive, HasHigh: bounds.hasHigh, High: bounds.high, HighExclusive: bounds.highExclusive})
	rootScanMillis := time.Since(rootScanStart).Milliseconds()
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed root scan")
	}
	if firstPageOffset > 0 {
		if int(firstPageOffset) >= len(roots) {
			roots = nil
		} else {
			roots = roots[firstPageOffset:]
		}
	}
	expansionStart := time.Now()
	expanded, err := s.expandIndexedSubtrees(ctx, tx, query, roots, idx.Name, rootStats)
	expansionMillis := time.Since(expansionStart).Milliseconds()
	if err != nil {
		return true, nil, err
	}
	next := nextShapedIndexCursor(rawNext, len(expanded.rows), 0, 0)
	if expanded.truncated {
		next = ""
	}
	result := queryResultFromRows(expanded.rows, next)
	result.Graph = expanded.graph
	diagnostics := &clientv1.QueryDiagnostics{Plan: indexedSubtreePlanName, Indexes: []string{idx.Name, expanded.stats.IndexName}, FullScan: false, IndexEntriesScanned: int32(expanded.stats.IndexEntriesScanned), NodesLoaded: int32(expanded.stats.NodesLoaded), EdgesLoaded: int32(expanded.stats.EdgesLoaded), RowsReturned: int32(len(expanded.rows)), NextCursorKind: indexedSubtreeCursorKind, RootCount: int32(len(expanded.rows)), Truncated: expanded.truncated, TruncationReason: expanded.reason, RootScanMillis: rootScanMillis, ExpansionMillis: expansionMillis, AdjacencyScanCalls: int32(expanded.stats.AdjacencyScanCalls), NodeReadCalls: int32(expanded.stats.NodeReadCalls)}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: expanded.rows, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func validateIndexedRootSubtreeShape(query *clientv1.GraphQuery) error {
	match := query.GetMatch()
	start := match.GetStart()
	step := match.GetSteps()[0]
	if len(match.GetSteps()) != 1 || len(query.GetOrderBy()) != 1 || len(start.GetLabels()) != 1 || strings.TrimSpace(start.GetAlias()) == "" {
		return status.Error(codes.FailedPrecondition, "indexed subtree requires one ordered single-label root pattern")
	}
	if strings.TrimSpace(step.GetEdgeAlias()) != "" {
		return status.Error(codes.FailedPrecondition, "indexed subtree traversal does not support edge aliases")
	}
	if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "indexed subtree traversal direction is required")
	}
	if strings.TrimSpace(step.GetEdgeKind()) == "" {
		return status.Error(codes.InvalidArgument, "indexed subtree traversal edge_kind is required")
	}
	if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
		return status.Error(codes.InvalidArgument, "indexed subtree traversal target alias is required")
	}
	minDepth, maxDepth, err := traversalDepthBounds(step.GetDepth())
	if err != nil {
		return err
	}
	if maxDepth != -1 && maxDepth < minDepth {
		return status.Error(codes.InvalidArgument, "indexed subtree traversal max_depth must be >= min_depth")
	}
	if maxDepth > indexedSubtreeMaxDepthCap {
		return status.Errorf(codes.InvalidArgument, "indexed subtree traversal max_depth must be <= %d", indexedSubtreeMaxDepthCap)
	}
	return nil
}

func traversalDepthBounds(depth *clientv1.DepthSpec) (int, int, error) {
	if depth == nil {
		return 1, 1, nil
	}
	minDepth := int(depth.GetMinDepth())
	maxDepth := int(depth.GetMaxDepth())
	if minDepth < 0 {
		return 0, 0, status.Error(codes.InvalidArgument, "traversal min_depth must be non-negative")
	}
	return minDepth, maxDepth, nil
}

func subtreeCaps(query *clientv1.GraphQuery) (int, int) {
	maxNodes := int(query.GetMaxNodes())
	if maxNodes <= 0 {
		maxNodes = defaultSubtreeMaxNodes
	}
	maxEdges := int(query.GetMaxEdges())
	if maxEdges <= 0 {
		maxEdges = defaultSubtreeMaxEdges
	}
	return maxNodes, maxEdges
}

func (s *QueryService) expandIndexedSubtrees(ctx context.Context, tx daemonsession.GraphTransaction, query *clientv1.GraphQuery, roots []domaingraph.Node, orderedIndexName string, rootStats daegraph.IndexedReadStats) (indexedSubtreeExpansion, error) {
	match := query.GetMatch()
	start := match.GetStart()
	step := match.GetSteps()[0]
	target := step.GetTarget()
	minDepth, maxDepth, err := traversalDepthBounds(step.GetDepth())
	if err != nil {
		return indexedSubtreeExpansion{}, err
	}
	maxNodes, maxEdges := subtreeCaps(query)
	direction := daegraph.AdjacencyDirectionOut
	if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
		direction = daegraph.AdjacencyDirectionIn
	}
	returns := query.GetReturns()
	if len(returns) == 0 {
		returns = []*clientv1.ReturnProjection{{Alias: target.GetAlias(), OutputName: "graph", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE}}
	}
	result, subtreeStats, err := s.graphs.ScanSubtree(ctx, tx, daegraph.SubtreeScan{Roots: roots, Label: step.GetEdgeKind(), Direction: direction, MinDepth: minDepth, MaxDepth: maxDepth, MaxNodes: maxNodes, MaxEdges: maxEdges, TargetLabels: append([]string(nil), target.GetLabels()...)})
	if err != nil {
		return indexedSubtreeExpansion{}, mapGraphError(err, "execute indexed subtree scan")
	}
	combined := daegraph.IndexedReadStats{Plan: indexedSubtreePlanName, IndexName: string(direction) + ":" + step.GetEdgeKind(), IndexEntriesScanned: rootStats.IndexEntriesScanned + subtreeStats.IndexEntriesScanned, NodesLoaded: rootStats.NodesLoaded + subtreeStats.NodesLoaded, EdgesLoaded: rootStats.EdgesLoaded + subtreeStats.EdgesLoaded, FullScan: rootStats.FullScan || subtreeStats.FullScan, NextCursorKind: indexedSubtreeCursorKind, AdjacencyScanCalls: subtreeStats.AdjacencyScanCalls, NodeReadCalls: subtreeStats.NodeReadCalls}
	exec := newQueryExecution(nil, nil)
	out := make([]*clientv1.QueryRow, 0, len(result.Roots))
	for _, root := range result.Roots {
		row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {root.Root}, target.GetAlias(): append([]domaingraph.Node(nil), root.Nodes...)}, parentByChild: root.ParentByChild, orderByChild: root.OrderByChild}
		protoRow, err := exec.projectRow(row, returns)
		if err != nil {
			return indexedSubtreeExpansion{}, status.Error(codes.InvalidArgument, err.Error())
		}
		out = append(out, protoRow)
	}
	graph := &clientv1.ResultGraph{Nodes: make([]*clientv1.Node, 0, len(result.GraphNodes)), Edges: make([]*clientv1.Edge, 0, len(result.GraphEdges))}
	for _, node := range result.GraphNodes {
		graph.Nodes = append(graph.Nodes, mapProtoNode(node))
	}
	for _, edge := range result.GraphEdges {
		graph.Edges = append(graph.Edges, mapProtoEdge(edge))
	}
	return indexedSubtreeExpansion{rows: out, graph: graph, stats: combined, truncated: result.Truncated, reason: result.TruncationReason}, nil
}

func isIndexedPathQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || strings.TrimSpace(query.GetPathAlias()) == "" || len(query.GetMatch().GetSteps()) == 0 {
		return false
	}
	start := query.GetMatch().GetStart()
	if strings.TrimSpace(start.GetAlias()) == "" {
		return false
	}
	if len(start.GetNodeIds()) == 0 {
		if len(start.GetLabels()) != 1 {
			return false
		}
		if query.GetWhere() != nil {
			_, predicateOK, predicateErr := indexedPredicateBranches(query.GetWhere(), start.GetAlias())
			_, tagOK, tagErr := indexedTagBranches(query.GetWhere(), start.GetAlias())
			if predicateErr != nil || tagErr != nil || (!predicateOK && !tagOK) {
				return false
			}
		}
	}
	for _, step := range query.GetMatch().GetSteps() {
		if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" || strings.TrimSpace(step.GetEdgeKind()) == "" || step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
			return false
		}
	}
	return true
}

func (s *QueryService) tryExecuteIndexedPathQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	if !isIndexedPathQuery(query) {
		return false, nil, nil
	}
	if s.graphs == nil {
		return true, nil, status.Error(codes.Internal, "graph manager is not configured")
	}
	match := query.GetMatch()
	start := match.GetStart()
	pathAlias := strings.TrimSpace(query.GetPathAlias())
	exec := newQueryExecution(nil, nil)
	rows := []*queryRowState{}
	stats := daegraph.IndexedReadStats{Plan: "IndexedMultiHopAdjacencyPathScan", FullScan: false, NextCursorKind: "offset"}
	indexes := map[string]struct{}{}
	starts, startIndexes, startStats, err := s.indexedPathStartNodes(ctx, tx, schemaCtx, query)
	if err != nil {
		return true, nil, err
	}
	for _, index := range startIndexes {
		indexes[index] = struct{}{}
	}
	stats.IndexEntriesScanned += startStats.IndexEntriesScanned
	stats.NodesLoaded += startStats.NodesLoaded
	if err := validateIndexedPathResourceCaps(stats); err != nil {
		return true, nil, err
	}
	for _, startNode := range starts {
		if !exec.nodeMatches(startNode, start) {
			continue
		}
		seed := newQueryRowState(start.GetAlias(), startNode)
		seed.pathBindings[pathAlias] = &clientv1.PathValue{Nodes: []*clientv1.Node{mapProtoNode(startNode)}}
		currentRows := []*queryRowState{seed}
		currentAlias := start.GetAlias()
		for _, step := range match.GetSteps() {
			nextRows := []*queryRowState{}
			direction := daegraph.AdjacencyDirectionOut
			if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
				direction = daegraph.AdjacencyDirectionIn
			}
			indexes[string(direction)+":"+step.GetEdgeKind()] = struct{}{}
			for _, row := range currentRows {
				for _, current := range row.bindings[currentAlias] {
					expanded, expandedStats, err := s.expandIndexedPathStep(ctx, tx, exec, row, pathAlias, current, step, direction)
					if err != nil {
						return true, nil, err
					}
					stats.IndexEntriesScanned += expandedStats.IndexEntriesScanned
					stats.EdgesLoaded += expandedStats.EdgesLoaded
					stats.NodesLoaded += expandedStats.NodesLoaded
					stats.AdjacencyScanCalls += expandedStats.AdjacencyScanCalls
					stats.NodeReadCalls += expandedStats.NodeReadCalls
					if err := validateIndexedPathResourceCaps(stats); err != nil {
						return true, nil, err
					}
					nextRows = append(nextRows, expanded...)
				}
			}
			currentRows = nextRows
			currentAlias = step.GetTarget().GetAlias()
		}
		rows = append(rows, currentRows...)
		if len(rows) > indexedPathMaxRows {
			return true, nil, status.Errorf(codes.FailedPrecondition, "indexed path query produced more than %d rows; add indexed start bounds or lower traversal depth", indexedPathMaxRows)
		}
	}
	if query.GetWhere() != nil {
		filtered := []*queryRowState{}
		for _, row := range rows {
			ok, err := exec.evalExpr(row, query.GetWhere())
			if err != nil {
				return true, nil, status.Error(codes.InvalidArgument, err.Error())
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	out, next, err := exec.shapeAndProjectRows(rows, query, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	idx := make([]string, 0, len(indexes))
	for index := range indexes {
		idx = append(idx, index)
	}
	sort.Strings(idx)
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: stats.Plan, Indexes: idx, FullScan: false, IndexEntriesScanned: int32(stats.IndexEntriesScanned), NodesLoaded: int32(stats.NodesLoaded), EdgesLoaded: int32(stats.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: stats.NextCursorKind, AdjacencyScanCalls: int32(stats.AdjacencyScanCalls), NodeReadCalls: int32(stats.NodeReadCalls)}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func validateIndexedPathResourceCaps(stats daegraph.IndexedReadStats) error {
	if stats.NodesLoaded > indexedPathMaxNodesLoaded {
		return status.Errorf(codes.FailedPrecondition, "indexed path query loaded more than %d nodes; add indexed start bounds or lower traversal depth", indexedPathMaxNodesLoaded)
	}
	if stats.EdgesLoaded > indexedPathMaxEdgesLoaded {
		return status.Errorf(codes.FailedPrecondition, "indexed path query loaded more than %d edges; add indexed start bounds or lower traversal depth", indexedPathMaxEdgesLoaded)
	}
	return nil
}

func (s *QueryService) indexedPathStartNodes(ctx context.Context, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, query *clientv1.GraphQuery) ([]domaingraph.Node, []string, daegraph.IndexedReadStats, error) {
	start := query.GetMatch().GetStart()
	stats := daegraph.IndexedReadStats{}
	if len(start.GetNodeIds()) > 0 {
		nodes := make([]domaingraph.Node, 0, len(start.GetNodeIds()))
		for _, startID := range start.GetNodeIds() {
			startNode, err := s.graphs.GetNode(ctx, tx, startID)
			if err != nil {
				return nil, nil, stats, mapGraphError(err, "query get path start node")
			}
			stats.NodesLoaded++
			nodes = append(nodes, startNode)
		}
		return nodes, nil, stats, nil
	}
	if len(start.GetLabels()) != 1 {
		return nil, nil, stats, status.Error(codes.FailedPrecondition, "indexed path start requires explicit node IDs or one indexed start label")
	}
	if query.GetWhere() == nil {
		nodes, next, labelStats, err := s.graphs.ScanLabel(ctx, tx, daegraph.LabelScan{Label: start.GetLabels()[0], Limit: indexedPathMaxStartNodes + 1})
		if err != nil {
			return nil, nil, stats, mapGraphError(err, "execute indexed path label scan")
		}
		stats.IndexEntriesScanned += labelStats.IndexEntriesScanned
		stats.NodesLoaded += labelStats.NodesLoaded
		if next != "" || len(nodes) > indexedPathMaxStartNodes {
			return nil, nil, stats, status.Errorf(codes.FailedPrecondition, "indexed label path start matched more than %d nodes; add indexed start predicates", indexedPathMaxStartNodes)
		}
		return nodes, []string{labelStats.IndexName}, stats, nil
	}
	if tagBranches, ok, err := indexedTagBranches(query.GetWhere(), start.GetAlias()); err != nil || ok {
		if err != nil {
			return nil, nil, stats, err
		}
		return s.indexedPathTagStartNodes(ctx, tx, start.GetLabels()[0], tagBranches, stats)
	}
	branches, ok, err := indexedPredicateBranches(query.GetWhere(), start.GetAlias())
	if err != nil || !ok || len(branches) == 0 {
		return nil, nil, stats, status.Error(codes.FailedPrecondition, "indexed path start requires explicit node IDs, label-only start, tag start, or indexed start predicates")
	}
	if schemaCtx.Schema == nil {
		return nil, nil, stats, status.Error(codes.FailedPrecondition, "indexed path start requires an active schema with ordered indexes")
	}
	if err := s.graphs.ConfigureIndexes(ctx, tx, schemacompile.Hash(*schemaCtx.Schema), schemaCtx.Schema.Indexes); err != nil {
		return nil, nil, stats, mapGraphError(err, "configure indexed path start indexes")
	}
	label := start.GetLabels()[0]
	union := map[string]domaingraph.Node{}
	indexNames := map[string]struct{}{}
	for _, branch := range branches {
		branchNodes, branchIndexes, branchStats, err := s.executeIndexedPredicateBranch(ctx, tx, schemaCtx, label, branch)
		if err != nil {
			return nil, nil, stats, err
		}
		stats.IndexEntriesScanned += branchStats.IndexEntriesScanned
		stats.NodesLoaded += branchStats.NodesLoaded
		for _, index := range branchIndexes {
			indexNames[index] = struct{}{}
		}
		for _, node := range branchNodes {
			union[node.ID.String()] = node
			if len(union) > indexedPathMaxStartNodes {
				return nil, nil, stats, status.Errorf(codes.FailedPrecondition, "indexed path start matched more than %d nodes; add tighter indexed bounds", indexedPathMaxStartNodes)
			}
		}
	}
	ids := make([]string, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]domaingraph.Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, union[id])
	}
	indexes := make([]string, 0, len(indexNames))
	for index := range indexNames {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return nodes, indexes, stats, nil
}

type indexedTagBranch struct {
	tags []string
}

func indexedTagBranches(expr *clientv1.Expr, alias string) ([]indexedTagBranch, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	if or := expr.GetOr(); or != nil {
		branches := []indexedTagBranch{}
		for _, child := range or.GetExprs() {
			childBranches, ok, err := indexedTagBranches(child, alias)
			if err != nil || !ok || len(childBranches) == 0 {
				return nil, ok, err
			}
			branches = append(branches, childBranches...)
		}
		return branches, len(branches) > 0, nil
	}
	branch, ok, err := indexedTagBranchForAnd(expr, alias)
	if err != nil || !ok {
		return nil, ok, err
	}
	return []indexedTagBranch{branch}, true, nil
}

func indexedTagBranchForAnd(expr *clientv1.Expr, alias string) (indexedTagBranch, bool, error) {
	if and := expr.GetAnd(); and != nil {
		branch := indexedTagBranch{}
		for _, child := range and.GetExprs() {
			childBranch, ok, err := indexedTagBranchForAnd(child, alias)
			if err != nil {
				return indexedTagBranch{}, true, err
			}
			if !ok {
				return indexedTagBranch{}, false, nil
			}
			branch.tags = append(branch.tags, childBranch.tags...)
		}
		return branch, len(branch.tags) > 0, nil
	}
	tag := expr.GetHasTag()
	if tag == nil {
		return indexedTagBranch{}, false, nil
	}
	if tag.GetAlias() != alias || strings.TrimSpace(tag.GetTag()) == "" {
		return indexedTagBranch{}, true, status.Error(codes.FailedPrecondition, "indexed tag predicate must target the start alias")
	}
	normalized, err := domaingraph.NormalizeTag(tag.GetTag())
	if err != nil {
		return indexedTagBranch{}, true, status.Error(codes.InvalidArgument, err.Error())
	}
	return indexedTagBranch{tags: []string{normalized}}, true, nil
}

func (s *QueryService) indexedPathTagStartNodes(ctx context.Context, tx daemonsession.GraphTransaction, label string, branches []indexedTagBranch, stats daegraph.IndexedReadStats) ([]domaingraph.Node, []string, daegraph.IndexedReadStats, error) {
	union := map[string]domaingraph.Node{}
	indexNames := map[string]struct{}{}
	for _, branch := range branches {
		var current map[string]domaingraph.Node
		for _, tag := range branch.tags {
			nodes, next, tagStats, err := s.graphs.ScanTag(ctx, tx, daegraph.TagScan{Tag: tag, Limit: indexedPathMaxStartNodes + 1})
			if err != nil {
				return nil, nil, stats, mapGraphError(err, "execute indexed path tag scan")
			}
			stats.IndexEntriesScanned += tagStats.IndexEntriesScanned
			stats.NodesLoaded += tagStats.NodesLoaded
			indexNames[tagStats.IndexName] = struct{}{}
			if next != "" || len(nodes) > indexedPathMaxStartNodes {
				return nil, nil, stats, status.Errorf(codes.FailedPrecondition, "indexed tag path start matched more than %d nodes; add tighter indexed start predicates", indexedPathMaxStartNodes)
			}
			nextSet := map[string]domaingraph.Node{}
			for _, node := range nodes {
				if !nodeHasLabels(node.Labels, []string{label}) {
					continue
				}
				nextSet[node.ID.String()] = node
			}
			if current == nil {
				current = nextSet
				continue
			}
			for id := range current {
				if _, ok := nextSet[id]; !ok {
					delete(current, id)
				}
			}
		}
		for id, node := range current {
			union[id] = node
			if len(union) > indexedPathMaxStartNodes {
				return nil, nil, stats, status.Errorf(codes.FailedPrecondition, "indexed tag path start matched more than %d nodes; add tighter indexed start predicates", indexedPathMaxStartNodes)
			}
		}
	}
	ids := make([]string, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]domaingraph.Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, union[id])
	}
	indexes := make([]string, 0, len(indexNames))
	for index := range indexNames {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return nodes, indexes, stats, nil
}

func (s *QueryService) expandIndexedPathStep(ctx context.Context, tx daemonsession.GraphTransaction, exec *queryExecution, row *queryRowState, pathAlias string, start domaingraph.Node, step *clientv1.TraversalStep, direction daegraph.AdjacencyDirection) ([]*queryRowState, daegraph.IndexedReadStats, error) {
	minDepth, maxDepth, err := traversalDepthBounds(step.GetDepth())
	if err != nil {
		return nil, daegraph.IndexedReadStats{}, err
	}
	if maxDepth == -1 || maxDepth > indexedSubtreeMaxDepthCap {
		return nil, daegraph.IndexedReadStats{}, status.Errorf(codes.InvalidArgument, "indexed path traversal max_depth must be between min_depth and %d", indexedSubtreeMaxDepthCap)
	}
	out := []*queryRowState{}
	stats := daegraph.IndexedReadStats{}
	var visit func(base *queryRowState, node domaingraph.Node, depth int, seen map[string]struct{}) error
	visit = func(base *queryRowState, node domaingraph.Node, depth int, seen map[string]struct{}) error {
		if depth >= maxDepth {
			return nil
		}
		edges, _, scanStats, err := s.graphs.ScanAdjacency(ctx, tx, daegraph.AdjacencyScan{NodeID: node.ID.String(), Label: step.GetEdgeKind(), Direction: direction})
		if err != nil {
			return mapGraphError(err, "execute indexed path adjacency scan")
		}
		stats.IndexEntriesScanned += scanStats.IndexEntriesScanned
		stats.EdgesLoaded += scanStats.EdgesLoaded
		stats.AdjacencyScanCalls++
		if err := validateIndexedPathResourceCaps(stats); err != nil {
			return err
		}
		for _, edge := range edges {
			endpointID := edge.ToID.String()
			if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
				endpointID = edge.FromID.String()
			}
			if _, cycle := seen[endpointID]; cycle {
				continue
			}
			endpoint, err := s.graphs.GetNode(ctx, tx, endpointID)
			if err != nil {
				return mapGraphError(err, "query get indexed path endpoint")
			}
			stats.NodesLoaded++
			stats.NodeReadCalls++
			if err := validateIndexedPathResourceCaps(stats); err != nil {
				return err
			}
			childDepth := depth + 1
			child := cloneQueryRowState(base)
			child.bindings[step.GetTarget().GetAlias()] = []domaingraph.Node{endpoint}
			if alias := strings.TrimSpace(step.GetEdgeAlias()); alias != "" {
				child.edgeBindings[alias] = []domaingraph.Edge{edge}
			}
			path := clonePathValue(child.pathBindings[pathAlias])
			path.Edges = append(path.Edges, mapProtoEdge(edge))
			path.Nodes = append(path.Nodes, mapProtoNode(endpoint))
			child.pathBindings[pathAlias] = path
			if childDepth >= minDepth && exec.nodeMatches(endpoint, step.GetTarget()) {
				out = append(out, child)
			}
			nextSeen := map[string]struct{}{}
			for key := range seen {
				nextSeen[key] = struct{}{}
			}
			nextSeen[endpointID] = struct{}{}
			if err := visit(child, endpoint, childDepth, nextSeen); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(row, start, 0, map[string]struct{}{start.ID.String(): {}}); err != nil {
		return nil, daegraph.IndexedReadStats{}, err
	}
	return out, stats, nil
}

func isIndexedAdjacencyQuery(query *clientv1.GraphQuery) bool {
	if query == nil || query.GetMatch() == nil || query.GetMatch().GetStart() == nil || len(query.GetOrderBy()) != 0 || len(query.GetMatch().GetSteps()) != 1 {
		return false
	}
	start := query.GetMatch().GetStart()
	step := query.GetMatch().GetSteps()[0]
	return strings.TrimSpace(start.GetAlias()) != "" && len(start.GetNodeIds()) > 0 && step.GetTarget() != nil && strings.TrimSpace(step.GetTarget().GetAlias()) != "" && strings.TrimSpace(step.GetEdgeAlias()) != ""
}

type indexedAdjacencyCursor struct {
	StartIndex   int    `json:"start_index"`
	EdgeCursor   string `json:"edge_cursor,omitempty"`
	RowsReturned int    `json:"rows_returned,omitempty"`
}

func encodeIndexedAdjacencyCursor(cursor indexedAdjacencyCursor) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return "multi:" + base64.RawURLEncoding.EncodeToString(payload)
}

func decodeIndexedAdjacencyCursor(token string) (indexedAdjacencyCursor, bool, error) {
	if !strings.HasPrefix(token, "multi:") {
		return indexedAdjacencyCursor{}, false, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "multi:"))
	if err != nil {
		return indexedAdjacencyCursor{}, true, err
	}
	var out indexedAdjacencyCursor
	if err := json.Unmarshal(payload, &out); err != nil {
		return indexedAdjacencyCursor{}, true, err
	}
	if out.StartIndex < 0 {
		return indexedAdjacencyCursor{}, true, fmt.Errorf("negative start_index")
	}
	if out.RowsReturned < 0 {
		return indexedAdjacencyCursor{}, true, fmt.Errorf("negative rows_returned")
	}
	return out, true, nil
}

func (s *QueryService) tryExecuteIndexedAdjacencyQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest, tx daemonsession.GraphTransaction, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteQueryResponse, error) {
	query := req.GetQuery()
	match := query.GetMatch()
	if len(query.GetOrderBy()) != 0 || len(match.GetSteps()) != 1 {
		return false, nil, nil
	}
	start := match.GetStart()
	step := match.GetSteps()[0]
	if strings.TrimSpace(step.GetEdgeAlias()) == "" || len(start.GetNodeIds()) == 0 {
		return false, nil, nil
	}
	if strings.TrimSpace(start.GetAlias()) == "" {
		return true, nil, status.Error(codes.InvalidArgument, "start alias is required")
	}
	if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
		return true, nil, status.Error(codes.InvalidArgument, "traversal target alias is required")
	}
	if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
		return true, nil, status.Error(codes.InvalidArgument, "traversal direction is required")
	}
	if strings.TrimSpace(step.GetEdgeKind()) == "" {
		return true, nil, status.Error(codes.InvalidArgument, "traversal edge_kind is required")
	}
	if depth := step.GetDepth(); depth != nil && (depth.GetMinDepth() > 1 || depth.GetMaxDepth() != 1) {
		return true, nil, status.Error(codes.InvalidArgument, "indexed edge traversal supports one-hop depth only")
	}
	multiCursor, isMultiCursor, err := decodeIndexedAdjacencyCursor(req.GetPageToken())
	if err != nil {
		return true, nil, status.Error(codes.InvalidArgument, "invalid adjacency page token")
	}
	if !isMultiCursor && req.GetPageToken() != "" && len(start.GetNodeIds()) > 1 {
		return true, nil, status.Error(codes.InvalidArgument, "multi-node adjacency pagination requires a multi-node page token")
	}
	if s.graphs == nil {
		return true, nil, status.Error(codes.Internal, "graph manager is not configured")
	}
	pageLimit := effectiveIndexedLimit(req.GetPageSize(), 0)
	queryLimit := int(query.GetLimit())
	rowsReturnedBefore := 0
	if isMultiCursor {
		rowsReturnedBefore = multiCursor.RowsReturned
	}
	if queryLimit > 0 {
		if rowsReturnedBefore >= queryLimit {
			return true, nil, status.Error(codes.InvalidArgument, "adjacency page token is beyond query limit")
		}
		if remainingLimit := queryLimit - rowsReturnedBefore; remainingLimit < pageLimit {
			pageLimit = remainingLimit
		}
	}
	remaining := pageLimit
	direction := daegraph.AdjacencyDirectionOut
	if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
		direction = daegraph.AdjacencyDirectionIn
	}
	returns := query.GetReturns()
	if len(returns) == 0 {
		returns = []*clientv1.ReturnProjection{{Alias: start.GetAlias(), OutputName: start.GetAlias(), Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}
	}
	exec := newQueryExecution(nil, nil)
	out := make([]*clientv1.QueryRow, 0, pageLimit)
	next := ""
	combined := daegraph.IndexedReadStats{Plan: "EdgeAdjacencyIndexScan", IndexName: string(direction) + ":" + step.GetEdgeKind(), NextCursorKind: "adjacency_key"}
	startIndex := 0
	initialEdgeCursor := ""
	if isMultiCursor {
		startIndex = multiCursor.StartIndex
		initialEdgeCursor = multiCursor.EdgeCursor
	} else if len(start.GetNodeIds()) == 1 {
		initialEdgeCursor = req.GetPageToken()
	}
	if startIndex >= len(start.GetNodeIds()) {
		return true, nil, status.Error(codes.InvalidArgument, "adjacency page token start_index is out of range")
	}
	for i := startIndex; i < len(start.GetNodeIds()); i++ {
		if remaining <= 0 {
			break
		}
		startID := start.GetNodeIds()[i]
		startNode, err := s.graphs.GetNode(ctx, tx, startID)
		if err != nil {
			return true, nil, mapGraphError(err, "query get start node")
		}
		combined.NodesLoaded++
		if !exec.nodeMatches(startNode, start) {
			continue
		}
		cursor := ""
		if i == startIndex {
			cursor = initialEdgeCursor
		}
		for remaining > 0 {
			edges, edgeNext, stats, err := s.graphs.ScanAdjacency(ctx, tx, daegraph.AdjacencyScan{NodeID: startID, Label: step.GetEdgeKind(), Direction: direction, Limit: remaining, Cursor: cursor})
			if err != nil {
				return true, nil, mapGraphError(err, "execute adjacency query")
			}
			combined.IndexEntriesScanned += stats.IndexEntriesScanned
			combined.EdgesLoaded += stats.EdgesLoaded
			for _, edge := range edges {
				endpointID := edge.ToID.String()
				if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
					endpointID = edge.FromID.String()
				}
				endpoint, err := s.graphs.GetNode(ctx, tx, endpointID)
				if err != nil {
					return true, nil, mapGraphError(err, "query get endpoint node")
				}
				combined.NodesLoaded++
				if !exec.nodeMatches(endpoint, step.GetTarget()) {
					continue
				}
				row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {startNode}, step.GetTarget().GetAlias(): {endpoint}}, edgeBindings: map[string][]domaingraph.Edge{step.GetEdgeAlias(): {edge}}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
				if query.GetWhere() != nil {
					ok, err := exec.evalExpr(row, query.GetWhere())
					if err != nil {
						return true, nil, status.Error(codes.InvalidArgument, err.Error())
					}
					if !ok {
						continue
					}
				}
				protoRow, err := exec.projectRow(row, returns)
				if err != nil {
					return true, nil, status.Error(codes.InvalidArgument, err.Error())
				}
				out = append(out, protoRow)
				remaining--
				if remaining <= 0 {
					break
				}
			}
			rowsReturnedTotal := rowsReturnedBefore + len(out)
			limitAllowsMore := queryLimit <= 0 || rowsReturnedTotal < queryLimit
			if len(start.GetNodeIds()) == 1 {
				if queryLimit > 0 {
					if edgeNext != "" && limitAllowsMore {
						next = encodeIndexedAdjacencyCursor(indexedAdjacencyCursor{StartIndex: i, EdgeCursor: edgeNext, RowsReturned: rowsReturnedTotal})
					}
				} else {
					next = edgeNext
				}
			} else if remaining <= 0 && limitAllowsMore {
				if edgeNext != "" {
					next = encodeIndexedAdjacencyCursor(indexedAdjacencyCursor{StartIndex: i, EdgeCursor: edgeNext, RowsReturned: rowsReturnedTotal})
				} else if i+1 < len(start.GetNodeIds()) {
					next = encodeIndexedAdjacencyCursor(indexedAdjacencyCursor{StartIndex: i + 1, RowsReturned: rowsReturnedTotal})
				}
			}
			if edgeNext == "" || remaining <= 0 {
				break
			}
			cursor = edgeNext
		}
		rowsReturnedTotal := rowsReturnedBefore + len(out)
		if len(start.GetNodeIds()) > 1 && remaining <= 0 && next == "" && i+1 < len(start.GetNodeIds()) && (queryLimit <= 0 || rowsReturnedTotal < queryLimit) {
			next = encodeIndexedAdjacencyCursor(indexedAdjacencyCursor{StartIndex: i + 1, RowsReturned: rowsReturnedTotal})
		}
	}
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: combined.Plan, Indexes: []string{combined.IndexName}, FullScan: combined.FullScan, IndexEntriesScanned: int32(combined.IndexEntriesScanned), NodesLoaded: int32(combined.NodesLoaded), EdgesLoaded: int32(combined.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: combined.NextCursorKind}
	diagnostics = completeQueryDiagnostics(diagnostics, "indexed", time.Time{}, 0, int(diagnostics.GetRowsReturned()))
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
}

func findOrderedNodeIndex(schemaDoc schemamodel.DomainSchema, label string, field string) (schemamodel.IndexDefinition, bool) {
	schemaDoc = schemaDoc.Normalize()
	for _, idx := range schemaDoc.Indexes {
		if idx.TargetKind != schemamodel.IndexTargetNode || idx.Kind != schemamodel.IndexKindOrdered || idx.Field.Namespace != "properties" || idx.Field.Name != field {
			continue
		}
		for _, idxLabel := range idx.Labels {
			if idxLabel == label {
				return idx, true
			}
		}
		if idx.TargetType == label {
			return idx, true
		}
	}
	return schemamodel.IndexDefinition{}, false
}

func effectiveIndexedLimit(pageSize int32, queryLimit int32) int {
	limit := int(pageSize)
	if limit <= 0 || limit > queryMaxPageSize {
		limit = queryMaxPageSize
	}
	if queryLimit > 0 && int(queryLimit) < limit {
		limit = int(queryLimit)
	}
	return limit
}

type shapedIndexCursor struct {
	IndexCursor  string `json:"index_cursor,omitempty"`
	RowsReturned int    `json:"rows_returned,omitempty"`
}

func encodeShapedIndexCursor(cursor shapedIndexCursor) string {
	payload, _ := json.Marshal(cursor)
	return shapedIndexCursorPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

func decodeShapedIndexCursor(token string) (shapedIndexCursor, bool, error) {
	if strings.TrimSpace(token) == "" || !strings.HasPrefix(token, shapedIndexCursorPrefix) {
		return shapedIndexCursor{IndexCursor: token}, false, nil
	}
	encoded := strings.TrimPrefix(token, shapedIndexCursorPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return shapedIndexCursor{}, true, err
	}
	var out shapedIndexCursor
	if err := json.Unmarshal(payload, &out); err != nil {
		return shapedIndexCursor{}, true, err
	}
	if out.RowsReturned < 0 {
		return shapedIndexCursor{}, true, fmt.Errorf("negative rows_returned")
	}
	return out, true, nil
}

func indexedShapingPageLimit(pageSize int32, queryLimit int32, rowsReturned int) int {
	limit := int(pageSize)
	if limit <= 0 || limit > queryMaxPageSize {
		limit = queryMaxPageSize
	}
	if queryLimit > 0 {
		remaining := int(queryLimit) - rowsReturned
		if remaining <= 0 {
			return 0
		}
		if remaining < limit {
			limit = remaining
		}
	}
	return limit
}

func indexedFirstPageOffset(query *clientv1.GraphQuery, hasShapedCursor bool) int32 {
	if hasShapedCursor || strings.TrimSpace(query.GetPathAlias()) != "" {
		return 0
	}
	return query.GetOffset()
}

func indexedShapingQuery(query *clientv1.GraphQuery, offset int32, limit int) *clientv1.GraphQuery {
	copyQuery := *query
	copyQuery.Offset = offset
	copyQuery.Limit = int32(limit)
	return &copyQuery
}

func nextShapedIndexCursor(rawNext string, pageRows int, priorRowsReturned int, queryLimit int32) string {
	if rawNext == "" || pageRows == 0 {
		return ""
	}
	rowsReturned := priorRowsReturned + pageRows
	if queryLimit > 0 && rowsReturned >= int(queryLimit) {
		return ""
	}
	return encodeShapedIndexCursor(shapedIndexCursor{IndexCursor: rawNext, RowsReturned: rowsReturned})
}

func (s *QueryService) tryExecuteIndexedGQL(ctx context.Context, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, plan planmodel.Plan, pageSize int, pageToken string, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteGQLResponse, error) {
	if len(plan.Operations) != 1 {
		return false, nil, nil
	}
	if path, ok := plan.Operations[0].(planmodel.QueryPathOperation); ok {
		return s.tryExecuteIndexedGQLPath(ctx, tx, schemaCtx, path, pageSize, pageToken, recorder)
	}
	op, ok := plan.Operations[0].(planmodel.QueryNodesOperation)
	if !ok {
		return false, nil, nil
	}
	returns := gqlStructuredReturns(op.Returns)
	queryLimit := int32(op.Limit)
	if len(op.OrderBy) == 0 {
		if schemaCtx.Schema == nil && !gqlNodeOperationHasSemanticPredicate(op) {
			return false, nil, nil
		}
		where, ok := gqlNodePredicatesToStructured(op)
		if !ok {
			return false, nil, nil
		}
		request := &clientv1.ExecuteQueryRequest{TransactionId: tx.ID, PageSize: int32(pageSize), PageToken: pageToken, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Variable, Labels: append([]string(nil), op.Labels...)}}, Where: where, Returns: returns, AggregateReturns: gqlStructuredAggregates(op.Aggregates), Distinct: op.Distinct, Offset: int32(op.Offset), Limit: queryLimit}}
		indexed, res, err := s.tryExecuteIndexedQuery(ctx, request, tx, schemaCtx, recorder)
		if !indexed || err != nil {
			return indexed, nil, err
		}
		return true, &clientv1.ExecuteGQLResponse{Result: res.GetResult(), ReadMetadata: res.GetReadMetadata(), Diagnostics: res.GetDiagnostics()}, nil
	}
	if len(op.OrderBy) != 1 || len(op.Labels) != 1 || len(op.Properties) != 0 || len(op.ComparisonPredicates) != 0 || len(op.TextPredicates) != 0 || len(op.SemanticPredicates) != 0 {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL ORDER BY requires an indexed single-label node query")
	}
	order := op.OrderBy[0]
	if order.Variable != op.Variable || order.Property == "" {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL ORDER BY requires an indexed property reference on the matched node")
	}
	direction := clientv1.SortDirection_SORT_DIRECTION_ASC
	if order.Direction == planmodel.SortDescending {
		direction = clientv1.SortDirection_SORT_DIRECTION_DESC
	}
	request := &clientv1.ExecuteQueryRequest{TransactionId: tx.ID, PageSize: int32(pageSize), PageToken: pageToken, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Variable, Labels: append([]string(nil), op.Labels...)}}, Returns: returns, OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: op.Variable, Name: order.Property}}}, Direction: direction}}, Limit: queryLimit}}
	indexed, res, err := s.tryExecuteIndexedQuery(ctx, request, tx, schemaCtx, recorder)
	if !indexed || err != nil {
		return indexed, nil, err
	}
	return true, &clientv1.ExecuteGQLResponse{Result: res.GetResult(), ReadMetadata: res.GetReadMetadata(), Diagnostics: res.GetDiagnostics()}, nil
}

func gqlStructuredReturns(returns []planmodel.ReturnItem) []*clientv1.ReturnProjection {
	out := make([]*clientv1.ReturnProjection, 0, len(returns))
	for _, ret := range returns {
		kind := clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE
		alias := ret.Variable
		if ret.Kind == planmodel.ReturnProperty {
			kind = clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR
			alias = gqlStructuredScalarAlias(ret)
		}
		output := ret.OutputName
		if output == "" {
			output = gqlReturnOutputName(ret)
		}
		out = append(out, &clientv1.ReturnProjection{Alias: alias, OutputName: output, Kind: kind})
	}
	return out
}

func gqlStructuredAggregates(aggregates []planmodel.AggregateItem) []*clientv1.AggregateProjection {
	out := make([]*clientv1.AggregateProjection, 0, len(aggregates))
	for _, agg := range aggregates {
		fn := clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT
		switch strings.ToLower(agg.Function) {
		case "sum":
			fn = clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM
		case "avg":
			fn = clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG
		case "min":
			fn = clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN
		case "max":
			fn = clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX
		}
		arg := &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}
		if fn == clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT {
			if !agg.Star && agg.Alias != "" {
				if agg.Property != "" {
					arg = &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: gqlAggregateValueExpr(agg)}}
				} else {
					arg = &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Alias{Alias: agg.Alias}}
				}
			}
		} else {
			arg = &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: gqlAggregateValueExpr(agg)}}
		}
		out = append(out, &clientv1.AggregateProjection{OutputName: agg.Output, Function: fn, Argument: arg})
	}
	return out
}

func gqlAggregateValueExpr(agg planmodel.AggregateItem) *clientv1.ValueExpr {
	name := agg.Property
	if agg.Namespace != "" && agg.Namespace != "properties" {
		name = agg.Namespace + "." + agg.Property
	}
	return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: agg.Alias, Name: name}}}
}

func gqlNodeOperationHasSemanticPredicate(op planmodel.QueryNodesOperation) bool {
	if len(op.SemanticPredicates) > 0 {
		return true
	}
	return gqlPredicateHasSemantic(op.Predicate)
}

func gqlPredicateHasSemantic(pred *planmodel.PredicateExpr) bool {
	if pred == nil {
		return false
	}
	if pred.Leaf != nil && pred.Leaf.Semantic != nil {
		return true
	}
	for i := range pred.Terms {
		if gqlPredicateHasSemantic(&pred.Terms[i]) {
			return true
		}
	}
	return false
}

func gqlNodePredicatesToStructured(op planmodel.QueryNodesOperation) (*clientv1.Expr, bool) {
	terms := []*clientv1.Expr{}
	keys := make([]string, 0, len(op.Properties))
	for key := range op.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: op.Variable, Name: key, Value: protoValue(op.Properties[key])}}})
	}
	if op.Predicate != nil {
		pred, ok := gqlPredicateToStructured(op.Predicate)
		if !ok {
			return nil, false
		}
		terms = append(terms, pred)
	} else {
		for _, pred := range op.ComparisonPredicates {
			if pred.Variable != op.Variable || pred.Operator != planmodel.ComparisonEqual {
				return nil, false
			}
			terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: pred.Variable, Name: pred.Property, Value: protoValue(pred.Value)}}})
		}
		for _, pred := range op.NullPredicates {
			name := pred.Property
			if pred.Namespace != "" && pred.Namespace != "properties" {
				name = pred.Namespace + "." + pred.Property
			}
			terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: pred.Variable, Name: name, IsNull: pred.IsNull}}})
		}
		for _, pred := range op.StringPredicates {
			mode := clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS
			if pred.Operator == planmodel.StringStartsWith {
				mode = clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH
			} else if pred.Operator == planmodel.StringEndsWith {
				mode = clientv1.StringPredicateMode_STRING_PREDICATE_MODE_ENDS_WITH
			}
			terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: pred.Variable, Name: pred.Property}}}, Query: pred.Query, Mode: mode}}})
		}
		for _, pred := range op.TextPredicates {
			field := pred.Property
			if pred.Namespace != "" && pred.Namespace != "properties" {
				field = pred.Namespace + "." + pred.Property
			}
			terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: pred.Variable, Field: field, Query: pred.Query}}})
		}
		for _, pred := range op.SemanticPredicates {
			terms = append(terms, &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: pred.Variable, Query: pred.Query, Limit: int32(pred.TopK)}}})
		}
	}
	if len(terms) == 0 {
		return nil, false
	}
	if len(terms) == 1 {
		return terms[0], true
	}
	return &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: terms}}}, true
}

func gqlPredicateToStructured(pred *planmodel.PredicateExpr) (*clientv1.Expr, bool) {
	if pred == nil {
		return nil, false
	}
	switch pred.Op {
	case planmodel.PredicateAnd:
		children := []*clientv1.Expr{}
		for i := range pred.Terms {
			child, ok := gqlPredicateToStructured(&pred.Terms[i])
			if !ok {
				return nil, false
			}
			children = append(children, child)
		}
		return &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: children}}}, true
	case planmodel.PredicateOr:
		children := []*clientv1.Expr{}
		for i := range pred.Terms {
			child, ok := gqlPredicateToStructured(&pred.Terms[i])
			if !ok {
				return nil, false
			}
			children = append(children, child)
		}
		return &clientv1.Expr{Expr: &clientv1.Expr_Or{Or: &clientv1.OrExpr{Exprs: children}}}, true
	default:
		if pred.Leaf == nil {
			return nil, false
		}
		switch pred.Leaf.Kind {
		case planmodel.PredicateLeafComparison:
			cmp := pred.Leaf.Comparison
			if cmp == nil || cmp.Operator != planmodel.ComparisonEqual {
				return nil, false
			}
			return &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: cmp.Variable, Name: cmp.Property, Value: protoValue(cmp.Value)}}}, true
		case planmodel.PredicateLeafNull:
			p := pred.Leaf.Null
			if p == nil {
				return nil, false
			}
			name := p.Property
			if p.Namespace != "" && p.Namespace != "properties" {
				name = p.Namespace + "." + p.Property
			}
			return &clientv1.Expr{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: p.Variable, Name: name, IsNull: p.IsNull}}}, true
		case planmodel.PredicateLeafString:
			p := pred.Leaf.String
			if p == nil {
				return nil, false
			}
			mode := clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS
			if p.Operator == planmodel.StringStartsWith {
				mode = clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH
			} else if p.Operator == planmodel.StringEndsWith {
				mode = clientv1.StringPredicateMode_STRING_PREDICATE_MODE_ENDS_WITH
			}
			return &clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: p.Variable, Name: p.Property}}}, Query: p.Query, Mode: mode}}}, true
		case planmodel.PredicateLeafText:
			p := pred.Leaf.Text
			if p == nil {
				return nil, false
			}
			field := p.Property
			if p.Namespace != "" && p.Namespace != "properties" {
				field = p.Namespace + "." + p.Property
			}
			return &clientv1.Expr{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: p.Variable, Field: field, Query: p.Query}}}, true
		case planmodel.PredicateLeafSemantic:
			p := pred.Leaf.Semantic
			if p == nil {
				return nil, false
			}
			return &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: p.Variable, Query: p.Query, Limit: int32(p.TopK)}}}, true
		default:
			return nil, false
		}
	}
}

func gqlStructuredScalarAlias(ret planmodel.ReturnItem) string {
	if ret.Namespace != "" {
		return ret.Variable + "." + ret.Namespace + "." + ret.Property
	}
	return ret.Variable + "." + ret.Property
}

func gqlReturnOutputName(ret planmodel.ReturnItem) string {
	if ret.OutputName != "" {
		return ret.OutputName
	}
	if ret.Kind == planmodel.ReturnProperty {
		return gqlStructuredScalarAlias(ret)
	}
	return ret.Variable
}

func (s *QueryService) tryExecuteIndexedGQLPath(ctx context.Context, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, op planmodel.QueryPathOperation, pageSize int, pageToken string, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteGQLResponse, error) {
	if op.PathVariable != "" && len(op.OrderBy) == 0 {
		return s.tryExecuteIndexedGQLPathValue(ctx, tx, schemaCtx, op, pageSize, pageToken, recorder)
	}
	if op.PathVariable != "" {
		return false, nil, nil
	}
	if len(op.OrderBy) == 0 {
		return false, nil, nil
	}
	if len(op.OrderBy) != 1 || len(op.Start.Labels) != 1 || len(op.Start.Properties) != 0 || len(op.Segments) != 1 || len(op.TextPredicates) != 0 || len(op.SemanticPredicates) != 0 {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal requires one ordered single-label root and one bounded traversal")
	}
	if !op.ReturnGraph {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal requires RETURN GRAPH")
	}
	order := op.OrderBy[0]
	if order.Variable != op.Start.Variable || order.Property == "" {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal ORDER BY must target the root property")
	}
	segment := op.Segments[0]
	if len(segment.Relationship.Labels) != 1 || len(segment.Relationship.Properties) != 0 || strings.TrimSpace(segment.Node.Variable) == "" {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal requires one edge label and a target variable")
	}
	direction := clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT
	if segment.Relationship.Direction == planmodel.RelationshipIncoming {
		direction = clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN
	} else if segment.Relationship.Direction == planmodel.RelationshipUndirected {
		return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal requires directed traversal")
	}
	minDepth, maxDepth := int32(1), int32(1)
	if segment.Relationship.Quantifier != nil {
		minDepth = int32(segment.Relationship.Quantifier.Min)
		maxDepth = int32(segment.Relationship.Quantifier.Max)
	}
	sortDirection := clientv1.SortDirection_SORT_DIRECTION_ASC
	if order.Direction == planmodel.SortDescending {
		sortDirection = clientv1.SortDirection_SORT_DIRECTION_DESC
	}
	where, err := gqlIndexedBoundsExpr(op.ComparisonPredicates, op.Start.Variable, order.Property)
	if err != nil {
		return true, nil, err
	}
	request := &clientv1.ExecuteQueryRequest{TransactionId: tx.ID, PageSize: int32(pageSize), PageToken: pageToken, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...)}, Steps: []*clientv1.TraversalStep{{Direction: direction, EdgeKind: segment.Relationship.Labels[0], Depth: &clientv1.DepthSpec{MinDepth: minDepth, MaxDepth: maxDepth}, Target: &clientv1.NodePattern{Alias: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...)}}}}, Where: where, Returns: []*clientv1.ReturnProjection{{Alias: op.Start.Variable, OutputName: op.Start.Variable, Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}, {Alias: segment.Node.Variable, OutputName: "graph", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE}}, OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: op.Start.Variable, Name: order.Property}}}, Direction: sortDirection}}, Limit: int32(op.Limit)}}
	indexed, res, err := s.tryExecuteIndexedQuery(ctx, request, tx, schemaCtx, recorder)
	if !indexed || err != nil {
		return indexed, nil, err
	}
	return true, &clientv1.ExecuteGQLResponse{Result: res.GetResult(), ReadMetadata: res.GetReadMetadata(), Diagnostics: res.GetDiagnostics()}, nil
}

func (s *QueryService) tryExecuteIndexedGQLPathValue(ctx context.Context, tx daemonsession.GraphTransaction, schemaCtx analysis.SchemaContext, op planmodel.QueryPathOperation, pageSize int, pageToken string, recorder *daegraph.ReadMetadataRecorder) (bool, *clientv1.ExecuteGQLResponse, error) {
	if len(op.Start.Labels) != 1 || len(op.Segments) == 0 || len(op.OrderBy) != 0 {
		return false, nil, nil
	}
	if schemaCtx.Schema == nil && gqlPathHasStartFilter(op) {
		return false, nil, nil
	}
	steps := make([]*clientv1.TraversalStep, 0, len(op.Segments))
	for _, segment := range op.Segments {
		if len(segment.Relationship.Labels) != 1 || len(segment.Relationship.Properties) != 0 || strings.TrimSpace(segment.Node.Variable) == "" {
			return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed path requires one edge label and a target variable per segment")
		}
		direction := clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT
		if segment.Relationship.Direction == planmodel.RelationshipIncoming {
			direction = clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN
		} else if segment.Relationship.Direction == planmodel.RelationshipUndirected {
			return true, nil, status.Error(codes.FailedPrecondition, "GQL indexed path requires directed traversal")
		}
		minDepth, maxDepth := int32(1), int32(1)
		if segment.Relationship.Quantifier != nil {
			minDepth = int32(segment.Relationship.Quantifier.Min)
			maxDepth = int32(segment.Relationship.Quantifier.Max)
		}
		steps = append(steps, &clientv1.TraversalStep{Direction: direction, EdgeKind: segment.Relationship.Labels[0], EdgeAlias: segment.Relationship.Variable, Depth: &clientv1.DepthSpec{MinDepth: minDepth, MaxDepth: maxDepth}, Target: &clientv1.NodePattern{Alias: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...)}})
	}
	where, ok := gqlPathPredicatesToStructured(op)
	if !ok {
		return false, nil, nil
	}
	request := &clientv1.ExecuteQueryRequest{TransactionId: tx.ID, PageSize: int32(pageSize), PageToken: pageToken, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...)}, Steps: steps}, PathAlias: op.PathVariable, Where: where, Returns: gqlStructuredReturnsWithPath(op.Returns, op.PathVariable), Distinct: op.Distinct, Offset: int32(op.Offset), Limit: int32(op.Limit)}}
	indexed, res, err := s.tryExecuteIndexedQuery(ctx, request, tx, schemaCtx, recorder)
	if !indexed || err != nil {
		return indexed, nil, err
	}
	return true, &clientv1.ExecuteGQLResponse{Result: res.GetResult(), ReadMetadata: res.GetReadMetadata(), Diagnostics: res.GetDiagnostics()}, nil
}

func gqlStructuredReturnsWithPath(returns []planmodel.ReturnItem, pathAlias string) []*clientv1.ReturnProjection {
	out := gqlStructuredReturns(returns)
	for _, ret := range out {
		if ret.GetAlias() == pathAlias {
			ret.Kind = clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH
		}
	}
	return out
}

func gqlPathHasStartFilter(op planmodel.QueryPathOperation) bool {
	return len(op.Start.Properties) > 0 || op.Predicate != nil || len(op.ComparisonPredicates) > 0 || len(op.NullPredicates) > 0 || len(op.StringPredicates) > 0 || len(op.TextPredicates) > 0 || len(op.SemanticPredicates) > 0
}

func gqlPathPredicatesToStructured(op planmodel.QueryPathOperation) (*clientv1.Expr, bool) {
	if !gqlPathHasStartFilter(op) {
		return nil, true
	}
	nodeOp := planmodel.QueryNodesOperation{Variable: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...), Properties: copyMapAny(op.Start.Properties), Predicate: op.Predicate, ComparisonPredicates: append([]planmodel.ComparisonPredicate(nil), op.ComparisonPredicates...), NullPredicates: append([]planmodel.NullPredicate(nil), op.NullPredicates...), StringPredicates: append([]planmodel.StringPredicate(nil), op.StringPredicates...), TextPredicates: append([]planmodel.TextContainsPredicate(nil), op.TextPredicates...), SemanticPredicates: append([]planmodel.SemanticSimilarPredicate(nil), op.SemanticPredicates...)}
	return gqlNodePredicatesToStructured(nodeOp)
}

func gqlIndexedBoundsExpr(predicates []planmodel.ComparisonPredicate, alias string, property string) (*clientv1.Expr, error) {
	var low *clientv1.ValueExpr
	var high *clientv1.ValueExpr
	var strictHigh *clientv1.ValueExpr
	for _, predicate := range predicates {
		if predicate.Variable != alias || predicate.Property != property {
			return nil, status.Error(codes.FailedPrecondition, "GQL indexed graph traversal WHERE must bound the ordered root property")
		}
		value := &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: protoValue(predicate.Value)}}}
		switch predicate.Operator {
		case planmodel.ComparisonGreaterThanOrEqual:
			low = value
		case planmodel.ComparisonLessThanOrEqual:
			high = value
		case planmodel.ComparisonLessThan:
			strictHigh = value
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "unsupported GQL indexed bound operator %q", predicate.Operator)
		}
	}
	if strictHigh != nil {
		if low != nil || high != nil || len(predicates) != 1 {
			return nil, status.Error(codes.FailedPrecondition, "GQL indexed less-than bounds cannot be combined with other predicates yet")
		}
		return &clientv1.Expr{Expr: &clientv1.Expr_LessThan{LessThan: &clientv1.LessThanExpr{Left: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: alias, Name: property}}}, Right: strictHigh}}}, nil
	}
	if low == nil && high == nil {
		return nil, nil
	}
	return &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: alias, Name: property}}}, Low: low, High: high}}}, nil
}

type indexedBounds struct {
	hasLow        bool
	low           any
	lowExclusive  bool
	hasHigh       bool
	high          any
	highExclusive bool
}

func indexedQueryBounds(expr *clientv1.Expr, alias string, property string) (indexedBounds, error) {
	if expr == nil {
		return indexedBounds{}, nil
	}
	if between := expr.GetBetween(); between != nil {
		if between.GetValue().GetProp() == nil {
			return indexedBounds{}, status.Error(codes.FailedPrecondition, "indexed ORDER BY bounds must target the ordered property")
		}
		prop := between.GetValue().GetProp()
		if prop.GetAlias() != alias || prop.GetName() != property {
			return indexedBounds{}, status.Error(codes.FailedPrecondition, "indexed ORDER BY bounds must target the ordered property")
		}
		bounds := indexedBounds{}
		if between.GetLow() != nil {
			value, err := staticIndexedValue(between.GetLow())
			if err != nil {
				return indexedBounds{}, err
			}
			bounds.hasLow = true
			bounds.low = value
		}
		if between.GetHigh() != nil {
			value, err := staticIndexedValue(between.GetHigh())
			if err != nil {
				return indexedBounds{}, err
			}
			bounds.hasHigh = true
			bounds.high = value
		}
		return bounds, nil
	}
	if less := expr.GetLessThan(); less != nil {
		prop := less.GetLeft().GetProp()
		if prop == nil || prop.GetAlias() != alias || prop.GetName() != property {
			return indexedBounds{}, status.Error(codes.FailedPrecondition, "indexed ORDER BY less-than bounds must compare the ordered property")
		}
		value, err := staticIndexedValue(less.GetRight())
		if err != nil {
			return indexedBounds{}, err
		}
		return indexedBounds{hasHigh: true, high: value, highExclusive: true}, nil
	}
	return indexedBounds{}, status.Error(codes.FailedPrecondition, "indexed ORDER BY currently supports only BETWEEN or less-than bounds on the ordered property")
}

func staticIndexedValue(value *clientv1.ValueExpr) (any, error) {
	if value == nil {
		return nil, status.Error(codes.InvalidArgument, "indexed bound value is required")
	}
	switch v := value.GetExpr().(type) {
	case *clientv1.ValueExpr_Literal:
		return v.Literal.GetValue().AsInterface(), nil
	case *clientv1.ValueExpr_Date:
		parsed, err := time.Parse("2006-01-02", v.Date.GetValue())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid date bound: %v", err)
		}
		return parsed.AddDate(0, 0, int(v.Date.GetOffsetDays())).Format("2006-01-02"), nil
	case *clientv1.ValueExpr_CurrentDate:
		now := time.Now()
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, int(v.CurrentDate.GetOffsetDays())).Format("2006-01-02"), nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "indexed bounds require literal/date values")
	}
}
