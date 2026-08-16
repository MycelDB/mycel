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
	indexedSubtreeMaxDepthCap = 64
)

type QueryService struct {
	clientv1.UnimplementedQueryServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
	spaces   daemonspace.Manager
	schemas  schemaservice.Manager
	router   ClientRequestRouter
}

func NewQueryService(sessions daemonsession.Manager, graphs daegraph.Manager, spaces daemonspace.Manager) *QueryService {
	return &QueryService{sessions: sessions, graphs: graphs, spaces: spaces}
}

func (s *QueryService) WithSchemaManager(manager schemaservice.Manager) *QueryService {
	s.schemas = manager
	return s
}

func (s *QueryService) WithClientRequestRouter(router ClientRequestRouter) *QueryService {
	s.router = router
	return s
}

func (s *QueryService) ExecuteQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest) (*clientv1.ExecuteQueryResponse, error) {
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
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, tx.SpaceID, tx.DomainID, "")
	if err != nil {
		return nil, mapDomainError(err, "query domain")
	}
	if !domaingraph.DomainBroadSearchable(domain) && !isIndexedAdjacencyQuery(req.GetQuery()) && !isIndexedRootSubtreeQuery(req.GetQuery()) && !isIndexedEqualityNodeQuery(req.GetQuery()) {
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
	nodes, err := s.allNodes(readCtx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list nodes")
	}
	edges, err := s.allEdges(readCtx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list edges")
	}
	exec := newQueryExecution(nodes, edges)
	rows, err := exec.match(req.GetQuery().GetMatch(), req.GetQuery().GetWhere())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(req.GetQuery().GetOrderBy()) > 0 {
		if err := exec.sortRows(rows, req.GetQuery().GetOrderBy()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if limit := int(req.GetQuery().GetLimit()); limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	pageRows, next, err := paginateQueryRows(rows, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out := make([]*clientv1.QueryRow, 0, len(pageRows))
	returns := req.GetQuery().GetReturns()
	if len(returns) == 0 {
		startAlias := req.GetQuery().GetMatch().GetStart().GetAlias()
		returns = []*clientv1.ReturnProjection{{Alias: startAlias, OutputName: startAlias, Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}
	}
	for _, row := range pageRows {
		protoRow, err := exec.projectRow(row, returns)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		out = append(out, protoRow)
	}
	result := queryResultFromRows(out, next)
	return &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
}

func (s *QueryService) ExecuteGQL(ctx context.Context, req *clientv1.ExecuteGQLRequest) (*clientv1.ExecuteGQLResponse, error) {
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
		return res, err
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
	return &clientv1.ExecuteGQLResponse{Result: result, ReadMetadata: protoReadMetadata(recorder.Summary())}, nil
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

func (e *queryExecution) match(pattern *clientv1.GraphPattern, where *clientv1.Expr) ([]*queryRowState, error) {
	start := pattern.GetStart()
	if strings.TrimSpace(start.GetAlias()) == "" {
		return nil, fmt.Errorf("start alias is required")
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
	return &queryRowState{bindings: map[string][]domaingraph.Node{alias: []domaingraph.Node{node}}, edgeBindings: map[string][]domaingraph.Edge{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
}

func cloneQueryRowState(row *queryRowState) *queryRowState {
	out := &queryRowState{bindings: map[string][]domaingraph.Node{}, edgeBindings: map[string][]domaingraph.Edge{}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
	for alias, nodes := range row.bindings {
		out.bindings[alias] = append([]domaingraph.Node(nil), nodes...)
	}
	for alias, edges := range row.edgeBindings {
		out.edgeBindings[alias] = append([]domaingraph.Edge(nil), edges...)
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

func (e *queryExecution) evalValue(row *queryRowState, value *clientv1.ValueExpr) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("value expression is required")
	}
	switch v := value.GetExpr().(type) {
	case *clientv1.ValueExpr_Prop:
		if edge, err := firstBoundEdge(row, v.Prop.GetAlias()); err == nil {
			return edgePropValue(edge, v.Prop.GetName()), nil
		}
		node, err := firstBoundNode(row, v.Prop.GetAlias())
		if err != nil {
			return nil, err
		}
		return propValue(node, v.Prop.GetName()), nil
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
				fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(gqlPathToScalar(*value.Path))}}
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

func gqlPathToScalar(path execmodel.Path) map[string]any {
	nodes := make([]any, 0, len(path.Nodes))
	for _, node := range path.Nodes {
		nodes = append(nodes, map[string]any{"nodeId": node.ID, "domainId": node.DomainID, "labels": stringListValue(node.Labels), "properties": node.Properties, "payload": node.Payload, "meta": node.Meta})
	}
	edges := make([]any, 0, len(path.Edges))
	for _, edge := range path.Edges {
		edges = append(edges, map[string]any{"edgeId": edge.ID, "domainId": edge.DomainID, "fromNodeId": edge.FromID, "toNodeId": edge.ToID, "labels": stringListValue(edge.Labels), "properties": edge.Properties, "payload": edge.Payload, "meta": edge.Meta})
	}
	return map[string]any{"nodes": nodes, "edges": edges}
}

func stringListValue(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
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
	if indexed, res, err := s.tryExecuteIndexedAdjacencyQuery(ctx, req, tx, recorder); indexed || err != nil {
		return indexed, res, err
	}
	if len(query.GetOrderBy()) == 0 {
		if indexed, res, err := s.tryExecuteIndexedEqualityNodeQuery(ctx, req, tx, schemaCtx, recorder); indexed || err != nil {
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
	limit := effectiveIndexedLimit(req.GetPageSize(), query.GetLimit())
	direction := order.GetDirection()
	indexDirection := schemamodel.IndexSortDirectionAsc
	if direction == clientv1.SortDirection_SORT_DIRECTION_DESC {
		indexDirection = schemamodel.IndexSortDirectionDesc
	}
	nodes, next, stats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: indexDirection, Limit: limit, Cursor: req.GetPageToken(), HasLow: bounds.hasLow, Low: bounds.low, LowExclusive: bounds.lowExclusive, HasHigh: bounds.hasHigh, High: bounds.high, HighExclusive: bounds.highExclusive})
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed query")
	}
	returns := query.GetReturns()
	if len(returns) == 0 {
		returns = []*clientv1.ReturnProjection{{Alias: start.GetAlias(), OutputName: start.GetAlias(), Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}
	}
	exec := newQueryExecution(nil, nil)
	out := make([]*clientv1.QueryRow, 0, len(nodes))
	for _, node := range nodes {
		row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): []domaingraph.Node{node}}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
		protoRow, err := exec.projectRow(row, returns)
		if err != nil {
			return true, nil, status.Error(codes.InvalidArgument, err.Error())
		}
		out = append(out, protoRow)
	}
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: stats.Plan, Indexes: []string{idx.Name}, FullScan: stats.FullScan, IndexEntriesScanned: int32(stats.IndexEntriesScanned), NodesLoaded: int32(stats.NodesLoaded), EdgesLoaded: int32(stats.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: stats.NextCursorKind}
	return true, &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result, ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: diagnostics}, nil
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
	limit := effectiveIndexedLimit(req.GetPageSize(), query.GetLimit())
	nodes, next, stats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: schemamodel.IndexSortDirectionAsc, Limit: limit, Cursor: req.GetPageToken(), HasLow: true, Low: value, HasHigh: true, High: value})
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed equality query")
	}
	returns := query.GetReturns()
	if len(returns) == 0 {
		returns = []*clientv1.ReturnProjection{{Alias: start.GetAlias(), OutputName: start.GetAlias(), Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}
	}
	exec := newQueryExecution(nil, nil)
	out := make([]*clientv1.QueryRow, 0, len(nodes))
	for _, node := range nodes {
		row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): {node}}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
		protoRow, err := exec.projectRow(row, returns)
		if err != nil {
			return true, nil, status.Error(codes.InvalidArgument, err.Error())
		}
		out = append(out, protoRow)
	}
	result := queryResultFromRows(out, next)
	diagnostics := &clientv1.QueryDiagnostics{Plan: "OrderedNodePropertyEqualityIndexScan", Indexes: []string{idx.Name}, FullScan: stats.FullScan, IndexEntriesScanned: int32(stats.IndexEntriesScanned), NodesLoaded: int32(stats.NodesLoaded), EdgesLoaded: int32(stats.EdgesLoaded), RowsReturned: int32(len(out)), NextCursorKind: stats.NextCursorKind}
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
	limit := effectiveIndexedLimit(req.GetPageSize(), query.GetLimit())
	indexDirection := schemamodel.IndexSortDirectionAsc
	if query.GetOrderBy()[0].GetDirection() == clientv1.SortDirection_SORT_DIRECTION_DESC {
		indexDirection = schemamodel.IndexSortDirectionDesc
	}
	rootScanStart := time.Now()
	roots, next, rootStats, err := s.graphs.ScanNodePropertyOrdered(ctx, tx, daegraph.OrderedNodePropertyScan{IndexName: idx.Name, Direction: indexDirection, Limit: limit, Cursor: req.GetPageToken(), HasLow: bounds.hasLow, Low: bounds.low, LowExclusive: bounds.lowExclusive, HasHigh: bounds.hasHigh, High: bounds.high, HighExclusive: bounds.highExclusive})
	rootScanMillis := time.Since(rootScanStart).Milliseconds()
	if err != nil {
		return true, nil, mapGraphError(err, "execute indexed root scan")
	}
	expansionStart := time.Now()
	expanded, err := s.expandIndexedSubtrees(ctx, tx, query, roots, idx.Name, rootStats)
	expansionMillis := time.Since(expansionStart).Milliseconds()
	if err != nil {
		return true, nil, err
	}
	if expanded.truncated {
		next = ""
	}
	result := queryResultFromRows(expanded.rows, next)
	result.Graph = expanded.graph
	diagnostics := &clientv1.QueryDiagnostics{Plan: indexedSubtreePlanName, Indexes: []string{idx.Name, expanded.stats.IndexName}, FullScan: false, IndexEntriesScanned: int32(expanded.stats.IndexEntriesScanned), NodesLoaded: int32(expanded.stats.NodesLoaded), EdgesLoaded: int32(expanded.stats.EdgesLoaded), RowsReturned: int32(len(expanded.rows)), NextCursorKind: indexedSubtreeCursorKind, RootCount: int32(len(expanded.rows)), Truncated: expanded.truncated, TruncationReason: expanded.reason, RootScanMillis: rootScanMillis, ExpansionMillis: expansionMillis, AdjacencyScanCalls: int32(expanded.stats.AdjacencyScanCalls), NodeReadCalls: int32(expanded.stats.NodeReadCalls)}
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
		return 0, 0, status.Error(codes.InvalidArgument, "indexed subtree traversal min_depth must be non-negative")
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
	if len(op.OrderBy) == 0 {
		return false, nil, nil
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
	returns := make([]*clientv1.ReturnProjection, 0, len(op.Returns))
	for _, ret := range op.Returns {
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
		returns = append(returns, &clientv1.ReturnProjection{Alias: alias, OutputName: output, Kind: kind})
	}
	queryLimit := int32(op.Limit)
	request := &clientv1.ExecuteQueryRequest{TransactionId: tx.ID, PageSize: int32(pageSize), PageToken: pageToken, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: op.Variable, Labels: append([]string(nil), op.Labels...)}}, Returns: returns, OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: op.Variable, Name: order.Property}}}, Direction: direction}}, Limit: queryLimit}}
	indexed, res, err := s.tryExecuteIndexedQuery(ctx, request, tx, schemaCtx, recorder)
	if !indexed || err != nil {
		return indexed, nil, err
	}
	return true, &clientv1.ExecuteGQLResponse{Result: res.GetResult(), ReadMetadata: res.GetReadMetadata(), Diagnostics: res.GetDiagnostics()}, nil
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
