package client

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const queryMaxPageSize = 500

type QueryService struct {
	clientv1.UnimplementedQueryServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
	spaces   daemonspace.Manager
}

func NewQueryService(sessions daemonsession.Manager, graphs daegraph.Manager, spaces daemonspace.Manager) *QueryService {
	return &QueryService{sessions: sessions, graphs: graphs, spaces: spaces}
}

func (s *QueryService) ExecuteQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest) (*clientv1.ExecuteQueryResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute query")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	if req.GetQuery() == nil || req.GetQuery().GetMatch() == nil || req.GetQuery().GetMatch().GetStart() == nil {
		return nil, status.Error(codes.InvalidArgument, "query.match.start is required")
	}
	domain, err := s.spaces.GetVisibleDomain(ctx, principal.UserID, tx.SpaceID, tx.DomainID, "")
	if err != nil {
		return nil, mapDomainError(err, "query domain")
	}
	if !domaingraph.DomainBroadSearchable(domain) {
		return nil, status.Error(codes.FailedPrecondition, "domain is excluded from broad query execution")
	}
	nodes, err := s.allNodes(ctx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list nodes")
	}
	edges, err := s.allEdges(ctx, tx)
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
	return &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next, Result: result}, nil
}

func (s *QueryService) ExecuteGQL(ctx context.Context, req *clientv1.ExecuteGQLRequest) (*clientv1.ExecuteGQLResponse, error) {
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	if len(req.GetParams()) > 0 {
		return nil, status.Error(codes.Unimplemented, "GQL parameters are reserved but not implemented yet")
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute gql")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	plan, err := gql.Compile(req.GetQuery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if plan.AccessMode == analysis.ReadWrite && tx.Mode != daemonsession.TransactionModeReadWrite {
		return nil, status.Error(codes.FailedPrecondition, "GQL query requires a read-write transaction")
	}
	execResult, err := execution.Execute(ctx, gqlDaemonGraph{service: s, tx: tx}, plan)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rows := gqlRowsToProto(execResult)
	pageRows, next, err := paginateProtoQueryRows(rows, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &clientv1.ExecuteGQLResponse{Result: queryResultFromRowsWithCounters(pageRows, next, execResult.Counters)}, nil
}

func (s *QueryService) ExecuteGQLScript(ctx context.Context, req *clientv1.ExecuteGQLScriptRequest) (*clientv1.ExecuteGQLScriptResponse, error) {
	if strings.TrimSpace(req.GetScript()) == "" {
		return nil, status.Error(codes.InvalidArgument, "script is required")
	}
	if len(req.GetParams()) > 0 {
		return nil, status.Error(codes.Unimplemented, "GQL parameters are reserved but not implemented yet")
	}
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, req.GetTransactionId())
	if err != nil {
		return nil, mapSessionError(err, "execute gql script")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return nil, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	scriptPlan, err := gql.CompileScript(req.GetScript())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if scriptPlan.AccessMode == analysis.ReadWrite && tx.Mode != daemonsession.TransactionModeReadWrite {
		return nil, status.Error(codes.FailedPrecondition, "GQL script requires a read-write transaction")
	}
	statementResults := []*clientv1.GQLStatementResult{}
	aggregate := &clientv1.QueryResult{Graph: &clientv1.ResultGraph{}, Counters: &clientv1.QueryCounters{}}
	for _, statement := range scriptPlan.Statements {
		execResult, err := execution.Execute(ctx, gqlDaemonGraph{service: s, tx: tx}, statement.Plan)
		if err != nil {
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: err.Error()})
			if req.GetStopOnError() {
				break
			}
			continue
		}
		rows := gqlRowsToProto(execResult)
		pageRows, next, err := paginateProtoQueryRows(rows, int(req.GetPageSize()), "")
		if err != nil {
			statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: false, Error: err.Error()})
			if req.GetStopOnError() {
				break
			}
			continue
		}
		result := queryResultFromRowsWithCounters(pageRows, next, execResult.Counters)
		statementResults = append(statementResults, &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: true, Result: result})
		mergeQueryResult(aggregate, result)
	}
	return &clientv1.ExecuteGQLScriptResponse{Statements: statementResults, Result: aggregate}, nil
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
	rows := []*queryRowState{}
	for _, node := range e.nodes {
		if !e.nodeMatches(node, start) {
			continue
		}
		row := &queryRowState{bindings: map[string][]domaingraph.Node{start.GetAlias(): []domaingraph.Node{node}}, parentByChild: map[string]string{}, orderByChild: map[string]any{}}
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
	if len(pattern.GetLabels()) > 0 && !nodeHasLabels(node.Labels, pattern.GetLabels()) {
		return false
	}
	return true
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
			node, err := firstBoundNode(row, ret.GetAlias())
			if err != nil {
				return nil, err
			}
			fields[name] = &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoValue(node.ID.String())}}
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

func firstBoundNode(row *queryRowState, alias string) (domaingraph.Node, error) {
	nodes := row.bindings[alias]
	if len(nodes) == 0 {
		return domaingraph.Node{}, fmt.Errorf("alias %q is not bound", alias)
	}
	return nodes[0], nil
}

func propValue(node domaingraph.Node, name string) any {
	if name == "node_id" {
		return node.ID.String()
	}
	if name == "content" {
		return node.Content
	}
	return node.Props[name]
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

func queryResultFromRows(rows []*clientv1.QueryRow, next string) *clientv1.QueryResult {
	return queryResultFromRowsWithCounters(rows, next, execmodel.Counters{})
}

func queryResultFromRowsWithCounters(rows []*clientv1.QueryRow, next string, counters execmodel.Counters) *clientv1.QueryResult {
	return &clientv1.QueryResult{Rows: rows, NextPageToken: next, Graph: graphFromRows(rows), Counters: &clientv1.QueryCounters{RowsReturned: int32(len(rows)), NodesInserted: int32(counters.NodesInserted), EdgesInserted: int32(counters.EdgesInserted)}}
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
	seen := map[string]bool{}
	for _, node := range aggregate.Graph.GetNodes() {
		seen[node.GetNodeId()] = true
	}
	for _, node := range result.GetGraph().GetNodes() {
		if seen[node.GetNodeId()] {
			continue
		}
		seen[node.GetNodeId()] = true
		aggregate.Graph.Nodes = append(aggregate.Graph.Nodes, node)
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
