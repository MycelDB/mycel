package client

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/domain/graph"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
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
	nodes, err := s.allNodes(ctx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list nodes")
	}
	edges, err := s.allEdges(ctx, tx)
	if err != nil {
		return nil, mapGraphError(err, "query list edges")
	}
	templates, err := s.spaces.ListVisibleTemplates(ctx, principal.UserID, tx.SpaceID, true, true)
	if err != nil {
		return nil, mapSessionError(err, "query list templates")
	}
	exec := newQueryExecution(nodes, edges, templates)
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
	return &clientv1.ExecuteQueryResponse{Rows: out, NextPageToken: next}, nil
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
	templateByID map[string]domaingraph.Template
	nodeByID     map[string]domaingraph.Node
	outEdgesByID map[string][]domaingraph.Edge
	inEdgesByID  map[string][]domaingraph.Edge
}

type queryRowState struct {
	bindings      map[string][]domaingraph.Node
	parentByChild map[string]string
	orderByChild  map[string]any
}

func newQueryExecution(nodes []domaingraph.Node, edges []domaingraph.Edge, templates []domaingraph.Template) *queryExecution {
	exec := &queryExecution{nodes: nodes, edges: edges, templateByID: map[string]domaingraph.Template{}, nodeByID: map[string]domaingraph.Node{}, outEdgesByID: map[string][]domaingraph.Edge{}, inEdgesByID: map[string][]domaingraph.Edge{}}
	for _, tmpl := range templates {
		exec.templateByID[tmpl.ID.String()] = tmpl
	}
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
	if pattern == nil || pattern.GetTemplateKey() == "" {
		return true
	}
	if node.TemplateID == nil {
		return false
	}
	tmpl, ok := e.templateByID[node.TemplateID.String()]
	return ok && tmpl.Key == pattern.GetTemplateKey()
}

func (e *queryExecution) applySteps(row *queryRowState, current []domaingraph.Node, steps []*clientv1.TraversalStep) error {
	for _, step := range steps {
		if step.GetTarget() == nil || strings.TrimSpace(step.GetTarget().GetAlias()) == "" {
			return fmt.Errorf("traversal target alias is required")
		}
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_UNSPECIFIED {
			return fmt.Errorf("traversal direction is required")
		}
		kind := strings.TrimSpace(step.GetEdgeKind())
		if kind == "" {
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
			if edge.Kind == domaingraph.EdgeKindContains && step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT {
				row.parentByChild[candidate.ID.String()] = node.ID.String()
				row.orderByChild[candidate.ID.String()] = edge.Props["order"]
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
		if string(edge.Kind) == step.GetEdgeKind() {
			out = append(out, edge)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return numericOrder(out[i].Props["order"], i) < numericOrder(out[j].Props["order"], j)
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
	tags, err := domaingraph.NormalizeTagsValue(node.Props[domaingraph.NodePropTags])
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
	props, err := domaingraph.NormalizeCustomPropertiesValue(node.Props[domaingraph.NodePropCustomProperties])
	if err != nil {
		return nil, false, nil
	}
	value, ok := props[want]
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
