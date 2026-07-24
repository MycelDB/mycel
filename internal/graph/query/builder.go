package query

import (
	"context"
	"fmt"
	"sort"

	"github.com/myceldb/mycel/internal/graph/model"
)

// Executor supplies data to the in-memory query engine.
type Executor interface {
	ListNodes(ctx context.Context) ([]graph.Node, error)
	ListEdges(ctx context.Context) ([]graph.Edge, error)
	ListTemplates(ctx context.Context) ([]graph.Template, error)
}

// Direction is a sort direction.
type Direction string

const (
	// Asc sorts in ascending order.
	Asc Direction = "asc"
	// Desc sorts in descending order.
	Desc Direction = "desc"
)

type orderSpec struct {
	value     ValueExpr
	direction Direction
}

// Builder builds and executes an in-memory graph query.
type Builder struct {
	executor Executor
	pattern  *GraphPattern
	where    Expr
	returns  []ReturnExpr
	orders   []orderSpec
}

// NewBuilder creates a query builder using executor as its data source.
func NewBuilder(executor Executor) *Builder { return &Builder{executor: executor} }

// Match sets the graph pattern to match.
func (b *Builder) Match(pattern *GraphPattern) *Builder {
	b.pattern = pattern
	return b
}

// Where sets the boolean expression used to filter rows.
func (b *Builder) Where(expr Expr) *Builder {
	b.where = expr
	return b
}

// Return sets the projected result values.
func (b *Builder) Return(exprs ...ReturnExpr) *Builder {
	b.returns = exprs
	return b
}

// OrderBy appends a result ordering.
func (b *Builder) OrderBy(value ValueExpr, direction Direction) *Builder {
	b.orders = append(b.orders, orderSpec{value: value, direction: direction})
	return b
}

// Execute evaluates the query in memory.
func (b *Builder) Execute(ctx context.Context) (*ResultSet, error) {
	if b.executor == nil {
		return nil, fmt.Errorf("query executor is required")
	}
	if b.pattern == nil || b.pattern.start == nil {
		return nil, fmt.Errorf("query pattern is required")
	}
	if b.pattern.pending != nil {
		return nil, fmt.Errorf("query pattern has an incomplete traversal")
	}
	nodes, err := b.executor.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	edges, err := b.executor.ListEdges(ctx)
	if err != nil {
		return nil, err
	}
	templates, err := b.executor.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	state := newExecutionState(nodes, edges, templates)
	rows := []executionRow{}
	for _, n := range nodes {
		if !state.nodeMatches(n, *b.pattern.start) {
			continue
		}
		row := executionRow{
			bindings:      map[string][]graph.Node{b.pattern.start.alias: []graph.Node{n}},
			parentByChild: map[graph.NodeID]graph.NodeID{},
			orderByChild:  map[graph.NodeID]any{},
		}
		if err := state.applySteps(&row, n, b.pattern.steps); err != nil {
			return nil, err
		}
		if b.where != nil {
			ok, err := b.where.eval(row.evalRow())
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		rows = append(rows, row)
	}
	if len(b.orders) > 0 {
		for _, row := range rows {
			for _, order := range b.orders {
				if _, err := order.value.evalValue(row.evalRow()); err != nil {
					return nil, err
				}
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			for _, order := range b.orders {
				cmp, err := compareRowValues(rows[i], rows[j], order.value)
				if err != nil || cmp == 0 {
					continue
				}
				if order.direction == Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	result := &ResultSet{Rows: make([]Row, 0, len(rows))}
	for _, row := range rows {
		out := Row{values: map[string]any{}}
		for _, ret := range b.returns {
			value, err := ret.project(row)
			if err != nil {
				return nil, err
			}
			out.values[ret.alias()] = value
		}
		result.Rows = append(result.Rows, out)
	}
	return result, nil
}

type containsChild struct {
	node  graph.Node
	edge  graph.Edge
	order any
}

type executionState struct {
	templateByID     map[graph.TemplateID]graph.Template
	nodeByID         map[graph.NodeID]graph.Node
	childrenByParent map[graph.NodeID][]containsChild
}

func newExecutionState(nodes []graph.Node, edges []graph.Edge, templates []graph.Template) executionState {
	state := executionState{
		templateByID:     map[graph.TemplateID]graph.Template{},
		nodeByID:         map[graph.NodeID]graph.Node{},
		childrenByParent: map[graph.NodeID][]containsChild{},
	}
	for _, tmpl := range templates {
		state.templateByID[tmpl.ID] = tmpl
	}
	for _, n := range nodes {
		state.nodeByID[n.ID] = n
	}
	for _, edge := range edges {
		if !graph.EdgeHasLabels(edge, []string{"contains"}) {
			continue
		}
		child, ok := state.nodeByID[edge.ToID]
		if !ok {
			continue
		}
		state.childrenByParent[edge.FromID] = append(state.childrenByParent[edge.FromID], containsChild{node: child, edge: edge})
	}
	for parentID := range state.childrenByParent {
		parent, ok := state.nodeByID[parentID]
		if !ok {
			continue
		}
		state.sortChildren(parent, state.childrenByParent[parentID])
	}
	return state
}

type executionRow struct {
	bindings      map[string][]graph.Node
	parentByChild map[graph.NodeID]graph.NodeID
	orderByChild  map[graph.NodeID]any
}

func (r executionRow) evalRow() evalRow {
	bindings := map[string][]any{}
	for alias, nodes := range r.bindings {
		values := make([]any, 0, len(nodes))
		for _, n := range nodes {
			values = append(values, n)
		}
		bindings[alias] = values
	}
	return evalRow{bindings: bindings}
}

func (s executionState) applySteps(row *executionRow, start graph.Node, steps []traversalStep) error {
	current := []graph.Node{start}
	for _, step := range steps {
		if step.direction != "out" || step.kind != "contains" {
			return fmt.Errorf("unsupported traversal %s %q", step.direction, step.kind)
		}
		next := []graph.Node{}
		for _, n := range current {
			matches := s.traverseContains(row, n, step.depth, step.target)
			next = append(next, matches...)
		}
		row.bindings[step.target.alias] = dedupeNodes(next)
		current = row.bindings[step.target.alias]
	}
	return nil
}

func (s executionState) traverseContains(row *executionRow, start graph.Node, depth DepthSpec, target nodePattern) []graph.Node {
	minDepth := depth.Min
	maxDepth := depth.Max
	if minDepth < 0 {
		minDepth = 0
	}
	out := []graph.Node{}
	var visit func(parent graph.Node, currentDepth int)
	visit = func(parent graph.Node, currentDepth int) {
		if maxDepth != Unbounded && currentDepth > maxDepth {
			return
		}
		for _, child := range s.childrenByParent[parent.ID] {
			childDepth := currentDepth + 1
			if maxDepth != Unbounded && childDepth > maxDepth {
				continue
			}
			row.parentByChild[child.node.ID] = parent.ID
			row.orderByChild[child.node.ID] = s.edgeOrderForParent(parent, child.edge)
			if childDepth >= minDepth && s.nodeMatches(child.node, target) {
				out = append(out, child.node)
			}
			visit(child.node, childDepth)
		}
	}
	visit(start, 0)
	return out
}

func (s executionState) nodeMatches(node graph.Node, pattern nodePattern) bool {
	if pattern.templateKey == "" {
		return true
	}
	if node.TemplateID == nil {
		return false
	}
	tmpl, ok := s.templateByID[*node.TemplateID]
	return ok && tmpl.Key == pattern.templateKey
}

func (s executionState) sortChildren(parent graph.Node, children []containsChild) {
	if parent.TemplateID == nil {
		return
	}
	tmpl, ok := s.templateByID[*parent.TemplateID]
	if !ok || tmpl.Children.Order == nil || tmpl.Children.Order.Mode != graph.ChildOrderModeEdgeProperty {
		return
	}
	desc := tmpl.Children.Order.Direction == graph.SortDirectionDesc
	sort.SliceStable(children, func(i, j int) bool {
		cmp, err := compareValues(children[i].edge.Properties[tmpl.Children.Order.Property], children[j].edge.Properties[tmpl.Children.Order.Property])
		if err != nil || cmp == 0 {
			return false
		}
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func (s executionState) edgeOrderForParent(parent graph.Node, edge graph.Edge) any {
	if parent.TemplateID == nil {
		return nil
	}
	tmpl, ok := s.templateByID[*parent.TemplateID]
	if !ok || tmpl.Children.Order == nil || tmpl.Children.Order.Mode != graph.ChildOrderModeEdgeProperty {
		return nil
	}
	return edge.Properties[tmpl.Children.Order.Property]
}

func dedupeNodes(nodes []graph.Node) []graph.Node {
	seen := map[graph.NodeID]struct{}{}
	out := make([]graph.Node, 0, len(nodes))
	for _, n := range nodes {
		if _, ok := seen[n.ID]; ok {
			continue
		}
		seen[n.ID] = struct{}{}
		out = append(out, n)
	}
	return out
}

func compareRowValues(left, right executionRow, expr ValueExpr) (int, error) {
	lv, err := expr.evalValue(left.evalRow())
	if err != nil {
		return 0, err
	}
	rv, err := expr.evalValue(right.evalRow())
	if err != nil {
		return 0, err
	}
	return compareValues(lv, rv)
}
