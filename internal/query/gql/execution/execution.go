// Package execution executes planned GQL operations.
package execution

import (
	"context"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

// InsertNode describes a node creation requested by a GQL plan.
type InsertNode struct {
	Labels     []string
	Properties map[string]any
}

type CreateEdge struct {
	FromNodeID string
	ToNodeID   string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
}

type UpdateNode struct {
	NodeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
}

type UpdateEdge struct {
	EdgeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
}

type QueryNodes struct {
	Variable   string
	Labels     []string
	Properties map[string]any
	Limit      int64
}

type QueryPattern struct {
	Start        QueryNodePattern
	Relationship QueryRelationshipPattern
	End          QueryNodePattern
	Limit        int64
}

type QueryNodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]any
}

type RelationshipDirection string

const (
	RelationshipOutgoing   RelationshipDirection = "outgoing"
	RelationshipIncoming   RelationshipDirection = "incoming"
	RelationshipUndirected RelationshipDirection = "undirected"
)

type QueryRelationshipPattern struct {
	Labels     []string
	Properties map[string]any
	Direction  RelationshipDirection
	Quantifier *RelationshipQuantifier
}

type RelationshipQuantifier struct {
	Min int
	Max int
}

type PatternRow struct {
	Start execmodel.Node
	Edge  execmodel.Edge
	End   execmodel.Node
}

type segmentExpansion struct {
	End   execmodel.Node
	Nodes []execmodel.Node
	Edges []execmodel.Edge
}

type pathBinding struct {
	Nodes        map[string]execmodel.Node
	Edges        map[string]execmodel.Edge
	OrderedNodes []execmodel.Node
	OrderedEdges []execmodel.Edge
}

// Graph is the graph capability required by the current executor.
type Graph interface {
	InsertNode(ctx context.Context, node InsertNode) (execmodel.NodeRef, error)
	CreateEdge(ctx context.Context, edge CreateEdge) (execmodel.Edge, error)
	UpdateNode(ctx context.Context, node UpdateNode) (execmodel.Node, error)
	UpdateEdge(ctx context.Context, edge UpdateEdge) (execmodel.Edge, error)
	DeleteNode(ctx context.Context, nodeID string) error
	DeleteEdge(ctx context.Context, edgeID string) error
	QueryNodes(ctx context.Context, query QueryNodes) ([]execmodel.Node, error)
	QueryPattern(ctx context.Context, query QueryPattern) ([]PatternRow, error)
}

// GraphWriter is kept as a compatibility alias for the current graph capability.
type GraphWriter = Graph

// Executor executes a planned GQL query.
type Executor interface {
	Execute(ctx context.Context, plan planmodel.Plan) (execmodel.Result, error)
}

type executor struct {
	graph Graph
}

func NewExecutor(graph Graph) Executor {
	return executor{graph: graph}
}

func Execute(ctx context.Context, graph Graph, plan planmodel.Plan) (execmodel.Result, error) {
	return NewExecutor(graph).Execute(ctx, plan)
}

func (e executor) Execute(ctx context.Context, plan planmodel.Plan) (execmodel.Result, error) {
	if e.graph == nil {
		return execmodel.Result{}, fmt.Errorf("graph is required")
	}
	var result execmodel.Result
	for _, op := range plan.Operations {
		switch op := op.(type) {
		case planmodel.InsertNodeOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("insert node requires read-write access mode")
			}
			if _, err := e.graph.InsertNode(ctx, InsertNode{Labels: append([]string(nil), op.Labels...), Properties: copyProperties(op.Properties)}); err != nil {
				return execmodel.Result{}, err
			}
			result.Counters.NodesInserted++
		case planmodel.MergeNodeOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("merge node requires read-write access mode")
			}
			nodes, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: op.Variable, Labels: append([]string(nil), op.Labels...), Properties: copyProperties(op.Properties)})
			if err != nil {
				return execmodel.Result{}, err
			}
			if len(nodes) == 0 {
				ref, err := e.graph.InsertNode(ctx, InsertNode{Labels: append([]string(nil), op.Labels...), Properties: copyProperties(op.Properties)})
				if err != nil {
					return execmodel.Result{}, err
				}
				nodes = []execmodel.Node{{ID: ref.ID, Labels: append([]string(nil), op.Labels...), Properties: copyProperties(op.Properties)}}
				result.Counters.NodesInserted++
			}
			if op.Limit > 0 && int64(len(nodes)) > op.Limit {
				nodes = nodes[:op.Limit]
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, node := range nodes {
				row, err := projectBindingRow(pathBinding{Nodes: map[string]execmodel.Node{op.Variable: node}, Edges: map[string]execmodel.Edge{}}, op.Returns)
				if err != nil {
					return execmodel.Result{}, err
				}
				result.Rows = append(result.Rows, row)
			}
		case planmodel.MatchCreateRelationshipOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("create relationship requires read-write access mode")
			}
			bindings := []map[string]execmodel.Node{{}}
			for _, match := range op.Matches {
				nodes, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: match.Variable, Labels: append([]string(nil), match.Labels...), Properties: copyProperties(match.Properties)})
				if err != nil {
					return execmodel.Result{}, err
				}
				next := []map[string]execmodel.Node{}
				for _, binding := range bindings {
					for _, node := range nodes {
						copyBinding := map[string]execmodel.Node{}
						for key, value := range binding {
							copyBinding[key] = value
						}
						copyBinding[match.Variable] = node
						next = append(next, copyBinding)
					}
				}
				bindings = next
			}
			for _, binding := range bindings {
				from, fromOK := binding[op.Relationship.FromVariable]
				to, toOK := binding[op.Relationship.ToVariable]
				if !fromOK || !toOK {
					return execmodel.Result{}, fmt.Errorf("relationship endpoint is not bound")
				}
				if _, err := e.graph.CreateEdge(ctx, CreateEdge{FromNodeID: from.ID, ToNodeID: to.ID, Labels: append([]string(nil), op.Relationship.Labels...), Properties: copyProperties(op.Relationship.Properties)}); err != nil {
					return execmodel.Result{}, err
				}
				result.Counters.EdgesInserted++
			}
		case planmodel.MatchMergeRelationshipOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("merge relationship requires read-write access mode")
			}
			nodeBindings, err := e.matchNodePatternBindings(ctx, op.Matches)
			if err != nil {
				return execmodel.Result{}, err
			}
			if len(nodeBindings) > 1 {
				return execmodel.Result{}, fmt.Errorf("merge relationship matched %d endpoint bindings; refine endpoint predicates", len(nodeBindings))
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, binding := range nodeBindings {
				from, fromOK := binding[op.Relationship.FromVariable]
				to, toOK := binding[op.Relationship.ToVariable]
				if !fromOK || !toOK {
					return execmodel.Result{}, fmt.Errorf("relationship endpoint is not bound")
				}
				rows, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": from.ID}}, Relationship: QueryRelationshipPattern{Labels: append([]string(nil), op.Relationship.Labels...), Properties: copyProperties(op.Relationship.Properties), Direction: RelationshipOutgoing}, End: QueryNodePattern{Properties: map[string]any{"__id": to.ID}}})
				if err != nil {
					return execmodel.Result{}, err
				}
				var edge execmodel.Edge
				if len(rows) > 0 {
					edge = rows[0].Edge
				} else {
					created, err := e.graph.CreateEdge(ctx, CreateEdge{FromNodeID: from.ID, ToNodeID: to.ID, Labels: append([]string(nil), op.Relationship.Labels...), Properties: copyProperties(op.Relationship.Properties)})
					if err != nil {
						return execmodel.Result{}, err
					}
					edge = created
					result.Counters.EdgesInserted++
				}
				path := pathBinding{Nodes: map[string]execmodel.Node{}, Edges: map[string]execmodel.Edge{}}
				for key, value := range binding {
					path.Nodes[key] = value
				}
				if op.Relationship.Variable != "" {
					path.Edges[op.Relationship.Variable] = edge
				}
				row, err := projectBindingRow(path, op.Returns)
				if err != nil {
					return execmodel.Result{}, err
				}
				result.Rows = append(result.Rows, row)
				if op.Limit > 0 && int64(len(result.Rows)) >= op.Limit {
					break
				}
			}
		case planmodel.MatchSetOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("set requires read-write access mode")
			}
			bindings, err := e.matchBindings(ctx, op.Start, op.Segments)
			if err != nil {
				return execmodel.Result{}, err
			}
			matched := []pathBinding{}
			for _, binding := range bindings {
				if bindingMatchesPredicates(binding, op.ComparisonPredicates, op.TextPredicates, op.SemanticPredicates) {
					matched = append(matched, binding)
				}
			}
			if op.Limit > 0 && int64(len(matched)) > op.Limit {
				matched = matched[:op.Limit]
			}
			updatedNodes := map[string]struct{}{}
			updatedEdges := map[string]struct{}{}
			for i := range matched {
				for _, assignment := range op.Assignments {
					if node, ok := matched[i].Nodes[assignment.Variable]; ok {
						updated, err := e.applyNodeAssignment(ctx, node, assignment)
						if err != nil {
							return execmodel.Result{}, err
						}
						matched[i].Nodes[assignment.Variable] = updated
						updatedNodes[updated.ID] = struct{}{}
						continue
					}
					if edge, ok := matched[i].Edges[assignment.Variable]; ok {
						updated, err := e.applyEdgeAssignment(ctx, edge, assignment)
						if err != nil {
							return execmodel.Result{}, err
						}
						matched[i].Edges[assignment.Variable] = updated
						updatedEdges[updated.ID] = struct{}{}
						continue
					}
					return execmodel.Result{}, fmt.Errorf("set variable %q is not bound", assignment.Variable)
				}
			}
			result.Counters.NodesUpdated += len(updatedNodes)
			result.Counters.EdgesUpdated += len(updatedEdges)
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, binding := range matched {
				row, err := projectBindingRow(binding, op.Returns)
				if err != nil {
					return execmodel.Result{}, err
				}
				result.Rows = append(result.Rows, row)
			}
		case planmodel.MatchDeleteOperation:
			if plan.AccessMode != analysis.ReadWrite {
				return execmodel.Result{}, fmt.Errorf("delete requires read-write access mode")
			}
			bindings, err := e.matchBindings(ctx, op.Start, op.Segments)
			if err != nil {
				return execmodel.Result{}, err
			}
			matched := []pathBinding{}
			for _, binding := range bindings {
				if bindingMatchesPredicates(binding, op.ComparisonPredicates, op.TextPredicates, op.SemanticPredicates) {
					matched = append(matched, binding)
				}
			}
			if op.Limit > 0 && int64(len(matched)) > op.Limit {
				matched = matched[:op.Limit]
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, binding := range matched {
				row, err := projectBindingRow(binding, op.Returns)
				if err != nil {
					return execmodel.Result{}, err
				}
				result.Rows = append(result.Rows, row)
			}
			deletedEdges := map[string]struct{}{}
			deletedNodes := map[string]struct{}{}
			for _, binding := range matched {
				for _, target := range op.Targets {
					if edge, ok := binding.Edges[target]; ok {
						deletedEdges[edge.ID] = struct{}{}
					}
					if node, ok := binding.Nodes[target]; ok {
						deletedNodes[node.ID] = struct{}{}
					}
				}
			}
			for nodeID := range deletedNodes {
				if err := e.validateNodeDeleteHasNoUndeletedIncidentEdges(ctx, nodeID, deletedEdges); err != nil {
					return execmodel.Result{}, err
				}
			}
			for edgeID := range deletedEdges {
				if err := e.graph.DeleteEdge(ctx, edgeID); err != nil {
					return execmodel.Result{}, err
				}
			}
			for nodeID := range deletedNodes {
				if err := e.graph.DeleteNode(ctx, nodeID); err != nil {
					return execmodel.Result{}, err
				}
			}
			result.Counters.EdgesDeleted += len(deletedEdges)
			result.Counters.NodesDeleted += len(deletedNodes)
		case planmodel.QueryPathOperation:
			if plan.AccessMode != analysis.ReadOnly {
				return execmodel.Result{}, fmt.Errorf("query path requires read-only access mode")
			}
			bindings := []pathBinding{}
			starts, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...), Properties: copyProperties(op.Start.Properties)})
			if err != nil {
				return execmodel.Result{}, err
			}
			for _, start := range starts {
				bindings = append(bindings, pathBinding{Nodes: map[string]execmodel.Node{op.Start.Variable: start}, Edges: map[string]execmodel.Edge{}, OrderedNodes: []execmodel.Node{start}})
			}
			currentVar := op.Start.Variable
			for _, segment := range op.Segments {
				next := []pathBinding{}
				for _, binding := range bindings {
					current, ok := binding.Nodes[currentVar]
					if !ok {
						return execmodel.Result{}, fmt.Errorf("path variable %q is not bound", currentVar)
					}
					rows, err := e.expandSegment(ctx, current, segment)
					if err != nil {
						return execmodel.Result{}, err
					}
					for _, matched := range rows {
						copyBinding := pathBinding{Nodes: map[string]execmodel.Node{}, Edges: map[string]execmodel.Edge{}, OrderedNodes: append([]execmodel.Node(nil), binding.OrderedNodes...), OrderedEdges: append([]execmodel.Edge(nil), binding.OrderedEdges...)}
						for key, value := range binding.Nodes {
							copyBinding.Nodes[key] = value
						}
						for key, value := range binding.Edges {
							copyBinding.Edges[key] = value
						}
						if segment.Node.Variable != "" {
							copyBinding.Nodes[segment.Node.Variable] = matched.End
						}
						if segment.Relationship.Variable != "" && len(matched.Edges) == 1 {
							copyBinding.Edges[segment.Relationship.Variable] = matched.Edges[0]
						}
						copyBinding.OrderedNodes = append(copyBinding.OrderedNodes, matched.Nodes...)
						copyBinding.OrderedEdges = append(copyBinding.OrderedEdges, matched.Edges...)
						next = append(next, copyBinding)
					}
				}
				bindings = next
				currentVar = segment.Node.Variable
			}
			if op.Limit > 0 && int64(len(bindings)) > op.Limit {
				bindings = bindings[:op.Limit]
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, binding := range bindings {
				if !bindingMatchesPredicates(binding, op.ComparisonPredicates, op.TextPredicates, op.SemanticPredicates) {
					continue
				}
				row := execmodel.Row{}
				for _, ret := range op.Returns {
					column := returnColumn(ret)
					if returnKind(ret) == planmodel.ReturnVariable {
						if ret.Variable == op.PathVariable {
							path := execmodel.Path{Nodes: append([]execmodel.Node(nil), binding.OrderedNodes...), Edges: append([]execmodel.Edge(nil), binding.OrderedEdges...)}
							row[column] = execmodel.Value{Path: &path}
							continue
						}
						if n, ok := binding.Nodes[ret.Variable]; ok {
							n := n
							row[column] = execmodel.Value{Node: &n}
							continue
						}
						if edge, ok := binding.Edges[ret.Variable]; ok {
							edge := edge
							row[column] = execmodel.Value{Edge: &edge}
							continue
						}
						return execmodel.Result{}, fmt.Errorf("return variable %q is not bound", ret.Variable)
					}
					if n, ok := binding.Nodes[ret.Variable]; ok {
						row[column] = execmodel.Value{Scalar: projectNodeField(n, ret)}
						continue
					}
					if edge, ok := binding.Edges[ret.Variable]; ok {
						row[column] = execmodel.Value{Scalar: projectEdgeField(edge, ret)}
						continue
					}
					return execmodel.Result{}, fmt.Errorf("return variable %q is not bound", ret.Variable)
				}
				result.Rows = append(result.Rows, row)
			}
		case planmodel.QueryPatternOperation:
			if plan.AccessMode != analysis.ReadOnly {
				return execmodel.Result{}, fmt.Errorf("query pattern requires read-only access mode")
			}
			rows, err := e.graph.QueryPattern(ctx, QueryPattern{
				Start:        QueryNodePattern{Variable: op.Start.Variable, Labels: append([]string(nil), op.Start.Labels...), Properties: copyProperties(op.Start.Properties)},
				Relationship: QueryRelationshipPattern{Labels: append([]string(nil), op.Relationship.Labels...), Properties: copyProperties(op.Relationship.Properties), Direction: RelationshipDirection(op.Relationship.Direction), Quantifier: executionQuantifier(op.Relationship.Quantifier)},
				End:          QueryNodePattern{Variable: op.End.Variable, Labels: append([]string(nil), op.End.Labels...), Properties: copyProperties(op.End.Properties)},
				Limit:        op.Limit,
			})
			if err != nil {
				return execmodel.Result{}, err
			}
			if op.Limit > 0 && int64(len(rows)) > op.Limit {
				rows = rows[:op.Limit]
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, matched := range rows {
				binding := pathBinding{Nodes: map[string]execmodel.Node{op.Start.Variable: matched.Start, op.End.Variable: matched.End}, Edges: map[string]execmodel.Edge{}}
				if op.Relationship.Variable != "" {
					binding.Edges[op.Relationship.Variable] = matched.Edge
				}
				if !bindingMatchesPredicates(binding, op.ComparisonPredicates, op.TextPredicates, op.SemanticPredicates) {
					continue
				}
				row := execmodel.Row{}
				for _, ret := range op.Returns {
					column := returnColumn(ret)
					switch returnKind(ret) {
					case planmodel.ReturnVariable:
						switch ret.Variable {
						case op.Start.Variable:
							n := matched.Start
							row[column] = execmodel.Value{Node: &n}
						case op.Relationship.Variable:
							edge := matched.Edge
							row[column] = execmodel.Value{Edge: &edge}
						case op.End.Variable:
							n := matched.End
							row[column] = execmodel.Value{Node: &n}
						default:
							return execmodel.Result{}, fmt.Errorf("return variable %q is not bound", ret.Variable)
						}
					case planmodel.ReturnProperty:
						switch ret.Variable {
						case op.Start.Variable:
							row[column] = execmodel.Value{Scalar: projectNodeField(matched.Start, ret)}
						case op.Relationship.Variable:
							row[column] = execmodel.Value{Scalar: projectEdgeField(matched.Edge, ret)}
						case op.End.Variable:
							row[column] = execmodel.Value{Scalar: projectNodeField(matched.End, ret)}
						default:
							return execmodel.Result{}, fmt.Errorf("return variable %q is not bound", ret.Variable)
						}
					default:
						return execmodel.Result{}, fmt.Errorf("unsupported return item kind %q", ret.Kind)
					}
				}
				result.Rows = append(result.Rows, row)
			}
		case planmodel.QueryNodesOperation:
			if plan.AccessMode != analysis.ReadOnly {
				return execmodel.Result{}, fmt.Errorf("query nodes requires read-only access mode")
			}
			nodes, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: op.Variable, Labels: append([]string(nil), op.Labels...), Properties: copyProperties(op.Properties), Limit: op.Limit})
			if err != nil {
				return execmodel.Result{}, err
			}
			if op.Limit > 0 && int64(len(nodes)) > op.Limit {
				nodes = nodes[:op.Limit]
			}
			for _, ret := range op.Returns {
				result.Columns = append(result.Columns, returnColumn(ret))
			}
			for _, node := range nodes {
				binding := pathBinding{Nodes: map[string]execmodel.Node{op.Variable: node}, Edges: map[string]execmodel.Edge{}}
				if !bindingMatchesPredicates(binding, op.ComparisonPredicates, op.TextPredicates, op.SemanticPredicates) {
					continue
				}
				row := execmodel.Row{}
				for _, ret := range op.Returns {
					column := returnColumn(ret)
					switch returnKind(ret) {
					case planmodel.ReturnVariable:
						n := node
						row[column] = execmodel.Value{Node: &n}
					case planmodel.ReturnProperty:
						row[column] = execmodel.Value{Scalar: projectNodeField(node, ret)}
					default:
						return execmodel.Result{}, fmt.Errorf("unsupported return item kind %q", ret.Kind)
					}
				}
				result.Rows = append(result.Rows, row)
			}
		default:
			return execmodel.Result{}, fmt.Errorf("unsupported operation %T", op)
		}
	}
	return result, nil
}

func (e executor) validateNodeDeleteHasNoUndeletedIncidentEdges(ctx context.Context, nodeID string, deletedEdges map[string]struct{}) error {
	outgoing, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": nodeID}}, Relationship: QueryRelationshipPattern{Direction: RelationshipOutgoing}, End: QueryNodePattern{}})
	if err != nil {
		return err
	}
	incoming, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": nodeID}}, Relationship: QueryRelationshipPattern{Direction: RelationshipIncoming}, End: QueryNodePattern{}})
	if err != nil {
		return err
	}
	for _, row := range append(outgoing, incoming...) {
		if _, ok := deletedEdges[row.Edge.ID]; !ok {
			return fmt.Errorf("delete node %q requires deleting incident edge %q first", nodeID, row.Edge.ID)
		}
	}
	return nil
}

func (e executor) matchNodePatternBindings(ctx context.Context, matches []planmodel.NodePattern) ([]map[string]execmodel.Node, error) {
	bindings := []map[string]execmodel.Node{{}}
	for _, match := range matches {
		nodes, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: match.Variable, Labels: append([]string(nil), match.Labels...), Properties: copyProperties(match.Properties)})
		if err != nil {
			return nil, err
		}
		next := []map[string]execmodel.Node{}
		for _, binding := range bindings {
			for _, node := range nodes {
				copyBinding := map[string]execmodel.Node{}
				for key, value := range binding {
					copyBinding[key] = value
				}
				copyBinding[match.Variable] = node
				next = append(next, copyBinding)
			}
		}
		bindings = next
	}
	return bindings, nil
}

func (e executor) matchBindings(ctx context.Context, start planmodel.NodePattern, segments []planmodel.PathSegment) ([]pathBinding, error) {
	starts, err := e.graph.QueryNodes(ctx, QueryNodes{Variable: start.Variable, Labels: append([]string(nil), start.Labels...), Properties: copyProperties(start.Properties)})
	if err != nil {
		return nil, err
	}
	bindings := make([]pathBinding, 0, len(starts))
	for _, startNode := range starts {
		bindings = append(bindings, pathBinding{Nodes: map[string]execmodel.Node{start.Variable: startNode}, Edges: map[string]execmodel.Edge{}})
	}
	currentVar := start.Variable
	for _, segment := range segments {
		next := []pathBinding{}
		for _, binding := range bindings {
			current, ok := binding.Nodes[currentVar]
			if !ok {
				return nil, fmt.Errorf("path variable %q is not bound", currentVar)
			}
			rows, err := e.expandSegment(ctx, current, segment)
			if err != nil {
				return nil, err
			}
			for _, matched := range rows {
				copyBinding := pathBinding{Nodes: map[string]execmodel.Node{}, Edges: map[string]execmodel.Edge{}}
				for key, value := range binding.Nodes {
					copyBinding.Nodes[key] = value
				}
				for key, value := range binding.Edges {
					copyBinding.Edges[key] = value
				}
				if segment.Node.Variable != "" {
					copyBinding.Nodes[segment.Node.Variable] = matched.End
				}
				if segment.Relationship.Variable != "" && len(matched.Edges) == 1 {
					copyBinding.Edges[segment.Relationship.Variable] = matched.Edges[0]
				}
				next = append(next, copyBinding)
			}
		}
		bindings = next
		currentVar = segment.Node.Variable
	}
	return bindings, nil
}

func (e executor) applyNodeAssignment(ctx context.Context, node execmodel.Node, assignment planmodel.SetAssignment) (execmodel.Node, error) {
	updated := node
	switch assignment.Namespace {
	case "", "properties":
		updated.Properties = copyProperties(updated.Properties)
		if updated.Properties == nil {
			updated.Properties = map[string]any{}
		}
		updated.Properties[assignment.Property] = assignment.Value
	case "payload":
		updated.Payload = copyProperties(updated.Payload)
		if updated.Payload == nil {
			updated.Payload = map[string]any{}
		}
		updated.Payload[assignment.Property] = assignment.Value
	default:
		return execmodel.Node{}, fmt.Errorf("unsupported set namespace %q", assignment.Namespace)
	}
	return e.graph.UpdateNode(ctx, UpdateNode{NodeID: updated.ID, Labels: append([]string(nil), updated.Labels...), Properties: copyProperties(updated.Properties), Payload: copyProperties(updated.Payload), Meta: copyProperties(updated.Meta)})
}

func (e executor) applyEdgeAssignment(ctx context.Context, edge execmodel.Edge, assignment planmodel.SetAssignment) (execmodel.Edge, error) {
	updated := edge
	switch assignment.Namespace {
	case "", "properties":
		updated.Properties = copyProperties(updated.Properties)
		if updated.Properties == nil {
			updated.Properties = map[string]any{}
		}
		updated.Properties[assignment.Property] = assignment.Value
	case "payload":
		updated.Payload = copyProperties(updated.Payload)
		if updated.Payload == nil {
			updated.Payload = map[string]any{}
		}
		updated.Payload[assignment.Property] = assignment.Value
	default:
		return execmodel.Edge{}, fmt.Errorf("unsupported set namespace %q", assignment.Namespace)
	}
	return e.graph.UpdateEdge(ctx, UpdateEdge{EdgeID: updated.ID, Labels: append([]string(nil), updated.Labels...), Properties: copyProperties(updated.Properties), Payload: copyProperties(updated.Payload), Meta: copyProperties(updated.Meta)})
}

func projectBindingRow(binding pathBinding, returns []planmodel.ReturnItem) (execmodel.Row, error) {
	row := execmodel.Row{}
	for _, ret := range returns {
		column := returnColumn(ret)
		if returnKind(ret) == planmodel.ReturnVariable {
			if n, ok := binding.Nodes[ret.Variable]; ok {
				n := n
				row[column] = execmodel.Value{Node: &n}
				continue
			}
			if edge, ok := binding.Edges[ret.Variable]; ok {
				edge := edge
				row[column] = execmodel.Value{Edge: &edge}
				continue
			}
			return nil, fmt.Errorf("return variable %q is not bound", ret.Variable)
		}
		if n, ok := binding.Nodes[ret.Variable]; ok {
			row[column] = execmodel.Value{Scalar: projectNodeField(n, ret)}
			continue
		}
		if edge, ok := binding.Edges[ret.Variable]; ok {
			row[column] = execmodel.Value{Scalar: projectEdgeField(edge, ret)}
			continue
		}
		return nil, fmt.Errorf("return variable %q is not bound", ret.Variable)
	}
	return row, nil
}

func (e executor) expandSegment(ctx context.Context, current execmodel.Node, segment planmodel.PathSegment) ([]segmentExpansion, error) {
	quant := segment.Relationship.Quantifier
	if quant == nil {
		rows, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": current.ID}}, Relationship: QueryRelationshipPattern{Labels: append([]string(nil), segment.Relationship.Labels...), Properties: copyProperties(segment.Relationship.Properties), Direction: RelationshipDirection(segment.Relationship.Direction)}, End: QueryNodePattern{Variable: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...), Properties: copyProperties(segment.Node.Properties)}})
		if err != nil {
			return nil, err
		}
		out := make([]segmentExpansion, 0, len(rows))
		for _, row := range rows {
			out = append(out, segmentExpansion{End: row.End, Nodes: []execmodel.Node{row.End}, Edges: []execmodel.Edge{row.Edge}})
		}
		return out, nil
	}
	type frontierPath struct {
		Node  execmodel.Node
		Nodes []execmodel.Node
		Edges []execmodel.Edge
	}
	frontier := []frontierPath{{Node: current}}
	out := []segmentExpansion{}
	maxDepth := quant.Max
	if maxDepth == -1 {
		maxDepth = 5
	}
	for depth := 1; depth <= maxDepth; depth++ {
		next := []frontierPath{}
		for _, path := range frontier {
			rows, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": path.Node.ID}}, Relationship: QueryRelationshipPattern{Labels: append([]string(nil), segment.Relationship.Labels...), Properties: copyProperties(segment.Relationship.Properties), Direction: RelationshipDirection(segment.Relationship.Direction)}, End: QueryNodePattern{Variable: segment.Node.Variable}})
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				nodes := append(append([]execmodel.Node(nil), path.Nodes...), row.End)
				edges := append(append([]execmodel.Edge(nil), path.Edges...), row.Edge)
				next = append(next, frontierPath{Node: row.End, Nodes: nodes, Edges: edges})
				if depth >= quant.Min && nodeMatchesPattern(row.End, segment.Node) {
					out = append(out, segmentExpansion{End: row.End, Nodes: nodes, Edges: edges})
				}
			}
		}
		frontier = next
	}
	return out, nil
}

func nodeMatchesPattern(node execmodel.Node, pattern planmodel.NodePattern) bool {
	return hasAllStrings(node.Labels, pattern.Labels) && execHasProperties(node.Properties, pattern.Properties)
}

func hasAllStrings(values, required []string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func execHasProperties(values, required map[string]any) bool {
	for key, value := range required {
		if values[key] != value {
			return false
		}
	}
	return true
}

func executionQuantifier(quant *planmodel.RelationshipQuantifier) *RelationshipQuantifier {
	if quant == nil {
		return nil
	}
	return &RelationshipQuantifier{Min: quant.Min, Max: quant.Max}
}

func bindingMatchesPredicates(binding pathBinding, comparisons []planmodel.ComparisonPredicate, texts []planmodel.TextContainsPredicate, semantics []planmodel.SemanticSimilarPredicate) bool {
	for _, pred := range comparisons {
		value, ok := bindingValue(binding, pred.Variable, "", pred.Property)
		if !ok || !compareValues(value, pred.Operator, pred.Value) {
			return false
		}
	}
	for _, pred := range texts {
		value, ok := bindingValue(binding, pred.Variable, pred.Namespace, pred.Property)
		if !ok || !strings.Contains(strings.ToLower(fmt.Sprint(value)), strings.ToLower(pred.Query)) {
			return false
		}
	}
	for _, pred := range semantics {
		// MVP semantic predicate uses available textual node fields as a local fallback until index-backed GQL search is wired.
		node, ok := binding.Nodes[pred.Variable]
		if !ok || !nodeContainsText(node, pred.Query) {
			return false
		}
	}
	return true
}

func bindingValue(binding pathBinding, variable, namespace, property string) (any, bool) {
	if node, ok := binding.Nodes[variable]; ok {
		return projectNodeField(node, planmodel.ReturnItem{Namespace: namespace, Property: property}), true
	}
	if edge, ok := binding.Edges[variable]; ok {
		return projectEdgeField(edge, planmodel.ReturnItem{Namespace: namespace, Property: property}), true
	}
	return nil, false
}

func compareValues(left any, op planmodel.ComparisonOperator, right any) bool {
	if op == planmodel.ComparisonEqual {
		return fmt.Sprint(left) == fmt.Sprint(right)
	}
	if op == planmodel.ComparisonNotEqual {
		return fmt.Sprint(left) != fmt.Sprint(right)
	}
	leftNum, leftOK := numericValue(left)
	rightNum, rightOK := numericValue(right)
	if leftOK && rightOK {
		switch op {
		case planmodel.ComparisonLessThan:
			return leftNum < rightNum
		case planmodel.ComparisonLessThanOrEqual:
			return leftNum <= rightNum
		case planmodel.ComparisonGreaterThan:
			return leftNum > rightNum
		case planmodel.ComparisonGreaterThanOrEqual:
			return leftNum >= rightNum
		}
	}
	leftString := fmt.Sprint(left)
	rightString := fmt.Sprint(right)
	switch op {
	case planmodel.ComparisonLessThan:
		return leftString < rightString
	case planmodel.ComparisonLessThanOrEqual:
		return leftString <= rightString
	case planmodel.ComparisonGreaterThan:
		return leftString > rightString
	case planmodel.ComparisonGreaterThanOrEqual:
		return leftString >= rightString
	default:
		return false
	}
}

func numericValue(value any) (float64, bool) {
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

func nodeContainsText(node execmodel.Node, query string) bool {
	needle := strings.ToLower(query)
	for _, m := range []map[string]any{node.Properties, node.Payload, node.Meta} {
		for _, value := range m {
			if strings.Contains(strings.ToLower(fmt.Sprint(value)), needle) {
				return true
			}
		}
	}
	return false
}

func returnKind(ret planmodel.ReturnItem) planmodel.ReturnItemKind {
	if ret.Kind == "" {
		return planmodel.ReturnVariable
	}
	return ret.Kind
}

func returnColumn(ret planmodel.ReturnItem) string {
	if ret.OutputName != "" {
		return ret.OutputName
	}
	if returnKind(ret) == planmodel.ReturnProperty {
		if ret.Namespace != "" {
			return ret.Variable + "." + ret.Namespace + "." + ret.Property
		}
		return ret.Variable + "." + ret.Property
	}
	return ret.Variable
}

func projectNodeField(node execmodel.Node, ret planmodel.ReturnItem) any {
	switch ret.Namespace {
	case "payload":
		return node.Payload[ret.Property]
	case "meta":
		return node.Meta[ret.Property]
	case "properties", "":
		return node.Properties[ret.Property]
	default:
		return nil
	}
}

func projectEdgeField(edge execmodel.Edge, ret planmodel.ReturnItem) any {
	switch ret.Namespace {
	case "payload":
		return edge.Payload[ret.Property]
	case "meta":
		return edge.Meta[ret.Property]
	case "properties", "":
		return edge.Properties[ret.Property]
	default:
		return nil
	}
}

func copyProperties(properties map[string]any) map[string]any {
	if properties == nil {
		return nil
	}
	copy := make(map[string]any, len(properties))
	for key, value := range properties {
		copy[key] = value
	}
	return copy
}
