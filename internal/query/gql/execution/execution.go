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

type pathBinding struct {
	Nodes map[string]execmodel.Node
	Edges map[string]execmodel.Edge
}

// Graph is the graph capability required by the current executor.
type Graph interface {
	InsertNode(ctx context.Context, node InsertNode) (execmodel.NodeRef, error)
	CreateEdge(ctx context.Context, edge CreateEdge) (execmodel.Edge, error)
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
				bindings = append(bindings, pathBinding{Nodes: map[string]execmodel.Node{op.Start.Variable: start}, Edges: map[string]execmodel.Edge{}})
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
						if segment.Relationship.Variable != "" {
							copyBinding.Edges[segment.Relationship.Variable] = matched.Edge
						}
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
				if !bindingMatchesPredicates(binding, op.TextPredicates, op.SemanticPredicates) {
					continue
				}
				row := execmodel.Row{}
				for _, ret := range op.Returns {
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
				if !bindingMatchesPredicates(binding, op.TextPredicates, op.SemanticPredicates) {
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
				if !bindingMatchesPredicates(binding, op.TextPredicates, op.SemanticPredicates) {
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

func (e executor) expandSegment(ctx context.Context, current execmodel.Node, segment planmodel.PathSegment) ([]PatternRow, error) {
	quant := segment.Relationship.Quantifier
	if quant == nil {
		return e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": current.ID}}, Relationship: QueryRelationshipPattern{Labels: append([]string(nil), segment.Relationship.Labels...), Properties: copyProperties(segment.Relationship.Properties), Direction: RelationshipDirection(segment.Relationship.Direction)}, End: QueryNodePattern{Variable: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...), Properties: copyProperties(segment.Node.Properties)}})
	}
	frontier := []execmodel.Node{current}
	out := []PatternRow{}
	for depth := 1; depth <= quant.Max; depth++ {
		next := []execmodel.Node{}
		for _, node := range frontier {
			rows, err := e.graph.QueryPattern(ctx, QueryPattern{Start: QueryNodePattern{Properties: map[string]any{"__id": node.ID}}, Relationship: QueryRelationshipPattern{Labels: append([]string(nil), segment.Relationship.Labels...), Properties: copyProperties(segment.Relationship.Properties), Direction: RelationshipDirection(segment.Relationship.Direction)}, End: QueryNodePattern{Variable: segment.Node.Variable}})
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				next = append(next, row.End)
				if depth >= quant.Min && nodeMatchesPattern(row.End, segment.Node) {
					out = append(out, PatternRow{Start: current, Edge: row.Edge, End: row.End})
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

func bindingMatchesPredicates(binding pathBinding, texts []planmodel.TextContainsPredicate, semantics []planmodel.SemanticSimilarPredicate) bool {
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
