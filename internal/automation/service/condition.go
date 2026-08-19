package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const conditionTimeout = 2 * time.Second
const conditionMaxRows = 100
const conditionFalseReason = "condition_false"

type conditionResult struct {
	Matched bool
	Aliases map[string]any
}

func (m *AutomationManager) conditionSchemaContext(ctx context.Context, tx sessionservice.GraphTransaction) (analysis.SchemaContext, error) {
	if m.schemas == nil || tx.DomainID == "" {
		return analysis.SchemaContext{}, nil
	}
	domainUUID, err := uuid.Parse(tx.DomainID)
	if err != nil {
		return analysis.SchemaContext{}, fmt.Errorf("invalid transaction domain id: %w", err)
	}
	domainID := graph.DomainID(domainUUID)
	schemaDoc, err := m.schemas.GetDomainSchema(ctx, domainID)
	if err == nil {
		return analysis.SchemaContext{Schema: &schemaDoc}, nil
	}
	if errors.Is(err, schemaservice.ErrSchemaNotFound) {
		return analysis.SchemaContext{}, nil
	}
	return analysis.SchemaContext{}, err
}

func (m *AutomationManager) evaluateCondition(ctx context.Context, tx sessionservice.GraphTransaction, def automation.Definition, changed graph.Node, old *graph.Node) (conditionResult, error) {
	if strings.TrimSpace(def.Condition.GQL) == "" {
		return conditionResult{Matched: true, Aliases: map[string]any{"changed": changed}}, nil
	}
	schemaCtx, err := m.conditionSchemaContext(ctx, tx)
	if err != nil {
		return conditionResult{}, err
	}
	plan, err := gql.CompileWithSchema(def.Condition.GQL, schemaCtx)
	if err != nil {
		return conditionResult{}, err
	}
	if plan.AccessMode == analysis.ReadWrite {
		return conditionResult{}, fmt.Errorf("automation condition must be read-only")
	}
	conditionCtx, cancel := context.WithTimeout(ctx, conditionTimeout)
	defer cancel()
	result, err := execution.Execute(conditionCtx, automationGQLGraph{graphs: m.graphs, tx: tx, changed: &changed, old: old}, plan)
	if err != nil {
		return conditionResult{}, err
	}
	if len(result.Rows) > conditionMaxRows {
		return conditionResult{}, fmt.Errorf("automation condition returned too many rows")
	}
	var matched map[string]any
	matchedRows := 0
	for _, row := range result.Rows {
		if rowContainsChanged(row, changed.ID.String()) {
			matchedRows++
			if matchedRows > 1 {
				return conditionResult{}, fmt.Errorf("automation condition returned multiple rows; refine condition or enable explicit fan-out")
			}
			matched = rowAliases(row)
		}
	}
	if matchedRows == 1 {
		return conditionResult{Matched: true, Aliases: matched}, nil
	}
	return conditionResult{Matched: false}, nil
}

type automationGQLGraph struct {
	graphs  graphservice.Manager
	tx      sessionservice.GraphTransaction
	changed *graph.Node
	old     *graph.Node
	aliases map[string]any
}

func (g automationGQLGraph) InsertNode(context.Context, execution.InsertNode) (execmodel.NodeRef, error) {
	return execmodel.NodeRef{}, fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) CreateEdge(context.Context, execution.CreateEdge) (execmodel.Edge, error) {
	return execmodel.Edge{}, fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) UpdateNode(context.Context, execution.UpdateNode) (execmodel.Node, error) {
	return execmodel.Node{}, fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) UpdateEdge(context.Context, execution.UpdateEdge) (execmodel.Edge, error) {
	return execmodel.Edge{}, fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) DeleteNode(context.Context, string) error {
	return fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) DeleteEdge(context.Context, string) error {
	return fmt.Errorf("automation conditions are read-only")
}

func (g automationGQLGraph) QueryNodes(ctx context.Context, query execution.QueryNodes) ([]execmodel.Node, error) {
	if g.requiresBoundVariable(query.Variable) {
		bound, ok := g.boundNode(query.Variable)
		if !ok {
			return nil, nil
		}
		if nodeMatchesConditionPattern(bound, query.Labels, query.Properties) {
			return []execmodel.Node{execNode(bound)}, nil
		}
		return nil, nil
	}
	nodes, err := listAllNodes(ctx, g.graphs, g.tx)
	if err != nil {
		return nil, err
	}
	out := []execmodel.Node{}
	for _, node := range nodes {
		if !nodeMatchesConditionPattern(node, query.Labels, query.Properties) {
			continue
		}
		out = append(out, execNode(node))
		if query.Limit > 0 && int64(len(out)) >= query.Limit {
			return out, nil
		}
	}
	return out, nil
}

func (g automationGQLGraph) QueryPattern(ctx context.Context, query execution.QueryPattern) ([]execution.PatternRow, error) {
	if g.requiresBoundVariable(query.Start.Variable) {
		bound, ok := g.boundNode(query.Start.Variable)
		if !ok {
			return nil, nil
		}
		query.Start.Properties = withIDProperty(query.Start.Properties, bound.ID.String())
	}
	if g.requiresBoundVariable(query.End.Variable) {
		bound, ok := g.boundNode(query.End.Variable)
		if !ok {
			return nil, nil
		}
		query.End.Properties = withIDProperty(query.End.Properties, bound.ID.String())
	}
	if g.requiresBoundVariable(query.Start.Variable) || g.requiresBoundVariable(query.End.Variable) {
		return g.queryBoundPattern(ctx, query)
	}
	nodes, err := listAllNodes(ctx, g.graphs, g.tx)
	if err != nil {
		return nil, err
	}
	edges, err := listAllEdges(ctx, g.graphs, g.tx)
	if err != nil {
		return nil, err
	}
	nodeByID := map[string]graph.Node{}
	for _, node := range nodes {
		nodeByID[node.ID.String()] = node
	}
	out := []execution.PatternRow{}
	for _, edge := range edges {
		if !hasLabels(edge.Labels, query.Relationship.Labels) || !hasProperties(edge.Properties, query.Relationship.Properties) {
			continue
		}
		from, fromOK := nodeByID[edge.FromID.String()]
		to, toOK := nodeByID[edge.ToID.String()]
		if !fromOK || !toOK {
			continue
		}
		appendIfMatch := func(start, end graph.Node) {
			if !nodeMatchesConditionPattern(start, query.Start.Labels, query.Start.Properties) || !nodeMatchesConditionPattern(end, query.End.Labels, query.End.Properties) {
				return
			}
			out = append(out, execution.PatternRow{Start: execNode(start), Edge: execEdge(edge), End: execNode(end)})
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

func (g automationGQLGraph) queryBoundPattern(ctx context.Context, query execution.QueryPattern) ([]execution.PatternRow, error) {
	edges, err := g.boundPatternCandidateEdges(ctx, query)
	if err != nil {
		return nil, err
	}
	out := []execution.PatternRow{}
	for _, edge := range edges {
		if !hasLabels(edge.Labels, query.Relationship.Labels) || !hasProperties(edge.Properties, query.Relationship.Properties) {
			continue
		}
		pairs := [][2]string{}
		switch query.Relationship.Direction {
		case execution.RelationshipIncoming:
			pairs = append(pairs, [2]string{edge.ToID.String(), edge.FromID.String()})
		case execution.RelationshipUndirected:
			pairs = append(pairs, [2]string{edge.FromID.String(), edge.ToID.String()}, [2]string{edge.ToID.String(), edge.FromID.String()})
		default:
			pairs = append(pairs, [2]string{edge.FromID.String(), edge.ToID.String()})
		}
		for _, pair := range pairs {
			if id, ok := query.Start.Properties["__id"].(string); ok && pair[0] != id {
				continue
			}
			if id, ok := query.End.Properties["__id"].(string); ok && pair[1] != id {
				continue
			}
			start, err := g.graphs.GetNode(ctx, g.tx, pair[0])
			if err != nil {
				continue
			}
			end, err := g.graphs.GetNode(ctx, g.tx, pair[1])
			if err != nil {
				continue
			}
			if !nodeMatchesConditionPattern(start, query.Start.Labels, query.Start.Properties) || !nodeMatchesConditionPattern(end, query.End.Labels, query.End.Properties) {
				continue
			}
			out = append(out, execution.PatternRow{Start: execNode(start), Edge: execEdge(edge), End: execNode(end)})
			if query.Limit > 0 && int64(len(out)) >= query.Limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func (g automationGQLGraph) boundPatternCandidateEdges(ctx context.Context, query execution.QueryPattern) ([]graph.Edge, error) {
	startID, startBound := query.Start.Properties["__id"].(string)
	endID, endBound := query.End.Properties["__id"].(string)
	label := ""
	if len(query.Relationship.Labels) > 0 {
		label = query.Relationship.Labels[0]
	}
	if label == "" {
		return listAllEdges(ctx, g.graphs, g.tx)
	}
	if startBound {
		switch query.Relationship.Direction {
		case execution.RelationshipIncoming:
			return g.scanAdjacencyEdges(ctx, startID, label, graphservice.AdjacencyDirectionIn)
		case execution.RelationshipUndirected:
			out, err := g.scanAdjacencyEdges(ctx, startID, label, graphservice.AdjacencyDirectionOut)
			if err != nil {
				return nil, err
			}
			in, err := g.scanAdjacencyEdges(ctx, startID, label, graphservice.AdjacencyDirectionIn)
			return append(out, in...), err
		default:
			return g.scanAdjacencyEdges(ctx, startID, label, graphservice.AdjacencyDirectionOut)
		}
	}
	if endBound {
		switch query.Relationship.Direction {
		case execution.RelationshipIncoming:
			return g.scanAdjacencyEdges(ctx, endID, label, graphservice.AdjacencyDirectionOut)
		case execution.RelationshipUndirected:
			out, err := g.scanAdjacencyEdges(ctx, endID, label, graphservice.AdjacencyDirectionIn)
			if err != nil {
				return nil, err
			}
			outgoing, err := g.scanAdjacencyEdges(ctx, endID, label, graphservice.AdjacencyDirectionOut)
			return append(out, outgoing...), err
		default:
			return g.scanAdjacencyEdges(ctx, endID, label, graphservice.AdjacencyDirectionIn)
		}
	}
	return listAllEdges(ctx, g.graphs, g.tx)
}

func (g automationGQLGraph) scanAdjacencyEdges(ctx context.Context, nodeID string, label string, direction graphservice.AdjacencyDirection) ([]graph.Edge, error) {
	out := []graph.Edge{}
	cursor := ""
	for {
		edges, next, _, err := g.graphs.ScanAdjacency(ctx, g.tx, graphservice.AdjacencyScan{NodeID: nodeID, Label: label, Direction: direction, Limit: 500, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		out = append(out, edges...)
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func listAllNodes(ctx context.Context, graphs graphservice.Manager, tx sessionservice.GraphTransaction) ([]graph.Node, error) {
	out := []graph.Node{}
	token := ""
	for {
		nodes, next, err := graphs.ListNodes(ctx, tx, 500, token)
		if err != nil {
			return nil, err
		}
		out = append(out, nodes...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

func listAllEdges(ctx context.Context, graphs graphservice.Manager, tx sessionservice.GraphTransaction) ([]graph.Edge, error) {
	out := []graph.Edge{}
	token := ""
	for {
		edges, next, err := graphs.ListEdges(ctx, tx, 500, token)
		if err != nil {
			return nil, err
		}
		out = append(out, edges...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

func (g automationGQLGraph) requiresBoundVariable(variable string) bool {
	if variable == "changed" || variable == "old" {
		return true
	}
	_, ok := g.aliases[variable]
	return ok
}

func (g automationGQLGraph) boundNode(variable string) (graph.Node, bool) {
	switch variable {
	case "changed":
		if g.changed != nil {
			return *g.changed, true
		}
	case "old":
		if g.old != nil {
			return *g.old, true
		}
	}
	if g.aliases != nil {
		if node, ok := aliasGraphNode(g.aliases[variable]); ok {
			return node, true
		}
	}
	return graph.Node{}, false
}

func aliasGraphNode(value any) (graph.Node, bool) {
	switch v := value.(type) {
	case graph.Node:
		return v, true
	case execmodel.Node:
		id, err := uuid.Parse(v.ID)
		if err != nil {
			return graph.Node{}, false
		}
		domainID, _ := uuid.Parse(v.DomainID)
		return graph.Node{ID: graph.NodeID(id), DomainID: graph.DomainID(domainID), Labels: append([]string(nil), v.Labels...), Properties: copyAnyMap(v.Properties), Payload: copyAnyMap(v.Payload), Meta: copyAnyMap(v.Meta)}, true
	default:
		return graph.Node{}, false
	}
}

func withIDProperty(properties map[string]any, id string) map[string]any {
	out := copyAnyMap(properties)
	if out == nil {
		out = map[string]any{}
	}
	out["__id"] = id
	return out
}

func nodeMatchesConditionPattern(node graph.Node, labels []string, properties map[string]any) bool {
	if id, ok := properties["__id"].(string); ok && node.ID.String() != id {
		return false
	}
	return hasLabels(node.Labels, labels) && hasProperties(node.Properties, properties)
}

func hasLabels(labels []string, required []string) bool {
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

func hasProperties(values map[string]any, required map[string]any) bool {
	for key, value := range required {
		if key == "__id" {
			continue
		}
		if !reflect.DeepEqual(values[key], value) {
			return false
		}
	}
	return true
}

func execNode(node graph.Node) execmodel.Node {
	return execmodel.Node{ID: node.ID.String(), DomainID: node.DomainID.String(), Labels: append([]string(nil), node.Labels...), Properties: copyAnyMap(node.Properties), Payload: copyAnyMap(node.Payload), Meta: copyAnyMap(node.Meta)}
}

func execEdge(edge graph.Edge) execmodel.Edge {
	return execmodel.Edge{ID: edge.ID.String(), DomainID: edge.DomainID.String(), FromID: edge.FromID.String(), ToID: edge.ToID.String(), Labels: append([]string(nil), edge.Labels...), Properties: copyAnyMap(edge.Properties), Payload: copyAnyMap(edge.Payload), Meta: copyAnyMap(edge.Meta)}
}

func copyAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func rowContainsChanged(row execmodel.Row, changedID string) bool {
	value, ok := row["changed"]
	return ok && value.Node != nil && value.Node.ID == changedID
}

func rowAliases(row execmodel.Row) map[string]any {
	out := map[string]any{}
	for key, value := range row {
		switch {
		case value.Node != nil:
			if node, ok := aliasGraphNode(*value.Node); ok {
				out[key] = node
			} else {
				out[key] = *value.Node
			}
		case value.Edge != nil:
			if edge, ok := aliasGraphEdge(*value.Edge); ok {
				out[key] = edge
			} else {
				out[key] = *value.Edge
			}
		default:
			out[key] = value.Scalar
		}
	}
	return out
}

func aliasGraphEdge(value any) (graph.Edge, bool) {
	switch v := value.(type) {
	case graph.Edge:
		return v, true
	case execmodel.Edge:
		id, err := uuid.Parse(v.ID)
		if err != nil {
			return graph.Edge{}, false
		}
		domainID, _ := uuid.Parse(v.DomainID)
		fromID, _ := uuid.Parse(v.FromID)
		toID, _ := uuid.Parse(v.ToID)
		return graph.Edge{ID: graph.EdgeID(id), DomainID: graph.DomainID(domainID), FromID: graph.NodeID(fromID), ToID: graph.NodeID(toID), Labels: append([]string(nil), v.Labels...), Properties: copyAnyMap(v.Properties), Payload: copyAnyMap(v.Payload), Meta: copyAnyMap(v.Meta)}, true
	default:
		return graph.Edge{}, false
	}
}
