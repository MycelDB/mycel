package client

import (
	"time"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	queryruntime "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// neutralLegacyBindings is the temporary QRX6 boundary between legacy access
// selection and the neutral shaping runtime.
func (e *queryExecution) neutralLegacyBindings(rows []*queryRowState, projections []querylogical.Projection) []queryruntime.BindingRow {
	out := make([]queryruntime.BindingRow, 0, len(rows))
	for _, row := range rows {
		binding := queryruntime.BindingRow{
			Nodes: make(map[string]domaingraph.Node, len(row.bindings)),
			Edges: make(map[string]domaingraph.Edge, len(row.edgeBindings)),
			Paths: make(map[string]querylogical.Path, len(row.pathBindings)),
			Trees: map[string]querylogical.Tree{},
		}
		for alias, nodes := range row.bindings {
			if len(nodes) > 0 {
				binding.Nodes[alias] = nodes[0]
			}
		}
		for alias, edges := range row.edgeBindings {
			if len(edges) > 0 {
				binding.Edges[alias] = edges[0]
			}
		}
		for alias, path := range row.pathBindings {
			if path != nil {
				binding.Paths[alias] = neutralLegacyPath(path)
			}
		}
		for _, projection := range projections {
			if projection.Kind == querylogical.ProjectionTree {
				binding.Trees[projection.Alias] = neutralLegacyTree(e.projectTree(row, projection.Alias))
			}
		}
		out = append(out, binding)
	}
	return out
}

func neutralLegacyPath(path *clientv1.PathValue) querylogical.Path {
	out := querylogical.Path{Nodes: make([]querylogical.Node, 0, len(path.GetNodes())), Edges: make([]querylogical.Edge, 0, len(path.GetEdges()))}
	for _, node := range path.GetNodes() {
		if node != nil {
			out.Nodes = append(out.Nodes, neutralLegacyNode(node))
		}
	}
	for _, edge := range path.GetEdges() {
		if edge != nil {
			out.Edges = append(out.Edges, neutralLegacyEdge(edge))
		}
	}
	return out
}

func neutralLegacyTree(tree *clientv1.Tree) querylogical.Tree {
	out := querylogical.Tree{Roots: make([]querylogical.TreeNode, 0, len(tree.GetRoots()))}
	for _, root := range tree.GetRoots() {
		if root != nil {
			out.Roots = append(out.Roots, neutralLegacyTreeNode(root))
		}
	}
	return out
}

func neutralLegacyTreeNode(node *clientv1.TreeNode) querylogical.TreeNode {
	out := querylogical.TreeNode{Node: neutralLegacyNode(node.GetNode()), Children: make([]querylogical.TreeNode, 0, len(node.GetChildren()))}
	for _, child := range node.GetChildren() {
		if child != nil {
			out.Children = append(out.Children, neutralLegacyTreeNode(child))
		}
	}
	return out
}

func neutralLegacyNode(node *clientv1.Node) querylogical.Node {
	if node == nil {
		return querylogical.Node{}
	}
	return querylogical.Node{
		ID: node.GetNodeId(), DomainID: node.GetDomainId(), Labels: append([]string(nil), node.GetLabels()...),
		Properties: neutralLegacyValues(structMap(node.GetProperties())), Payload: neutralLegacyValues(structMap(node.GetPayload())),
		CreatedAt: neutralLegacyTime(node.GetCreateTime()), UpdatedAt: neutralLegacyTime(node.GetUpdateTime()),
	}
}

func neutralLegacyEdge(edge *clientv1.Edge) querylogical.Edge {
	if edge == nil {
		return querylogical.Edge{}
	}
	return querylogical.Edge{
		ID: edge.GetEdgeId(), DomainID: edge.GetDomainId(), FromNodeID: edge.GetFromNodeId(), ToNodeID: edge.GetToNodeId(), Labels: append([]string(nil), edge.GetLabels()...),
		Properties: neutralLegacyValues(structMap(edge.GetProperties())), Payload: neutralLegacyValues(structMap(edge.GetPayload())),
		CreatedAt: neutralLegacyTime(edge.GetCreateTime()), UpdatedAt: neutralLegacyTime(edge.GetUpdateTime()),
	}
}

func neutralLegacyTime(value *timestamppb.Timestamp) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.AsTime()
}

func neutralLegacyValues(values map[string]any) map[string]querylogical.Value {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]querylogical.Value, len(values))
	for name, value := range values {
		out[name] = querylogical.NativeValue(value)
	}
	return out
}
