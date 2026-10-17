package client

import (
	"context"
	"errors"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// protoNeutralQueryResult is the daemon-owned transport boundary for the
// neutral executor result. Query execution packages never import generated API
// types.
func protoNeutralQueryResult(result querylogical.Result) *clientv1.QueryResult {
	rows := make([]*clientv1.QueryRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		fields := make(map[string]*clientv1.QueryValue, len(row.Fields))
		for name, cell := range row.Fields {
			fields[name] = protoNeutralCell(cell)
		}
		rows = append(rows, &clientv1.QueryRow{Fields: fields})
	}
	graph := &clientv1.ResultGraph{
		Nodes: make([]*clientv1.Node, 0, len(result.Graph.Nodes)),
		Edges: make([]*clientv1.Edge, 0, len(result.Graph.Edges)),
	}
	for _, node := range result.Graph.Nodes {
		graph.Nodes = append(graph.Nodes, protoNeutralNode(node))
	}
	for _, edge := range result.Graph.Edges {
		graph.Edges = append(graph.Edges, protoNeutralEdge(edge))
	}
	return &clientv1.QueryResult{
		Rows:          rows,
		Graph:         graph,
		NextPageToken: result.NextCursor,
		Counters: &clientv1.QueryCounters{
			RowsReturned:  diagnosticInt32(result.Counters.RowsReturned),
			NodesInserted: diagnosticInt32(result.Counters.NodesInserted),
			NodesUpdated:  diagnosticInt32(result.Counters.NodesUpdated),
			NodesDeleted:  diagnosticInt32(result.Counters.NodesDeleted),
			EdgesInserted: diagnosticInt32(result.Counters.EdgesInserted),
			EdgesDeleted:  diagnosticInt32(result.Counters.EdgesDeleted),
		},
	}
}

func protoNeutralCell(cell querylogical.Cell) *clientv1.QueryValue {
	switch cell.Kind {
	case querylogical.CellNode:
		if cell.Node != nil {
			return &clientv1.QueryValue{Value: &clientv1.QueryValue_Node{Node: protoNeutralNode(*cell.Node)}}
		}
	case querylogical.CellEdge:
		if cell.Edge != nil {
			return &clientv1.QueryValue{Value: &clientv1.QueryValue_Edge{Edge: protoNeutralEdge(*cell.Edge)}}
		}
	case querylogical.CellPath:
		if cell.Path != nil {
			return &clientv1.QueryValue{Value: &clientv1.QueryValue_Path{Path: protoNeutralPath(*cell.Path)}}
		}
	case querylogical.CellTree:
		if cell.Tree != nil {
			return &clientv1.QueryValue{Value: &clientv1.QueryValue_Tree{Tree: protoNeutralTree(*cell.Tree)}}
		}
	case querylogical.CellScalar:
		if cell.Scalar != nil {
			return &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: protoNeutralValue(*cell.Scalar)}}
		}
	}
	return &clientv1.QueryValue{Value: &clientv1.QueryValue_Scalar{Scalar: structpb.NewNullValue()}}
}

func protoNeutralNode(node querylogical.Node) *clientv1.Node {
	return &clientv1.Node{
		NodeId: node.ID, DomainId: node.DomainID, Labels: append([]string(nil), node.Labels...),
		Properties: protoNeutralStruct(node.Properties), Payload: protoNeutralStruct(node.Payload),
		CreateTime: timestamppb.New(node.CreatedAt), UpdateTime: timestamppb.New(node.UpdatedAt),
	}
}

func protoNeutralEdge(edge querylogical.Edge) *clientv1.Edge {
	return &clientv1.Edge{
		EdgeId: edge.ID, DomainId: edge.DomainID, FromNodeId: edge.FromNodeID, ToNodeId: edge.ToNodeID,
		Labels: append([]string(nil), edge.Labels...), Properties: protoNeutralStruct(edge.Properties), Payload: protoNeutralStruct(edge.Payload),
		CreateTime: timestamppb.New(edge.CreatedAt), UpdateTime: timestamppb.New(edge.UpdatedAt),
	}
}

func protoNeutralPath(path querylogical.Path) *clientv1.PathValue {
	out := &clientv1.PathValue{Nodes: make([]*clientv1.Node, 0, len(path.Nodes)), Edges: make([]*clientv1.Edge, 0, len(path.Edges))}
	for _, node := range path.Nodes {
		out.Nodes = append(out.Nodes, protoNeutralNode(node))
	}
	for _, edge := range path.Edges {
		out.Edges = append(out.Edges, protoNeutralEdge(edge))
	}
	return out
}

func protoNeutralTree(tree querylogical.Tree) *clientv1.Tree {
	out := &clientv1.Tree{Roots: make([]*clientv1.TreeNode, 0, len(tree.Roots))}
	for _, root := range tree.Roots {
		out.Roots = append(out.Roots, protoNeutralTreeNode(root))
	}
	return out
}

func protoNeutralTreeNode(node querylogical.TreeNode) *clientv1.TreeNode {
	out := &clientv1.TreeNode{Node: protoNeutralNode(node.Node), Children: make([]*clientv1.TreeNode, 0, len(node.Children))}
	for _, child := range node.Children {
		out.Children = append(out.Children, protoNeutralTreeNode(child))
	}
	return out
}

func protoNeutralStruct(values map[string]querylogical.Value) *structpb.Struct {
	native := make(map[string]any, len(values))
	for name, value := range values {
		native[name] = nativeNeutralValue(value)
	}
	out, err := structpb.NewStruct(native)
	if err != nil {
		return &structpb.Struct{}
	}
	return out
}

func protoNeutralValue(value querylogical.Value) *structpb.Value {
	out, err := structpb.NewValue(nativeNeutralValue(value))
	if err != nil {
		return structpb.NewNullValue()
	}
	return out
}

func nativeNeutralValue(value querylogical.Value) any {
	switch value.Kind {
	case querylogical.ValueNull:
		return nil
	case querylogical.ValueBool:
		return value.Bool
	case querylogical.ValueNumber:
		return value.Number
	case querylogical.ValueInteger:
		return value.Integer
	case querylogical.ValueUnsigned:
		return value.Unsigned
	case querylogical.ValueString, querylogical.ValueDate, querylogical.ValueUnknown:
		return value.String
	case querylogical.ValueList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, nativeNeutralValue(item))
		}
		return out
	case querylogical.ValueObject:
		out := make(map[string]any, len(value.Object))
		for name, item := range value.Object {
			out[name] = nativeNeutralValue(item)
		}
		return out
	default:
		return nil
	}
}

func preserveQueryShapingError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.InvalidArgument, err.Error())
}

func mapQueryEngineError(err error) error {
	if err == nil {
		return nil
	}
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) {
		return status.Error(codes.Internal, err.Error())
	}
	switch queryErr.Code {
	case querylogical.ErrorInvalidQuery, querylogical.ErrorStaleCursor:
		return status.Error(codes.InvalidArgument, queryErr.Error())
	case querylogical.ErrorResourceExhausted:
		return status.Error(codes.ResourceExhausted, queryErr.Error())
	case querylogical.ErrorUnavailableAccessPath:
		return status.Error(codes.FailedPrecondition, queryErr.Error())
	case querylogical.ErrorUnsupportedPlan:
		return status.Error(codes.Unimplemented, queryErr.Error())
	case querylogical.ErrorCanceled:
		if errors.Is(queryErr, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, queryErr.Error())
		}
		return status.Error(codes.Canceled, queryErr.Error())
	case querylogical.ErrorDependencyFailure:
		return status.Error(codes.Unavailable, queryErr.Error())
	default:
		return status.Error(codes.Internal, queryErr.Error())
	}
}
