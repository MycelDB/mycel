package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProtoNeutralQueryResultMapsEveryCellFamilyAndCounters(t *testing.T) {
	timestamp := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	node := querylogical.Node{ID: "node-1", DomainID: "domain-1", Labels: []string{"Note"}, Properties: map[string]querylogical.Value{"title": querylogical.NativeValue("hello")}, Payload: map[string]querylogical.Value{"rank": querylogical.NativeValue(7)}, CreatedAt: timestamp, UpdatedAt: timestamp}
	edge := querylogical.Edge{ID: "edge-1", DomainID: "domain-1", FromNodeID: "node-1", ToNodeID: "node-2", Labels: []string{"LINKS"}, CreatedAt: timestamp, UpdatedAt: timestamp}
	scalar := querylogical.Value{Kind: querylogical.ValueObject, Object: map[string]querylogical.Value{"items": {Kind: querylogical.ValueList, List: []querylogical.Value{querylogical.NativeValue(true), querylogical.NativeValue("value")}}}}
	result := querylogical.Result{
		Rows: []querylogical.Row{{Fields: map[string]querylogical.Cell{
			"node":   {Kind: querylogical.CellNode, Node: &node},
			"edge":   {Kind: querylogical.CellEdge, Edge: &edge},
			"path":   {Kind: querylogical.CellPath, Path: &querylogical.Path{Nodes: []querylogical.Node{node}, Edges: []querylogical.Edge{edge}}},
			"tree":   {Kind: querylogical.CellTree, Tree: &querylogical.Tree{Roots: []querylogical.TreeNode{{Node: node}}}},
			"scalar": {Kind: querylogical.CellScalar, Scalar: &scalar},
			"null":   {Kind: querylogical.CellNull},
		}}},
		Graph:      querylogical.ResultGraph{Nodes: []querylogical.Node{node}, Edges: []querylogical.Edge{edge}},
		Counters:   querylogical.Counters{RowsReturned: 1, NodesInserted: 2, NodesUpdated: 3, NodesDeleted: 4, EdgesInserted: 5, EdgesDeleted: 6},
		NextCursor: "cursor-1",
	}

	mapped := protoNeutralQueryResult(result)
	if len(mapped.GetRows()) != 1 || mapped.GetRows()[0].GetFields()["node"].GetNode().GetNodeId() != "node-1" {
		t.Fatalf("mapped rows = %#v", mapped.GetRows())
	}
	fields := mapped.GetRows()[0].GetFields()
	if fields["edge"].GetEdge().GetEdgeId() != "edge-1" || len(fields["path"].GetPath().GetNodes()) != 1 || len(fields["tree"].GetTree().GetRoots()) != 1 {
		t.Fatalf("mapped entity cells = %#v", fields)
	}
	if fields["scalar"].GetScalar().GetStructValue().AsMap()["items"] == nil || fields["null"].GetScalar().GetNullValue() != 0 {
		t.Fatalf("mapped scalar cells = %#v", fields)
	}
	if mapped.GetNextPageToken() != "cursor-1" || len(mapped.GetGraph().GetNodes()) != 1 || mapped.GetCounters().GetEdgesDeleted() != 6 {
		t.Fatalf("mapped result = %#v", mapped)
	}
}

func TestMapQueryEngineErrorUsesStableGRPCCategories(t *testing.T) {
	tests := []struct {
		code querylogical.ErrorCode
		want codes.Code
	}{
		{querylogical.ErrorInvalidQuery, codes.InvalidArgument},
		{querylogical.ErrorStaleCursor, codes.InvalidArgument},
		{querylogical.ErrorResourceExhausted, codes.ResourceExhausted},
		{querylogical.ErrorUnavailableAccessPath, codes.FailedPrecondition},
		{querylogical.ErrorUnsupportedPlan, codes.Unimplemented},
		{querylogical.ErrorCanceled, codes.Canceled},
		{querylogical.ErrorDependencyFailure, codes.Unavailable},
		{querylogical.ErrorInternal, codes.Internal},
	}
	for _, test := range tests {
		t.Run(string(test.code), func(t *testing.T) {
			err := querylogical.NewError(test.code, "execute query", "failed", nil)
			if got := status.Code(mapQueryEngineError(err)); got != test.want {
				t.Fatalf("status = %s, want %s", got, test.want)
			}
		})
	}
	deadline := querylogical.NewError(querylogical.ErrorCanceled, "execute query", "deadline", context.DeadlineExceeded)
	if got := status.Code(mapQueryEngineError(deadline)); got != codes.DeadlineExceeded {
		t.Fatalf("deadline status = %s", got)
	}
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		dependency := querylogical.NewError(querylogical.ErrorDependencyFailure, "scan index", "dependency canceled", fmt.Errorf("wrapped dependency: %w", status.Error(code, "injected")))
		if got := status.Code(mapQueryEngineError(dependency)); got != code {
			t.Fatalf("status-coded dependency = %s, want %s", got, code)
		}
	}
}
