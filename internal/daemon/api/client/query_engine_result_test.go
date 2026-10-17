package client

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
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

func TestLegacyStructuredShapingMapsBudgetsAndCancellation(t *testing.T) {
	node := domaingraph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), DomainID: uuid.MustParse("10000000-0000-0000-0000-000000000001")}
	rows := make([]*queryRowState, 10_001)
	for index := range rows {
		rows[index] = newQueryRowState("n", node)
	}
	query := &clientv1.GraphQuery{
		Match:    &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}},
		Returns:  []*clientv1.ReturnProjection{{Alias: "n", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
		Distinct: true,
	}
	exec := newQueryExecution(nil, nil)
	_, _, err := exec.shapeAndProjectRows(context.Background(), rows, query, 500, "")
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("budget status = %s, error %v", status.Code(err), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = exec.shapeAndProjectRows(ctx, rows[:1], query, 500, "")
	if status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation status = %s, error %v", status.Code(err), err)
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
}
