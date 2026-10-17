package execution_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

func TestPagedNodeSourceResumesInsideFinitePageAndReleasesMemory(t *testing.T) {
	nodes := []graph.Node{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001")},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002")},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003")},
	}
	reader := func(_ context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
		start := 0
		if cursor != "" {
			if _, err := fmt.Sscan(cursor, &start); err != nil {
				return queryexecution.NodePage{}, err
			}
		}
		end := start + limit
		if end > len(nodes) {
			end = len(nodes)
		}
		next := ""
		if end < len(nodes) {
			next = fmt.Sprint(end)
		}
		return queryexecution.NodePage{Nodes: nodes[start:end], NextCursor: next, IndexEntries: int64(end - start), NodesLoaded: int64(end - start)}, nil
	}
	budget := queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 2}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	source := queryexecution.NewPagedNodeSource("n", reader, func(cursor string, offset int) string { return fmt.Sprintf("%s:%d", cursor, offset) }, "", 1, true, 2, tracker)
	if err := source.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch, err := source.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Rows) != 2 || batch.Rows[0].Nodes["n"].ID != nodes[1].ID || batch.Rows[0].Cursor != ":1" || batch.Rows[1].Nodes["n"].ID != nodes[2].ID || batch.Rows[1].Cursor != "2:0" {
		t.Fatalf("batch = %#v", batch)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	usage := tracker.Snapshot()
	if usage.IndexEntries != 3 || usage.NodesLoaded != 3 || usage.CandidatesExamined != 2 || usage.RetainedMemory != 0 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestPagedNodeSourceFailsClosedOnEmptyNonTerminalPage(t *testing.T) {
	budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	source := queryexecution.NewPagedNodeSource("n", func(context.Context, int, string) (queryexecution.NodePage, error) {
		return queryexecution.NodePage{NextCursor: "again"}, nil
	}, nil, "", 0, false, 2, tracker)
	if err := source.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = source.Next(context.Background())
	assertQueryErrorCode(t, err, querylogical.ErrorDependencyFailure)
	if tracker.Snapshot().RetainedMemory != 0 {
		t.Fatalf("retained memory = %d", tracker.Snapshot().RetainedMemory)
	}
}

func TestPagedNodeSourceRejectsResumeOffsetOutsideActualPage(t *testing.T) {
	for _, test := range []struct {
		name   string
		nodes  []graph.Node
		offset int
	}{
		{name: "empty terminal page at offset zero", offset: 0},
		{name: "short terminal page", nodes: []graph.Node{{ID: uuid.New()}}, offset: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 2}, queryexecution.Budget{})
			tracker, err := queryexecution.NewTracker(budget)
			if err != nil {
				t.Fatal(err)
			}
			source := queryexecution.NewPagedNodeSource("n", func(context.Context, int, string) (queryexecution.NodePage, error) {
				return queryexecution.NodePage{Nodes: test.nodes}, nil
			}, nil, "terminal", test.offset, true, 2, tracker)
			if err := source.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = source.Next(context.Background())
			assertQueryErrorCode(t, err, querylogical.ErrorStaleCursor)
		})
	}
}

func TestPagedNodeSourcePreservesCancellationFromAccessCall(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
			tracker, err := queryexecution.NewTracker(budget)
			if err != nil {
				t.Fatal(err)
			}
			source := queryexecution.NewPagedNodeSource("n", func(context.Context, int, string) (queryexecution.NodePage, error) {
				return queryexecution.NodePage{}, cause
			}, nil, "", 0, false, 2, tracker)
			if err := source.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = source.Next(context.Background())
			assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
			if !errors.Is(err, cause) {
				t.Fatalf("error = %v, want cause %v", err, cause)
			}
		})
	}
}

func TestPagedNodeSourceCancellationWinsOverSuccessfulDependencyReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	source := queryexecution.NewPagedNodeSource("n", func(context.Context, int, string) (queryexecution.NodePage, error) {
		cancel()
		return queryexecution.NodePage{}, nil
	}, nil, "", 0, false, 2, tracker)
	if err := source.Open(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = source.Next(ctx)
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}

func TestTraversalOperatorsCancellationWinsAndReleasesMemory(t *testing.T) {
	start := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Labels: []string{"Document"}}
	step := querylogical.Step{
		Edge:  querylogical.EdgePattern{Alias: "e", Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing},
		Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
	}

	t.Run("adjacency", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tracked := tracker(t, queryexecution.Budget{BatchSize: 1})
		source := queryexecution.NewAdjacencySource(
			"n", querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}}, step, []string{start.ID.String()},
			func(context.Context, string) (graph.Node, error) { return start, nil },
			func(context.Context, string, int, string) (queryexecution.TraversalEdgePage, error) {
				cancel()
				return queryexecution.TraversalEdgePage{}, nil
			}, nil, 0, "", 0, false, 1, nil, time.Time{}, tracked,
		)
		if err := source.Open(ctx); err != nil {
			t.Fatal(err)
		}
		_, err := source.Next(ctx)
		assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
		if tracked.Snapshot().RetainedMemory != 0 {
			t.Fatalf("retained memory = %d", tracked.Snapshot().RetainedMemory)
		}
	})

	t.Run("path", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tracked := tracker(t, queryexecution.Budget{BatchSize: 1})
		input := queryexecution.NewLegacySource([]queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": start}}}, 1, tracked)
		source := queryexecution.NewPathExpand(input, "n", step, "path", func(context.Context, string, int, string) (queryexecution.TraversalEdgePage, error) {
			cancel()
			return queryexecution.TraversalEdgePage{}, nil
		}, func(context.Context, string) (graph.Node, error) { return graph.Node{}, nil }, 1, nil, time.Time{}, tracked)
		if err := source.Open(ctx); err != nil {
			t.Fatal(err)
		}
		_, err := source.Next(ctx)
		assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
		if tracked.Snapshot().RetainedMemory != 0 {
			t.Fatalf("retained memory = %d", tracked.Snapshot().RetainedMemory)
		}
	})

	t.Run("subtree", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tracked := tracker(t, queryexecution.Budget{BatchSize: 1})
		input := queryexecution.NewLegacySource([]queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": start}}}, 1, tracked)
		source := queryexecution.NewSubtreeExpand(input, "n", step, func(context.Context, string, int, string) (queryexecution.TraversalEdgePage, error) {
			cancel()
			return queryexecution.TraversalEdgePage{}, nil
		}, func(context.Context, string) (graph.Node, error) { return graph.Node{}, nil }, 1, nil, time.Time{}, tracked)
		if err := source.Open(ctx); err != nil {
			t.Fatal(err)
		}
		_, err := source.Next(ctx)
		assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
		if tracked.Snapshot().RetainedMemory != 0 {
			t.Fatalf("retained memory = %d", tracked.Snapshot().RetainedMemory)
		}
	})
}

func TestPagedNodeSourceClampsTraversalStartReadsBeforeDependencyWork(t *testing.T) {
	node := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	for _, test := range []struct {
		name     string
		truncate bool
	}{
		{name: "path fails closed"},
		{name: "subtree truncates at root boundary", truncate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracked := tracker(t, queryexecution.Budget{BatchSize: 10, MaxNodesLoaded: 1, MaxStartNodes: 1})
			var limits []int
			source := queryexecution.NewPagedNodeSource("n", func(_ context.Context, limit int, cursor string) (queryexecution.NodePage, error) {
				limits = append(limits, limit)
				return queryexecution.NodePage{Nodes: []graph.Node{node}, NextCursor: "more", IndexEntries: 1, NodesLoaded: 1}, nil
			}, nil, "", 0, false, 1, tracked).WithStartNodeBudget(test.truncate)
			if err := source.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			batch, err := source.Next(context.Background())
			if err != nil || len(batch.Rows) != 1 {
				t.Fatalf("first batch = %#v, %v", batch, err)
			}
			_, err = source.Next(context.Background())
			if test.truncate {
				if !errors.Is(err, io.EOF) || !tracked.Snapshot().Truncated {
					t.Fatalf("terminal error/usage = %v, %#v", err, tracked.Snapshot())
				}
			} else {
				assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
			}
			if !reflect.DeepEqual(limits, []int{1}) {
				t.Fatalf("dependency limits = %v, want [1]", limits)
			}
			usage := tracked.Snapshot()
			if usage.NodesLoaded != 1 || usage.StartNodes != 1 {
				t.Fatalf("usage = %#v", usage)
			}
		})
	}
}

func TestSubtreeTreeHierarchyMatchesLegacyContainmentRules(t *testing.T) {
	root := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Labels: []string{"Document"}}
	child := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Labels: []string{"Document"}}
	grandchild := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Labels: []string{"Document"}}
	nodes := map[string]graph.Node{child.ID.String(): child, grandchild.ID.String(): grandchild}
	for _, test := range []struct {
		name      string
		label     string
		direction querylogical.Direction
	}{
		{name: "outgoing non-containment", label: "LINKS", direction: querylogical.DirectionOutgoing},
		{name: "incoming containment", label: "contains", direction: querylogical.DirectionIncoming},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracked := tracker(t, queryexecution.Budget{BatchSize: 1})
			step := querylogical.Step{
				Edge:  querylogical.EdgePattern{Labels: []string{test.label}, Direction: test.direction},
				Depth: querylogical.DepthBounds{Min: 1, Max: 2}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
			}
			edgeFor := func(from, to graph.Node) graph.Edge {
				if test.direction == querylogical.DirectionIncoming {
					from, to = to, from
				}
				return graph.Edge{ID: uuid.New(), FromID: from.ID, ToID: to.ID, Labels: []string{test.label}, Properties: map[string]any{"order": 1}}
			}
			edges := map[string]graph.Edge{root.ID.String(): edgeFor(root, child), child.ID.String(): edgeFor(child, grandchild)}
			input := queryexecution.NewLegacySource([]queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": root}}}, 1, tracked)
			source := queryexecution.NewSubtreeExpand(input, "n", step, func(_ context.Context, nodeID string, _ int, _ string) (queryexecution.TraversalEdgePage, error) {
				edge, ok := edges[nodeID]
				if !ok {
					return queryexecution.TraversalEdgePage{}, nil
				}
				return queryexecution.TraversalEdgePage{Edges: []graph.Edge{edge}, IndexEntries: 1, EdgesLoaded: 1}, nil
			}, func(_ context.Context, nodeID string) (graph.Node, error) { return nodes[nodeID], nil }, 1, nil, time.Time{}, tracked)
			if err := source.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			batch, err := source.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			tree := batch.Rows[0].Trees["m"]
			if len(tree.Roots) != 2 || len(tree.Roots[0].Children) != 0 || len(tree.Roots[1].Children) != 0 {
				t.Fatalf("tree = %#v, want two flat roots", tree)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if tracked.Snapshot().RetainedMemory != 0 {
				t.Fatalf("retained memory = %d", tracked.Snapshot().RetainedMemory)
			}
		})
	}
}

func TestPathAndSubtreeAccountAdjacencyPageMemory(t *testing.T) {
	root := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Labels: []string{"Document"}}
	child := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Labels: []string{"Document"}}
	edge := graph.Edge{
		ID: uuid.New(), FromID: root.ID, ToID: child.ID, Labels: []string{"LINKS"},
		Properties: map[string]any{"kind": "drop", "payload": string(make([]byte, 32<<10))},
	}
	step := querylogical.Step{
		Edge:  querylogical.EdgePattern{Labels: []string{"LINKS"}, Properties: map[string]querylogical.Value{"kind": querylogical.NativeValue("keep")}, Direction: querylogical.DirectionOutgoing},
		Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
	}
	for _, test := range []struct {
		name string
		new  func(queryexecution.Operator, *queryexecution.Tracker) queryexecution.Operator
	}{
		{name: "path", new: func(input queryexecution.Operator, tracked *queryexecution.Tracker) queryexecution.Operator {
			return queryexecution.NewPathExpand(input, "n", step, "path", func(context.Context, string, int, string) (queryexecution.TraversalEdgePage, error) {
				return queryexecution.TraversalEdgePage{Edges: []graph.Edge{edge}, IndexEntries: 1, EdgesLoaded: 1}, nil
			}, func(context.Context, string) (graph.Node, error) {
				t.Fatal("filtered endpoint was hydrated")
				return graph.Node{}, nil
			}, 1, nil, time.Time{}, tracked)
		}},
		{name: "subtree", new: func(input queryexecution.Operator, tracked *queryexecution.Tracker) queryexecution.Operator {
			return queryexecution.NewSubtreeExpand(input, "n", step, func(context.Context, string, int, string) (queryexecution.TraversalEdgePage, error) {
				return queryexecution.TraversalEdgePage{Edges: []graph.Edge{edge}, IndexEntries: 1, EdgesLoaded: 1}, nil
			}, func(context.Context, string) (graph.Node, error) {
				t.Fatal("filtered endpoint was hydrated")
				return graph.Node{}, nil
			}, 1, nil, time.Time{}, tracked)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracked := tracker(t, queryexecution.Budget{BatchSize: 1, MaxMemoryBytes: 4 << 10})
			input := queryexecution.NewLegacySource([]queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": root}}}, 1, tracked)
			source := test.new(input, tracked)
			if err := source.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := source.Next(context.Background())
			assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
			if usage := tracked.Snapshot(); usage.RetainedMemory != 0 {
				t.Fatalf("retained memory after failure = %d", usage.RetainedMemory)
			}
		})
	}
}

func TestNodeSetOperatorsBoundIntersectionAndUnionMemory(t *testing.T) {
	rows := []queryexecution.BindingRow{
		bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", 1),
		bindingRow("00000000-0000-0000-0000-000000000002", "b", "x", 2),
		bindingRow("00000000-0000-0000-0000-000000000003", "c", "x", 3),
	}
	budget := queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 1}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	intersection := queryexecution.NewNodeSet([]queryexecution.Operator{
		queryexecution.NewLegacySource(rows[:2], 1, tracker),
		queryexecution.NewLegacySource(rows[1:], 1, tracker),
	}, queryexecution.NodeSetIntersection, 1, tracker)
	union := queryexecution.NewNodeSet([]queryexecution.Operator{
		intersection,
		queryexecution.NewLegacySource(rows[2:], 1, tracker),
	}, queryexecution.NodeSetUnion, 1, tracker)
	if err := union.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		batch, err := union.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, batch.Rows[0].Nodes["n"].Properties["name"].(string))
	}
	if err := union.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"b", "c"}) || tracker.Snapshot().RetainedMemory != 0 {
		t.Fatalf("rows=%v usage=%#v", got, tracker.Snapshot())
	}
}

func TestShapeRunsDeterministicBoundedPipeline(t *testing.T) {
	rows := []queryexecution.BindingRow{
		bindingRow("00000000-0000-0000-0000-000000000001", "alpha", "x", 1),
		bindingRow("00000000-0000-0000-0000-000000000002", "beta", "x", 4),
		bindingRow("00000000-0000-0000-0000-000000000003", "gamma", "y", 3),
		bindingRow("00000000-0000-0000-0000-000000000004", "delta", "y", 2),
	}
	score := fieldValue("n", "score")
	minimum := querylogical.NativeValue(2)
	query := querylogical.Query{
		Predicate: &querylogical.Predicate{Op: querylogical.PredicateLeafOp, Leaf: &querylogical.Leaf{Kind: querylogical.LeafComparison, Operator: ">=", Left: &score, Right: &minimum}},
		Returns:   []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "name"), OutputName: "name"}},
		OrderBy:   []querylogical.Order{{Value: score, Direction: querylogical.SortDescending}},
		Page:      querylogical.PageSpec{Offset: 1, Limit: 2},
	}

	first := shape(t, rows, query, queryexecution.Budget{BatchSize: 1})
	second := shape(t, rows, query, queryexecution.Budget{BatchSize: 2})
	if !reflect.DeepEqual(first.Rows, second.Rows) {
		t.Fatalf("results differ by batch size: %#v != %#v", first.Rows, second.Rows)
	}
	if got := scalarString(first.Rows[0], "name"); got != "gamma" {
		t.Fatalf("first name = %q, want gamma", got)
	}
	if got := scalarString(first.Rows[1], "name"); got != "delta" {
		t.Fatalf("second name = %q, want delta", got)
	}
	if first.Counters.RowsReturned != 2 || first.Diagnostics.RowsReturned != 2 || first.Diagnostics.CandidateCount != 4 {
		t.Fatalf("result counters/diagnostics = %#v / %#v", first.Counters, first.Diagnostics)
	}
	if first.Diagnostics.RetainedMemoryBytes != 0 || first.Diagnostics.PeakMemoryBytes == 0 {
		t.Fatalf("memory diagnostics = %#v", first.Diagnostics)
	}
}

func TestShapePreservesLegacyBareAndSyntheticEntityFields(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	edgeID := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	fromID := uuid.MustParse("00000000-0000-0000-0000-000000000011")
	toID := uuid.MustParse("00000000-0000-0000-0000-000000000012")
	row := queryexecution.BindingRow{
		Nodes: map[string]graph.Node{"n": {ID: nodeID, Payload: map[string]any{"text": "payload"}}},
		Edges: map[string]graph.Edge{"e": {ID: edgeID, FromID: fromID, ToID: toID}},
	}
	projections := []querylogical.Projection{
		{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", ""), OutputName: "bare"},
		{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "node_id"), OutputName: "node_id"},
		{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "content"), OutputName: "content"},
		{Kind: querylogical.ProjectionScalar, Alias: "e", Field: fieldRef("e", "edge_id"), OutputName: "edge_id"},
		{Kind: querylogical.ProjectionScalar, Alias: "e", Field: fieldRef("e", "from_node_id"), OutputName: "from_node_id"},
		{Kind: querylogical.ProjectionScalar, Alias: "e", Field: fieldRef("e", "to_node_id"), OutputName: "to_node_id"},
	}
	result := shape(t, []queryexecution.BindingRow{row}, querylogical.Query{Returns: projections}, queryexecution.Budget{})
	want := map[string]string{
		"bare":         nodeID.String(),
		"node_id":      nodeID.String(),
		"content":      "payload",
		"edge_id":      edgeID.String(),
		"from_node_id": fromID.String(),
		"to_node_id":   toID.String(),
	}
	for name, expected := range want {
		if got := scalarString(result.Rows[0], name); got != expected {
			t.Fatalf("%s = %q, want %q", name, got, expected)
		}
	}
}

func TestShapePreservesAggregateArgumentCompatibility(t *testing.T) {
	rows := []queryexecution.BindingRow{{Paths: map[string]querylogical.Path{"p": {}}}}
	argument := querylogical.BindingValue("p")
	count := shape(t, rows, querylogical.Query{Aggregates: []querylogical.Aggregate{{Function: querylogical.AggregateCount, Argument: &argument, OutputName: "count"}}}, queryexecution.Budget{})
	if got := count.Rows[0].Fields["count"].Scalar.Number; got != 1 {
		t.Fatalf("COUNT(path) = %v, want 1", got)
	}

	for _, function := range []querylogical.AggregateFunction{querylogical.AggregateSum, querylogical.AggregateAvg, querylogical.AggregateMin, querylogical.AggregateMax} {
		budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
		tracker, err := queryexecution.NewTracker(budget)
		if err != nil {
			t.Fatal(err)
		}
		_, err = queryexecution.Shape(context.Background(), queryexecution.NewLegacySource(rows, budget.BatchSize, tracker), querylogical.Query{Aggregates: []querylogical.Aggregate{{Function: function, Star: true}}}, tracker, queryexecution.ShapeOptions{})
		assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
	}
}

func TestShapeStopsStreamingSourceAfterBoundedPage(t *testing.T) {
	rows := make([]queryexecution.BindingRow, 1000)
	for index := range rows {
		rows[index] = bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", index)
	}
	budget := queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 8}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	result, err := queryexecution.Shape(context.Background(), queryexecution.NewLegacySource(rows, budget.BatchSize, tracker), querylogical.Query{Returns: nodeProjection(), Page: querylogical.PageSpec{Size: 2}}, tracker, queryexecution.ShapeOptions{})
	if err != nil {
		t.Fatalf("Shape: %v", err)
	}
	if len(result.Rows) != 2 || result.NextCursor != "2" {
		t.Fatalf("page = %#v, cursor %q", result.Rows, result.NextCursor)
	}
	if candidates := tracker.Snapshot().CandidatesExamined; candidates != 8 {
		t.Fatalf("candidates = %d, want one bounded source batch", candidates)
	}
}

func TestShapeAppliesLegacyOffsetPaginationAfterQueryLimit(t *testing.T) {
	rows := []queryexecution.BindingRow{
		bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", 1),
		bindingRow("00000000-0000-0000-0000-000000000002", "b", "x", 2),
		bindingRow("00000000-0000-0000-0000-000000000003", "c", "x", 3),
	}
	query := querylogical.Query{
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "name"), OutputName: "name"}},
		Page:    querylogical.PageSpec{Limit: 3, Size: 1},
	}
	first := shape(t, rows, query, queryexecution.Budget{})
	if len(first.Rows) != 1 || scalarString(first.Rows[0], "name") != "a" || first.NextCursor != "1" {
		t.Fatalf("first page = %#v, cursor %q", first.Rows, first.NextCursor)
	}
	query.Page.Cursor = first.NextCursor
	second := shape(t, rows, query, queryexecution.Budget{})
	if len(second.Rows) != 1 || scalarString(second.Rows[0], "name") != "b" || second.NextCursor != "2" {
		t.Fatalf("second page = %#v, cursor %q", second.Rows, second.NextCursor)
	}
	query.Page.Cursor = "invalid"
	budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	_, err = queryexecution.Shape(context.Background(), queryexecution.NewLegacySource(rows, budget.BatchSize, tracker), query, tracker, queryexecution.ShapeOptions{})
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
}

func TestShapeOrdersLargeIntegersWithoutPrecisionLoss(t *testing.T) {
	left := bindingRow("00000000-0000-0000-0000-000000000001", "lower", "x", 1)
	right := bindingRow("00000000-0000-0000-0000-000000000002", "higher", "x", 1)
	left.Nodes["n"] = withProperty(left.Nodes["n"], "score", uint64(9_007_199_254_740_992))
	right.Nodes["n"] = withProperty(right.Nodes["n"], "score", uint64(9_007_199_254_740_993))
	query := querylogical.Query{
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "name"), OutputName: "name"}},
		OrderBy: []querylogical.Order{{Value: fieldValue("n", "score"), Direction: querylogical.SortDescending}},
	}
	result := shape(t, []queryexecution.BindingRow{left, right}, query, queryexecution.Budget{})
	if got := scalarString(result.Rows[0], "name"); got != "higher" {
		t.Fatalf("first name = %q, want higher", got)
	}
}

func TestShapeResidualPredicatesUseTypedFieldsParametersAndDates(t *testing.T) {
	row := bindingRow("00000000-0000-0000-0000-000000000001", "Alpha journal", "x", 3)
	node := row.Nodes["n"]
	node.Properties["optional"] = nil
	node.Properties["tags"] = []string{"urgent", "journal"}
	node.Properties["day"] = "2026-10-17"
	row.Nodes["n"] = node

	score := fieldValue("n", "score")
	low := querylogical.Value{Kind: querylogical.ValueParameter, Parameter: "low"}
	high := querylogical.NativeValue(4)
	name := fieldValue("n", "name")
	day := fieldValue("n", "day")
	current := querylogical.Value{Kind: querylogical.ValueCurrentDate}
	optional := fieldRef("n", "optional")
	tagField := fieldRef("n", "")
	predicate := querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafBetween, Left: &score, Low: &low, High: &high}),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafString, Operand: &name, Operator: "starts_with", Query: "alpha"}),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafNull, Field: optional, IsNull: true}),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafHasTag, Field: tagField, Tag: "Urgent"}),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &day, Right: &current, Operator: "="}),
	})
	budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	result, err := queryexecution.Shape(context.Background(), queryexecution.NewLegacySource([]queryexecution.BindingRow{row}, budget.BatchSize, tracker), querylogical.Query{Predicate: predicate, Returns: nodeProjection()}, tracker, queryexecution.ShapeOptions{
		Parameters: map[string]querylogical.Value{"low": querylogical.NativeValue(2)},
		Now:        time.Date(2026, 10, 17, 23, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Shape: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v", result.Rows)
	}
}

func TestShapeDistinctPreservesFirstOccurrence(t *testing.T) {
	rows := []queryexecution.BindingRow{
		bindingRow("00000000-0000-0000-0000-000000000001", "same", "x", 1),
		bindingRow("00000000-0000-0000-0000-000000000002", "same", "x", 2),
		bindingRow("00000000-0000-0000-0000-000000000003", "other", "x", 3),
	}
	query := querylogical.Query{
		Returns:  []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "name"), OutputName: "name"}},
		Distinct: true,
	}
	result := shape(t, rows, query, queryexecution.Budget{})
	if len(result.Rows) != 2 || scalarString(result.Rows[0], "name") != "same" || scalarString(result.Rows[1], "name") != "other" {
		t.Fatalf("distinct rows = %#v", result.Rows)
	}
}

func TestShapeAggregationAndGrouping(t *testing.T) {
	rows := []queryexecution.BindingRow{
		bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", 2),
		bindingRow("00000000-0000-0000-0000-000000000002", "b", "y", 7),
		bindingRow("00000000-0000-0000-0000-000000000003", "c", "x", 4),
	}
	score := fieldValue("n", "score")
	query := querylogical.Query{
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: fieldRef("n", "category"), OutputName: "category"}},
		Aggregates: []querylogical.Aggregate{
			{Function: querylogical.AggregateCount, Star: true, OutputName: "count"},
			{Function: querylogical.AggregateSum, Argument: &score, OutputName: "sum"},
			{Function: querylogical.AggregateAvg, Argument: &score, OutputName: "avg"},
			{Function: querylogical.AggregateMin, Argument: &score, OutputName: "min"},
			{Function: querylogical.AggregateMax, Argument: &score, OutputName: "max"},
		},
	}
	result := shape(t, rows, query, queryexecution.Budget{})
	if len(result.Rows) != 2 {
		t.Fatalf("group rows = %#v", result.Rows)
	}
	assertNumber(t, result.Rows[0], "count", 2)
	assertNumber(t, result.Rows[0], "sum", 6)
	assertNumber(t, result.Rows[0], "avg", 3)
	assertNumber(t, result.Rows[0], "min", 2)
	assertNumber(t, result.Rows[0], "max", 4)
	if scalarString(result.Rows[0], "category") != "x" || scalarString(result.Rows[1], "category") != "y" {
		t.Fatalf("group order = %#v", result.Rows)
	}
}

func TestShapeGlobalCountReturnsRowForEmptyInput(t *testing.T) {
	query := querylogical.Query{Aggregates: []querylogical.Aggregate{{Function: querylogical.AggregateCount, Star: true, OutputName: "count"}}}
	result := shape(t, []queryexecution.BindingRow{}, query, queryexecution.Budget{})
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v", result.Rows)
	}
	assertNumber(t, result.Rows[0], "count", 0)
}

func TestBlockingOperatorsFailClosedOnMemoryBudget(t *testing.T) {
	rows := []queryexecution.BindingRow{bindingRow("00000000-0000-0000-0000-000000000001", "alpha", "x", 1)}
	tests := []struct {
		name  string
		query querylogical.Query
	}{
		{name: "sort", query: querylogical.Query{Returns: nodeProjection(), OrderBy: []querylogical.Order{{Value: fieldValue("n", "score"), Direction: querylogical.SortAscending}}}},
		{name: "distinct", query: querylogical.Query{Returns: nodeProjection(), Distinct: true}},
		{name: "aggregation", query: querylogical.Query{Aggregates: []querylogical.Aggregate{{Function: querylogical.AggregateCount, Star: true}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := queryexecution.ResolveBudget(queryexecution.Budget{MaxMemoryBytes: 1}, queryexecution.Budget{})
			tracker, err := queryexecution.NewTracker(budget)
			if err != nil {
				t.Fatal(err)
			}
			source := queryexecution.NewLegacySource(rows, budget.BatchSize, tracker)
			_, err = queryexecution.Shape(context.Background(), source, test.query, tracker, queryexecution.ShapeOptions{})
			assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
			if usage := tracker.Snapshot(); usage.RetainedMemory != 0 {
				t.Fatalf("retained memory after failure = %d", usage.RetainedMemory)
			}
		})
	}
}

func TestShapeClosesSourceOnProjectionError(t *testing.T) {
	source := &recordingSource{rows: []queryexecution.BindingRow{{Nodes: map[string]graph.Node{}}}}
	tracker := tracker(t, queryexecution.Budget{})
	query := querylogical.Query{Returns: nodeProjection()}
	_, err := queryexecution.Shape(context.Background(), source, query, tracker, queryexecution.ShapeOptions{})
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
	if source.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", source.closeCalls)
	}
}

func TestShapeMapsOperatorCloseFailure(t *testing.T) {
	source := &recordingSource{
		rows:     []queryexecution.BindingRow{bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", 1)},
		closeErr: errors.New("close failed"),
	}
	_, err := queryexecution.Shape(context.Background(), source, querylogical.Query{Returns: nodeProjection()}, tracker(t, queryexecution.Budget{}), queryexecution.ShapeOptions{})
	assertQueryErrorCode(t, err, querylogical.ErrorDependencyFailure)
	if source.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", source.closeCalls)
	}
}

func TestShapeCancellationClosesBlockingPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &recordingSource{rows: []queryexecution.BindingRow{bindingRow("00000000-0000-0000-0000-000000000001", "a", "x", 1)}, afterNext: cancel}
	query := querylogical.Query{Returns: nodeProjection(), OrderBy: []querylogical.Order{{Value: fieldValue("n", "score"), Direction: querylogical.SortAscending}}}
	_, err := queryexecution.Shape(ctx, source, query, tracker(t, queryexecution.Budget{}), queryexecution.ShapeOptions{})
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	if source.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", source.closeCalls)
	}
}

func TestLegacySourceLifecycleAndBoundedBatches(t *testing.T) {
	budget := queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 2}, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	source := queryexecution.NewLegacySource(make([]queryexecution.BindingRow, 5), budget.BatchSize, tracker)
	if _, err := source.Next(context.Background()); err == nil {
		t.Fatal("Next before Open succeeded")
	}
	if err := source.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index, want := range []int{2, 2, 1} {
		batch, err := source.Next(context.Background())
		if err != nil || len(batch.Rows) != want {
			t.Fatalf("batch %d = %d, %v; want %d", index, len(batch.Rows), err, want)
		}
	}
	if _, err := source.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal error = %v, want EOF", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if usage := tracker.Snapshot(); usage.CandidatesExamined != 5 {
		t.Fatalf("candidates = %d, want 5", usage.CandidatesExamined)
	}
}

func TestTrackerFailsClosedOnAdjacencyAndDepthBudgets(t *testing.T) {
	tracked := tracker(t, queryexecution.Budget{MaxAdjacencyCalls: 1, MaxTraversalDepth: 2})
	if err := tracked.UseAdjacency(1); err != nil {
		t.Fatalf("first adjacency call: %v", err)
	}
	assertQueryErrorCode(t, tracked.UseAdjacency(1), querylogical.ErrorResourceExhausted)
	assertQueryErrorCode(t, tracked.ObserveTraversalDepth(3), querylogical.ErrorResourceExhausted)
}

func TestResolveBudgetUsesFiniteDefaultsAndStaticCaps(t *testing.T) {
	budget := queryexecution.ResolveBudget(queryexecution.Budget{}, queryexecution.Budget{MaxIntermediateRows: 7, MaxTraversalDepth: 3})
	if budget.MaxCandidates <= 0 || budget.MaxAdjacencyCalls <= 0 || budget.MaxMemoryBytes <= 0 || budget.BatchSize <= 0 {
		t.Fatalf("budget is not finite: %#v", budget)
	}
	if budget.MaxIntermediateRows != 7 || budget.MaxTraversalDepth != 3 {
		t.Fatalf("static caps not applied: %#v", budget)
	}
}

func shape(t *testing.T, rows []queryexecution.BindingRow, query querylogical.Query, requested queryexecution.Budget) querylogical.Result {
	t.Helper()
	budget := queryexecution.ResolveBudget(requested, queryexecution.Budget{})
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		t.Fatal(err)
	}
	result, err := queryexecution.Shape(context.Background(), queryexecution.NewLegacySource(rows, budget.BatchSize, tracker), query, tracker, queryexecution.ShapeOptions{Now: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Shape: %v", err)
	}
	return result
}

func tracker(t *testing.T, requested queryexecution.Budget) *queryexecution.Tracker {
	t.Helper()
	out, err := queryexecution.NewTracker(queryexecution.ResolveBudget(requested, queryexecution.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func bindingRow(id string, name string, category string, score int) queryexecution.BindingRow {
	nodeID := uuid.MustParse(id)
	domainID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	return queryexecution.BindingRow{Nodes: map[string]graph.Node{"n": {
		ID: nodeID, DomainID: domainID, Labels: []string{"Item"},
		Properties: map[string]any{"name": name, "category": category, "score": score},
	}}}
}

func withProperty(node graph.Node, name string, value any) graph.Node {
	node.Properties[name] = value
	return node
}

func nodeProjection() []querylogical.Projection {
	return []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "n"}}
}

func fieldRef(alias string, name string) *querylogical.FieldRef {
	return &querylogical.FieldRef{Alias: alias, Namespace: querylogical.NamespaceProperties, Name: name}
}

func fieldValue(alias string, name string) querylogical.Value {
	return querylogical.Value{Kind: querylogical.ValueField, Field: fieldRef(alias, name)}
}

func scalarString(row querylogical.Row, name string) string {
	cell := row.Fields[name]
	if cell.Scalar == nil {
		return ""
	}
	return cell.Scalar.String
}

func assertNumber(t *testing.T, row querylogical.Row, name string, want float64) {
	t.Helper()
	cell := row.Fields[name]
	if cell.Scalar == nil || cell.Scalar.Kind != querylogical.ValueNumber || cell.Scalar.Number != want {
		t.Fatalf("%s = %#v, want %v", name, cell, want)
	}
}

func assertQueryErrorCode(t *testing.T, err error, want querylogical.ErrorCode) {
	t.Helper()
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != want {
		t.Fatalf("error = %v, want code %q", err, want)
	}
}

type recordingSource struct {
	rows       []queryexecution.BindingRow
	afterNext  func()
	opened     bool
	delivered  bool
	closeCalls int
	closeErr   error
}

func (s *recordingSource) Open(context.Context) error {
	s.opened = true
	return nil
}

func (s *recordingSource) Next(context.Context) (queryexecution.Batch, error) {
	if !s.opened {
		return queryexecution.Batch{}, errors.New("not open")
	}
	if s.delivered {
		return queryexecution.Batch{}, io.EOF
	}
	s.delivered = true
	if s.afterNext != nil {
		s.afterNext()
	}
	return queryexecution.Batch{Rows: append([]queryexecution.BindingRow(nil), s.rows...)}, nil
}

func (s *recordingSource) Close() error {
	s.closeCalls++
	return s.closeErr
}
