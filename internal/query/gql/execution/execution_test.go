package execution

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

func TestExecutorExecutesInsertNodePlan(t *testing.T) {
	graph := &fakeGraphWriter{nextID: "node-1"}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadWrite,
		Operations: []planmodel.Operation{
			planmodel.InsertNodeOperation{
				Labels: []string{"Person"},
				Properties: map[string]any{
					"name": "Alice",
					"age":  int64(42),
				},
			},
		},
	}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Counters.NodesInserted != 1 {
		t.Fatalf("NodesInserted = %d, want 1", result.Counters.NodesInserted)
	}

	want := []InsertNode{{
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
			"age":  int64(42),
		},
	}}
	if !reflect.DeepEqual(graph.inserted, want) {
		t.Fatalf("inserted = %#v, want %#v", graph.inserted, want)
	}
}

func TestExecutorExecutesQueryNodesPlan(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{{ID: "node-1", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}}}}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}},
		},
	}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(graph.queried, []QueryNodes{{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}}}) {
		t.Fatalf("queried = %#v", graph.queried)
	}
	if len(result.Rows) != 1 || result.Rows[0]["p"].Node == nil || result.Rows[0]["p"].Node.ID != "node-1" {
		t.Fatalf("unexpected result rows: %#v", result.Rows)
	}
}

func TestExecutorQueryNodesFirstAndLastNameScenarios(t *testing.T) {
	nodes := []execmodel.Node{
		{ID: "node-alice-jones", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}},
		{ID: "node-alice-brown", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Brown"}},
	}
	tests := []struct {
		name       string
		props      map[string]any
		wantNodeID []string
	}{
		{name: "Alice Jones", props: map[string]any{"firstName": "Alice", "lastName": "Jones"}, wantNodeID: []string{"node-alice-jones"}},
		{name: "Alice", props: map[string]any{"firstName": "Alice"}, wantNodeID: []string{"node-alice-jones", "node-alice-brown"}},
		{name: "John", props: map[string]any{"firstName": "John"}, wantNodeID: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := &fakeGraphWriter{nodes: nodes}
			plan := planmodel.Plan{
				AccessMode: analysis.ReadOnly,
				Operations: []planmodel.Operation{
					planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: tt.props, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}},
				},
			}

			result, err := Execute(context.Background(), graph, plan)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			got := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				if row["p"].Node != nil {
					got = append(got, row["p"].Node.ID)
				}
			}
			if !reflect.DeepEqual(got, tt.wantNodeID) {
				t.Fatalf("node ids = %#v, want %#v; rows=%#v", got, tt.wantNodeID, result.Rows)
			}
		})
	}
}

func TestExecutorExecutesPropertyReturnProjection(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{{ID: "node-1", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}}}}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Returns: []planmodel.ReturnItem{
				{Kind: planmodel.ReturnProperty, Variable: "p", Property: "firstName"},
				{Kind: planmodel.ReturnProperty, Variable: "p", Property: "lastName"},
			}},
		},
	}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"p.firstName", "p.lastName"}) {
		t.Fatalf("columns = %#v", result.Columns)
	}
	if len(result.Rows) != 1 || result.Rows[0]["p.firstName"].Scalar != "Alice" || result.Rows[0]["p.lastName"].Scalar != "Jones" {
		t.Fatalf("unexpected result rows: %#v", result.Rows)
	}
}

func TestExecutorExecutesPayloadReturnProjection(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{{ID: "node-1", Labels: []string{"Note"}, Payload: map[string]any{"text": "hello payload"}}}}
	plan := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{planmodel.QueryNodesOperation{Variable: "n", Labels: []string{"Note"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnProperty, Variable: "n", Namespace: "payload", Property: "text"}}}}}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"n.payload.text"}) {
		t.Fatalf("columns = %#v", result.Columns)
	}
	if len(result.Rows) != 1 || result.Rows[0]["n.payload.text"].Scalar != "hello payload" {
		t.Fatalf("unexpected result rows: %#v", result.Rows)
	}
}

func TestExecutorExecutesMixedReturnProjection(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{{ID: "node-1", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice"}}}}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Returns: []planmodel.ReturnItem{
				{Kind: planmodel.ReturnVariable, Variable: "p"},
				{Kind: planmodel.ReturnProperty, Variable: "p", Property: "firstName"},
			}},
		},
	}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"p", "p.firstName"}) {
		t.Fatalf("columns = %#v", result.Columns)
	}
	if len(result.Rows) != 1 || result.Rows[0]["p"].Node == nil || result.Rows[0]["p"].Node.ID != "node-1" || result.Rows[0]["p.firstName"].Scalar != "Alice" {
		t.Fatalf("unexpected result rows: %#v", result.Rows)
	}
}

func TestExecutorMissingPropertyProjectionReturnsNilScalar(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{{ID: "node-1", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice"}}}}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnProperty, Variable: "p", Property: "middleName"}}},
		},
	}

	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("row count = %d", len(result.Rows))
	}
	if _, ok := result.Rows[0]["p.middleName"]; !ok {
		t.Fatalf("missing projected column: %#v", result.Rows[0])
	}
	if result.Rows[0]["p.middleName"].Scalar != nil {
		t.Fatalf("middleName scalar = %#v, want nil", result.Rows[0]["p.middleName"].Scalar)
	}
}

func TestExecutorExecutesRelationshipPattern(t *testing.T) {
	graph := &fakeGraphWriter{patternRows: []PatternRow{{
		Start: execmodel.Node{ID: "source", Labels: []string{"Note"}, Properties: map[string]any{"title": "Source"}},
		Edge:  execmodel.Edge{ID: "edge-1", Labels: []string{"REFERENCES"}, Properties: map[string]any{"confidence": 0.9}},
		End:   execmodel.Node{ID: "target", Labels: []string{"Note"}, Properties: map[string]any{"title": "Target"}},
	}}}
	plan := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{planmodel.QueryPatternOperation{
		Start:        planmodel.NodePattern{Variable: "a", Labels: []string{"Note"}},
		Relationship: planmodel.RelationshipPattern{Variable: "r", Labels: []string{"REFERENCES"}, Properties: map[string]any{"confidence": 0.9}, Direction: planmodel.RelationshipOutgoing},
		End:          planmodel.NodePattern{Variable: "b", Labels: []string{"Note"}},
		Returns: []planmodel.ReturnItem{
			{Kind: planmodel.ReturnVariable, Variable: "a"},
			{Kind: planmodel.ReturnVariable, Variable: "r"},
			{Kind: planmodel.ReturnProperty, Variable: "r", Property: "confidence"},
			{Kind: planmodel.ReturnProperty, Variable: "b", Property: "title"},
		},
		Limit: 1,
	}}}
	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(graph.patternQueries) != 1 || graph.patternQueries[0].Limit != 1 || graph.patternQueries[0].Relationship.Direction != RelationshipOutgoing {
		t.Fatalf("pattern queries = %#v", graph.patternQueries)
	}
	if !reflect.DeepEqual(result.Columns, []string{"a", "r", "r.confidence", "b.title"}) {
		t.Fatalf("columns = %#v", result.Columns)
	}
	if len(result.Rows) != 1 || result.Rows[0]["a"].Node == nil || result.Rows[0]["r"].Edge == nil || result.Rows[0]["r.confidence"].Scalar != 0.9 || result.Rows[0]["b.title"].Scalar != "Target" {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
}

func TestExecutorAppliesFetchFirstLimit(t *testing.T) {
	graph := &fakeGraphWriter{nodes: []execmodel.Node{
		{ID: "node-1", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice"}},
		{ID: "node-2", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice"}},
	}}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}, Limit: 1},
		},
	}
	result, err := Execute(context.Background(), graph, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["p"].Node == nil || result.Rows[0]["p"].Node.ID != "node-1" {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
	if len(graph.queried) != 1 || graph.queried[0].Limit != 1 {
		t.Fatalf("queried = %#v, want limit 1", graph.queried)
	}
}

func TestExecutorRejectsMissingGraphWriter(t *testing.T) {
	_, err := Execute(context.Background(), nil, planmodel.Plan{AccessMode: analysis.ReadWrite})
	if err == nil {
		t.Fatal("Execute() error = nil, want error")
	}
}

func TestExecutorPropagatesInsertNodeError(t *testing.T) {
	wantErr := errors.New("insert failed")
	graph := &fakeGraphWriter{err: wantErr}
	plan := planmodel.Plan{
		AccessMode: analysis.ReadWrite,
		Operations: []planmodel.Operation{
			planmodel.InsertNodeOperation{Labels: []string{"Person"}},
		},
	}

	_, err := Execute(context.Background(), graph, plan)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute() error = %v, want %v", err, wantErr)
	}
}

type fakeGraphWriter struct {
	nextID         string
	err            error
	inserted       []InsertNode
	queried        []QueryNodes
	patternQueries []QueryPattern
	nodes          []execmodel.Node
	patternRows    []PatternRow
	createdEdges   []CreateEdge
}

func (f *fakeGraphWriter) InsertNode(_ context.Context, node InsertNode) (execmodel.NodeRef, error) {
	if f.err != nil {
		return execmodel.NodeRef{}, f.err
	}
	f.inserted = append(f.inserted, node)
	return execmodel.NodeRef{ID: f.nextID}, nil
}

func (f *fakeGraphWriter) CreateEdge(_ context.Context, edge CreateEdge) (execmodel.Edge, error) {
	if f.err != nil {
		return execmodel.Edge{}, f.err
	}
	f.createdEdges = append(f.createdEdges, edge)
	return execmodel.Edge{ID: "edge-created", FromID: edge.FromNodeID, ToID: edge.ToNodeID, Labels: append([]string(nil), edge.Labels...), Properties: copyProperties(edge.Properties)}, nil
}

func (f *fakeGraphWriter) QueryPattern(_ context.Context, query QueryPattern) ([]PatternRow, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.patternQueries = append(f.patternQueries, query)
	return append([]PatternRow(nil), f.patternRows...), nil
}

func (f *fakeGraphWriter) QueryNodes(_ context.Context, query QueryNodes) ([]execmodel.Node, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.queried = append(f.queried, query)
	out := []execmodel.Node{}
	for _, node := range f.nodes {
		if !hasAllLabels(node.Labels, query.Labels) || !hasProperties(node.Properties, query.Properties) {
			continue
		}
		out = append(out, node)
	}
	return out, nil
}

func hasAllLabels(labels, required []string) bool {
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

func hasProperties(properties, required map[string]any) bool {
	for key, want := range required {
		if got, ok := properties[key]; !ok || !reflect.DeepEqual(got, want) {
			return false
		}
	}
	return true
}
