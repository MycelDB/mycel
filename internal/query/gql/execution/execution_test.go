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
	nextID   string
	err      error
	inserted []InsertNode
	queried  []QueryNodes
	nodes    []execmodel.Node
}

func (f *fakeGraphWriter) InsertNode(_ context.Context, node InsertNode) (execmodel.NodeRef, error) {
	if f.err != nil {
		return execmodel.NodeRef{}, f.err
	}
	f.inserted = append(f.inserted, node)
	return execmodel.NodeRef{ID: f.nextID}, nil
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
