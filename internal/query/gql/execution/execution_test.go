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
}

func (f *fakeGraphWriter) InsertNode(_ context.Context, node InsertNode) (execmodel.NodeRef, error) {
	if f.err != nil {
		return execmodel.NodeRef{}, f.err
	}
	f.inserted = append(f.inserted, node)
	return execmodel.NodeRef{ID: f.nextID}, nil
}
