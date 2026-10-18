package engine

import (
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

func TestPrepareSetStatesCoalescesRepeatedNodeAndEdgeBindings(t *testing.T) {
	node := graph.Node{ID: uuid.New(), Properties: map[string]any{"name": "loop"}}
	edge := graph.Edge{ID: uuid.New(), Properties: map[string]any{"kind": "self"}}
	rows := []queryexecution.BindingRow{
		{
			Nodes: map[string]graph.Node{"n": node, "m": node},
			Edges: map[string]graph.Edge{"r": edge, "s": edge},
		},
		{
			Nodes: map[string]graph.Node{"n": node, "m": node},
			Edges: map[string]graph.Edge{"r": edge, "s": edge},
		},
	}
	assignments := []querylogical.Assignment{
		{Field: querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "alpha"}},
		{Field: querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "beta"}},
		{Field: querylogical.FieldRef{Alias: "r", Namespace: querylogical.NamespaceProperties, Name: "gamma"}},
		{Field: querylogical.FieldRef{Alias: "s", Namespace: querylogical.NamespaceProperties, Name: "delta"}},
	}
	values := []any{int64(1), int64(2), "a", "b"}
	tracker, err := queryexecution.NewTracker(queryexecution.ResolveBudget(queryexecution.Budget{MaxMemoryBytes: 1 << 20}, queryexecution.Budget{}))
	if err != nil {
		t.Fatalf("NewTracker() error = %v", err)
	}
	nodes, edges, retained, err := prepareSetStates(rows, assignments, values, tracker)
	defer tracker.ReleaseMemory(retained)
	if err != nil {
		t.Fatalf("prepareSetStates() error = %v", err)
	}
	if len(nodes) != 1 || len(edges) != 1 {
		t.Fatalf("coalesced nodes=%d edges=%d, want one each", len(nodes), len(edges))
	}
	preparedNode := nodes[node.ID.String()].node
	if preparedNode.Properties["alpha"] != int64(1) || preparedNode.Properties["beta"] != int64(2) {
		t.Fatalf("prepared node properties = %+v", preparedNode.Properties)
	}
	preparedEdge := edges[edge.ID.String()].edge
	if preparedEdge.Properties["gamma"] != "a" || preparedEdge.Properties["delta"] != "b" {
		t.Fatalf("prepared edge properties = %+v", preparedEdge.Properties)
	}
}
