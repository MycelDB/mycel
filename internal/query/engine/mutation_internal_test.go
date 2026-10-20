package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type incidentAdjacencyReader struct {
	edgesByDirection map[graphquery.AdjacencyDirection][]graph.Edge
	calls            []graphquery.AdjacencyScan
}

func (r *incidentAdjacencyReader) ScanAdjacency(_ context.Context, _ sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error) {
	r.calls = append(r.calls, scan)
	if scan.Label != "" || scan.Limit <= 0 || scan.ExamineLimit <= 0 {
		return nil, "", graphquery.IndexedReadStats{}, graphquery.ErrScanBudgetExceeded
	}
	start := 0
	if scan.Cursor != "" {
		start = 1
	}
	edges := r.edgesByDirection[scan.Direction]
	if start >= len(edges) {
		return nil, "", graphquery.IndexedReadStats{}, nil
	}
	next := ""
	if start+1 < len(edges) {
		next = "next"
	}
	return []graph.Edge{edges[start]}, next, graphquery.IndexedReadStats{IndexEntriesScanned: 1, EdgesLoaded: 1}, nil
}

func TestValidateNodeDeletesUsesBoundedWildcardIncidentAdjacency(t *testing.T) {
	nodeID := uuid.New()
	otherID := uuid.New()
	self := graph.Edge{ID: uuid.New(), FromID: nodeID, ToID: nodeID, Labels: []string{"self"}}
	outgoing := graph.Edge{ID: uuid.New(), FromID: nodeID, ToID: otherID, Labels: []string{"out"}}
	incoming := graph.Edge{ID: uuid.New(), FromID: otherID, ToID: nodeID, Labels: []string{"in"}}
	reader := &incidentAdjacencyReader{edgesByDirection: map[graphquery.AdjacencyDirection][]graph.Edge{
		graphquery.AdjacencyDirectionOut: {self, outgoing},
		graphquery.AdjacencyDirectionIn:  {self, incoming},
	}}
	engine := &Engine{ports: Ports{Traversals: reader}}
	tracker, err := queryexecution.NewTracker(queryexecution.ResolveBudget(queryexecution.Budget{
		BatchSize: 1, MaxIndexEntries: 8, MaxEdgesLoaded: 8, MaxCandidates: 8, MaxAdjacencyCalls: 8,
	}, queryexecution.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	deleted := map[string]struct{}{self.ID.String(): {}, outgoing.ID.String(): {}, incoming.ID.String(): {}}
	if err := engine.validateNodeDeletes(context.Background(), ExecutionContext{Transaction: sessionmodel.GraphTransaction{}}, map[string]struct{}{nodeID.String(): {}}, deleted, tracker); err != nil {
		t.Fatalf("validateNodeDeletes() error = %v", err)
	}
	if len(reader.calls) != 4 {
		t.Fatalf("adjacency calls = %d, want 4", len(reader.calls))
	}
	for _, scan := range reader.calls {
		if scan.NodeID != nodeID.String() || scan.Label != "" || scan.Limit != 1 || scan.ExamineLimit <= 0 {
			t.Fatalf("incident scan = %+v", scan)
		}
	}

	tight, err := queryexecution.NewTracker(queryexecution.ResolveBudget(queryexecution.Budget{
		BatchSize: 1, MaxIndexEntries: 8, MaxEdgesLoaded: 1, MaxCandidates: 8, MaxAdjacencyCalls: 8,
	}, queryexecution.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	err = engine.validateNodeDeletes(context.Background(), ExecutionContext{Transaction: sessionmodel.GraphTransaction{}}, map[string]struct{}{nodeID.String(): {}}, deleted, tight)
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorResourceExhausted {
		t.Fatalf("tight validation error = %v", err)
	}
}

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
