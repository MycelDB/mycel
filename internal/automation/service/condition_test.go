package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

func TestEvaluateConditionMatchesChangedRow(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "A"}}
	mgr := NewManager(nil).WithGraphRuntime(nil, fakeConditionGraph{nodes: []graph.Node{node}})
	res, err := mgr.evaluateCondition(context.Background(), conditionTx(domainID), automation.Definition{Condition: automation.Condition{GQL: "MATCH (changed:Page) RETURN changed"}}, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Matched {
		t.Fatal("expected condition to match")
	}
	if _, ok := res.Aliases["changed"]; !ok {
		t.Fatalf("expected changed alias, got %+v", res.Aliases)
	}
}

func TestEvaluateConditionDefaultsToChangedWhenOmitted(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "A"}}
	mgr := NewManager(nil).WithGraphRuntime(nil, fakeConditionGraph{nodes: []graph.Node{node}})
	res, err := mgr.evaluateCondition(context.Background(), conditionTx(domainID), automation.Definition{}, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Matched {
		t.Fatal("expected omitted condition to match")
	}
	alias, ok := res.Aliases["changed"].(graph.Node)
	if !ok || alias.ID != node.ID {
		t.Fatalf("expected changed alias for omitted condition, got %+v", res.Aliases)
	}
}

func TestAutomationGQLGraphUsesOldBinding(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	old := graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "Before"}}
	changed := graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Page"}, Properties: map[string]any{"title": "After"}}
	g := automationGQLGraph{graphs: fakeConditionGraph{nodes: []graph.Node{changed}}, tx: conditionTx(domainID), changed: &changed, old: &old}
	nodes, err := g.QueryNodes(context.Background(), execution.QueryNodes{Variable: "old", Labels: []string{"Page"}, Properties: map[string]any{"title": "Before"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].ID != nodeID.String() || nodes[0].Properties["title"] != "Before" {
		t.Fatalf("unexpected old binding result: %+v", nodes)
	}
}

func TestAutomationGQLGraphOldBindingUnavailable(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	changed := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}}
	g := automationGQLGraph{graphs: fakeConditionGraph{nodes: []graph.Node{changed}}, tx: conditionTx(domainID), changed: &changed}
	nodes, err := g.QueryNodes(context.Background(), execution.QueryNodes{Variable: "old", Labels: []string{"Page"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no old binding result, got %+v", nodes)
	}
}

func TestEvaluateConditionFalseWhenChangedNotReturned(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	node := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Page"}}
	other := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Other"}}
	mgr := NewManager(nil).WithGraphRuntime(nil, fakeConditionGraph{nodes: []graph.Node{node, other}})
	res, err := mgr.evaluateCondition(context.Background(), conditionTx(domainID), automation.Definition{Condition: automation.Condition{GQL: "MATCH (changed:Other) RETURN changed"}}, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Matched {
		t.Fatal("expected condition not to match triggering node")
	}
}

func TestEvaluateConditionFailsClosedOnMultipleChangedRows(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	changed := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"JournalEntry"}}
	journalA := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Journal"}}
	journalB := graph.Node{ID: graph.NodeID(uuid.New()), DomainID: domainID, Labels: []string{"Journal"}}
	edges := []graph.Edge{
		{ID: graph.EdgeID(uuid.New()), DomainID: domainID, FromID: journalA.ID, ToID: changed.ID, Labels: []string{"HAS_ENTRY"}},
		{ID: graph.EdgeID(uuid.New()), DomainID: domainID, FromID: journalB.ID, ToID: changed.ID, Labels: []string{"HAS_ENTRY"}},
	}
	mgr := NewManager(nil).WithGraphRuntime(nil, fakeConditionGraph{nodes: []graph.Node{changed, journalA, journalB}, edges: edges})
	_, err := mgr.evaluateCondition(context.Background(), conditionTx(domainID), automation.Definition{Condition: automation.Condition{GQL: "MATCH (journal:Journal)-[:HAS_ENTRY]->(changed:JournalEntry) RETURN changed, journal"}}, changed, nil)
	if err == nil || !strings.Contains(err.Error(), "returned multiple rows") {
		t.Fatalf("expected multiple-row error, got %v", err)
	}
}

func conditionTx(domainID graph.DomainID) sessionservice.GraphTransaction {
	return sessionservice.GraphTransaction{ID: "tx", DomainID: domainID.String(), Mode: sessionservice.TransactionModeReadOnly, State: sessionservice.TransactionStateActive}
}

type fakeConditionGraph struct {
	nodes []graph.Node
	edges []graph.Edge
}

func (f fakeConditionGraph) GetNode(_ context.Context, _ sessionservice.GraphTransaction, id string) (graph.Node, error) {
	for _, node := range f.nodes {
		if node.ID.String() == id {
			return node, nil
		}
	}
	return graph.Node{}, fmt.Errorf("node not found")
}
func (f fakeConditionGraph) ListNodes(_ context.Context, _ sessionservice.GraphTransaction, _ int, token string) ([]graph.Node, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return f.nodes, "", nil
}
func (f fakeConditionGraph) CreateNode(context.Context, sessionservice.GraphTransaction, graphservice.NodeInput) (graph.Node, error) {
	panic("unused")
}
func (f fakeConditionGraph) UpdateNode(context.Context, sessionservice.GraphTransaction, graphservice.UpdateNodeInput) (graph.Node, error) {
	panic("unused")
}
func (f fakeConditionGraph) UpsertNode(context.Context, sessionservice.GraphTransaction, graphservice.NodeInput) (graph.Node, error) {
	panic("unused")
}
func (f fakeConditionGraph) DeleteNode(context.Context, sessionservice.GraphTransaction, string, bool) ([]string, []string, error) {
	panic("unused")
}
func (f fakeConditionGraph) GetEdge(context.Context, sessionservice.GraphTransaction, string) (graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) ListEdges(_ context.Context, _ sessionservice.GraphTransaction, _ int, token string) ([]graph.Edge, string, error) {
	if token != "" {
		return nil, "", nil
	}
	return f.edges, "", nil
}
func (f fakeConditionGraph) CreateEdge(context.Context, sessionservice.GraphTransaction, graphservice.EdgeInput) (graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) UpdateEdge(context.Context, sessionservice.GraphTransaction, graphservice.UpdateEdgeInput) (graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) DeleteEdge(context.Context, sessionservice.GraphTransaction, string) (string, error) {
	panic("unused")
}
func (f fakeConditionGraph) ListChildren(context.Context, sessionservice.GraphTransaction, string) ([]graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) GetParent(context.Context, sessionservice.GraphTransaction, string) (*graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) MoveSubtree(context.Context, sessionservice.GraphTransaction, string, string, *int32) (graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) ReorderChildren(context.Context, sessionservice.GraphTransaction, string, []string) ([]graph.Edge, error) {
	panic("unused")
}
func (f fakeConditionGraph) CurrentRevision(context.Context, string) (int64, error) { panic("unused") }
func (f fakeConditionGraph) CommitTransactionGraph(context.Context, sessionservice.GraphTransaction) (graphservice.CommitResult, error) {
	panic("unused")
}
func (f fakeConditionGraph) DiscardTransactionGraph(context.Context, string) { panic("unused") }
func (f fakeConditionGraph) ConfigureIndexes(context.Context, sessionservice.GraphTransaction, string, []schemamodel.IndexDefinition) error {
	panic("unused")
}
func (f fakeConditionGraph) ScanLabel(context.Context, sessionservice.GraphTransaction, graphservice.LabelScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	panic("unused")
}
func (f fakeConditionGraph) ScanTag(context.Context, sessionservice.GraphTransaction, graphservice.TagScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	panic("unused")
}
func (f fakeConditionGraph) ScanNodePropertyOrdered(context.Context, sessionservice.GraphTransaction, graphservice.OrderedNodePropertyScan) ([]graph.Node, string, graphservice.IndexedReadStats, error) {
	panic("unused")
}
func (f fakeConditionGraph) ScanAdjacency(_ context.Context, _ sessionservice.GraphTransaction, scan graphservice.AdjacencyScan) ([]graph.Edge, string, graphservice.IndexedReadStats, error) {
	if scan.Cursor != "" {
		return nil, "", graphservice.IndexedReadStats{}, nil
	}
	out := []graph.Edge{}
	for _, edge := range f.edges {
		if scan.Label != "" && !containsString(edge.Labels, scan.Label) {
			continue
		}
		switch scan.Direction {
		case graphservice.AdjacencyDirectionIn:
			if edge.ToID.String() == scan.NodeID {
				out = append(out, edge)
			}
		default:
			if edge.FromID.String() == scan.NodeID {
				out = append(out, edge)
			}
		}
	}
	return out, "", graphservice.IndexedReadStats{}, nil
}
func (f fakeConditionGraph) ScanSubtree(context.Context, sessionservice.GraphTransaction, graphservice.SubtreeScan) (graphservice.SubtreeResult, graphservice.IndexedReadStats, error) {
	panic("unused")
}
func (f fakeConditionGraph) BlobRefCount(context.Context, string, string) (int, error) {
	panic("unused")
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
