package embedding

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
)

func TestAssembleSubtreeOrdersChildren(t *testing.T) {
	root := graph.Node{ID: uuid.New(), Content: "root"}
	first := graph.Node{ID: uuid.New(), Content: "first"}
	second := graph.Node{ID: uuid.New(), Content: "second"}
	nodes := []graph.Node{root, second, first}
	edges := []graph.Edge{
		{ID: uuid.New(), FromID: root.ID, ToID: second.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 2}},
		{ID: uuid.New(), FromID: root.ID, ToID: first.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 1}},
	}
	res := AssembleSource(SourceInput{Root: root, Nodes: nodes, Edges: edges, Mode: domainembedding.SourceModeSubtree})
	if !strings.Contains(res.Text, "root\n- first\n- second") {
		t.Fatalf("unexpected source text: %q", res.Text)
	}
}

func TestAssembleSourceHashChangesWithChildContent(t *testing.T) {
	root := graph.Node{ID: uuid.New(), Content: "root"}
	child := graph.Node{ID: uuid.New(), Content: "child"}
	edge := graph.Edge{ID: uuid.New(), FromID: root.ID, ToID: child.ID, Kind: graph.EdgeKindContains}
	before := AssembleSource(SourceInput{Root: root, Nodes: []graph.Node{root, child}, Edges: []graph.Edge{edge}, Mode: domainembedding.SourceModeSubtree})
	child.Content = "changed"
	after := AssembleSource(SourceInput{Root: root, Nodes: []graph.Node{root, child}, Edges: []graph.Edge{edge}, Mode: domainembedding.SourceModeSubtree})
	if before.Hash == after.Hash {
		t.Fatalf("expected hash to change")
	}
}
