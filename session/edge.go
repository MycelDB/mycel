package session

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"

// AddEdgeInput is the write payload used when creating an edge.
type AddEdgeInput struct {
	ID     *graph.EdgeID
	FromID graph.NodeID
	ToID   graph.NodeID
	Kind   graph.EdgeKind
	Props  map[string]any
}

// AddGraphInput is a batch write payload containing nodes and edges.
//
// If Atomic is true, implementations should apply all changes as an
// all-or-nothing operation.
type AddGraphInput struct {
	Nodes  []AddNodeInput
	Edges  []AddEdgeInput
	Atomic bool
}
