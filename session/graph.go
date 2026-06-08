package session

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"

// ApplyGraphInput batches graph mutations so implementations can apply them
// with a single read/validate/write cycle.
//
// V1 supports recursive node deletes, node creation, and edge creation. When
// edges reference newly-created nodes, callers should provide explicit node IDs
// in AddNodes.
type ApplyGraphInput struct {
	DeleteNodes []DeleteNodeInput
	AddNodes    []AddNodeInput
	AddEdges    []AddEdgeInput
	Atomic      bool
}

// ApplyGraphResult describes mutations applied by ApplyGraph.
type ApplyGraphResult struct {
	DeletedNodeIDs []graph.NodeID
	AddedNodes     []graph.Node
	AddedEdges     []graph.Edge
}
