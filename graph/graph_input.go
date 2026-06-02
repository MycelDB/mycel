package graph

// GraphInput is a batch write payload containing nodes and edges.
//
// If Atomic is true, implementations should apply all changes as an
// all-or-nothing operation.
type GraphInput struct {
	Nodes  []NodeInput
	Edges  []EdgeInput
	Atomic bool
}
