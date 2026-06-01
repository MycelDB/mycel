package api

type GraphInput struct {
	Nodes  []NodeInput
	Edges  []EdgeInput
	Atomic bool
}
