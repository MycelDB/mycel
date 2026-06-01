package api

type Edge struct {
	ID     EdgeID
	FromID NodeID
	ToID   NodeID
	Type   string
}

type EdgeInput struct {
	ID     *EdgeID
	FromID NodeID
	ToID   NodeID
	Type   string
}
