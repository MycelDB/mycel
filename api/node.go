package api

type Node struct {
	ID          NodeID
	TemplateKey string
	ParentID    *NodeID
	Content     string
	Props       map[string]any
}

type NodeInput struct {
	ID          *NodeID
	TemplateKey string
	ParentID    *NodeID
	Content     string
	Props       map[string]any
}
