package session

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"

// AddNodeInput is the write payload used when creating a node.
//
// ID is optional so callers can provide one or let the implementation assign it.
// TemplateID is optional; nil means no template is associated.
type AddNodeInput struct {
	ID         *graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// UpsertNodeInput is the write payload used when creating or replacing a node.
type UpsertNodeInput struct {
	ID         *graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// UpdateNodeInput is the write payload used when updating an existing node.
type UpdateNodeInput struct {
	ID         graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// DeleteNodeInput is the hard-delete payload for a graph node.
// Incident edges are removed. Outgoing contains descendants require Recursive=true.
type DeleteNodeInput struct {
	ID        graph.NodeID
	Recursive bool
}
