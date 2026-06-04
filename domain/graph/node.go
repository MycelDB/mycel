package graph

import "github.com/google/uuid"

// NodeID uniquely identifies a node in the graph.
type NodeID = uuid.UUID

// Node is the canonical persisted graph node representation.
//
// Node is intentionally generic so higher-level domains (e.g. PKM) can model
// pages, blocks, journals, and tasks via template and properties.
// TemplateID is optional; nodes may exist without a template.
type Node struct {
	ID         NodeID
	TemplateID *TemplateID
	ParentID   *NodeID
	Content    string
	Props      map[string]any
}

// NodeInput is the write payload used when creating or upserting a node.
//
// ID is optional so callers can provide one or let the implementation assign it.
// TemplateID is optional; nil means no template is associated.
type NodeInput struct {
	ID         *NodeID
	TemplateID *TemplateID
	ParentID   *NodeID
	Content    string
	Props      map[string]any
}

// UpdateNodeInput is the write payload used when updating an existing node.
type UpdateNodeInput struct {
	ID         NodeID
	TemplateID *TemplateID
	ParentID   *NodeID
	Content    string
	Props      map[string]any
}

// DeleteNodeInput is the hard-delete payload for a graph node.
// Incident edges are removed. Child nodes require Recursive=true.
type DeleteNodeInput struct {
	ID        NodeID
	Recursive bool
}
