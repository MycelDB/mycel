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
