package graph

import (
	"time"

	"github.com/google/uuid"
)

// NodeID uniquely identifies a node in the graph.
type NodeID = uuid.UUID

// Node is the canonical persisted graph node representation.
//
// Node is intentionally generic so higher-level domains (e.g. knowledge management) can model
// pages, blocks, journals, and tasks via template and properties.
// TemplateID is optional; nodes may exist without a template. Hierarchy is
// represented by contains edges rather than a parent field.
// BlobRef is optional; when set, the node's content lives in the space's
// content-addressed blob store and Content must be empty: a node has inline
// text content or blob content, never both. Captions and similar text about
// a blob belong in Props (e.g. caption, alt_text) or annotation children.
type Node struct {
	ID         NodeID
	TemplateID *TemplateID
	BlobRef    *BlobID
	Content    string
	Props      map[string]any
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
