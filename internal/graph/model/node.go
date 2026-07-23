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
// Labels are graph classifications, Properties are user/domain-defined
// queryable values, Payload is primary text/blob payload data, and Meta is
// Mycel-controlled metadata. Legacy BlobRef/Content/Props fields remain during
// the refactor until all subsystems are migrated to the new shape.
type Node struct {
	ID         NodeID
	DomainID   DomainID
	TemplateID *TemplateID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any

	// Deprecated: use Payload["blob_id"].
	BlobRef *BlobID
	// Deprecated: use Payload["text"].
	Content string
	// Deprecated: use Properties/Meta.
	Props map[string]any

	CreatedAt time.Time
	UpdatedAt time.Time
}
