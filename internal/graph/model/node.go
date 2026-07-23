package graph

import (
	"strings"
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

// PayloadText returns the canonical text payload for indexing/display. It
// falls back to the legacy Content field while the node refactor is in flight.
func PayloadText(node Node) string {
	if node.Payload != nil {
		if text, ok := node.Payload["text"].(string); ok {
			return text
		}
	}
	return node.Content
}

// Property returns a user/domain property, falling back to legacy Props while
// the node refactor is in flight.
func Property(node Node, name string) (any, bool) {
	if node.Properties != nil {
		if value, ok := node.Properties[name]; ok {
			return value, true
		}
	}
	if node.Props != nil {
		if value, ok := node.Props[name]; ok {
			return value, true
		}
		if nested, ok := node.Props["properties"].(map[string]any); ok {
			value, ok := nested[name]
			return value, ok
		}
	}
	return nil, false
}

// HasLabels reports whether node contains every required label.
func HasLabels(node Node, required []string) bool {
	seen := map[string]struct{}{}
	for _, label := range node.Labels {
		seen[strings.TrimSpace(label)] = struct{}{}
	}
	for _, label := range required {
		if _, ok := seen[strings.TrimSpace(label)]; !ok {
			return false
		}
	}
	return true
}
