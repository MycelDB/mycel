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
// pages, blocks, journals, and tasks via labels, payload, and properties.
// Hierarchy is represented by contains edges rather than a parent field.
// Labels are graph classifications, Properties are user/domain-defined
// queryable values, Payload is primary text/blob payload data, and Meta is
// Mycel-controlled metadata. Legacy BlobRef/Content/Props fields remain during
// the refactor until all subsystems are migrated to the new shape.
type Node struct {
	ID         NodeID
	DomainID   DomainID
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

// Property returns a normalized user/domain property. It accepts canonical
// direct properties and the legacy nested properties bucket in Properties or
// Props while the node refactor is in flight.
func Property(node Node, name string) (any, bool) {
	want := name
	if normalized, err := NormalizePropertyName(name); err == nil {
		want = normalized
	}
	lookup := func(values map[string]any) (any, bool) {
		if value, ok := values[name]; ok {
			return value, true
		}
		if want != name {
			if value, ok := values[want]; ok {
				return value, true
			}
		}
		if nested, err := NormalizeCustomPropertiesValue(values[NodePropCustomProperties]); err == nil {
			value, ok := nested[want]
			return value, ok
		}
		return nil, false
	}
	if value, ok := lookup(node.Properties); ok {
		return value, true
	}
	return lookup(node.Props)
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
