package graph

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// EdgeID uniquely identifies an edge in the graph.
type EdgeID = uuid.UUID

// Edge is the canonical persisted graph edge representation.
//
// Edges are first-class graph elements. They have the same labels,
// properties, payload, and meta buckets as nodes, plus directed connectivity
// from FromID to ToID. Edge labels are open-ended and domain-defined; MycelDB
// core does not define a closed set of edge types.
type Edge struct {
	ID         EdgeID
	DomainID   DomainID
	FromID     NodeID
	ToID       NodeID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// EdgeProperty returns a user/domain property from an edge.
func EdgeProperty(edge Edge, name string) (any, bool) {
	if edge.Properties == nil {
		return nil, false
	}
	value, ok := edge.Properties[name]
	return value, ok
}

// EdgeHasLabels reports whether edge contains every required label.
func EdgeHasLabels(edge Edge, required []string) bool {
	seen := map[string]struct{}{}
	for _, label := range edge.Labels {
		seen[strings.TrimSpace(label)] = struct{}{}
	}
	for _, label := range required {
		if _, ok := seen[strings.TrimSpace(label)]; !ok {
			return false
		}
	}
	return true
}
