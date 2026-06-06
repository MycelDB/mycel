package graph

import "github.com/google/uuid"

// EdgeID uniquely identifies an edge in the graph.
type EdgeID = uuid.UUID

// EdgeKind represents graph-structural semantics only.
type EdgeKind string

const (
	// EdgeKindContains expresses hierarchy/containment.
	EdgeKindContains EdgeKind = "contains"
	// EdgeKindReferences expresses a cross-node reference/link.
	EdgeKindReferences EdgeKind = "references"
	// EdgeKindAssociates expresses a generic non-hierarchical relation.
	EdgeKindAssociates EdgeKind = "associates"
)

// Edge is the canonical persisted graph edge representation.
type Edge struct {
	ID     EdgeID
	FromID NodeID
	ToID   NodeID
	Kind   EdgeKind
	Props  map[string]any
}
