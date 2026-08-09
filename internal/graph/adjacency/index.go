package adjacency

import (
	"context"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

// EdgeIndex is a derived, rebuildable index over committed graph edge
// endpoints. It is not authoritative storage; callers resolve returned edge IDs
// against graph storage.
type EdgeIndex interface {
	// Rebuild replaces the entire derived index from the authoritative committed
	// edge set for one loaded space graph store.
	Rebuild(ctx context.Context, edges []graph.Edge) error

	// Put adds or replaces one committed edge in the derived index.
	Put(ctx context.Context, edge graph.Edge) error

	// Delete removes one committed edge from the derived index.
	Delete(ctx context.Context, edge graph.Edge) error

	// Incoming returns edge IDs whose ToID is nodeID.
	Incoming(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error)

	// Outgoing returns edge IDs whose FromID is nodeID.
	Outgoing(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error)
}
