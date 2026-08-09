package adjacency

import (
	"bytes"
	"context"
	"sort"
	"sync"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

type edgeEndpoints struct {
	from graph.NodeID
	to   graph.NodeID
}

type memoryEdgeIndex struct {
	mu sync.RWMutex

	incoming          map[graph.NodeID]map[graph.EdgeID]struct{}
	outgoing          map[graph.NodeID]map[graph.EdgeID]struct{}
	endpointsByEdgeID map[graph.EdgeID]edgeEndpoints
}

// NewMemoryEdgeIndex returns an in-memory edge adjacency index.
func NewMemoryEdgeIndex() EdgeIndex {
	return &memoryEdgeIndex{
		incoming:          map[graph.NodeID]map[graph.EdgeID]struct{}{},
		outgoing:          map[graph.NodeID]map[graph.EdgeID]struct{}{},
		endpointsByEdgeID: map[graph.EdgeID]edgeEndpoints{},
	}
}

func (i *memoryEdgeIndex) Rebuild(ctx context.Context, edges []graph.Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.incoming = map[graph.NodeID]map[graph.EdgeID]struct{}{}
	i.outgoing = map[graph.NodeID]map[graph.EdgeID]struct{}{}
	i.endpointsByEdgeID = map[graph.EdgeID]edgeEndpoints{}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		i.putLocked(edge)
	}
	return nil
}

func (i *memoryEdgeIndex) Put(ctx context.Context, edge graph.Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.putLocked(edge)
	return nil
}

func (i *memoryEdgeIndex) Delete(ctx context.Context, edge graph.Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.deleteLocked(edge.ID)
	return nil
}

func (i *memoryEdgeIndex) Incoming(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return sortedIDs(i.incoming[nodeID]), nil
}

func (i *memoryEdgeIndex) Outgoing(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return sortedIDs(i.outgoing[nodeID]), nil
}

func (i *memoryEdgeIndex) putLocked(edge graph.Edge) {
	i.deleteLocked(edge.ID)
	ensureEdgeSet(i.outgoing, edge.FromID)[edge.ID] = struct{}{}
	ensureEdgeSet(i.incoming, edge.ToID)[edge.ID] = struct{}{}
	i.endpointsByEdgeID[edge.ID] = edgeEndpoints{from: edge.FromID, to: edge.ToID}
}

func (i *memoryEdgeIndex) deleteLocked(edgeID graph.EdgeID) {
	endpoints, ok := i.endpointsByEdgeID[edgeID]
	if !ok {
		return
	}
	deleteEdgeID(i.outgoing, endpoints.from, edgeID)
	deleteEdgeID(i.incoming, endpoints.to, edgeID)
	delete(i.endpointsByEdgeID, edgeID)
}

func ensureEdgeSet(m map[graph.NodeID]map[graph.EdgeID]struct{}, nodeID graph.NodeID) map[graph.EdgeID]struct{} {
	set := m[nodeID]
	if set == nil {
		set = map[graph.EdgeID]struct{}{}
		m[nodeID] = set
	}
	return set
}

func deleteEdgeID(m map[graph.NodeID]map[graph.EdgeID]struct{}, nodeID graph.NodeID, edgeID graph.EdgeID) {
	set := m[nodeID]
	if set == nil {
		return
	}
	delete(set, edgeID)
	if len(set) == 0 {
		delete(m, nodeID)
	}
}

func sortedIDs(set map[graph.EdgeID]struct{}) []graph.EdgeID {
	out := make([]graph.EdgeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(a, b int) bool { return bytes.Compare(out[a][:], out[b][:]) < 0 })
	return out
}
