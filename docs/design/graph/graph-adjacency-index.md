# Graph adjacency index

## Summary

The graph adjacency index is a derived, in-memory index maintained by each loaded
space graph store. It accelerates endpoint-scoped edge lookups used by hierarchy
validation and graph traversal paths, especially for Logseq-like imports that
create many block hierarchy and reference edges.

The index is not authoritative storage. Authoritative graph state remains the
committed graph records applied through local commits, WAL replay, Raft apply, and
snapshot recovery. The adjacency index is rebuilt from those committed records
when a space graph store is opened or reloaded, and it is updated synchronously by
the same store mutation paths that apply edge puts and deletes.

## Motivation

A synthetic Logseq datastore import workload exposed O(N²)-style behavior in
edge-heavy graph commits. The dominant profile hotspots were in edge creation,
hierarchy checks, full edge listing, sorting, cloning, and UUID string conversion.

The current internal validation paths can repeatedly enumerate all edges in a
space, merge transaction overlays, sort results, and clone full edge payloads when
they only need endpoint-scoped answers such as:

- does this child already have a hierarchy parent?
- what children does this parent have?
- does a hierarchy path already exist between two nodes?

For Logseq-shaped data, every block creates hierarchy edges and page reference
edges. Repeated full scans become progressively more expensive as the import
grows.

## Goals

- Add a reusable derived index for incoming and outgoing edge lookup.
- Keep authoritative graph storage unchanged.
- Rebuild the index deterministically from committed graph records.
- Update the index synchronously on every committed edge put/delete.
- Remove full edge scans/sorts/clones from internal hierarchy validation paths.
- Preserve public list/pagination semantics while optimizing internal paths.

## Non-goals

- This is not a public graph-change callback/subscription mechanism.
- This is not a persisted authoritative index.
- This is not automatic repair, restore, merge, rebalance, or authoritative-node
  selection.
- This is not a semantic subsystem index.
- This does not change graph commit authority, Raft ownership, or read
  consistency semantics.

## Proposed package

Create a small internal package:

```text
internal/graph/adjacency
```

Suggested files:

```text
internal/graph/adjacency/index.go
internal/graph/adjacency/memory.go
internal/graph/adjacency/memory_test.go
```

## Interface

```go
package adjacency

import (
    "context"

    graph "github.com/myceldb/mycel/internal/graph/model"
)

type EdgeIndex interface {
    // Rebuild replaces the entire derived index from the authoritative
    // committed edge set for one loaded space graph store.
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
```

The index returns edge IDs rather than full edge records. The graph store remains
the owner of authoritative edge bodies.

## In-memory implementation

A memory implementation can maintain endpoint maps and an endpoint map by edge ID
so replacement is correct:

```go
type memoryEdgeIndex struct {
    mu sync.RWMutex

    incoming map[graph.NodeID]map[graph.EdgeID]struct{}
    outgoing map[graph.NodeID]map[graph.EdgeID]struct{}

    endpointsByEdgeID map[graph.EdgeID]edgeEndpoints
}

type edgeEndpoints struct {
    from graph.NodeID
    to   graph.NodeID
}
```

Behavior:

- `Rebuild` clears all maps and inserts all committed edges.
- `Put` removes previous endpoint mappings for the same edge ID, then inserts the
  new mappings.
- `Delete` removes the edge ID from endpoint maps and is a no-op if the edge is
  absent.
- `Incoming` and `Outgoing` return copies so callers cannot mutate index state.
- Returned IDs may be sorted if deterministic tests require it, but validation
  callers should not depend on global edge ordering.

## LocalStore integration

The index should live with the per-space graph `LocalStore`:

```text
one loaded space graph LocalStore
  authoritative maps/segments
  derived adjacency index
```

Add a field to `LocalStore`:

```go
type LocalStore struct {
    ...
    edgeIndex adjacency.EdgeIndex
}
```

Initialize it when indexes are reset:

```go
func (s *LocalStore) resetIndexes() {
    ...
    s.edgeIndex = adjacency.NewMemoryEdgeIndex()
}
```

The existing `LocalStore` lifecycle already rebuilds indexes when opening a
space graph store:

```text
graphstorage.Open(graphs/<space_id>)
  -> open manifest and active segments
  -> rebuildIndexes
  -> replay committed node/edge records
  -> ready
```

The adjacency index should be populated during this same rebuild. The preferred
approach is to update it inside the existing edge application helpers so rebuild,
commit apply, WAL replay, and Raft apply all share the same code path.

## Commit/apply update path

The graph store should update the index synchronously inside authoritative edge
mutation methods, not through callbacks:

```go
func (s *LocalStore) applyEdgePut(e graph.Edge, loc RecordLocation) {
    // update authoritative maps
    s.edgeRecords[e.ID] = e
    s.edgeMeta[e.ID] = ...

    // update derived index
    _ = s.edgeIndex.Put(context.Background(), e)
}

func (s *LocalStore) applyEdgeDelete(id graph.EdgeID, loc RecordLocation) {
    old := s.edgeRecords[id]

    // update authoritative maps
    delete(s.edgeRecords, id)
    delete(s.edgeMeta, id)

    // update derived index
    _ = s.edgeIndex.Delete(context.Background(), old)
}
```

The final implementation should avoid silently ignoring unexpected index errors;
the example above is illustrative.

This applies uniformly to:

- local graph commits;
- WAL replay;
- Raft commands applied from another pod;
- snapshot reload/recovery paths that rebuild stores from authoritative records.

Graph-change callbacks/subscriptions remain an observation layer for consumers
such as automation and semantic dirty-event persistence. They should not maintain
this internal graph store index.

## Store methods

Add endpoint-scoped store methods:

```go
func (s *LocalStore) IncomingEdges(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
func (s *LocalStore) OutgoingEdges(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
```

Implementation outline:

1. Get edge IDs from the adjacency index.
2. Resolve edge bodies from `edgeRecords`.
3. Return copied edge values only at the storage boundary.
4. Do not enumerate or sort all edges in the space.

## Transaction overlay handling

The base adjacency index reflects committed graph state only. Write transactions
also have staged overlay changes:

```text
committed adjacency index
+ overlay putEdges
- overlay deleteEdges
```

Internal graph service validation should use an overlay-aware view:

```go
type transactionEdgeView struct {
    store    *graphstorage.LocalStore
    overlay  *overlay
    domainID graph.DomainID
}

func (v transactionEdgeView) incoming(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
func (v transactionEdgeView) outgoing(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
```

Overlay merge rules:

- start from indexed committed incoming/outgoing edges;
- drop committed edges deleted in the transaction overlay;
- include staged `putEdges` matching the requested endpoint;
- avoid global sorting for validation;
- sort only for public list/pagination result stability if required.

## Graph service refactor targets

Internal paths should stop using full `ListEdges` for endpoint-scoped hierarchy
work.

Refactor these paths first:

- `storeHierarchyParent`
- `parentEdgeLocal`
- `listChildrenLocal`
- `hierarchyPathExists`

Expected behavior after refactor:

- `GetParent(child)` uses incoming edges for the child and filters hierarchy
  edges/domain as needed.
- `ListChildren(parent)` uses outgoing edges for the parent and filters hierarchy
  edges/domain as needed.
- `hierarchyPathExists(from, target)` performs DFS/BFS over outgoing hierarchy
  edges instead of repeatedly scanning all edges.
- `CreateEdge` hierarchy validation uses endpoint-scoped lookups and adjacency
  traversal.

Public `ListEdges` can retain existing sorted pagination behavior initially. The
critical requirement is that internal validation paths no longer call public-style
full list/merge/sort code.

## Consistency and lifecycle

The adjacency index lifecycle matches the loaded `LocalStore` lifecycle:

```text
first access to a space graph store
  -> LocalStore is opened
  -> committed records are replayed
  -> adjacency index is rebuilt

committed graph mutation
  -> authoritative edge map is updated
  -> adjacency index is updated synchronously

store close/reload/daemon shutdown
  -> LocalStore becomes unreachable/closed
  -> adjacency index is unloaded with it

next open/reload
  -> adjacency index is rebuilt from committed records
```

There is currently no idle eviction mechanism for loaded space stores. The new
index should therefore store edge IDs/endpoints, not cloned edge payloads, to keep
retained memory proportional and modest. If many loaded spaces later create
retained-memory pressure, store-level LRU/idle eviction can be considered as a
separate graph subsystem improvement.

## Testing plan

### Adjacency package tests

Add tests for:

- rebuild with multiple incoming/outgoing edges;
- put new edge;
- put replacing an existing edge with different endpoints;
- delete existing edge;
- delete absent edge as a no-op;
- returned slices cannot mutate internal index state.

### Graph storage tests

Add tests for:

- `IncomingEdges` and `OutgoingEdges` after commit;
- adjacency survives close/reopen/rebuild;
- edge replacement/move updates endpoint mappings;
- edge deletion removes endpoint mappings;
- no full edge list is needed for endpoint-scoped methods.

### Graph service regression tests

Add or update tests for:

- single-parent hierarchy validation;
- hierarchy cycle detection;
- staged overlay edges are visible to hierarchy validation;
- overlay-deleted edges are ignored by hierarchy validation;
- `ListChildren` and `GetParent` preserve behavior.

### Performance reproduction

Use the synthetic Logseq import test to compare before/after behavior:

```sh
MYCEL_RUN_LARGE_LOGSEQ_IMPORT_TEST=1 \
MYCEL_LOGSEQ_IMPORT_TIMEOUT_SECONDS=900 \
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -timeout=20m \
  -cpuprofile=/tmp/logseq-import.cpu.pprof \
  -memprofile=/tmp/logseq-import.mem.pprof
```

Expected profile improvements:

- reduced time in `CreateEdge`;
- reduced time in `listEdgesLocal` / `ListEdges` from internal validation;
- reduced `sort.Slice` / `pdqsort` cost;
- reduced UUID string allocation/churn;
- reduced `mergeEdges`, `cloneEdge`, and `cloneProps` allocations.

## Implementation order

1. Add `internal/graph/adjacency` package and unit tests.
2. Wire `LocalStore` to maintain an adjacency index during rebuild and edge
   apply/delete.
3. Add `IncomingEdges` and `OutgoingEdges` store methods.
4. Add graph service overlay-aware endpoint edge view.
5. Refactor hierarchy internals to use endpoint-scoped edge lookups.
6. Run existing graph/storage/service tests.
7. Run the small synthetic Logseq import test.
8. Run the large synthetic Logseq import profiling test.
9. If needed, optimize `graphChangeEvent` next.
