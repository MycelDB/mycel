# Graph adjacency index implementation plan

## Status

Proposed. This plan implements the design in
[Graph adjacency index](../../design/graph/graph-adjacency-index.md).

## Goal

Add a derived, in-memory graph adjacency index per loaded space graph store so
internal hierarchy validation and endpoint-scoped traversal avoid repeated
full-edge scans, global sorts, and clone-heavy merge paths. The work should make
Logseq-shaped imports substantially faster while preserving existing graph
storage authority, Raft apply semantics, WAL replay behavior, and public list API
semantics.

## Scope

In scope:

- `internal/graph/adjacency` package and unit tests.
- `graphstorage.LocalStore` ownership of a per-space edge adjacency index.
- Synchronous index updates during edge put/delete apply paths.
- Endpoint-scoped storage methods for incoming/outgoing edges.
- Overlay-aware graph service helpers for hierarchy validation.
- Refactoring internal hierarchy paths to use endpoint-scoped lookups.
- Validation with existing tests and the synthetic Logseq import workload.

Out of scope:

- Persisting the adjacency index as authoritative state.
- Public API contract changes.
- Public graph-change callback/subscription changes.
- Store idle eviction/LRU behavior.
- Automatic repair, restore, merge, rebalance, or authoritative-node selection.
- Semantic worker/provider performance optimization, except as indirectly affected
  by faster graph commit paths.

## Constraints

- Authoritative graph state remains committed graph records and their Raft/WAL
  apply paths.
- The index is derived and rebuildable.
- The index must not be maintained through graph-change callbacks.
- The index must be updated synchronously in the same store mutation path that
  applies committed edge changes.
- Existing public `ListEdges` pagination/sorting behavior should remain stable
  unless deliberately changed in a later plan.
- Each tranche must leave the graph subsystem functional.

## Current bottleneck summary

The large synthetic Logseq import profiling run timed out after 900 seconds near
op 37,661 of about 40,100 operations. Profiles showed dominant time and
allocation in:

- `graphservice.CreateEdge`
- `graphservice.listEdgesLocal`
- `graphstorage.LocalStore.ListEdges`
- `sort.Slice` / `pdqsort`
- `graphservice.mergeEdges`
- `graphservice.graphChangeEvent`
- `graphservice.storeHierarchyParent`
- `graphservice.hierarchyPathExists`
- `graphservice.parentEdge` / `parentEdgeLocal`
- repeated `uuid.UUID.String()` calls

This indicates O(N²)-style edge handling for hierarchy-heavy imports.

## Phase 1 — Add adjacency package

### Tasks

1. Create package:

   ```text
   internal/graph/adjacency
   ```

2. Add interface in `index.go`:

   ```go
   type EdgeIndex interface {
       Rebuild(ctx context.Context, edges []graph.Edge) error
       Put(ctx context.Context, edge graph.Edge) error
       Delete(ctx context.Context, edge graph.Edge) error
       Incoming(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error)
       Outgoing(ctx context.Context, nodeID graph.NodeID) ([]graph.EdgeID, error)
   }
   ```

3. Add in-memory implementation in `memory.go`.
4. Add constructor:

   ```go
   func NewMemoryEdgeIndex() EdgeIndex
   ```

5. Keep implementation edge-ID based, not edge-body based.
6. Ensure `Put` replaces old endpoint mappings for an existing edge ID.
7. Ensure query methods return defensive copies.

### Tests

Add `internal/graph/adjacency/memory_test.go` covering:

- rebuild with multiple incoming/outgoing edges;
- put new edge;
- put replacing an existing edge with different endpoints;
- delete existing edge;
- delete absent edge as no-op;
- returned slices do not mutate internal state.

### Validation

```sh
go test ./internal/graph/adjacency
```

## Phase 2 — Wire LocalStore to maintain the index

### Tasks

1. Add adjacency index field to `graphstorage.LocalStore`.
2. Initialize it in `resetIndexes`.
3. Update `applyEdgePut` to update the adjacency index after authoritative edge
   maps are updated.
4. Update `applyEdgeDelete` to remove the previous edge from the adjacency index.
5. Ensure rebuild/reopen uses the same `applyEdgePut` and `applyEdgeDelete`
   paths so index rebuild behavior matches commit apply behavior.
6. Avoid callback/subscription involvement.

### Tests

Extend `internal/graph/storage/store_test.go` with coverage for:

- incoming/outgoing index state after a committed transaction;
- index state after store close/reopen;
- edge delete removes endpoint mappings;
- edge replacement or move updates endpoint mappings if supported by existing
  storage semantics.

### Validation

```sh
go test ./internal/graph/storage
```

## Phase 3 — Add endpoint-scoped store methods

### Tasks

Add methods to `graphstorage.LocalStore`:

```go
func (s *LocalStore) IncomingEdges(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
func (s *LocalStore) OutgoingEdges(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
```

Implementation notes:

- Lock consistently with existing store methods.
- Call adjacency index for edge IDs.
- Resolve edge bodies from `edgeRecords`.
- Return edge copies at the storage boundary.
- Do not call `ListEdges`.
- Do not globally sort all space edges.

### Tests

Add or extend storage tests for:

- `IncomingEdges` returns only edges targeting a node;
- `OutgoingEdges` returns only edges from a node;
- methods work after reopen/rebuild;
- methods return copies that callers cannot use to mutate store state.

### Validation

```sh
go test ./internal/graph/storage
```

## Phase 4 — Add overlay-aware endpoint edge view in graph service

### Tasks

1. Add a graph service helper that combines committed endpoint lookups with a
   transaction overlay:

   ```go
   type transactionEdgeView struct {
       store    *graphstorage.LocalStore
       overlay  *overlay
       domainID graph.DomainID
   }
   ```

2. Add methods:

   ```go
   func (v transactionEdgeView) incoming(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
   func (v transactionEdgeView) outgoing(ctx context.Context, nodeID graph.NodeID) ([]graph.Edge, error)
   ```

3. Merge rules:

   - start with `LocalStore.IncomingEdges` / `OutgoingEdges`;
   - omit committed edges deleted in the overlay;
   - include staged overlay `putEdges` matching the endpoint;
   - preserve domain filtering where the existing service expects it;
   - avoid full-edge sorting for validation paths.

4. Keep public list behavior unchanged initially.

### Tests

Add graph service tests for:

- staged incoming edge is visible to parent validation;
- staged outgoing edge is visible to path validation;
- overlay-deleted committed edge is ignored;
- behavior matches current hierarchy semantics.

### Validation

```sh
go test ./internal/graph/service
```

## Phase 5 — Refactor hierarchy internals

### Tasks

Refactor internal hierarchy helpers to use endpoint-scoped lookups:

- `storeHierarchyParent`
- `parentEdgeLocal`
- `listChildrenLocal`
- `hierarchyPathExists`

Expected behavior:

- `GetParent(child)` uses incoming edges for `child`.
- `ListChildren(parent)` uses outgoing edges for `parent`.
- `hierarchyPathExists(from, target)` performs DFS/BFS through outgoing
  hierarchy edges.
- `CreateEdge` single-parent and acyclic checks no longer call generic
  full-space `ListEdges`.

Implementation notes:

- Maintain existing schema-based hierarchy label/policy behavior.
- Avoid repeated policy resolution inside tight loops where possible. If needed,
  resolve the hierarchy labels/policy once per validation call.
- Ensure transaction overlays are included for correctness.

### Tests

Run and extend tests for:

- single-parent hierarchy rejection;
- hierarchy cycle rejection;
- hierarchy moves/reorders if applicable;
- `ListChildren` and `GetParent` behavior;
- graph schema validation behavior that depends on hierarchy edges.

### Validation

```sh
go test ./internal/graph/service
```

## Phase 6 — Validate graph subsystem and raft integration

### Tasks

Run broader graph and daemon app tests to ensure Raft/WAL/snapshot paths still
apply edge changes correctly.

### Validation

```sh
go test ./internal/graph/...
go test ./internal/daemon/app
```

If time permits, run the full mycel test suite:

```sh
MYCEL_API_ROOT=../mycel-api make test
make docs-check
git diff --check
```

## Phase 7 — Re-profile synthetic Logseq import

### Small correctness run

```sh
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -v
```

Expected default workload:

- `pages=4`
- `blocks_per_page=10`
- `refs_per_block=1`
- `ops=124`
- `commits=3`

### Large profiling run

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

Expected improvements:

- Large import should complete or progress substantially faster than the baseline
  failure near op 37,661 at 900 seconds.
- CPU time in `CreateEdge` should drop.
- Internal hierarchy paths should no longer spend significant time in
  `listEdgesLocal`, `ListEdges`, `sort.Slice`, or `mergeEdges`.
- Allocation volume from `uuid.UUID.String`, `cloneEdge`, `cloneProps`, and
  `mergeEdges` should drop significantly.

## Phase 8 — Follow-up hotspot review

If the large import still fails or remains too slow, inspect the new profile.
Likely next targets:

1. `graphChangeEvent`
   - Avoid full edge scans for old/new hierarchy parent context.
   - Use endpoint-scoped parent lookups.
2. UUID string churn
   - Avoid UUID string conversion inside sort comparators/hot loops.
   - Precompute sort keys where sorting remains necessary.
3. Public `ListEdges`
   - Optimize public list paths separately if they become the next bottleneck.
4. Semantic maintenance persistence
   - Revisit dirty-event/work-item persistence only after graph edge/hierarchy
     bottlenecks are reduced.

## Acceptance criteria

- `internal/graph/adjacency` has focused unit coverage.
- `LocalStore` rebuilds and maintains adjacency index deterministically.
- Endpoint-scoped store methods do not call full `ListEdges`.
- Internal hierarchy validation paths avoid full-space edge scans and global edge
  sorting.
- Existing graph behavior is preserved by tests.
- Small synthetic Logseq import test passes.
- Large synthetic Logseq import profile shows reduced `CreateEdge` /
  `ListEdges` / `sort.Slice` / `mergeEdges` dominance.
- No public API contract changes are introduced.
- No graph-change callback dependency is introduced for maintaining the index.

## Rollback plan

Because the adjacency index is derived and in-memory, rollback is straightforward:

1. Revert graph service hierarchy refactor to previous full-list behavior.
2. Remove endpoint-scoped store method usage.
3. Remove LocalStore adjacency index field/update calls.
4. Remove `internal/graph/adjacency` package.

No persisted data migration should be required.
