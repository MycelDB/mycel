# QRX8: Adjacency, path, and subtree migration

QRX8 moves indexed graph traversal from daemon API execution shortcuts into the
transport-neutral physical planner and bounded query runtime introduced by
QRX2–QRX7.

## Migrated plan families

The shared engine now executes these physical families directly:

- explicit-start one-hop adjacency;
- bounded multi-hop paths with explicit-ID, label, tag, or indexed-property
  start sources; and
- ordered-property root scans followed by bounded subtree expansion.

The daemon no longer dispatches these families through its indexed adjacency,
path, subtree, or GQL path shortcuts. Those legacy implementations remain only
as cleanup debt for QRX11; migrated requests execute once through the query
engine.

## Bounded traversal operators

`internal/query/execution` owns three traversal operators:

- `AdjacencySource` pages `ScanAdjacency` for explicit start nodes and emits a
  source checkpoint before each candidate;
- `PathExpand` performs finite, simple-path expansion and materializes only
  memory-accounted output bounded by the shared work budget; and
- `SubtreeExpand` performs a breadth-first traversal per stable indexed root,
  using only finite `ScanAdjacency` calls and finite endpoint reads before
  constructing tree and graph projections.

All operators share one tracker for start nodes, candidates, index entries,
node and edge payloads, adjacency calls, intermediate rows, output rows,
traversal depth, and retained memory. Direct ID, label, tag, property, and
subtree root requests are clamped to remaining node/start capacity before
dependency work. Compound tag/property branches remain separately bounded
candidate work; their final deduplicated set consumes the start-node budget
once, before traversal expansion. Subtree roots are pulled one at a time so
exhaustion stops at a stable root boundary. Every adjacency page is
memory-accounted while it is processed, including filtered edges and pages
retained across recursive path expansion. Caller cancellation wins even if a
dependency returns success after cancellation, and operator close/error paths
release all retained memory.

Open GQL depth bounds are resolved by the physical planner and the resolved
finite bound, rather than the logical `-1` marker, is passed to execution.
Simple paths reject repeated nodes, retain edge order, and preserve path node
and edge payloads. Subtrees apply edge and target patterns during traversal,
preserve hierarchy order for outgoing `contains` edges, preserve legacy flat
roots for incoming or non-`contains` tree projections, and stop at root
boundaries when `max_nodes` or `max_edges` truncates the result.

## Transactions and graph results

Every adjacency page and endpoint read receives the complete
`sessionmodel.GraphTransaction`. Snapshot revision and read-write overlay
visibility therefore remain graph-owned. Regression coverage exercises both a
read-only snapshot across a later commit and uncommitted adjacency, path, and
subtree reads.

Binding rows carry neutral graph nodes and edges in addition to node, edge,
path, and tree bindings. Result assembly uses only returned page rows, so graph
projections cannot leak pagination lookahead work.

Ordered GQL `RETURN GRAPH` subtree shapes normalize directly to a root-node and
tree projection. No execution-only protobuf rewrite is required.

## Continuations

Traversal continuations use the checksum-protected `qrx7:` envelope introduced
in QRX7 (the prefix remains stable for compatibility):

- adjacency stores the explicit start position, adjacency page cursor, and
  within-page candidate offset;
- subtree stores the ordered root-source checkpoint and resumes only at root
  boundaries; and
- path and other blocking shapes use an output offset and deterministically
  replay bounded work.

Continuation identity remains bound to the canonical statement, parameters,
physical access plan, transaction snapshot, and schema version. Compatible
legacy raw adjacency and shaped ordered-root cursors are accepted and upgraded;
incompatible source cursors fail stale rather than being reinterpreted.

For compatibility with the previous indexed subtree API, its query `LIMIT`
continues to cap each response page rather than the complete continuation
sequence. Truncated subtree responses never emit a continuation.

## Compatibility gate

The QRX0/QRX4 GQL non-equality and `BETWEEN` broad-fallback gate remains
unchanged. Traversal shapes inside the normalized compatibility subset use the
shared native planner and executor; the documented GQL node-query fallback is
not silently reclassified.

## Verification

Coverage includes:

- structured/GQL adjacency, path, and subtree parity;
- incoming/outgoing direction, edge and target patterns, variable depth,
  simple-path cycle prevention, containment ordering, and flat-tree legacy
  semantics for incoming and non-containment subtrees;
- finite adjacency limits and absence of subtree-wide/unbounded index calls;
- stable adjacency and root-boundary continuation, legacy cursor upgrade, and
  stale/tampered cursor rejection;
- query limits, subtree per-page limits, adjacency and direct indexed-root
  pre-read budget checks, deduplicated compound-root accounting, truncation,
  adjacency-page memory accounting, and resource exhaustion;
- read-only snapshots and read-write overlays;
- cancellation and retained-memory cleanup; and
- static Explain versus runtime traversal diagnostics.
