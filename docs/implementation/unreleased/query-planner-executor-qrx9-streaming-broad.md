# QRX9: Streaming broad fallback and bounded blocking work

QRX9 moves broad read plans from whole-domain daemon materialization into the
transport-neutral query engine. Full scans remain an explicit compatibility
fallback, but they now use finite pull-based access and the same resource
tracker as indexed and traversal plans.

## Native broad plans

`FamilyBoundedFullScan` is a native physical family. Its access path is
`node_scan`, its Explain tree contains an explicit `NodeScan`, and it no longer
requires a legacy bridge. The composed daemon executes normalized structured
and GQL broad reads once through the engine; API-local `queryExecution`,
`allNodes`/`allEdges`, and `gqlDaemonGraph` are not invoked for these prepared
read plans. Removal of the remaining unreachable compatibility helpers and
mutation/script bridges remains QRX11 work.

`NodeScan` calls `ListNodes` only with finite positive limits. It retains at
most one finite page, accounts node payload memory and node/candidate work, and
closes its source when downstream demand is satisfied. Start labels, IDs, and
literal properties are applied before traversal. Predicates that reference
only the start alias are also pushed ahead of expansion; predicates involving
later bindings remain residual.

A small response page therefore reads only the finite page needed for the
response and its lookahead. A finite query `LIMIT` further clamps source page
requests when no continuation can exist after that limit.

## Broad traversal

Broad traversal starts from the paged node scan and uses finite
`ScanAdjacency` pages plus endpoint `GetNode` reads. It never lists all domain
edges.

GQL and explicit path queries use the bounded `PathExpand` operator. Structured
queries without a path alias use `CollectExpand`, preserving the existing
one-row-per-root contract, first target/edge bindings, deduplicated matched
nodes, and tree projections. Only outgoing `contains` edges establish tree
parent/child relationships; incoming and non-containment results remain flat.
All root, edge, endpoint, depth, intermediate-row, and retained-memory work is
charged to the shared tracker.

The complete `sessionmodel.GraphTransaction` is passed to every node page,
adjacency page, and endpoint read, preserving graph-owned snapshot and
read-write overlay semantics.

## Blocking work and pagination

Broad plans expose their blocking operators in Explain and runtime diagnostics:

- ordered queries with a safe finite query limit use memory-accounted Top-N;
- other orderings use the bounded complete sort;
- distinct, grouping, and aggregation use the existing memory- and row-bounded
  neutral operators; and
- budget exhaustion fails with `resource_exhausted` rather than continuing
  unbounded or returning a partial success.

Unordered node-only scans use source checkpoints in the checksum-protected
`qrx7:` cursor envelope (the established prefix remains stable). Blocking and
traversal broad plans use protected output-offset replay. Replay is honest: a
broad ordered page without a supporting index rescans and re-sorts the bounded
candidate set, and diagnostics report `output_offset_replay`; it is not
presented as efficient index continuation. Compatible legacy decimal broad
cursors remain accepted as output offsets, while tampered or plan, parameter,
snapshot, or schema mismatches fail stale.

Structured ordered queries, and schema-declared indexed shapes whose required
index is missing, continue to fail closed with `unavailable_access_path`.
Schemaless broad-searchable GQL retains its legacy scan behavior because no
schema declares a required index. The documented QRX0/QRX4 GQL non-equality and
`BETWEEN` compatibility gate may also select broad ordering; those plans use
Top-N when safe and otherwise the bounded complete sort.

## Diagnostics and verification

Static and runtime diagnostics identify `StreamingNodeScan` or
`StreamingNodeScan+EdgeAdjacencyIndexScan`, the `bounded_full_scan` family,
`node_scan` access, full-scan status, explicit filter/traversal/blocking
operators, cursor strategy, resource limits, and actual nodes, edges,
candidates, adjacency calls, endpoint reads, rows, depth, and peak memory.

Coverage includes:

- finite node pages, early source closure, protected continuation, and
  tampered-cursor rejection;
- structured and GQL daemon reads that bypass whole-graph materialization;
- paged adjacency traversal without `ListEdges`, including structured
  collection and tree compatibility;
- start-predicate pushdown, Top-N planning, output replay, snapshot transaction
  propagation, and caller cancellation after dependency return;
- deterministic memory/resource exhaustion and retained-memory cleanup; and
- full query, daemon API, CLI, build, documentation, architecture-boundary,
  and race validation.
