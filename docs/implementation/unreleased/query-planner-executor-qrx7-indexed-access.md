# QRX7: Indexed node and semantic plan migration

QRX7 moves the first storage-backed read families from the daemon API adapter
into the transport-neutral query engine introduced by QRX2–QRX6.

## Migrated plan families

The physical planner and shared executor now own:

- explicit node-ID lookup;
- label index scan;
- tag index scan, including `AND` intersection and `OR` union/deduplication;
- ordered node-property index scan;
- equality and range property scans;
- compound indexed predicate intersection/union; and
- semantic candidate scan with fail-closed node hydration.

Adjacency, path, subtree, mutation, and broad fallback plans remain on their
explicit legacy bridges for later tranches.

## Bounded access execution

`internal/query/execution.PagedNodeSource` adapts cursor-based graph access to
the bounded `Open`/`Next`/`Close` operator lifecycle. Every new index request
has a finite positive limit. Sources continue through underlying cursors only
until response demand is met, the source ends, cancellation occurs, or the
shared work budget is exhausted.

Start-node labels, IDs, and literal properties are checked after access.
Residual predicates run after candidate production. Ordered-property plans
preserve the index order instead of sorting the same rows again. Compound and
tag plans use explicit memory-accounted set operators; intersections and
unions are deduplicated and emitted in deterministic node-ID order.

Semantic execution preserves candidate rank, deduplicates repeated node IDs,
hydrates only finite requested batches, and fails closed on candidate-search or
hydration errors.

## Cursor transition

Native QRX7 continuations use a `qrx7:` envelope bound to:

- the canonical statement, bound parameter values, and physical access plan;
- transaction ID and base revision; and
- schema version.

Streaming source cursors retain the underlying page cursor plus the offset of
the response lookahead row. Materialized set/blocking shapes use a bounded
output offset. Compatible QRX6 decimal offsets, shaped index cursors, and raw
ordered-index cursors remain accepted; successful continuation emits the new
envelope.

A changed plan, snapshot, schema, malformed position, or unsupported cursor
fails with the typed stale-cursor error.

## Daemon boundary

Structured queries and normalized GQL reads prepare once and execute native
indexed plans through `internal/query/engine`. Migrated GQL plans no longer
round-trip through protobuf `GraphQuery`, and migrated plans do not enter the
daemon-local `queryExecution` implementation. The daemon still owns protobuf
conversion, gRPC status mapping, read metadata recording, and dependency
composition.

Explain remains side-effect-free. Runtime index configuration happens only
during execution, before ordered-property scans.

## Verification

Coverage includes:

- structured/GQL shared-plan execution without broad legacy reads;
- finite scan limits and multi-page residual filtering;
- explicit-ID, label, tag, ordered, equality/range, compound, and semantic
  families;
- deterministic intersection/union and released retained memory;
- source checkpoint continuation and legacy cursor upgrade;
- plan/snapshot cursor rejection;
- semantic rank, bounded hydration, and fail-closed errors; and
- cancellation, budget, operator lifecycle, and transport mapping regressions.
