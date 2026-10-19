# QRX11: query adapter cleanup and native execution ownership

## Status

Merged into `improved_querying` through PR #228 for [mycel#213](https://github.com/MycelDB/mycel/issues/213). Post-QRX0–QRX11 aggregate-review corrections are implemented and independently accepted on the follow-up branch; CI/SonarCloud, integration-branch merge, and promotion to `develop` remain separately gated.

## Scope

QRX11 completes the ownership move started in QRX0. Every accepted structured query, GQL read, supported GQL mutation, and GQL script statement now prepares and executes through `internal/query`. The daemon API package owns transport and request context only.

The request path is:

```text
gRPC request
  -> routing, identity, active transaction, domain, and schema lookup
  -> structured protobuf mapping or GQL compilation
  -> neutral statement binding and physical planning
  -> native bounded operator execution
  -> neutral result/diagnostics
  -> protobuf response and gRPC error mapping
```

The daemon remains the composition root and injects graph entity, traversal, index, catalog, semantic-candidate, mutation, and transaction-validation capabilities into the engine.

## Removed migration surfaces

QRX11 removes the temporary physical and engine ownership bridge:

- `physical.OperatorLegacyBridge`, `FamilyLegacyMutation`, `Plan.RequiresLegacy`, and bridge diagnostics;
- `engine.PlanKind`, `PlanLegacyBridge`, `PlanIndexed`, `PreparedQuery.RequiresLegacy`, `ExecutionContext.LegacyRows`, and unsupported-plan handoff helpers;
- adapter-owned indexed-shape recognition, matching, traversal, whole-graph hydration, predicate evaluation, aggregation, sorting, result shaping, pagination, and query cursor codecs;
- the GQL-plan-to-structured-protobuf execution bridge;
- `query_engine_legacy_source.go` and the duplicate strict-schema validator in `query_service_schema_structured.go`.

`query_service.go` no longer receives or stores a graph manager. Its handlers route requests, resolve request context, map inputs, invoke prepare/explain/execute or script application services, record read metadata, and map neutral outputs/errors.

The bounded materialized-row operator is now named `execution.SliceSource`; it is used for finite internal result slices and no longer implies a daemon handoff.

## Native ownership and compatibility

Physical planning always emits the executable operator tree for the selected family. Invalid prepared states fail as internal query errors rather than requesting a second executor. Explain remains side-effect-free and serializes the same physical plan used by execution.

Compatibility retained at the boundary includes:

- active transaction, domain visibility, read-only/read-write, snapshot, overlay, and strong-read behavior;
- finite structured traversal safety and the established gRPC failure category for open or over-cap depth;
- default structured start-node projection when no return or aggregate projection is supplied;
- runtime node/edge identity fields (`node_id`, `edge_id`, `from_node_id`, and `to_node_id`) under strict schemas;
- ordered-property-bounded `TEXT_CONTAINS` execution in explicitly searchable domains without requiring a standalone full-text-index capability; disabled-domain text predicates fail closed during binding;
- compatible historical indexed cursors only for source-safe plan families and unchanged transaction views;
- the documented QRX0/QRX4 GQL non-equality and `BETWEEN` broad-plan compatibility gates.

Historical cursor decoding remains in query core because it is a public continuation compatibility requirement, not an execution bridge. Search pagination has a separate historical-token contract and is outside the query-engine cleanup.

## Boundaries

Source-relative tests enforce that query-core production packages do not import daemon, generated protobuf, gRPC, or service implementation packages, including build-constrained Go files. A daemon adapter boundary test prevents query service sources from importing graph-query/operator executors or reintroducing graph scan and legacy bridge helpers.

Generated protobuf conversion remains under `internal/daemon/api/client`. Query-core packages exchange transport-neutral statements, values, rows, graph envelopes, counters, diagnostics, and typed query errors.

## Validation

QRX11 coverage includes:

- query package compile and full tests;
- daemon client compile and full behavior tests through the native engine;
- structured/GQL result, graph, pagination, diagnostics, snapshot, overlay, semantic, traversal, mutation, and script compatibility;
- Raft transaction-home routing and strong-read metadata;
- dependency cancellation and fail-closed error mapping;
- source-relative import and adapter-ownership checks.

The merge gate additionally requires focused race tests, `make test`, `make build`, `make docs-check`, daemon-only/public-surface checks, `git diff --check`, independent exact-head review, CI, and SonarCloud. Broad daemon-client race runs can still expose the known pre-existing consensus waiter race in `consensus.Group.completeWaiter` versus `Propose`; query-only and non-Raft daemon query race suites are the attribution gate for this tranche.

## Aggregate review resolution

The complete QRX0–QRX11 range received independent architecture, transaction/resource-safety, and compatibility review. Three exact-head follow-up reviews verified the corrections and their boundedness/accounting seams; the final review found no issues and returned `Merge verdict: OK`. Follow-up corrections:

- distinguish missing values from explicit null during equality while retaining `IS NULL` compatibility;
- apply `DISTINCT` to projected rows before ordering;
- reject mutation continuations and roll back statements whose returned mutation rows cannot fit one response page;
- page node-delete incident-edge validation through finite wildcard incoming/outgoing adjacency indexes rather than whole-domain edge materialization;
- bind semantic cursors to deterministic ranked-candidate identity;
- execute default GQL undirected relationships through finite outgoing/incoming adjacency scans with self-loop deduplication;
- back current and historical ordered-property scans with derived ordered trees, finite examine limits, and actual examined-entry/node-load accounting; read-write overlay merges are preflighted against the supplied examine bound and fail closed before snapshotting when they cannot fit;
- execute revision-pinned label and tag scans through finite snapshot pages rather than loading the complete visible domain;
- refresh the QRX0 inventory so resolved bridge-era mismatches are no longer documented as current behavior.

## Explicit follow-ups

This tranche does not add cardinality statistics, cost-based optimization, disk spill, public range/date filter APIs, indexed sorting metadata, or explicit search capability metadata. Snapshot retention/pinning/GC and restore acceptance remain tracked separately from #213.
