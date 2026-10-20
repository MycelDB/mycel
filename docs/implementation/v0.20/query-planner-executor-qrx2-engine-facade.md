# QRX2 query engine façade and access ports

QRX2 establishes the daemon-independent engine boundary required by the
[query planner and executor refactoring plan](query-planner-executor-refactoring-implementation-plan.md).
It does not migrate a production plan family. Existing structured Query and GQL
execution and Explain behavior remain authoritative.

## Engine façade

`internal/query/engine` owns three separate operations:

- `Prepare(context.Context, PlanningContext, logical.Query)`;
- `Explain(context.Context, PlanningContext, logical.Query)`;
- `Execute(context.Context, ExecutionContext, PreparedQuery)`.

Planning and execution contexts retain the complete
`sessionmodel.GraphTransaction`. Physical operators can therefore pass the
transaction unchanged to graph access methods and preserve committed snapshot
visibility, read-write overlays, routing identity, and base revision semantics.
`ExecutionContext` also carries a work budget independent of response page
size. Later executor tranches must resolve zero values to configured defaults
before performing work.

## Consumer-owned ports

The engine owns focused capability interfaces rather than depending on daemon
or subsystem service implementations:

- `EntityReader` for bounded node and edge reads;
- `TraversalReader` for adjacency and subtree traversal;
- `IndexReader` for label, tag, and ordered property scans;
- `CatalogReader` for schema and index definitions;
- `SemanticCandidateReader` for ranked semantic candidates;
- `MutationWriter` for read-write plans.

Every graph read, traversal, index, and mutation call accepts the complete graph
transaction. Semantic requests and results use query-owned DTOs; the daemon API
seam converts them to semantic service vocabulary.

The daemon server remains the composition root. It injects the graph manager
into the graph ports, the schema manager into the catalog port, and a daemon
adapter around the semantic manager into the candidate port.

## Legacy bridge

All QRX2 preparations produce `PlanLegacyBridge`. The engine does not invoke an
access port for such plans. `Execute` returns a transport-neutral
`logical.QueryError` categorized as `unsupported_plan` and wrapping
`engine.ErrUnsupportedPlan`.

The daemon adapter observes `Prepare` for execution requests and `Explain` for
Explain requests, but intentionally discards the provisional engine result. It
never calls engine `Execute` in this tranche, so the existing implementation is
the sole execution owner and a request is never executed twice. Later tranches
will replace the legacy marker one plan family at a time.

Canceled contexts are represented by the existing neutral `canceled` query
error category. The other QRX1 error categories remain the vocabulary for
invalid queries, resource exhaustion, stale cursors, unavailable access paths,
dependency failures, and internal failures.

## Architecture checks

Source-relative boundary tests reject production imports from the engine to:

- daemon packages;
- generated API packages or protobuf;
- graph, schema, semantic, session, or space service implementations.

Focused tests cover façade separation, full transaction preservation,
capability discovery, cancellation, unsupported-plan identity, absence of
access work on legacy plans, adapter no-double-execution behavior, and semantic
port conversion.
