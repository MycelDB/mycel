# QRX3 shared binding and semantic analysis

QRX3 implements the shared semantic boundary from the
[query planner and executor refactoring plan](query-planner-executor-refactoring-implementation-plan.md).
It does not migrate execution or public error mapping. Existing structured and
GQL validation, planning, execution, and Explain responses remain authoritative
while the common binder runs observationally.

## Neutral statement family

`internal/query/logical.Statement` now represents both reads and the existing
GQL mutation families:

- node insert and merge;
- matched set and delete;
- relationship create and merge.

`StatementFromGQLPlan` preserves the QRX1 read normalization path and converts
resolved GQL mutation plans to neutral node patterns, match patterns,
assignments, delete targets, relationship endpoints, projections, limits, and
typed values. Mutation execution remains entirely on the existing GQL executor.
GQL scripts normalize and bind each compiled statement before the legacy loop.

The value vocabulary includes an explicit parameter reference for future direct
front-end lowering. The compatibility bridge still receives resolved GQL plan
values; the binder also validates manually supplied parameter bindings and
rejects missing or recursive references.

## Common binder

`internal/query/schema` centralizes side-effect-free type/label, namespaced
field, and endpoint lookup shared by legacy GQL/structured validation and the
new binder. `internal/query/binding` validates a neutral statement and returns a
`BoundStatement` containing deterministic alias metadata and schema type names.
The binder covers:

- unique node, edge, and path aliases and reference scope;
- strict-schema node/edge labels, namespaced fields, and directed endpoint
  constraints;
- intrinsic typed values, finite numbers, dates, collections, objects, and
  parameters;
- predicates, semantic/text shapes, projections, aggregates, implicit grouping,
  output names, and ordering;
- transaction access mode and read-only domain policy;
- pagination, traversal bounds, and node/edge safety limits;
- semantic/text domain capabilities;
- current GQL mutation shapes and their assignments, targets, and endpoints.

The binding environment is immutable and explicit: full
`sessionmodel.GraphTransaction`, optional domain schema, domain policy, safety
limits, and parameters. The shared catalog normalizes a private schema copy. Binding performs no graph entity reads,
index scans, semantic calls, mutations, or storage access.

For compatibility, default traversal profiles preserve current front-end
limits: structured traversal uses the existing depth cap of 64 and requires a
finite maximum, while GQL uses its current depth cap of 5 and maps an open upper
bound to that cap. Both surfaces use the same binder and typed error vocabulary.

## Engine and daemon wiring

The engine façade now accepts `logical.Statement`. `Prepare` invokes the common
binder and stores its result in `PreparedQuery` before selecting the unchanged
`PlanLegacyBridge`. `Explain` uses the same preparation boundary. Engine
`Execute` remains unsupported for every plan family.

The daemon adapter supplies schema and explicit domain policy after existing
schema/domain validation. It observes binder results and errors but discards
them, and it never calls engine `Execute`. Production requests therefore still
execute exactly once through the existing path.

`query_service_schema_structured.go` intentionally remains during the comparison
period. Tests compare its strict-schema outcomes with common binding, verify
structured/GQL binding equivalence, and exercise every normalized GQL mutation
family. Removal of duplicate adapter validation waits for production error
mapping and parity approval in a later tranche.

## Dependency boundary

The source-relative query-engine boundary test scans `internal/query/engine`,
`internal/query/binding`, and `internal/query/schema`. These packages may not
import daemon, generated API, protobuf, or graph/schema/semantic/session/space
service implementation packages. Source parsing includes production files
excluded by the active build context.
