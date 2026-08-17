# Client Query API

## Status

Implemented daemon-oriented Client Query API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/query.proto
```

This document depends on:

```text
docs/design/access-control.md
docs/design/api/session-transaction.md
docs/design/api/graph.md
```

## Purpose

`QueryService` is the transaction-scoped Client API for structured graph queries.

The current daemon MVP executes structured protobuf queries through daemon graph transactions. Read-write transactions include read-your-writes from their local overlay. Read-only transactions are linearizable current-read contexts and may observe commits newer than their `base_revision`; they are not historical repeatable snapshots in V1.

The v1 query API is a structured protobuf API, not a raw query-string language. It mirrors Mycel's current in-process query builder while leaving room for connector-generated helper APIs.

Textual GQL and structured protobuf queries normalize into an internal logical query model before physical planning where practical. That model captures aliases, patterns, predicates, projections, aggregates, shaping controls, and predicate pushdown/residual classification without leaking parser internals into lower-level execution code.

Graph/query operations target `transaction_id`, not `session_id`.

## Existing model alignment

The existing Go query builder supports:

- graph pattern match
- outgoing traversal steps
- edge kind filtering
- traversal depth min/max
- node aliases
- optional template-key filter
- property references
- boolean `and`
- `between` comparisons
- date/current-date/minus-days expressions
- node variable projections
- tree projections preserving `contains` hierarchy
- result ordering

`QueryService` should preserve these concepts in a wire-compatible structured shape.

## Scope

`QueryService` includes:

- read-only structured graph pattern queries
- graph traversal predicates
- metadata tag/property predicates composable with graph traversal
- node and tree result projections
- property/scalar filtering
- ordering
- pagination fields

`QueryService` does not include:

- graph mutation
- semantic search
- metadata catalog discovery such as listing all known tags/property names
- blob operations
- template management
- raw query-string execution

Semantic search belongs to `SemanticService`. Metadata catalog discovery belongs to `MetadataCatalogService`. Metadata filtering/search belongs in `QueryService` so graph relationships and metadata predicates can be composed in one query.

## Service definition

```protobuf
service QueryService {
  rpc ExecuteQuery(ExecuteQueryRequest) returns (ExecuteQueryResponse);
}
```

## CLI

The daemon-backed CLI includes a basic node-query helper:

```sh
./bin/mycel -u alice -p '<password>' query nodes --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' query nodes --transaction-id '<tx-id>' --tag test1
./bin/mycel -u alice -p '<password>' query nodes --transaction-id '<tx-id>' --property-exists status
./bin/mycel -u alice -p '<password>' query nodes --transaction-id '<tx-id>' --property-equals status=active
./bin/mycel -u alice -p '<password>' query nodes --transaction-id '<tx-id>' --template-key logseq.journal
```

These commands construct a `GraphQuery` with start alias `n` and return the matched node as `node`.

## Current implementation notes

- Supports transaction-scoped node pattern starts.
- Supports linear traversal steps in the protobuf API for `out` and `in` directions.
- Supports node, edge, tree, scalar, and path projections for accepted indexed shapes.
- Supports `and`, `or`, `has_tag`, `property_exists`, `property_equals`, null predicates, string/text/semantic predicates, `between`, and less-than expressions.
- Supports aggregate count projections, distinct rows, order specs, offset, query limit, and response pagination.
- Scalar projections can address `alias.property`, `alias.payload.field`, or `alias.meta.field`; a plain `alias` scalar projection preserves the legacy node-id behavior.
- The CLI currently exposes the common node-query subset; richer traversal query construction is available via gRPC clients.

## Transaction scoping

Every query request includes:

```text
transaction_id
```

The transaction determines:

- space
- domain
- read context/base revision
- authorization context
- read-your-writes behavior for read-write transactions

QueryService performs reads only. It may execute against either a read-only or read-write transaction.

## Structured query shape

Recommended request shape:

```protobuf
message ExecuteQueryRequest {
  string transaction_id = 1;
  GraphQuery query = 2;
  int32 page_size = 3;
  string page_token = 4;
}
```

Recommended response shape:

```protobuf
message ExecuteQueryResponse {
  repeated QueryRow rows = 1;
  string next_page_token = 2;
  QueryResult result = 3;
  ReadMetadata read_metadata = 4;
  QueryDiagnostics diagnostics = 5;
}

message ExplainQueryRequest {
  string transaction_id = 1;
  GraphQuery query = 2;
  ReadOptions read_options = 3;
}

message ExplainQueryResponse {
  QueryDiagnostics diagnostics = 1;
}

message ExplainGQLRequest {
  string transaction_id = 1;
  string query = 2;
  map<string, google.protobuf.Value> params = 3;
  ReadOptions read_options = 4;
}

message ExplainGQLResponse {
  QueryDiagnostics diagnostics = 1;
}
```

## GraphQuery

A graph query contains:

- a match pattern
- optional where expression
- return projections
- order specs
- optional limit
- optional traversal safety caps (`max_nodes`, `max_edges`)

```protobuf
message GraphQuery {
  GraphPattern match = 1;
  optional Expr where = 2;
  repeated ReturnProjection returns = 3;
  repeated OrderSpec order_by = 4;
  int32 limit = 5;
  int32 max_nodes = 6;
  int32 max_edges = 7;
  string path_alias = 8;
  repeated AggregateProjection aggregate_returns = 9;
  bool distinct = 10;
  int32 offset = 11;
}
```

## Pattern model

The pattern model describes one linear graph traversal.

```text
start node pattern
  -> traversal step
  -> node pattern
  -> traversal step
  -> node pattern
```

A node pattern includes:

- alias
- optional template key

A traversal step includes:

- direction
- edge kind
- optional edge alias
- depth min/max
- target node pattern

The current builder only exposes outgoing traversal, but the wire model includes incoming traversal direction as a natural extension.

## Depth

Depth is inclusive:

```text
min_depth <= traversal depth <= max_depth
```

`max_depth = -1` is reserved as the historical unbounded marker, but the daemon QueryService rejects unbounded structured traversal for now. Accepted structured traversal must use a finite `max_depth` with `0 <= min_depth <= max_depth <= 64`. Textual GQL fallback caps unbounded variable-length traversal internally, but unbounded GQL is not accepted production query support.

## Expressions

The v1 expression model supports current builder functionality plus explicit metadata predicates:

- property reference
- literal scalar value
- date value
- current date
- date minus days
- between
- less than
- and
- or
- has tag
- property exists
- property equals
- null checks
- string predicates
- text predicates
- semantic predicates

The wire model can use recursive oneof expressions.

Property references identify values by:

```text
alias + property name
```

Explicit metadata predicates identify canonical metadata by:

```text
alias + tag
alias + property name
alias + property name + scalar value
```

The daemon applies Mycel metadata normalization rules for tags and custom property names before evaluating metadata predicates. These explicit predicates exist because tags and custom properties have canonical normalization/indexing semantics that are not captured by a generic property reference alone.

## Return projections

Projection kinds:

- node variable
- edge variable
- tree projection
- scalar value
- path value

A tree projection returns a forest preserving `contains` edge hierarchy. A path projection returns ordered path nodes and edges through `QueryValue.path`. Scalar projections use `ReturnProjection.alias` as a field reference in the current wire shape: `n.title`, `n.payload.text`, `n.meta.created_at`, or edge equivalents such as `r.weight`. `ReturnProjection.output_name` controls the row field name.

## Result model

A result row is a map from output field name to typed query value.

Known value kinds:

- node
- edge
- tree
- scalar
- path

Scalar values use `google.protobuf.Value` so string, number, boolean, and null-like values can be represented. Path values are first-class `PathValue` messages rather than scalar fallback envelopes.

## Pagination

`ExecuteQueryRequest` includes `page_size` and `page_token`.

Implementations may cap page size. Indexed query plans use opaque cursors; accepted ordered-index node plans wrap the underlying index-key cursor with shaping state so first-page offsets and `FETCH` limits remain stable across pages.

## Accepted predicate and shaping semantics

QPC0 freezes these MVP semantics so GQL and structured API behavior can converge:

- Boolean precedence is `AND` before `OR`; parentheses explicitly group predicates.
- Missing fields and explicit null values are treated as null for `IS NULL` / structured `NullExpr(is_null=true)`. `IS NOT NULL` / `is_null=false` requires a present non-null field value.
- Property equality compares normalized property values using the daemon query value comparison rules. Missing values do not match equality predicates.
- String predicates (`CONTAINS`, `STARTS WITH`, `ENDS WITH`) are case-insensitive and evaluate against the string rendering of the target value.
- Text predicates remain case-insensitive textual fallback predicates unless bounded by supported indexed property plans. Semantic predicates have an accepted vector-index execution slice for single-label, node-only start-alias predicates; the daemon routes those through the semantic subsystem and preserves semantic-score order. Semantic textual fallback is only development fallback for broad-searchable domains when the accepted vector shape cannot be planned.
- `COUNT(*)` counts matched rows. `COUNT(alias)` counts rows where the alias is bound as a node, edge, or path. `COUNT(value)` counts rows where the value expression is non-null. `SUM`, `AVG`, `MIN`, and `MAX` require value arguments. Missing/null values are skipped; `SUM` over no values returns `0`, while `AVG`/`MIN`/`MAX` over no values return null. When non-aggregate returns are present, aggregation groups by the projected return row.
- Result shaping order is stable: project or aggregate/group, apply `DISTINCT` to encoded projected rows, apply `ORDER BY`, apply zero-based non-negative `OFFSET`, apply `limit` / `FETCH`, then apply response cursor pagination.
- General `ORDER BY` is production-supported for accepted indexed ordered-property node plans. Broad-searchable GQL fallback can use in-memory ordering for development/exploration; that fallback is not production support for non-broad-searchable domains.

## Path semantics

- A first-class path value contains ordered `nodes` and ordered `edges`; for a valid path, `len(nodes) == len(edges) + 1`.
- Path traversal does not repeat a node within the same path branch. This avoids simple cycles but does not attempt exhaustive cyclic path enumeration.
- Edge-distinct paths are preserved: two different edges between the same endpoints produce two path rows when both satisfy the pattern.
- Structured path traversal accepted for production currently requires directed edge labels and finite depth bounds. Starts may be explicit node IDs, label-index starts, structured tag-index starts, or schema-backed ordered-property indexed starts for supported start-alias predicates.
- Indexed path execution fails closed when start-node, row, loaded-node, loaded-edge, or depth caps are exceeded; callers should add indexed start predicates or reduce traversal depth.
- Indexed root-subtree traversal uses `limit` and cursors for root rows; descendants are bounded by `max_nodes`, `max_edges`, and depth caps.
- When graph expansion hits `max_nodes` or `max_edges`, diagnostics set truncation fields and execution does not fall back to a full scan.

## Indexed execution and diagnostics

The accepted production paths for single-label node starts use schema-declared ordered property indexes. `PropertyEqualsExpr` on the start alias is planned as `OrderedNodePropertyEqualityIndexScan`; compound indexed predicates are planned as `OrderedNodePropertyPredicateIndexScan`; `ORDER BY` on one indexed property is planned as `OrderedNodePropertyIndexScan`. Missing indexes fail closed rather than silently scanning the domain. The ordered path also supports bounds on the ordered property, including inclusive `BetweenExpr` bounds and strict upper bounds via `LessThanExpr`, so callers can ask for entries before a timestamp sorted descending with a limit.

`OrderedNodePropertyPredicateIndexScan` supports indexed property equality, property-exists scans for indexed fields, inclusive `BETWEEN`, strict less-than, `AND` intersection, and `OR` union over indexed predicate branches. String and text predicates over schema-indexed property fields use the ordered property index to bound the candidate set, then apply the case-insensitive `CONTAINS`, `STARTS WITH`, `ENDS WITH`, or `TEXT_CONTAINS` predicate as a residual filter. Non-indexed residual predicates may be evaluated only after an indexed candidate set has bounded the row set. Tag-specific pushdown is intentionally deferred until mycel exposes a tag index in the graph/query planner surface. True full-text index ranking/pushdown is separate from this ordered-property bounded residual path.

The indexed-root subtree path combines that ordered root scan with adjacency-index traversal. A query with a single ordered root label, one traversal step, finite or safety-capped depth, `limit`, and optional `max_nodes`/`max_edges` first selects root rows from the ordered property index, then expands only those roots through the edge adjacency index. `limit` and `page_token` apply to root nodes, not descendants. The response `result.graph` includes selected roots, traversed descendant nodes, and traversed edges. If `max_nodes` or `max_edges` is hit, diagnostics set `truncated=true` and include `truncation_reason`; execution does not fall back to a full scan.

Example structured shape:

```text
match.start = (d:pkm.journal)
where = d.journal_day BETWEEN 20260701 AND 20260731
order_by = d.journal_day DESC
limit = 7
match.steps[0] = d -[:contains*0..2]-> n
returns = node(d), tree(n)
max_nodes/max_edges = caller safety caps
```

`QueryDiagnostics` reports planner name/version, plan shape, plan kind, indexes used, pushed and residual predicate summaries, fallback or rejection reasons, required indexes, full-scan status, index entries scanned, candidate and row counts, loaded nodes/edges, root count, truncation state, root scan/expansion/planning/execution/shaping timing, adjacency scan calls, node read calls, rows returned, and cursor kind. For the indexed journal subtree query, diagnostics should show `planner=mycel-query`, `plan=OrderedNodePropertyIndexScan+EdgeAdjacencyIndexScan`, `plan_kind=indexed`, `full_scan=false`, the ordered journal index, an adjacency index such as `out:contains`, and `next_cursor_kind=root_index_key`.

`ExplainQuery` and `ExplainGQL` return `QueryDiagnostics` with `explain_only=true` and do not execute graph reads or mutations. Accepted indexed shapes report their selected indexes and `full_scan=false`. Rejected production shapes report `plan_kind=rejected`, `rejected_reason`, and `required_index` when applicable. Broad-searchable development fallback plans report `plan_kind=fallback`, `full_scan=true`, and a `fallback_mode` string.

## Authorization

All query operations require:

```text
query.run
```

and generally also require readable graph access for the transaction's space/domain:

```text
graph.read
```

The transaction must be active and readable.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| transaction not found or expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| missing query capability | `PERMISSION_DENIED` |
| malformed query | `INVALID_ARGUMENT` |
| incomplete traversal pattern | `INVALID_ARGUMENT` |
| unknown alias | `INVALID_ARGUMENT` |
| unsupported expression/value comparison | `INVALID_ARGUMENT` |
| query too expensive | `RESOURCE_EXHAUSTED` |
| transaction conflict/abort | `ABORTED` |
| service unavailable | `UNAVAILABLE` |

## Connectors

Connectors should offer idiomatic query builders over this structured API.

For example:

```text
tx.query()
  .match(pattern().node("journal", template("journal")).out("contains", depth(1, -1)).node("entry"))
  .where(...)
  .return(node("journal"), tree("entry").as("entries"))
```

The wire API remains structured protobuf; connectors provide ergonomic builders.

## Mesh implications

QueryService reads through the daemon graph manager for the target transaction. In raft mode, read-only query reads inherit graph strong-read behavior and may observe newer committed revisions than the transaction `base_revision`; read-write query reads include staged overlay mutations on the transaction home node. Query responses include optional `read_metadata` with `strong` read-index/apply proof details or `overlay` context where applicable. Query requests include optional `read_options`; `allow_stale=true` is rejected by the current daemon because no stale-read daemon config/implementation is enabled. Query requests are not themselves replicated.

Committed graph mutations and domain revisions determine what query read contexts observe across a mesh. The detailed mesh consistency model is covered by the Phase F read-consistency plan.

## Open questions

- Should v1 include incoming traversal implementation, or only reserve it in the proto?
- Should tree projections include contains edge metadata in addition to nodes?
- Should result pagination be row-based only, or support streaming query results later?
- Should expensive query planning/cost estimation be exposed as a separate method later?
