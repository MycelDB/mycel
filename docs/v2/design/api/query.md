# Client Query API

## Status

Implemented daemon-oriented Client Query API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/query.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/session-transaction.md
docs/v2/design/api/graph.md
```

## Purpose

`QueryService` is the transaction-scoped Client API for structured graph queries.

The current daemon MVP executes structured protobuf queries over daemon graph transaction snapshots, including read-your-writes for active read-write transactions.

The v1 query API is a structured protobuf API, not a raw query-string language. It mirrors Mycel's current in-process query builder while leaving room for connector-generated helper APIs.

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
- Supports node and tree projections.
- Supports `and`, `has_tag`, `property_exists`, `property_equals`, and `between` expressions.
- Supports order specs, query limit, and response pagination.
- Scalar projections are minimal and currently return the bound node id for the requested alias.
- The CLI currently exposes the common node-query subset; richer traversal query construction is available via gRPC clients.

## Transaction scoping

Every query request includes:

```text
transaction_id
```

The transaction determines:

- space
- domain
- read snapshot/base revision
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
}
```

## GraphQuery

A graph query contains:

- a match pattern
- optional where expression
- return projections
- order specs
- optional limit

```protobuf
message GraphQuery {
  GraphPattern match = 1;
  optional Expr where = 2;
  repeated ReturnProjection returns = 3;
  repeated OrderSpec order_by = 4;
  int32 limit = 5;
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
- depth min/max
- target node pattern

The current builder only exposes outgoing traversal, but the wire model includes incoming traversal direction as a natural extension.

## Depth

Depth is inclusive:

```text
min_depth <= traversal depth <= max_depth
```

`max_depth = -1` means unbounded, matching the existing `query.Unbounded` constant.

## Expressions

The v1 expression model should support current builder functionality plus explicit metadata predicates:

- property reference
- literal scalar value
- date value
- current date
- date minus days
- between
- and
- has tag
- property exists
- property equals

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

Initial projection kinds:

- node variable
- tree projection
- scalar value, reserved for future/current ordering/filtering convenience

A tree projection returns a forest preserving `contains` edge hierarchy.

## Result model

A result row is a map from output field name to typed query value.

Known value kinds:

- node
- tree
- scalar

Scalar values use `google.protobuf.Value` so string, number, boolean, and null-like values can be represented.

## Pagination

`ExecuteQueryRequest` includes `page_size` and `page_token`.

Implementations may cap page size. The initial implementation may have limitations, but pagination should be part of the API from the start.

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

QueryService reads from the daemon-local transaction snapshot. Query requests are not themselves replicated.

Committed graph mutations and domain revisions determine what query snapshots observe across a mesh. The detailed mesh consistency model is future design work.

## Open questions

- Should v1 include incoming traversal implementation, or only reserve it in the proto?
- Should tree projections include contains edge metadata in addition to nodes?
- Should result pagination be row-based only, or support streaming query results later?
- Should expensive query planning/cost estimation be exposed as a separate method later?
