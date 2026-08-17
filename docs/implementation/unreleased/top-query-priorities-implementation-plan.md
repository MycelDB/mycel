# GQL and Structured API Top Query Priorities Implementation Plan

## Status

Implemented for the initial QP0-QP7 MVP. This plan covers the implemented baseline and remaining follow-ups for the three highest-value query feature bundles across both query surfaces:

- textual Mycel GQL via `QueryService.ExecuteGQL` / `ExecuteGQLScript`;
- structured protobuf queries via `QueryService.ExecuteQuery`.

Roadmap sources:

- [GQL roadmap](../../roadmap/gql-roadmap.md)
- [Structured Query API roadmap](../../roadmap/api-roadmap.md)
- [Query expansion design](../../design/api/query-expansion.md)
- [GWL indexes and query planning](../../design/schema/gwl-indexes-and-query-planning.md)

## Goals

Implement these three feature bundles for both GQL and the structured API. The initial implementation includes first-class path values, explicit-start indexed structured path traversal, GQL/API `COUNT`/distinct/offset basics, and GQL/API boolean/null/string predicate fallback evaluation:

1. **Indexed multi-hop traversal and path projection**
   - GQL: keep path syntax canonical and move accepted path execution toward the same indexed traversal engine.
   - API: add first-class path projection/value support and accepted indexed multi-hop / bounded-depth traversal plans.
2. **Aggregation and result shaping**
   - GQL: add `COUNT`, `RETURN DISTINCT`, `OFFSET`, and grouped count basics.
   - API: add protobuf aggregate/distinct/offset shapes after GQL semantics are settled, backed by the same execution stages.
3. **Predicate expressiveness and index pushdown**
   - GQL: add boolean expression trees, `OR`, parentheses, null checks, and string predicates.
   - API: add compatible protobuf expressions and accepted indexed plans for tag/property-exists/compound/text/semantic predicates.

The plan must preserve these principles:

- mycel remains standalone and domain-agnostic;
- daemon/API authorization remains authoritative;
- frontend capability gates remain UX hints only;
- generated protobuf files are regenerated, not hand-edited;
- full-domain fallback behavior is not considered accepted production query support;
- unsupported production query shapes fail closed unless a domain explicitly allows broad searchable fallback.

## Non-goals

- Do not introduce Knot PKM-specific graph concepts.
- Do not convert Mycel Console from Tauri to a web console.
- Do not add pricing, credits, billing, or raw API-key concepts.
- Do not require backward compatibility for prototype query fallback behavior.
- Do not implement broader write syntax such as standard `CREATE` in this tranche.

## Current implementation summary

### Current GQL surface

Implemented GQL capabilities include:

- `MATCH`, `RETURN`, `RETURN GRAPH`, aliases, `ORDER BY`, and `FETCH FIRST`;
- parameters;
- `MATCH ... SET ... RETURN`;
- `MATCH ... DELETE ... RETURN`;
- node `MERGE` and matched-endpoint relationship `MERGE`;
- comparison predicates and `BETWEEN`;
- path binding/projection, including `MATCH path = (...) RETURN GRAPH path`;
- local/fallback `TEXT_CONTAINS(...)` and `SEMANTIC_SIMILAR(...)` predicates.

Important GQL gaps:

- broader aggregate functions beyond `COUNT`;
- advanced grouped aggregate semantics beyond the initial grouping support;
- no true full-text or semantic index pushdown.

### Current structured API surface

`mycel-api/api/proto/mycel/client/v1/query.proto` currently exposes:

- `GraphQuery.match` as a linear `GraphPattern` with `NodePattern` plus repeated `TraversalStep`;
- `TraversalStep.depth` with `min_depth` / `max_depth`, but accepted production execution is still limited;
- `GraphQuery.where` with `BetweenExpr`, `AndExpr`, `HasTagExpr`, `PropertyExistsExpr`, `PropertyEqualsExpr`, and `LessThanExpr`;
- `GraphQuery.returns` with projection kinds `NODE`, `TREE`, `SCALAR`, and `EDGE`;
- `GraphQuery.order_by`, `limit`, `max_nodes`, and `max_edges`;
- `QueryResult.graph`, `QueryCounters`, and `QueryDiagnostics`.

Accepted structured execution currently includes:

- schema-indexed equality node starts through `PropertyEqualsExpr` on a single-label start alias;
- schema-indexed ordered node scans;
- one-hop outgoing/incoming adjacency traversal from explicit start node IDs;
- edge aliases, edge projections, edge predicates, scalar projections, and cursor pagination for accepted one-hop adjacency plans;
- indexed ordered root-subtree graph reads with bounded expansion and truncation diagnostics.

Important structured API gaps:

- no accepted production plan for indexed multi-hop traversal from non-explicit indexed starts;
- no accepted production plan for unbounded path enumeration;
- no accepted indexed tag/property-exists/compound/text/semantic pushdown beyond the current narrow indexed plans.

## Shared architecture target

Implement a shared query pipeline that both GQL and structured API can target:

```text
GQL text            Structured GraphQuery
   |                       |
parser/AST            proto validation
   |                       |
semantic analysis      semantic analysis
   |                       |
       shared logical query model
                 |
       planner with index/fallback decisions
                 |
       execution stages
       - indexed node starts
       - indexed adjacency traversal
       - path construction
       - predicate pushdown/residual filtering
       - projection
       - aggregate/distinct/order/offset/limit
                 |
       QueryResult + QueryDiagnostics
```

The shared logical model should be internal and should not leak generated protobuf types into low-level planner/executor packages. GQL and protobuf queries should normalize into this model, then share planning and execution behavior wherever practical.

## Cross-repo touch points

### `mycel-api`

All public API changes start in `mycel-api/api/proto/mycel/client/v1/query.proto` and are regenerated downstream.

Likely public additions:

- `PathValue` and `RETURN_PROJECTION_KIND_PATH`;
- `QueryValue.path` oneof arm;
- aggregate projection/value definitions;
- `GraphQuery.distinct` and `GraphQuery.offset` or equivalent result-shaping fields;
- `OrExpr`, `NullExpr`, `StringPredicateExpr`, `TextSearchExpr`, and `SemanticSearchExpr` or equivalent expression shapes;
- diagnostics fields for pushed-down predicates and residual predicates if existing strings are insufficient.

### `mycel`

Main implementation repo:

- query proto regeneration and generated code ingestion;
- GQL grammar, AST, analysis, planning, execution;
- structured `QueryService.ExecuteQuery` planning/execution;
- graph storage/index reads and adjacency traversal;
- semantic subsystem search integration for semantic predicate pushdown;
- CLI rendering and tests;
- docs and examples.

### SDKs and Mycel Console

After public protobuf changes:

- regenerate/update `mycel-go-sdk` and `mycel-rust-sdk` helpers;
- update Mycel Console result decoding/rendering for path and aggregate values;
- keep daemon authorization authoritative.

## Feature 1: Indexed multi-hop traversal and path projection

### Desired behavior

GQL examples:

```gql
MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person)
RETURN path
```

```gql
MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person)
RETURN GRAPH path
FETCH FIRST 25 ROWS ONLY
```

Structured API target:

```protobuf
GraphQuery {
  match: {
    start: { alias: "a", labels: ["Person"] }
    steps: [{
      direction: TRAVERSAL_DIRECTION_OUT
      edge_kind: "FRIEND_OF"
      depth: { min_depth: 1 max_depth: 3 }
      target: { alias: "b", labels: ["Person"] }
      edge_alias: "r"
    }]
    path_alias: "path" // proposed addition, or equivalent projection-level alias
  }
  returns: [{ alias: "path" output_name: "path" kind: RETURN_PROJECTION_KIND_PATH }]
  limit: 25
}
```

### API changes

Add a first-class path result value:

```protobuf
message PathValue {
  repeated Node nodes = 1;
  repeated Edge edges = 2;
}

message QueryValue {
  oneof value {
    Node node = 1;
    Tree tree = 2;
    google.protobuf.Value scalar = 3;
    Edge edge = 4;
    PathValue path = 5;
  }
}

enum ReturnProjectionKind {
  RETURN_PROJECTION_KIND_UNSPECIFIED = 0;
  RETURN_PROJECTION_KIND_NODE = 1;
  RETURN_PROJECTION_KIND_TREE = 2;
  RETURN_PROJECTION_KIND_SCALAR = 3;
  RETURN_PROJECTION_KIND_EDGE = 4;
  RETURN_PROJECTION_KIND_PATH = 5;
}
```

Decide whether to add `GraphPattern.path_alias` or model path projection as a return projection over a reserved internal path alias. Prefer explicit `path_alias` because path values are not just nodes/edges bound to a single alias.

### GQL implementation tasks

1. Preserve current path grammar and AST compatibility.
2. Normalize GQL path patterns into the shared logical query model.
3. Replace or bypass GQL-specific broad path execution when an accepted indexed path plan is available.
4. Return GQL path values using `QueryValue.path` once the protobuf arm exists.
5. Keep scalar-object path fallback only as a temporary compatibility path if public API rollout is split.
6. Update `RETURN GRAPH path` to populate `QueryResult.graph` from the page of returned paths only.

### Structured API implementation tasks

1. Extend protobuf query model with path projection/value support.
2. Validate structured path projections:
   - path alias must be declared;
   - `RETURN_PROJECTION_KIND_PATH` must point at a path alias;
   - `TREE` projection and `PATH` projection combinations must be deterministic or rejected.
3. Add accepted indexed fixed-depth multi-hop traversal plans.
4. Add accepted indexed bounded variable-depth traversal plans with `DepthSpec.min_depth/max_depth`.
5. Reject unbounded `max_depth = -1` for accepted production execution unless a hard daemon cap is also present.
6. Support path pagination through stable continuation tokens.
7. Ensure read-write transaction contexts see staged graph changes.

### Shared engine/index tasks

1. Add an internal `PathBinding` representation with ordered node IDs and edge IDs.
2. Add an indexed adjacency traversal executor that expands through `ScanAdjacency`, not full edge scans.
3. Apply label and edge-kind filters during expansion.
4. Apply compatible edge/node predicates as early as possible.
5. Deduplicate `QueryResult.graph` nodes/edges by ID while preserving path row order.
6. Add cycle handling policy:
   - default: simple paths only for variable-depth traversal;
   - optionally allow revisits later through an explicit option.
7. Add caps:
   - max frontier size;
   - max paths per query/page;
   - max depth;
   - max returned graph nodes/edges.

### Diagnostics

Accepted path plans must report:

- `plan`, e.g. `IndexedMultiHopAdjacencyPathScan`;
- adjacency indexes used;
- `full_scan=false`;
- `adjacency_scan_calls`;
- `node_read_calls`;
- `edges_loaded` / `nodes_loaded`;
- `rows_returned`;
- cursor kind;
- truncation/cap reasons.

### Tests

- GQL path query returns the same ordered path rows as structured path query for an equivalent pattern.
- Structured two-hop traversal uses adjacency indexes and does not list every edge in the domain.
- Structured `*1..3` traversal respects min/max depth.
- Path projection returns ordered nodes and edges.
- `RETURN GRAPH path` and structured path projection populate the graph envelope from returned rows.
- Pagination is stable and does not duplicate/skip paths.
- Unsupported unbounded or missing-index shapes fail closed in non-broad domains.
- Read-write transactions see staged path changes.

## Feature 2: Aggregation and result shaping

### Desired behavior

GQL examples:

```gql
MATCH (p:Person)
RETURN COUNT(p) AS people
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
RETURN COUNT(r) AS friendships
```

```gql
MATCH (p:Person)
RETURN DISTINCT p.sex AS sex
ORDER BY sex
```

```gql
MATCH (p:Person)
RETURN p.name AS name
ORDER BY p.name
OFFSET 20
FETCH FIRST 20 ROWS ONLY
```

Structured API target:

```protobuf
GraphQuery {
  match: { start: { alias: "p", labels: ["Person"] } }
  aggregate_returns: [{
    output_name: "people"
    function: AGGREGATE_FUNCTION_COUNT
    argument: { alias: "p" }
  }]
}
```

```protobuf
GraphQuery {
  match: { start: { alias: "p", labels: ["Person"] } }
  returns: [{ alias: "p" output_name: "sex" kind: RETURN_PROJECTION_KIND_SCALAR property: "sex" }]
  distinct: true
  order_by: [{ value: { prop: { alias: "p" name: "sex" } } direction: SORT_DIRECTION_ASC }]
  offset: 20
  limit: 20
}
```

The exact protobuf field names should be finalized in `mycel-api`, but the API must expose aggregate and result-shaping semantics directly rather than relying on CLI/client-side behavior.

### Initial semantics

Implement in this order:

1. global `COUNT(*)` and `COUNT(alias)`;
2. `RETURN DISTINCT` over scalar projections;
3. `OFFSET` with existing `FETCH FIRST` / API `limit`;
4. grouped `COUNT` by scalar projection keys.

Conservative first-tranche restrictions:

- no `HAVING`;
- no aggregate arithmetic;
- no nested aggregate expressions;
- no collection aggregates;
- no mixed aggregate/non-aggregate returns unless all non-aggregate expressions are grouping keys;
- no approximate counts unless explicitly named later.

### API changes

Add structured aggregate/result-shaping fields, for example:

```protobuf
enum AggregateFunction {
  AGGREGATE_FUNCTION_UNSPECIFIED = 0;
  AGGREGATE_FUNCTION_COUNT = 1;
}

message AggregateProjection {
  string output_name = 1;
  AggregateFunction function = 2;
  AggregateArgument argument = 3;
}

message AggregateArgument {
  oneof argument {
    bool star = 1;
    string alias = 2;
    ValueExpr value = 3;
  }
}

message GraphQuery {
  GraphPattern match = 1;
  optional Expr where = 2;
  repeated ReturnProjection returns = 3;
  repeated OrderSpec order_by = 4;
  int32 limit = 5;
  int32 max_nodes = 6;
  int32 max_edges = 7;
  repeated AggregateProjection aggregate_returns = 8;
  bool distinct = 9;
  int32 offset = 10;
}
```

If `ReturnProjection` is generalized later to avoid parallel `aggregate_returns`, preserve wire compatibility by reserving/choosing field numbers carefully during API review.

### GQL implementation tasks

1. Extend grammar:
   - `COUNT(*)`;
   - `COUNT(alias)`;
   - `RETURN DISTINCT`;
   - `OFFSET <n>` with existing `FETCH FIRST` syntax.
2. Extend AST:
   - aggregate return-item kind;
   - statement-level distinct flag;
   - offset clause;
   - grouping metadata derived during analysis.
3. Extend analysis:
   - validate aggregate arguments are bound;
   - reject invalid aggregate/scalar mixes;
   - reject duplicate output names;
   - validate `OFFSET >= 0` and limits are sane.
4. Extend planning:
   - add aggregate stage after match/filter and before final projection output;
   - add distinct stage after projection, before offset/limit;
   - define deterministic `ORDER BY` + `OFFSET` behavior.
5. Extend execution:
   - count matched bindings without producing all result values when possible;
   - for grouped count, key by scalar grouping projections;
   - return counts as scalar `QueryValue` values;
   - ensure `QueryCounters.rows_returned` means output rows returned, not matched rows.

### Structured API implementation tasks

1. Add protobuf fields and validation.
2. Normalize aggregate/query-shaping fields into the same logical model used by GQL.
3. Support aggregate over accepted indexed node scans and accepted adjacency/path plans.
4. Support `distinct`, `offset`, and `limit` consistently with GQL.
5. Reject structured aggregate shapes that require unsupported broad scans in non-broad domains.
6. Add SDK helper builders for `Count`, `Distinct`, and `Offset` after API regeneration.

### Shared executor tasks

1. Add projection-stage value hashing/equality for scalar distinct/group keys.
2. Add aggregate accumulators, starting with count.
3. Optimize count over indexed scans when possible:
   - use index cardinality only if exact and authorization/filter semantics are preserved;
   - otherwise count accepted cursor scan results without materializing full rows beyond caps.
4. Define memory limits for grouping/distinct state.
5. Emit diagnostics for aggregate strategy:
   - `AggregateCountIndexScan`;
   - `AggregateCountTraversal`;
   - `DistinctProjection`;
   - state cap/truncation warnings.

### Tests

- GQL parses and rejects aggregate/distinct/offset syntax correctly.
- `COUNT(*)` and `COUNT(alias)` return one scalar row for ungrouped queries.
- Grouped `COUNT` returns one row per grouping key.
- `RETURN DISTINCT` deduplicates scalar result rows.
- `OFFSET` applies after `ORDER BY` and before/with `FETCH FIRST` consistently.
- Structured API aggregate query returns the same result as equivalent GQL query.
- Structured `distinct`/`offset` result shaping matches GQL semantics.
- Unsupported aggregate over non-accepted fallback paths fails closed in non-broad domains.
- CLI and Mycel Console render aggregate scalar values unchanged.

## Feature 3: Predicate expressiveness and index pushdown

### Desired behavior

GQL examples:

```gql
MATCH (p:Person)
WHERE (p.age > 11 OR p.role = 'adult') AND p.name IS NOT NULL
RETURN p
```

```gql
MATCH (n:Note)
WHERE n.payload.text CONTAINS 'graph memory'
RETURN n
```

```gql
MATCH (n:Note)
WHERE n.title STARTS WITH 'Project'
RETURN n
```

Structured API target:

```protobuf
Expr {
  and: { exprs: [
    { or: { exprs: [
      { less_than: ... }
      { property_equals: ... }
    ]}}
    { null: { alias: "p" name: "name" is_null: false }}
  ]}
}
```

```protobuf
Expr {
  text: {
    alias: "n"
    field: "payload.text"
    query: "graph memory"
    mode: TEXT_PREDICATE_MODE_CONTAINS
  }
}
```

### API changes

Add or extend expression messages in `query.proto`, for example:

```protobuf
message Expr {
  oneof expr {
    BetweenExpr between = 1;
    AndExpr and = 2;
    HasTagExpr has_tag = 3;
    PropertyExistsExpr property_exists = 4;
    PropertyEqualsExpr property_equals = 5;
    LessThanExpr less_than = 6;
    OrExpr or = 7;
    NullExpr null = 8;
    StringPredicateExpr string_predicate = 9;
    TextSearchExpr text = 10;
    SemanticSearchExpr semantic = 11;
  }
}

message OrExpr {
  repeated Expr exprs = 1;
}

message NullExpr {
  string alias = 1;
  string name = 2;
  bool is_null = 3;
}

enum StringPredicateMode {
  STRING_PREDICATE_MODE_UNSPECIFIED = 0;
  STRING_PREDICATE_MODE_CONTAINS = 1;
  STRING_PREDICATE_MODE_STARTS_WITH = 2;
  STRING_PREDICATE_MODE_ENDS_WITH = 3;
}

message StringPredicateExpr {
  ValueExpr value = 1;
  string query = 2;
  StringPredicateMode mode = 3;
}

message TextSearchExpr {
  string alias = 1;
  string field = 2;
  string query = 3;
}

message SemanticSearchExpr {
  string alias = 1;
  string field = 2;
  string query = 3;
  string index_ref = 4;
  int32 limit = 5;
}
```

Exact shape should be reviewed against existing semantic subsystem terminology. Graph automations and query APIs should reference inference profiles/model refs/capabilities where needed, never raw API keys.

### GQL implementation tasks

1. Replace flat `AND` predicate parsing with precedence-aware boolean expression parsing:
   - parentheses;
   - `AND` tighter than `OR`;
   - optional `NOT` can be deferred if not needed.
2. Add leaf predicates:
   - `expr IS NULL`;
   - `expr IS NOT NULL`;
   - `expr CONTAINS 'x'`;
   - `expr STARTS WITH 'x'`;
   - `expr ENDS WITH 'x'`.
3. Keep `TEXT_CONTAINS(...)` and `SEMANTIC_SIMILAR(...)` as compatibility spellings while mapping them into the shared logical predicate model.
4. Extend AST and analysis validation for expression trees.
5. Extend planner to split predicates into:
   - pushdown predicates;
   - residual predicates;
   - rejected predicates.
6. Extend fallback evaluator for boolean expression trees so broad-searchable domains continue to work explicitly.

### Structured API implementation tasks

1. Add protobuf expression shapes.
2. Regenerate generated protobufs in `mycel`, SDKs, and Mycel Console dependencies as needed.
3. Normalize structured expressions into the shared logical predicate model.
4. Add validation for unsupported predicate/index combinations.
5. Add accepted plans for:
   - tag index scans;
   - property-exists index scans;
   - compatible `AND` intersections;
   - supported `OR` unions when all branches are index-backed;
   - string/text predicates backed by declared text indexes;
   - semantic predicates backed by semantic indexes.
6. Preserve fail-closed behavior for non-broad domains when no accepted indexed plan exists.

### Shared index/pushdown tasks

1. Add index reader interfaces for:
   - tag candidates;
   - property-exists candidates;
   - text candidates;
   - semantic candidates with score/order metadata.
2. Add candidate set operations:
   - intersection for `AND`;
   - union for supported `OR`;
   - score-preserving merge for semantic candidates.
3. Add residual filtering after candidate reads.
4. Add cost/plan selection rules:
   - prefer most selective index for starts;
   - push relationship predicates into adjacency traversal when possible;
   - reject broad scans unless allowed by domain/searchability options.
5. Add diagnostics:
   - pushed-down predicate descriptions;
   - residual predicate descriptions;
   - candidate counts;
   - selected indexes;
   - full-scan/fallback reason;
   - semantic index/model/profile reference where applicable.

### Tests

- GQL parser accepts nested boolean predicates and rejects ambiguous/invalid syntax.
- GQL and structured API produce equivalent results for equivalent `AND`/`OR`/null/string predicates.
- Fallback evaluator handles expression trees correctly in broad-searchable domains.
- Structured tag/property-exists queries use indexes and do not load all nodes/edges.
- Compatible `AND` plans intersect indexed candidate sets.
- Supported `OR` plans union indexed candidate sets.
- Text predicates use text indexes when declared/available.
- Semantic predicates use semantic subsystem indexes and return stable score/order diagnostics.
- Unsupported predicate combinations fail closed in non-broad domains.

## Phased delivery plan

### QP0 — Design and compatibility gates

- Finalize protobuf shape for path, aggregate, result-shaping, and predicate additions.
- Decide field numbers and reserve any removed/abandoned fields.
- Add planner diagnostic taxonomy for accepted, fallback, and rejected query shapes.
- Add golden tests for current GQL/API behavior before changing semantics.

Acceptance:

```sh
make docs-check
go test ./internal/query/gql/... ./internal/daemon/api/client -count=1
```

### QP1 — Shared logical query model

- Add internal logical query structs for patterns, path aliases, predicates, projections, aggregates, ordering, offset, limits, and caps.
- Normalize current GQL AST into the logical model.
- Normalize current `GraphQuery` into the logical model.
- Keep current tests passing through compatibility adapters.

Acceptance:

```sh
go test ./internal/query/gql/... ./internal/daemon/api/client -count=1
```

### QP2 — Public API path support and indexed path engine

- Add `PathValue`, `QueryValue.path`, and `RETURN_PROJECTION_KIND_PATH` to `mycel-api`.
- Regenerate protobufs in `mycel` and SDKs.
- Implement shared indexed multi-hop/path executor.
- Wire structured API path projection.
- Move GQL path values to `QueryValue.path`.

Acceptance:

```sh
# in mycel-api, then downstream as applicable
make generate

# in mycel
make generate-proto
go test ./internal/daemon/api/client ./internal/query/gql/... ./internal/graph/service ./internal/graph/storage -count=1
```

### QP3 — Aggregation and result shaping in GQL

- Add GQL `COUNT`, `RETURN DISTINCT`, and `OFFSET` parser/AST/analysis/planning/execution.
- Use shared projection/aggregate stages.
- Update CLI and Console examples if needed.

Acceptance:

```sh
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
```

### QP4 — Aggregation and result shaping in structured API

- Add public aggregate/distinct/offset protobuf fields.
- Regenerate protobufs and SDKs.
- Normalize API aggregate/result-shaping fields into shared logical model.
- Add structured API parity tests against equivalent GQL queries.

Acceptance:

```sh
make generate-proto
go test ./internal/daemon/api/client ./internal/query/gql/... -count=1
```

Run SDK and Console builds after generated bindings update.

### QP5 — GQL predicate expressiveness

- Add GQL boolean expression tree with `OR` and parentheses.
- Add null and string predicates.
- Map compatibility predicates into shared model.
- Add fallback evaluator support for expression trees.

Acceptance:

```sh
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
```

### QP6 — Structured predicate API and indexed pushdown

- Add public API expression shapes.
- Add tag/property-exists/text/semantic index readers and planner rules.
- Add candidate intersection/union and residual-filter diagnostics.
- Wire GQL and structured API predicates into the same pushdown planner.

Acceptance:

```sh
make generate-proto
go test ./internal/daemon/api/client ./internal/query/gql/... ./internal/graph/service ./internal/graph/storage ./internal/semantic/... ./internal/schema/... -count=1
```

### QP7 — SDKs, Console, docs, and release validation

Status: implemented for the initial MVP. Go SDK generation, Rust SDK build-time generation, and Mycel Console path-value JSON rendering were updated/validated against the new API.

- Regenerate and update `mycel-go-sdk` and `mycel-rust-sdk`.
- Add SDK helpers for path, count, distinct, offset, and new predicates.
- Update Mycel Console result rendering for path and aggregate values if needed.
- Update query docs, roadmap statuses, and examples.

Acceptance:

```sh
# mycel
go test ./... -count=1
make docs-check
git diff --check

# mycel-go-sdk
make test

# mycel-rust-sdk
cargo test --workspace

# mycel-console / current checkout name may still be mycel-admin
npm test -- --runInBand --watch=false
npm run build
```

## Production acceptance criteria

A feature is considered implemented only when both GQL and structured API satisfy the relevant criteria:

- public API shape is defined in `mycel-api` when the feature is externally visible;
- generated protobufs are updated through normal generation commands;
- both GQL and structured API normalize into the shared logical model;
- accepted production plans use indexes/storage-backed reads and report `full_scan=false`;
- unsupported non-broad query shapes fail closed with clear diagnostics;
- broad-searchable fallback paths are explicitly diagnosed as fallback and are not documented as production-scalable behavior;
- CLI, SDK, and Mycel Console surfaces either support or safely render the new result values;
- roadmap statuses and operations docs are updated.

## Open decisions

1. Should `GraphPattern.path_alias` be added, or should path projection use a projection-level path binding model?
2. Should `QueryValue.path` be required before marking GQL path projection fully implemented, or only before marking structured path projection implemented?
3. Should API aggregate fields be separate `aggregate_returns` or a generalized `ReturnProjection` oneof?
4. Should `OFFSET` require an ordered query for deterministic production use?
5. Should unbounded `DepthSpec.max_depth = -1` be rejected unless a daemon max-depth cap is present?
6. Which text index implementation should back initial `CONTAINS` / `STARTS WITH` / `ENDS WITH` pushdown?
7. How should semantic predicate diagnostics expose index/model/profile references without exposing raw credentials?
8. Should broad-searchable fallback require an explicit request option for these new predicate shapes?
