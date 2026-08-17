# Top Query Priorities Completion Implementation Plan

## Status

Planned. This plan defines the remaining work required to consider the QP0-QP7 query-priority tranche complete beyond the initial MVP.

The MVP delivered:

- first-class `PathValue` / `QueryValue.path` support;
- structured explicit-start indexed path traversal and path projection;
- GQL/API `COUNT`, `DISTINCT`, and `OFFSET` basics;
- GQL/API boolean/null/string/text/semantic predicate surfaces with fallback evaluation;
- Mycel Console path-value JSON handling;
- generated Go SDK query bindings.

This completion plan turns those MVP features into accepted production query support by removing broad fallback dependence from accepted shapes, adding index pushdown, completing aggregate/result-shaping semantics, and adding planner diagnostics.

Roadmap sources:

- [GQL roadmap](../../roadmap/gql-roadmap.md)
- [Structured Query API roadmap](../../roadmap/api-roadmap.md)
- [Top query priorities MVP implementation plan](top-query-priorities-implementation-plan.md)
- [Query expansion design](../../design/api/query-expansion.md)
- [GWL indexes and query planning](../../design/schema/gwl-indexes-and-query-planning.md)

## Goals

1. Make accepted query shapes production-safe without relying on full-domain fallback scans.
2. Add index pushdown for common boolean, property, text, and semantic predicates.
3. Broaden indexed path planning beyond explicit start node IDs.
4. Complete aggregate and result-shaping semantics for GQL and the structured API.
5. Provide diagnostics/explain output that proves when a query used indexes, residual filters, fallback, truncation, and pagination.
6. Keep textual GQL and structured protobuf queries aligned through shared planning/execution behavior wherever practical.

## Non-goals

- Do not introduce domain-specific concepts; mycel remains standalone.
- Do not make Mycel Console authorization authoritative; daemon/API authorization remains authoritative.
- Do not hand-edit generated protobuf files.
- Do not require compatibility for prototype full-domain fallback behavior.
- Do not add product pricing, credits, billing, or raw provider-key concepts.
- Do not convert Mycel Console from Tauri in this tranche.

## Completion definition

This phase is complete when all of these are true:

- accepted GQL/API predicate, path, aggregate, and shaping features either use accepted indexed plans or fail closed with actionable diagnostics;
- full-domain fallback is not counted as accepted production support;
- structured API and GQL semantics match for equivalent queries;
- diagnostics identify selected plan, indexes used, residual predicates, row counts, scanned/loaded counts, truncation, and fallback rejection/use;
- SDKs and Mycel Console understand the final public query surface;
- release validation passes across mycel, mycel-api, Go SDK, Rust SDK, and Mycel Console.

## Phase QPC0 — Semantics and compatibility gates

Status: implemented for the initial completion baseline. The semantics are documented in the query API design docs, structured unbounded traversal now fails closed, and GQL fallback execution is gated by domain broad-searchability after indexed plans are attempted.

### Tasks

- Freeze the public semantics for:
  - boolean expression precedence and grouping;
  - null/missing-property behavior;
  - string predicate case sensitivity;
  - text predicate target fields and ranking behavior;
  - semantic predicate score/ranking behavior;
  - path uniqueness, cycle handling, row ordering, and truncation;
  - aggregate grouping, null handling, and mixed aggregate/non-aggregate projections;
  - stable ordering with `DISTINCT`, `OFFSET`, and `FETCH`/`limit`.
- Update `docs/design/api/query.md` and `docs/design/api/query-expansion.md` with the accepted semantics.
- Add planner-level rejection rules for shapes that cannot yet be executed safely.

### Acceptance criteria

- Each accepted query feature has documented semantics.
- Unsupported production shapes return deterministic errors instead of silently relying on broad fallback.
- Existing MVP tests remain green.

## Phase QPC1 — Shared logical query model hardening

Status: implemented for the normalization baseline. `internal/query/logical` now defines the shared logical query model, normalizes both structured `GraphQuery` and compiled GQL read plans, and classifies predicate leaves as pushdown candidates or residual filters before physical planning.

### Tasks

- Consolidate GQL and structured query normalization around an internal logical query model for:
  - aliases and bindings;
  - predicates and residual predicates;
  - path patterns and path aliases;
  - projections and aggregate projections;
  - ordering, distinct, offset, limit, and cursor pagination.
- Preserve protobuf and GQL parser boundaries; lower-level planner/executor packages should not depend on generated parser internals.
- Add normalization tests proving equivalent GQL/API queries produce equivalent logical plans.

### Acceptance criteria

- GQL and structured API equivalent queries produce matching logical-query snapshots in tests.
- Planner code can distinguish accepted indexed predicates from residual predicates before execution.

## Phase QPC2 — Predicate index pushdown

Status: partially implemented for the ordered-property index slice. Structured queries and equivalent simple GQL node queries can now use schema-declared ordered property indexes for equality, property-exists, less-than, between, `AND` intersection, and `OR` union, with residual filtering after indexed candidate selection. Tag-specific pushdown remains deferred until a tag index is exposed through the graph/query planner surface.

### Tasks

- Implement indexed plans for:
  - single-label property equality;
  - tag predicates;
  - property-exists predicates;
  - ordered comparisons where an ordered property index exists;
  - `AND` as index narrowing/intersection where supported;
  - `OR` as index union where supported;
  - null/missing predicates where the index model can answer them safely.
- Add residual-filter handling for predicates that remain after partial pushdown.
- Add diagnostics for pushed vs residual predicates.
- Fail closed when an accepted query shape would otherwise require broad fallback and fallback is not explicitly allowed.

### Acceptance criteria

- Tests prove index usage for equality, tag, property-exists, comparison, `AND`, and `OR` predicates.
- Tests prove fallback is rejected for production shapes lacking a safe plan.
- Diagnostics identify the pushed predicates and residual predicates.

## Phase QPC3 — Text/string predicate pushdown

Status: partially implemented for schema-indexed property fields. String predicates and property-field `TEXT_CONTAINS` now use ordered property indexes to bound candidate rows and then apply case-insensitive residual matching. Unsupported text targets, such as unindexed properties or payload fields without a compatible index, fail closed in non-broad-searchable domains. True full-text index ranking/pushdown remains deferred.

### Tasks

- Define the text-index target surface:
  - node payload text;
  - selected node properties;
  - optional edge properties if supported by the index subsystem.
- Add indexed execution for:
  - `CONTAINS`;
  - `STARTS WITH` where indexable;
  - `ENDS WITH` where indexable or explicitly residual/rejected;
  - `TEXT_CONTAINS` / structured text predicates.
- Normalize case sensitivity and Unicode behavior.
- Return stable ranking/order semantics for text matches or document that ranking is not provided.

### Acceptance criteria

- Text/string predicates have tests proving index pushdown where claimed.
- Non-indexable string predicates are residual only when bounded by a safe indexed candidate set; otherwise they fail closed.
- Diagnostics identify text index usage and residual string filters.

## Phase QPC4 — Semantic/vector predicate execution

Status: implemented for the first accepted semantic-vector slice. Structured `SemanticSearchExpr` and equivalent GQL `SEMANTIC_SIMILAR` predicates now route through the semantic subsystem search path for single-label, node-only start-alias predicates. Broad textual fallback remains available only for broad-searchable development domains when the accepted vector shape cannot be planned.

### Tasks

- [x] Replace semantic textual fallback with semantic subsystem search for accepted semantic predicates.
- [x] Define initial request/result semantics for query text, top-k/limit interaction, and stable semantic-score ordering from the semantic subsystem.
- [x] Integrate authorization and semantic-mode checks through the daemon/API boundary.
- [ ] Add public score projection and score-threshold controls beyond semantic search diagnostics.
- [ ] Extend vector planning beyond one start-alias semantic predicate and into richer boolean combinations.
- [ ] Ensure graph automations reference inference profiles/model refs/capabilities rather than raw API keys.

### Acceptance criteria

- [x] Semantic predicates execute through semantic/vector indexes for accepted shapes.
- [x] Tests prove fallback textual matching is not used for accepted semantic predicates.
- [ ] Diagnostics include full semantic index name/source, candidate count, score threshold, and residual filters; the initial slice reports `SemanticVectorSearch`, semantic index IDs, row counts, and no full scan.

## Phase QPC5 — Broader indexed path planning

Status: implemented for the broader-start path-planning slice covering explicit node IDs, label-index starts, tag-index starts, and schema-backed ordered-property starts. Structured path queries can derive start nodes from the label index, tag index, or ordered property indexes, then execute bounded multi-hop traversal through adjacency indexes. Equivalent GQL `MATCH path = ... RETURN path` queries lower into the same structured indexed path engine for label-only starts and ordered-property bounded starts; tag-start GQL parity is waiting on a GQL tag predicate syntax. The indexed path engine now enforces start-node, row, node-load, edge-load, and depth caps, skips per-path cycles, and preserves edge-distinct duplicate paths.

### Tasks

- [ ] Extend structured indexed path traversal from explicit start node IDs to indexed starts derived from:
  - [x] labels;
  - [x] property equality;
  - [x] ordered property bounds;
  - [x] property-exists/string/text predicates where QPC2/QPC3 supports them through ordered-property candidate scans;
  - [x] tag predicates through the storage-backed tag index for structured `HasTag` starts.
- [x] Implement production-safe multi-hop and bounded variable-depth traversal using adjacency indexes for indexed starts.
- [ ] Define and enforce:
  - [x] max depth;
  - [x] max path rows for the indexed path engine;
  - [x] max start nodes for label-index and ordered-index-derived starts;
  - [x] separate max nodes/edges loaded diagnostics/caps;
  - [x] edge-distinct duplicate path semantics inherited from deterministic indexed starts plus adjacency traversal;
  - [x] cycle handling via per-path visited-node sets;
  - [x] deterministic row ordering for label-index and ordered-property-index starts.
- [x] Align GQL path execution with the same indexed path engine for accepted path-variable queries.

### Acceptance criteria

- [x] Equivalent GQL/API path queries use the same accepted indexed path engine for label-index and ordered-index-derived starts.
- [x] Tests cover indexed-start multi-hop paths, bounded variable-depth paths, fail-closed start-cap behavior, cycle handling, duplicate handling, and graph envelope extraction.
- [x] Unbounded or unsafe path queries fail closed unless explicitly allowed as non-production fallback.

## Phase QPC6 — Aggregation and grouping completion

Status: implemented for the aggregate-function/grouping slice across structured API and GQL. Public `AggregateFunction` now includes `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`. Structured queries use `AggregateArgument.value` for value aggregates; GQL supports `SUM(alias.property)`, `AVG(alias.property)`, `MIN(alias.property)`, `MAX(alias.property)`, and `COUNT(alias.property)` in addition to existing `COUNT(*)`/`COUNT(alias)`. Non-aggregate returns define grouping keys.

### Tasks

- [x] Add or finalize aggregate functions:
  - [x] `COUNT`;
  - [x] `SUM`;
  - [x] `AVG`;
  - [x] `MIN`;
  - [x] `MAX`.
- [x] Define grouped aggregate semantics for GQL and structured API.
- [x] Validate mixed aggregate/non-aggregate returns as grouped aggregate queries.
- [x] Define null and missing-value behavior per aggregate.
- [x] Decide and implement aggregate aliases and scalar output types.
- [ ] Add aggregate diagnostics where useful.

### Acceptance criteria

- [x] GQL and structured API aggregate semantics match for equivalent queries.
- [x] Invalid aggregate calls are rejected with clear errors.
- [ ] Tests cover null/missing values, aliases, grouping, distinct interaction, offset/limit interaction, and empty result sets. The initial QPC6 tests cover null/missing values, aliases, grouping, and GQL/API equivalence; distinct/offset/limit and empty result sets remain follow-up.

## Phase QPC7 — Result shaping and pagination completion

Status: implemented for the main structured API and GQL query-row shaping path, broad-searchable GQL fallback, and accepted ordered-index node scans. The stable shaping order is now projection or aggregation/grouping, distinct, ordering, offset, fetch/limit, and response cursor pagination. Indexed ordered node/equality scans use a shaped cursor wrapper so first-page offsets and total fetch limits compose with underlying index-key cursors. Indexed root-subtree scans preserve their existing root-page `limit` semantics while applying first-page offsets before expansion. Result graph envelopes are derived from shaped returned rows for node/edge/path projections.

### Tasks

- [x] Implement general `ORDER BY` for safe bounded result sets and indexed ordered plans where available.
- [x] Define stable operation order for:
  - [x] projection;
  - [x] aggregation/grouping;
  - [x] distinct;
  - [x] ordering;
  - [x] offset;
  - [x] fetch/limit;
  - [x] cursor pagination.
- [x] Extend cursor pagination compatibility with indexed plans and shaped result sets.
- [x] Ensure result graph envelopes remain consistent with shaped rows.

### Acceptance criteria

- [x] Tests cover `ORDER BY` + `DISTINCT` + `OFFSET` + `FETCH` combinations.
- [x] Cursor pagination is stable for accepted indexed ordered node plans.
- [x] Result graph envelopes include exactly the nodes/edges reachable from shaped returned rows/projections.

## Phase QPC8 — Diagnostics, explain, and observability

Status: implemented for planner diagnostics and explain surfaces. The public API now exposes `ExplainQuery` and `ExplainGQL` for transaction-scoped planning without graph reads or mutations. `QueryDiagnostics` includes planner/version, plan kind, pushed/residual predicate summaries, fallback/rejection details, row/candidate counters, existing truncation details, and timing fields. The CLI supports `mycel query gql --explain` and renders diagnostics in text or JSON mode.

### Tasks

- [x] Extend `QueryDiagnostics` with fields for:
  - [x] planner name/version;
  - [x] selected plan kind;
  - [x] indexes used;
  - [x] pushed predicates;
  - [x] residual predicates;
  - [x] fallback mode/rejection reason;
  - [x] rows scanned/produced;
  - [x] nodes/edges loaded;
  - [x] candidate counts;
  - [x] truncation flags/reasons;
  - [x] timing breakdown.
- [x] Add explain-style execution that returns a planned query without executing mutations or reads.
- [x] Update CLI output for diagnostics/explain.
- [x] Add tests that assert diagnostics for representative accepted/rejected plans.

### Acceptance criteria

- [x] Every accepted indexed plan reports enough diagnostics to prove it did not use broad fallback.
- [x] Rejected shapes include actionable diagnostics.
- [x] CLI and API tests cover explain output.

## Phase QPC9 — SDKs, Console, docs, and release validation

### Tasks

- [x] Regenerate generated code after all public protobuf changes:
  - mycel internal generated protobufs;
  - Go SDK generated bindings;
  - Rust SDK build-time protobuf outputs.
- [x] Add SDK helpers/builders for common query shapes:
  - indexed node lookup;
  - ordered node query;
  - text/semantic predicate query;
  - path query;
  - aggregate query;
  - explain helpers.
- [x] Update Mycel Console to render/extract:
  - path values;
  - aggregate rows;
  - diagnostics/explain details;
  - truncation/fallback rejection messages.
- [x] Update docs and roadmaps to distinguish implemented, partial, and deferred query features.

### Acceptance criteria

- [x] SDK tests cover helper-generated query shapes.
- [x] Mycel Console tests cover path, aggregate, diagnostics, fallback, truncation, and rejection rendering helpers; distinct/offset shaping is covered by QPC7 daemon tests.
- [x] Roadmaps match implemented status.

## Validation plan

Run targeted validation during development:

```sh
cd mycel
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
make docs-check
git diff --check
```

Run public API and SDK validation after protobuf changes:

```sh
cd mycel-api
make test
git diff --check

cd ../mycel
MYCEL_API_ROOT=../mycel-api make generate-proto
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1

cd ../mycel-go-sdk
MYCEL_API_ROOT=../mycel-api make generate
go test ./...
git diff --check

cd ../mycel-rust-sdk
MYCEL_API_ROOT=../mycel-api cargo test --workspace
git diff --check

cd ../mycel-console/src-tauri
MYCEL_API_ROOT=../../mycel-api cargo check
```

Run release validation before considering the phase complete:

```sh
cd mycel
go test ./... -count=1
make docs-check
git diff --check
```

Also run the Mycel Console frontend test/build commands appropriate for the current package scripts.

## Risks and mitigations

- **Planner complexity grows too quickly.** Mitigate by landing one accepted plan family at a time with explicit diagnostics.
- **Fallback behavior masks missing index support.** Mitigate by making accepted production shapes fail closed unless a safe indexed candidate set bounds residual filtering.
- **GQL/API semantic drift.** Mitigate with paired equivalence tests for representative queries.
- **Text and semantic ranking ambiguity.** Mitigate by documenting ranking semantics before implementation and exposing diagnostics.
- **Path explosion.** Mitigate with required depth/row/load caps, truncation diagnostics, and deterministic ordering.
- **Public protobuf churn.** Mitigate by batching API changes per phase and regenerating downstream repositories immediately.

## Suggested implementation order

1. QPC0 and QPC1 to lock semantics and normalization.
2. QPC2 and QPC3 to remove fallback dependence for common predicates.
3. QPC5 to broaden indexed path support once predicate starts are indexable.
4. QPC6 and QPC7 to complete aggregation and result shaping.
5. QPC4 can proceed in parallel if semantic subsystem capacity is available.
6. QPC8 and QPC9 as cross-cutting release gates.
