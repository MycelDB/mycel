# Structured Query API Roadmap

This document tracks the roadmap for the structured protobuf query API, sometimes
called the dynamic API in planning notes. It complements the textual
[GQL roadmap](gql-roadmap.md): GQL is the human-readable query language, while
this API is the typed `mycel.client.v1.QueryService.ExecuteQuery` surface used by
SDKs, generated clients, and application builders that prefer protobuf request
construction over query strings.

## Current Reality

The structured query executor now has accepted indexed paths for the highest-priority node, traversal, path, aggregation, shaping, and diagnostics shapes. Unsupported shapes may still use a legacy broad executor only in domains that explicitly allow broad search, and that fallback remains development/exploration behavior rather than production-sized graph functionality.

Accepted production query support means the daemon can plan an indexed, semantic, or otherwise bounded execution path and will fail closed for non-broad-searchable domains. Full-domain fallback is not considered implementation for roadmap purposes.

## Scope

The structured query API aims to provide a safe, typed, transaction-scoped graph
query surface for dynamic applications. The production implementation must
support graph pattern reads, metadata/property predicates, projections, ordering,
pagination, schema-aware validation, and dynamic application builders without
requiring full-domain scans.

The structured API is read-only. Graph mutations remain in the Graph API or textual GQL write statements.

## Current Status Values

The **Current status** column uses:

- **Y**: accepted as currently implemented for roadmap purposes.
- **Partial**: useful surface exists, but production planning/pushdown parity is incomplete.
- **N**: not accepted as implemented. This includes missing features and features whose only behavior depends on broad fallback execution.

## Feature Matrix

Desirability values are relative priorities:

- **Very High**: foundational for near-term graph/product workflows.
- **High**: broadly useful and expected by users.
- **Medium**: useful, but can follow foundational query features.
- **Low**: niche or mostly developer/operator oriented.

| Feature | Short description | Desirability (all) | Desirability (knot_pkm) | Current status | Notes |
|---|---|---:|---:|:---:|---|
| Transaction-scoped execution | Execute a structured query inside an existing graph transaction. | Very High | Very High | Y | Request/transaction plumbing exists. |
| Strong/current read consistency | Use current strong/read-index semantics and return read metadata. | Very High | Very High | Y | Read metadata is returned. |
| Read-your-writes overlay | Query staged writes in a read-write transaction. | High | High | Y | Transaction layer supports overlay-visible reads; query executor still needs replacement. |
| Node pattern start | Match from a required start node pattern and alias. | Very High | Very High | Y | Accepted for schema-indexed equality and ordered single-label node starts. |
| Node labels | Match nodes with required labels. | Very High | Very High | Y | Accepted for indexed single-label node starts and indexed adjacency targets. |
| Inline node property maps | Put property constraints directly on `NodePattern`. | Medium | Medium | N | Missing. |
| Tag predicate | Filter nodes using normalized mycel tags. | High | Very High | N | Current behavior is post-scan. |
| Property exists predicate | Filter nodes with a normalized custom property name. | High | High | N | Current behavior is post-scan. |
| Property equals predicate | Filter nodes by custom property equality. | High | High | Y | Accepted for start-alias equality predicates backed by ordered property indexes. |
| Boolean `AND` | Combine predicates conjunctively. | High | High | N | Current behavior is only in-memory expression evaluation. |
| Boolean `OR` | Combine alternatives. | Medium | Medium | Y | Implemented in fallback expression evaluation; indexed union pushdown remains missing. |
| Parenthesized/grouped predicates | Group boolean expressions explicitly. | Medium | Medium | Y | Implemented in the expression model and fallback evaluator. |
| Between predicate | Compare values against low/high bounds. | High | High | N | General execution is incomplete; inclusive ordered-property bounds are supported only inside the indexed single-label ordering path. |
| General comparison operators | Support `<`, `<=`, `>`, `>=`, `!=` as first-class expressions. | High | High | N | General comparator set is missing; `LessThanExpr` exists only for strict ordered-property upper bounds in the indexed single-label ordering path. |
| Date/current-date expressions | Compare date literals and current date with day offsets. | Medium | Medium | N | Expression values exist, but database execution is missing. |
| Text contains predicate | Full-text-style filtering over payload/properties. | High | Very High | Partial | Text/string predicate API and fallback evaluation are implemented; text index pushdown remains missing. |
| Semantic predicate | Semantic/vector similarity from the structured query API. | Medium | Very High | Partial | Semantic predicate API and textual fallback evaluation are implemented; vector index pushdown remains missing. |
| Outgoing traversal | Traverse outgoing edges by label. | Very High | Very High | Y | Accepted for one-hop adjacency-index traversal from explicit start node IDs. |
| Incoming traversal | Traverse incoming edges by label. | High | High | Y | Accepted for one-hop adjacency-index traversal from explicit start node IDs. |
| Undirected traversal | Traverse without direction. | Medium | Medium | N | Missing. |
| Edge label filter | Restrict traversal by edge kind/label. | Very High | Very High | Y | Accepted for one-hop adjacency-index traversal. |
| Edge property filter | Restrict traversal by edge properties. | High | High | Y | Accepted for one-hop adjacency-index traversal with edge aliases. |
| Bind edge alias | Bind traversed edges as returnable values. | High | High | Y | Accepted for one-hop adjacency-index traversal. |
| Return edge | Return traversed edge objects. | High | High | Y | Accepted for one-hop adjacency-index traversal. |
| Multi-hop traversal | Chain multiple traversal steps. | Very High | Very High | Partial | Accepted indexed execution exists for explicit start node IDs through adjacency indexes; broader indexed starts remain follow-up. |
| Variable-depth traversal | Traverse bounded or unbounded depth with `DepthSpec`. | High | Very High | Partial | Accepted bounded indexed execution exists for explicit start node IDs; unbounded production traversal remains rejected/capped. |
| Path binding | Bind a whole path as a single result value. | Medium | High | Y | Implemented with first-class `PathValue`. |
| Path projection | Return ordered nodes/edges for a path. | Medium | High | Y | Implemented with `RETURN_PROJECTION_KIND_PATH`. |
| Node projection | Return matched nodes. | Very High | Very High | Y | Output mapping exists; matching itself is not accepted. |
| Tree projection | Project matched containment hierarchy as a tree. | High | Very High | N | Current path derives from loaded edges. |
| Scalar projection | Return scalar fields. Current implementation supports field-addressed scalar projections and legacy node-id scalar aliases. | High | High | Y | Accepted for indexed structured paths. |
| Property/payload/meta scalar projection | Return arbitrary node/edge properties, payload fields, or meta fields. | High | Very High | Y | Encoded as `ReturnProjection.alias` values such as `n.title`, `n.payload.text`, or `r.weight`. |
| Mixed projections | Return nodes, trees, edges, and scalars in one row. | Medium | Medium | Y | Accepted for indexed structured paths. |
| Result graph envelope | Deduplicate returned nodes/edges into a graph envelope. | Medium | Medium | Y | Implemented for projected nodes/edges and indexed-root subtree graph results. |
| Query-level limit | Limit total rows before paging. | High | High | Y | Accepted for indexed ordered, equality, adjacency, and root-subtree paths. |
| Response pagination | Page with `page_size` and opaque `page_token`. | High | High | Y | Accepted for index-cursor paths. |
| Offset | Skip the first N rows explicitly. | Medium | Medium | Y | Implemented for fallback and shaped indexed node results. |
| Indexed single-label property ordering | Sort a node-only single-label query by a schema-declared ordered property index, e.g. `JournalEntry ORDER BY date`. | Very High | Very High | Y | Implemented for node-only reads and as the root selector for indexed subtree graph reads, including inclusive `BetweenExpr` bounds and strict `LessThanExpr` upper bounds on the ordered property. |
| General ordering | Sort arbitrary result rows by value expressions. | High | High | Partial | Shaping supports ordered projected/aggregate rows; full arbitrary-index pushdown remains incomplete. |
| Parameters | Reuse a query shape with external parameter values. | High | High | N | Missing. |
| Mutations | Create/update/delete nodes or edges through the structured query API. | Medium | Medium | N | Missing. |
| Schema-aware validation | Validate labels/properties/edge labels in strict schema mode. | Very High | High | Y | Validation does not depend on the rejected executor. |
| Dynamic/schema-free domains | Continue accepting unknown labels/properties without an active strict schema. | Very High | Very High | Y | Accepted API behavior. |
| Warn-mode diagnostics | Surface warnings for schema warn mode. | Medium | Medium | N | Missing. |
| Query counters | Return row and mutation counters. | Medium | Medium | Partial | Row counters and mutation counters are surfaced; edge-updated public counter remains missing. |
| Indexed query diagnostics | Return plan/index/full-scan/load-count diagnostics for accepted indexed query plans. | High | High | Y | Implemented for indexed equality, ordered node scans, adjacency traversal, indexed-root subtree graph reads, aggregation, shaping, and rejection paths. |
| General query diagnostics | Return timing, scan counts, warnings, and planner diagnostics for all query shapes. | Medium | Medium | Y | `QueryDiagnostics` reports planner/version, plan kind, pushed/residual predicates, row/candidate counts, timing, fallback mode, and rejection details where available. |
| Explain plan | Return a planned/optimized form without executing. | Medium | Low | Y | `ExplainQuery` returns diagnostics without graph reads or mutations. |
| General index pushdown | Use graph/metadata/semantic search indexes for all compatible predicates/traversals instead of full in-memory scans. | Very High | Very High | N | Accepted indexed node, ordered, adjacency, and root-subtree paths exist; broader predicate/traversal/text/semantic pushdown is still missing. |
| Node-only execution path | Avoid loading edges when the query only matches and returns nodes. | Very High | Very High | Y | Implemented for indexed equality and ordered node-property scans. |
| Indexed-root subtree graph read | Select ordered/bounded root nodes from an index, expand bounded adjacency traversal, and return one result graph. | Very High | Very High | Y | Implemented for one traversal step using adjacency indexes with `max_nodes`/`max_edges` truncation diagnostics. |
| Cursor pagination from index | Page directly from a stable index cursor. | Very High | Very High | Y | Implemented for ordered node scans and root pagination in indexed subtree graph reads. |
| Ordered property index scans | Support date/name/updated-time ordered reads without full sort. | Very High | Very High | Y | Implemented for schema-declared ordered node property indexes. |
| CLI node-query helper | Build common structured node queries from CLI flags. | Medium | Medium | N | Current helper uses the rejected executor. |
| SDK convenience helpers | Provide ergonomic builders around generated protobuf clients. | Medium | High | Y | Go and Rust SDKs expose helpers/builders for common query and transaction shapes; schema-derived helpers remain separate. |
| Schema-derived client helpers | Generate typed dynamic query helpers from domain schemas. | Medium | Very High | N | Missing. |
| Stored query templates | Save reusable structured query shapes. | Medium | High | N | Missing. |

## Accepted Indexed Query Surface

The accepted production-oriented structured query surface now covers these indexed read shapes:

- schema-indexed equality node starts using `PropertyEqualsExpr` on a single-label start alias;
- schema-indexed ordered node starts using `ORDER BY` on one declared ordered node property, including compatible bounds;
- one-hop outgoing/incoming adjacency traversal from explicit start node IDs, with edge aliases, edge property filters, edge returns, and scalar projections;
- explicit-start and indexed-label/schema-root multi-hop and bounded-depth path traversal through adjacency indexes, with first-class path values;
- indexed root-subtree graph reads that select ordered roots and expand bounded adjacency subtrees into `result.graph`;
- node, edge, tree, scalar, and path projections for accepted indexed shapes;
- aggregate/grouping rows, distinct/order/offset/fetch shaping, stable cursor pagination, read metadata, read-your-writes overlay behavior, and diagnostics/explain output that report plan, predicates, fallback/rejection, row/candidate counts, timing, and indexes.

Unsupported shapes may still execute through the legacy prototype path when the domain allows broad search. Those fallback behaviors are useful for small development graphs, but they are not accepted as production query functionality.

The CLI exposes a common node-query subset:

```sh
mycel query nodes --transaction-id '<tx-id>' --label Note
mycel query nodes --transaction-id '<tx-id>' --tag project
mycel query nodes --transaction-id '<tx-id>' --property-exists status
mycel query nodes --transaction-id '<tx-id>' --property-equals status=active
```

These commands are convenience helpers and should not be treated as proof that every equivalent structured query shape has an accepted indexed plan.

## Example: Indexed Journal Entries by Date

The API shape for journal entries ordered by date is:

```go
query := &clientv1.GraphQuery{
	Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
	Returns: []*clientv1.ReturnProjection{{Alias: "j", OutputName: "journal", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
	OrderBy: []*clientv1.OrderSpec{{
		Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "date"}}},
		Direction: clientv1.SortDirection_SORT_DIRECTION_ASC,
	}},
	Limit: 100,
}
```

When the active domain schema declares an ordered node-property index for `JournalEntry.properties.date`, this shape is accepted as indexed database behavior. The daemon uses an ordered label/property index scan, reads matching journal nodes in date order, and returns a stable cursor plus diagnostics such as `plan=OrderedNodePropertyIndexScan` and `full_scan=false`.

## Top Cross-Roadmap Remaining Query Priorities

After the top-query-priorities completion work, the highest-value remaining query bundles are:

1. **Broader predicate/index pushdown.** Add accepted indexed plans for tag/property-exists combinations, null/missing pushdown, richer comparisons/intersections, full-text ranking, and richer semantic/vector controls.
2. **Cost-based path planning.** Extend indexed path starts and traversal planning beyond the current explicit ID, label, tag, and schema-backed ordered-property root selectors.
3. **Schema-derived and stored query ergonomics.** Generate typed helpers/templates from domain schemas and expose reusable stored query shapes.

## Next Querying Tranche

The next structured API tranche should continue replacing fallback behavior with accepted indexed plans. See [GWL index declarations and indexed query execution](../design/schema/gwl-indexes-and-query-planning.md) for the schema, persistence, mutation-maintenance, and planning model.

1. Add accepted multi-hop and bounded variable-depth traversal plans that do not load every edge in the domain.
2. Add path binding/projection to the structured API, preferably with a dedicated public path value.
3. Add accepted indexed plans for tag, property-exists, and additional property-equality predicates beyond the start-alias equality shape.
4. Add accepted `AND` combinations across compatible indexed predicates.
5. Add text and semantic predicates with index-aware plans.
6. Add planner diagnostics for unsupported shapes before falling back in searchable domains.
7. Add aggregation/result-shaping API shapes after the GQL semantics and public protobuf model are settled.
8. Add schema-derived typed builders and stored query templates after the indexed semantics are stable.

## Production Acceptance Criteria

A structured node query must not be considered implemented until it can:

- answer `JournalEntry ORDER BY date` without scanning unrelated nodes;
- avoid loading edges for node-only queries;
- return stable paginated results from an index cursor;
- preserve strong/read-index consistency and read-your-writes semantics;
- explain whether it used an index or fell back to a scan;
- expose scanned/matched/returned counts in diagnostics.

A structured traversal query must not be considered implemented until it can
start from indexed node candidates and traverse using deterministic adjacency
lookups instead of loading every edge in the domain.

## Relationship to GQL

The structured API and GQL should converge semantically even though they expose
different surfaces:

- GQL currently has richer graph-shaped syntax, edge binding, scalar projection,
  comparisons, text predicates, and a semantic fallback predicate.
- The structured API currently has typed request shapes for pagination, ordering,
  date expressions, tree projection, and tag/property metadata predicates, but
  the current execution strategy is rejected for real database use.
- Features should be added in both layers when they become core query semantics;
  syntax-only conveniences can remain GQL-specific, and typed builder conveniences
  can remain structured-API-specific.
- Scalable query execution should be shared below both surfaces so GQL and the
  structured API do not diverge in consistency or performance.
