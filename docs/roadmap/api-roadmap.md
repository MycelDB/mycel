# Structured Query API Roadmap

This document tracks the roadmap for the structured protobuf query API, sometimes
called the dynamic API in planning notes. It complements the textual
[GQL roadmap](gql-roadmap.md): GQL is the human-readable query language, while
this API is the typed `mycel.client.v1.QueryService.ExecuteQuery` surface used by
SDKs, generated clients, and application builders that prefer protobuf request
construction over query strings.

## Current Reality

The general structured query executor is not a real database query
implementation. Accepted indexed paths are production-oriented, but unsupported
shapes may still use the legacy prototype executor in searchable domains and
should not be treated as production-sized graph functionality.

`ExecuteQuery` currently:

1. reads all nodes in the transaction's domain;
2. reads all edges in the transaction's domain, even for node-only queries;
3. builds in-process lookup maps;
4. evaluates label filters, predicates, traversal, ordering, and projection in
   daemon memory;
5. applies limit and page-token pagination after matching and sorting.

That execution model is not acceptable for large PKM graphs or general database
use. Features whose only implementation depends on this full-domain in-memory
executor are not considered implemented for roadmap purposes; they need to be
implemented again on top of proper indexed/storage-backed execution.

## Scope

The structured query API aims to provide a safe, typed, transaction-scoped graph
query surface for dynamic applications. The production implementation must
support graph pattern reads, metadata/property predicates, projections, ordering,
pagination, schema-aware validation, and dynamic application builders without
requiring full-domain scans.

The current API is read-only. Graph mutations remain in the Graph API or textual
GQL write statements.

## Current Status Values

The **Current status** column intentionally uses only **Y** or **N**:

- **Y**: accepted as currently implemented for roadmap purposes.
- **N**: not accepted as implemented. This includes missing features,
  incomplete API surfaces, and features whose only behavior depends on the
  rejected full-domain in-memory executor.

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
| Boolean `OR` | Combine alternatives. | Medium | Medium | N | Missing. |
| Parenthesized/grouped predicates | Group boolean expressions explicitly. | Medium | Medium | N | Missing. |
| Between predicate | Compare values against low/high bounds. | High | High | N | General execution is incomplete; inclusive ordered-property bounds are supported only inside the indexed single-label ordering path. |
| General comparison operators | Support `<`, `<=`, `>`, `>=`, `!=` as first-class expressions. | High | High | N | General comparator set is missing; `LessThanExpr` exists only for strict ordered-property upper bounds in the indexed single-label ordering path. |
| Date/current-date expressions | Compare date literals and current date with day offsets. | Medium | Medium | N | Expression values exist, but database execution is missing. |
| Text contains predicate | Full-text-style filtering over payload/properties. | High | Very High | N | Missing. |
| Semantic predicate | Semantic/vector similarity from the structured query API. | Medium | Very High | N | Missing. |
| Outgoing traversal | Traverse outgoing edges by label. | Very High | Very High | Y | Accepted for one-hop adjacency-index traversal from explicit start node IDs. |
| Incoming traversal | Traverse incoming edges by label. | High | High | Y | Accepted for one-hop adjacency-index traversal from explicit start node IDs. |
| Undirected traversal | Traverse without direction. | Medium | Medium | N | Missing. |
| Edge label filter | Restrict traversal by edge kind/label. | Very High | Very High | Y | Accepted for one-hop adjacency-index traversal. |
| Edge property filter | Restrict traversal by edge properties. | High | High | Y | Accepted for one-hop adjacency-index traversal with edge aliases. |
| Bind edge alias | Bind traversed edges as returnable values. | High | High | Y | Accepted for one-hop adjacency-index traversal. |
| Return edge | Return traversed edge objects. | High | High | Y | Accepted for one-hop adjacency-index traversal. |
| Multi-hop traversal | Chain multiple traversal steps. | Very High | Very High | N | Current path is in-memory traversal. |
| Variable-depth traversal | Traverse bounded or unbounded depth with `DepthSpec`. | High | Very High | N | Current path is in-memory traversal. |
| Path binding | Bind a whole path as a single result value. | Medium | High | N | Missing. |
| Path projection | Return ordered nodes/edges for a path. | Medium | High | N | Missing. |
| Node projection | Return matched nodes. | Very High | Very High | Y | Output mapping exists; matching itself is not accepted. |
| Tree projection | Project matched containment hierarchy as a tree. | High | Very High | N | Current path derives from loaded edges. |
| Scalar projection | Return scalar fields. Current implementation supports field-addressed scalar projections and legacy node-id scalar aliases. | High | High | Y | Accepted for indexed structured paths. |
| Property/payload/meta scalar projection | Return arbitrary node/edge properties, payload fields, or meta fields. | High | Very High | Y | Encoded as `ReturnProjection.alias` values such as `n.title`, `n.payload.text`, or `r.weight`. |
| Mixed projections | Return nodes, trees, edges, and scalars in one row. | Medium | Medium | Y | Accepted for indexed structured paths. |
| Result graph envelope | Deduplicate returned nodes/edges into a graph envelope. | Medium | Medium | Y | Implemented for projected nodes/edges and indexed-root subtree graph results. |
| Query-level limit | Limit total rows before paging. | High | High | Y | Accepted for indexed ordered, equality, adjacency, and root-subtree paths. |
| Response pagination | Page with `page_size` and opaque `page_token`. | High | High | Y | Accepted for index-cursor paths. |
| Offset | Skip the first N rows explicitly. | Medium | Medium | N | Missing. |
| Indexed single-label property ordering | Sort a node-only single-label query by a schema-declared ordered property index, e.g. `JournalEntry ORDER BY date`. | Very High | Very High | Y | Implemented for node-only reads and as the root selector for indexed subtree graph reads, including inclusive `BetweenExpr` bounds and strict `LessThanExpr` upper bounds on the ordered property. |
| General ordering | Sort arbitrary result rows by value expressions. | High | High | N | General ordering outside the indexed single-label property shape is not accepted. |
| Parameters | Reuse a query shape with external parameter values. | High | High | N | Missing. |
| Mutations | Create/update/delete nodes or edges through the structured query API. | Medium | Medium | N | Missing. |
| Schema-aware validation | Validate labels/properties/edge labels in strict schema mode. | Very High | High | Y | Validation does not depend on the rejected executor. |
| Dynamic/schema-free domains | Continue accepting unknown labels/properties without an active strict schema. | Very High | Very High | Y | Accepted API behavior. |
| Warn-mode diagnostics | Surface warnings for schema warn mode. | Medium | Medium | N | Missing. |
| Query counters | Return row and mutation counters. | Medium | Medium | N | Incomplete surface. |
| Indexed query diagnostics | Return plan/index/full-scan/load-count diagnostics for accepted indexed query plans. | High | High | Y | Implemented for indexed equality, ordered node scans, adjacency traversal, and indexed-root subtree graph reads. |
| General query diagnostics | Return timing, scan counts, warnings, and planner diagnostics for all query shapes. | Medium | Medium | N | Missing outside accepted indexed plans. |
| Explain plan | Return a planned/optimized form without executing. | Medium | Low | N | Missing. |
| Index pushdown | Use graph/metadata/semantic indexes instead of full in-memory scans where possible. | Very High | Very High | N | Missing. |
| Node-only execution path | Avoid loading edges when the query only matches and returns nodes. | Very High | Very High | Y | Implemented for indexed equality and ordered node-property scans. |
| Indexed-root subtree graph read | Select ordered/bounded root nodes from an index, expand bounded adjacency traversal, and return one result graph. | Very High | Very High | Y | Implemented for one traversal step using adjacency indexes with `max_nodes`/`max_edges` truncation diagnostics. |
| Cursor pagination from index | Page directly from a stable index cursor. | Very High | Very High | Y | Implemented for ordered node scans and root pagination in indexed subtree graph reads. |
| Ordered property index scans | Support date/name/updated-time ordered reads without full sort. | Very High | Very High | Y | Implemented for schema-declared ordered node property indexes. |
| CLI node-query helper | Build common structured node queries from CLI flags. | Medium | Medium | N | Current helper uses the rejected executor. |
| SDK convenience helpers | Provide ergonomic builders around generated protobuf clients. | Medium | High | N | Only thin generated/helper calls exist. |
| Schema-derived client helpers | Generate typed dynamic query helpers from domain schemas. | Medium | Very High | N | Missing. |
| Stored query templates | Save reusable structured query shapes. | Medium | High | N | Missing. |

## Accepted Indexed Query Surface

The accepted production-oriented structured query surface now covers these indexed read shapes:

- schema-indexed equality node starts using `PropertyEqualsExpr` on a single-label start alias;
- schema-indexed ordered node starts using `ORDER BY` on one declared ordered node property, including compatible bounds;
- one-hop outgoing/incoming adjacency traversal from explicit start node IDs, with edge aliases, edge property filters, edge returns, and scalar projections;
- indexed root-subtree graph reads that select ordered roots and expand bounded adjacency subtrees into `result.graph`;
- node, edge, tree, and scalar projections for accepted indexed shapes;
- stable index-cursor pagination, read metadata, read-your-writes overlay behavior, and diagnostics that report plan/index/full-scan/load counts.

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

## Next Querying Tranche

The next structured API tranche should continue replacing fallback behavior with accepted indexed plans. See [GWL index declarations and indexed query execution](../design/schema/gwl-indexes-and-query-planning.md) for the schema, persistence, mutation-maintenance, and planning model.

1. Add accepted indexed plans for tag, property-exists, and additional property-equality predicates beyond the start-alias equality shape.
2. Add accepted `AND` combinations across compatible indexed predicates.
3. Add accepted multi-hop and bounded variable-depth traversal plans that do not load every edge in the domain.
4. Add path binding/projection to the structured API, preferably with a dedicated public path value.
5. Add text and semantic predicates with index-aware plans.
6. Add planner diagnostics for unsupported shapes before falling back in searchable domains.
7. Add schema-derived typed builders and stored query templates after the indexed semantics are stable.

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
