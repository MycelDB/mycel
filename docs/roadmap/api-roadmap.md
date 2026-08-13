# Structured Query API Roadmap

This document tracks the roadmap for the structured protobuf query API, sometimes
called the dynamic API in planning notes. It complements the textual
[GQL roadmap](gql-roadmap.md): GQL is the human-readable query language, while
this API is the typed `mycel.client.v1.QueryService.ExecuteQuery` surface used by
SDKs, generated clients, and application builders that prefer protobuf request
construction over query strings.

## Current Reality

The current structured query executor is not a real database query
implementation. It is an early API-shape prototype and should be treated as a
faulty execution strategy for production-sized graphs.

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
| Node pattern start | Match from a required start node pattern and alias. | Very High | Very High | N | Current behavior depends on full-domain node scan. |
| Node labels | Match nodes with required labels. | Very High | Very High | N | Current behavior filters labels after scanning nodes. |
| Inline node property maps | Put property constraints directly on `NodePattern`. | Medium | Medium | N | Missing. |
| Tag predicate | Filter nodes using normalized mycel tags. | High | Very High | N | Current behavior is post-scan. |
| Property exists predicate | Filter nodes with a normalized custom property name. | High | High | N | Current behavior is post-scan. |
| Property equals predicate | Filter nodes by custom property equality. | High | High | N | Current behavior is post-scan. |
| Boolean `AND` | Combine predicates conjunctively. | High | High | N | Current behavior is only in-memory expression evaluation. |
| Boolean `OR` | Combine alternatives. | Medium | Medium | N | Missing. |
| Parenthesized/grouped predicates | Group boolean expressions explicitly. | Medium | Medium | N | Missing. |
| Between predicate | Compare values against low/high bounds. | High | High | N | General execution is incomplete; inclusive ordered-property bounds are supported only inside the indexed single-label ordering path. |
| General comparison operators | Support `<`, `<=`, `>`, `>=`, `!=` as first-class expressions. | High | High | N | General comparator set is missing; `LessThanExpr` exists only for strict ordered-property upper bounds in the indexed single-label ordering path. |
| Date/current-date expressions | Compare date literals and current date with day offsets. | Medium | Medium | N | Expression values exist, but database execution is missing. |
| Text contains predicate | Full-text-style filtering over payload/properties. | High | Very High | N | Missing. |
| Semantic predicate | Semantic/vector similarity from the structured query API. | Medium | Very High | N | Missing. |
| Outgoing traversal | Traverse outgoing edges by label. | Very High | Very High | N | Current path loads all edges. |
| Incoming traversal | Traverse incoming edges by label. | High | High | N | Current path loads all edges. |
| Undirected traversal | Traverse without direction. | Medium | Medium | N | Missing. |
| Edge label filter | Restrict traversal by edge kind/label. | Very High | Very High | N | Current behavior filters edges after loading all edges. |
| Edge property filter | Restrict traversal by edge properties. | High | High | N | Missing. |
| Bind edge alias | Bind traversed edges as returnable values. | High | High | N | Missing. |
| Return edge | Return traversed edge objects. | High | High | N | Missing. |
| Multi-hop traversal | Chain multiple traversal steps. | Very High | Very High | N | Current path is in-memory traversal. |
| Variable-depth traversal | Traverse bounded or unbounded depth with `DepthSpec`. | High | Very High | N | Current path is in-memory traversal. |
| Path binding | Bind a whole path as a single result value. | Medium | High | N | Missing. |
| Path projection | Return ordered nodes/edges for a path. | Medium | High | N | Missing. |
| Node projection | Return matched nodes. | Very High | Very High | Y | Output mapping exists; matching itself is not accepted. |
| Tree projection | Project matched containment hierarchy as a tree. | High | Very High | N | Current path derives from loaded edges. |
| Scalar projection | Return scalar fields. Current implementation returns the bound node ID for scalar projections. | High | High | N | Incomplete surface. |
| Property/payload/meta scalar projection | Return arbitrary node/edge properties, payload fields, or meta fields. | High | Very High | N | Missing. |
| Mixed projections | Return nodes, trees, and scalars in one row. | Medium | Medium | N | Incomplete surface. |
| Result graph envelope | Deduplicate returned nodes/edges into a graph envelope. | Medium | Medium | Y | Implemented for projected nodes/edges and indexed-root subtree graph results. |
| Query-level limit | Limit total rows before paging. | High | High | N | Current limit is post-materialization. |
| Response pagination | Page with `page_size` and opaque `page_token`. | High | High | N | Current pagination is post-materialization. |
| Offset | Skip the first N rows explicitly. | Medium | Medium | N | Missing. |
| Indexed single-label property ordering | Sort a node-only single-label query by a schema-declared ordered property index, e.g. `JournalEntry ORDER BY date`. | Very High | Very High | Y | Implemented for node-only reads and as the root selector for indexed subtree graph reads, including inclusive `BetweenExpr` bounds and strict `LessThanExpr` upper bounds on the ordered property. |
| General ordering | Sort arbitrary result rows by value expressions. | High | High | N | General ordering outside the indexed single-label property shape is not accepted. |
| Parameters | Reuse a query shape with external parameter values. | High | High | N | Missing. |
| Mutations | Create/update/delete nodes or edges through the structured query API. | Medium | Medium | N | Missing. |
| Schema-aware validation | Validate labels/properties/edge labels in strict schema mode. | Very High | High | Y | Validation does not depend on the rejected executor. |
| Dynamic/schema-free domains | Continue accepting unknown labels/properties without an active strict schema. | Very High | Very High | Y | Accepted API behavior. |
| Warn-mode diagnostics | Surface warnings for schema warn mode. | Medium | Medium | N | Missing. |
| Query counters | Return row and mutation counters. | Medium | Medium | N | Incomplete surface. |
| Indexed query diagnostics | Return plan/index/full-scan/load-count diagnostics for accepted indexed query plans. | High | High | Y | Implemented for indexed single-label property ordering. |
| General query diagnostics | Return timing, scan counts, warnings, and planner diagnostics for all query shapes. | Medium | Medium | N | Missing outside accepted indexed plans. |
| Explain plan | Return a planned/optimized form without executing. | Medium | Low | N | Missing. |
| Index pushdown | Use graph/metadata/semantic indexes instead of full in-memory scans where possible. | Very High | Very High | N | Missing. |
| Node-only execution path | Avoid loading edges when the query only matches and returns nodes. | Very High | Very High | Y | Implemented for indexed ordered node-property scans. |
| Indexed-root subtree graph read | Select ordered/bounded root nodes from an index, expand bounded adjacency traversal, and return one result graph. | Very High | Very High | Y | Implemented for one traversal step using adjacency indexes with `max_nodes`/`max_edges` truncation diagnostics. |
| Cursor pagination from index | Page directly from a stable index cursor. | Very High | Very High | Y | Implemented for ordered node scans and root pagination in indexed subtree graph reads. |
| Ordered property index scans | Support date/name/updated-time ordered reads without full sort. | Very High | Very High | Y | Implemented for schema-declared ordered node property indexes. |
| CLI node-query helper | Build common structured node queries from CLI flags. | Medium | Medium | N | Current helper uses the rejected executor. |
| SDK convenience helpers | Provide ergonomic builders around generated protobuf clients. | Medium | High | N | Only thin generated/helper calls exist. |
| Schema-derived client helpers | Generate typed dynamic query helpers from domain schemas. | Medium | Very High | N | Missing. |
| Stored query templates | Save reusable structured query shapes. | Medium | High | N | Missing. |

## Current Prototype Behavior

The current structured API can express small read queries such as:

- match nodes by label;
- filter by tag, custom property exists, custom property equality, `AND`, and
  `between`;
- use date and current-date value expressions;
- traverse outgoing/incoming edges by edge label;
- chain traversal steps and use depth ranges;
- return nodes and containment trees;
- sort, limit, and page results;
- validate against strict domain schemas while preserving dynamic behavior for
  schema-free/permissive domains;
- return read metadata and normalized result envelopes.

Except for validation, transaction scoping, read metadata, and basic output
mapping, this list describes prototype behavior only. It should not be described
as implemented database query functionality because it depends on full-domain
node and edge loads.

The CLI exposes only a common node-query subset:

```sh
mycel query nodes --transaction-id '<tx-id>' --label Note
mycel query nodes --transaction-id '<tx-id>' --tag project
mycel query nodes --transaction-id '<tx-id>' --property-exists status
mycel query nodes --transaction-id '<tx-id>' --property-equals status=active
```

These commands also use the rejected full-scan structured query executor.

## Example: Journal Entries by Date

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

That is only an API shape today. It is not accepted as implemented database
behavior. The current executor scans the domain, filters `JournalEntry` rows in
memory, sorts rows in memory, and only then pages results.

The production implementation must perform an ordered label/property index scan
that reads only matching journal nodes in date order and returns a stable cursor.

## Required Reimplementation Tranche

Before adding more structured query surface area, replace the rejected executor
with a storage/index-backed query path. See
[GWL index declarations and indexed query execution](../design/schema/gwl-indexes-and-query-planning.md)
for the proposed schema, persistence, mutation-maintenance, and planning model.

1. Define queryable field addressing for `properties`, `payload`, and `meta`.
2. Add label, tag, property-exists, property-equality, and ordered property index
   reads with strong/read-index consistency.
3. Add a node-only query path that does not load edges unless traversal or edge
   projection requires them.
4. Push compatible predicates, ordering, `limit`, and page size into index reads
   before materialization.
5. Return stable cursor pagination from the index/read plan, not from an
   in-memory result slice.
6. Add diagnostics that report the plan, index use, scanned counts, matched
   counts, returned counts, and whether a fallback scan occurred.
7. Use deterministic adjacency lookups for traversal instead of loading all
   domain edges.
8. Preserve read-your-writes semantics by overlaying transaction writes onto the
   indexed read plan without reverting to full-domain scans.

## Later API Surface Work

After the execution engine is reimplemented, add or complete:

1. property/payload/meta scalar projections;
2. edge binding and edge return projections;
3. edge property predicates;
4. `OR` and general comparison operators;
5. text predicates and semantic predicates with index-aware plans;
6. schema-derived typed builders for dynamic application APIs;
7. stored query templates.

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
