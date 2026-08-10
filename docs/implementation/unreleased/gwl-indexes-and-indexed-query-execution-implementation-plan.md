# GWL Indexes and Indexed Query Execution Implementation Plan

## Status

In progress. IDX1-IDX2 are implemented for schema/GWL index declarations,
validation, persistence, and diffing. IDX3-IDX4 are implemented at the graph
storage level for node label indexes, ordered node property index definitions,
foreground backfill on configuration, restart rebuild, and synchronous
insert/update/delete maintenance. IDX5-IDX6 are implemented at the graph storage
level for label-aware adjacency scans, ordered edge property indexes,
insert/update/delete maintenance, restart rebuild, and explicit configure/remove/change lifecycle handling. IDX7-IDX8 are implemented for the structured API's node-only ordered-property query shape, including index-backed execution and public query diagnostics. IDX9 is implemented for equivalent GQL `ORDER BY` syntax/planning and daemon execution through the same indexed path. IDX10 docs are updated for the implemented scope. Automatic schema-service-to-graph index lifecycle wiring remains a follow-up.

Design reference:
[GWL index declarations and indexed query execution](../../design/schema/gwl-indexes-and-query-planning.md).

This plan replaces the current full-domain in-memory structured query executor
with an index-backed execution path for production query shapes. It should be
implemented in tranches so each tranche leaves mycel functional.

## Problem Statement

The current structured query path accepts useful query shapes, but executes them
by reading all nodes and all edges in a domain, then filtering, sorting, limiting,
and paging in daemon memory. That is not acceptable for a real database query
path.

The first motivating query is:

```text
all JournalEntry nodes ordered by properties.date
```

This must execute as an ordered index scan, not a full-domain graph scan.

## Goals

- Add GWL/schema support for node and edge index declarations.
- Persist index definitions with domain schema records.
- Add graph-subsystem persisted index data for labels, ordered node properties,
  and edge adjacency.
- Maintain indexes synchronously on graph mutation apply.
- Backfill indexes when schemas are modified after data already exists.
- Add an indexed planner/executor path for structured node queries.
- Fail closed when a production query needs an unavailable index.
- Add diagnostics proving whether a query used indexes or attempted a scan.
- Keep GQL and structured query planning aligned through shared internals where
  practical.

## Non-goals

- Do not make indexes authoritative graph data. Indexes are derived from graph
  records plus schema index definitions.
- Do not introduce automatic graph repair or authoritative-node selection.
- Do not silently fall back to full-domain scans for production query execution.
- Do not commit generated ANTLR/internal parser artifacts unless explicitly
  approved.
- Do not require full text, semantic, composite, or covering indexes in the first
  tranche.

## Implementation Principles

- **Schema definitions are authoritative metadata.** Index declarations live with
  the domain schema/GWL source and compiled schema model.
- **Graph/WAL/raft records are authoritative data.** Index data is derived and
  rebuildable.
- **Synchronous maintenance.** Successful graph writes must not leave ready index
  data stale.
- **Fail closed.** If a query requires an index that is missing, building, stale,
  or failed, return a clear error instead of scanning the domain.
- **Node-only queries must not load edges.** Edge loads should occur only for
  traversal or edge projection plans.
- **Diagnostics are required.** Query responses or debug surfaces must prove plan
  choice, index use, and scanned/loaded counts.

## Phase IDX1: Schema Model and GWL Parser

### Tasks

1. Add first-class index definitions to `internal/schema/model`.

   Proposed structs:

   ```go
   type IndexTargetKind string
   const (
       IndexTargetNode IndexTargetKind = "node"
       IndexTargetEdge IndexTargetKind = "edge"
   )

   type IndexKind string
   const (
       IndexKindEquality IndexKind = "equality"
       IndexKindOrdered  IndexKind = "ordered"
   )

   type FieldPath struct {
       Namespace string // properties | payload | meta
       Name      string
   }

   type IndexDefinition struct {
       Name       string
       TargetKind IndexTargetKind
       TargetType string
       Labels     []string
       Field      FieldPath
       Kind       IndexKind
       Direction  string // asc | desc for ordered indexes
       Unique     bool
       Required   bool
   }
   ```

2. Add `Indexes []IndexDefinition` to `DomainSchema`.
3. Extend schema normalization and validation:
   - unique index names per domain schema;
   - target node/edge type exists;
   - field namespace is initially `properties`;
   - field exists on target type in strict schemas;
   - ordered indexes require scalar comparable field types;
   - index direction defaults to `asc`;
   - generated labels resolve from target type.
4. Extend `internal/schema/dsl/parser.go` for explicit declarations:

   ```gwl
   index journal_entries_by_date on node JournalEntry field properties.date ordered asc
   index references_by_confidence on edge REFERENCES field properties.confidence ordered desc
   ```

5. Add parser/validator tests.
6. Defer compact field annotation syntax unless the explicit syntax is complete
   and well tested.

### Acceptance

- GWL with node and edge index declarations parses into `DomainSchema.Indexes`.
- Invalid target types, field paths, duplicate names, and non-orderable fields
  fail validation.
- Existing schemas without index declarations continue to parse and validate.

## Phase IDX2: Schema Service Persistence and Diffing

### Tasks

1. Ensure schema storage preserves index definitions in canonical persisted schema
   records.
2. Add schema diff helpers:
   - added indexes;
   - removed indexes;
   - changed indexes, treated as remove + add.
3. Expose compiled index definitions to callers that need planning and graph
   index lifecycle coordination.
4. Update raft/WAL schema apply paths so all nodes receive identical schema index
   definitions.
5. Add tests for put/get/delete schema with indexes and raft snapshot/reload.

### Acceptance

- `PutDomainSchema` persists index definitions and `GetDomainSchema` returns
  source that still includes them.
- Schema raft/WAL replay restores index definitions.
- A schema update can identify newly added indexes for backfill.

## Phase IDX3: Graph Index Storage Interfaces

### Tasks

1. Add graph index abstractions under `internal/graph/storage` or a subpackage:

   ```go
   type NodeIndexReader interface {
       ScanLabel(ctx context.Context, domainID, label, cursor string, limit int) (...)
       ScanNodePropertyOrdered(ctx context.Context, spec OrderedNodePropertyScan) (...)
   }

   type NodeIndexWriter interface {
       PutNodeIndexEntries(ctx context.Context, node graph.Node, indexes []schema.IndexDefinition) error
       DeleteNodeIndexEntries(ctx context.Context, node graph.Node, indexes []schema.IndexDefinition) error
   }
   ```

2. Add deterministic key encoding for:
   - label index: `domain | label | node_id`;
   - ordered node property index: `domain | index_name | value | node_id`;
   - edge adjacency: `domain | from/to | edge_label | order | edge_id`.
3. Add cursor encoding/decoding from index keys.
4. Store index metadata:

   ```text
   index_name
   schema_hash
   target_kind
   field_path
   build_state
   last_indexed_graph_revision
   key_encoding_version
   ```

5. Implement a simple durable index file/table format. The first tranche may use
   append/rebuild-friendly segments if it preserves deterministic ordering and
   restart behavior.
6. Add rebuild APIs that derive index data from authoritative graph records and
   schema index definitions.

### Acceptance

- Index data survives restart or is rebuilt deterministically at startup.
- Ordered scans return stable `(value, node_id)` order.
- Cursor resume returns the next page without duplicating or skipping records.
- Corrupt/missing index data marks the affected index unavailable rather than
  returning wrong query results.

## Phase IDX4: Synchronous Node Index Maintenance

### Tasks

1. Wire schema/index definitions into graph store mutation apply paths.
2. On node insert:
   - validate indexed values;
   - insert label index entries;
   - insert ordered property index entries for matching node index definitions.
3. On node update:
   - load old node state;
   - delete old derived index entries;
   - insert new derived index entries.
4. On node delete:
   - delete all derived index entries for the old node.
5. Add tests for:
   - inserting `JournalEntry.date`;
   - updating date;
   - adding/removing `JournalEntry` label;
   - deleting node;
   - optional indexed field absent;
   - invalid required indexed field;
   - rebuild parity after restart.

### Acceptance

- A committed node write and corresponding index updates are atomic from query
  perspective.
- `JournalEntry.properties.date` index reflects insert/update/delete without a
  manual refresh.
- Writes fail if they would violate required indexed field/type rules.

## Phase IDX5: Edge Adjacency and Edge Property Indexes

### Tasks

1. Treat adjacency as a required system index for traversal:
   - outgoing by `from_node_id + edge_label`;
   - incoming by `to_node_id + edge_label`;
   - stable ordering by explicit `order` property when present, then edge ID.
2. Persist adjacency index data or ensure the existing adjacency structure is
   rebuilt/maintained in a query-usable way without full edge loads.
3. Add explicit edge property index maintenance for declared edge indexes:

   ```gwl
   index references_by_confidence on edge REFERENCES field properties.confidence ordered desc
   ```

4. On edge insert/update/delete, synchronously maintain adjacency and declared
   edge property index entries.
5. Add tests for edge insert/update/delete, confidence changes, label changes,
   and restart/rebuild parity.

### Acceptance

- Traversal can fetch candidate edges from adjacency indexes without loading all
  domain edges.
- Declared ordered edge indexes can scan edge IDs in value order.
- Edge index data is rebuildable from authoritative edge records.

## Phase IDX6: Index Backfill on Schema Changes

### Tasks

1. On schema update, diff old and new index definitions.
2. For added indexes:
   - create index metadata in `building` state;
   - start foreground or background backfill;
   - reject queries requiring the index while it is building.
3. Backfill by scanning authoritative graph records for the domain and inserting
   entries for matching records.
4. Handle concurrent writes using one of:
   - maintain new index synchronously after schema metadata is installed while
     historical records are backfilled idempotently;
   - record build start revision and apply a catch-up pass;
   - temporarily reject domain writes during foreground build.
5. Mark index `ready` only after backfill and catch-up reach a consistent graph
   revision.
6. For removed indexes:
   - remove from planner eligibility immediately;
   - retire/delete physical data after active readers drain.
7. For changed indexes:
   - treat as remove + add.
8. Add operator-visible status for index build states.

### Acceptance

- Adding `journal_entries_by_date` after data exists builds a correct index from
  existing `JournalEntry` nodes.
- Queries requiring the new index fail closed until it is ready.
- Concurrent writes during backfill are included in the ready index.
- Removing an index prevents future planner use without changing graph data.

## Phase IDX7: Indexed Structured Query Planner

### Tasks

1. Split structured query handling into analysis, planning, and execution.
2. Add plan nodes:

   ```text
   LabelIndexScan
   OrderedNodePropertyIndexScan
   NodeFetch
   AdjacencyTraversal
   Projection
   ```

3. Recognize node-only ordered query shape:

   ```text
   labels=[JournalEntry]
   order_by=properties.date ASC
   returns=node
   no traversal
   ```

4. Compile it to `OrderedNodePropertyIndexScan`.
5. Push page size and limit into the scan.
6. Fetch only returned node IDs.
7. Do not load edges for node-only plans.
8. Add transaction overlay merge for staged writes without domain scans.
9. Reject missing/unready index plans with `FailedPrecondition`.
10. Add tests proving no call to `allNodes`/`allEdges` occurs for the indexed
    journal query.

### Acceptance

- Structured `JournalEntry ORDER BY date` executes through an ordered index scan.
- The query returns stable pages using index cursors.
- It does not load unrelated nodes.
- It does not load any edges.
- Missing or building indexes return clear `FailedPrecondition` errors.

## Phase IDX8: Query Diagnostics

### Tasks

1. Add diagnostics model to internal query result and public API if API changes
   are approved.
2. Include:
   - plan name;
   - index names used;
   - full scan attempted/used flag;
   - index entries scanned;
   - nodes loaded;
   - edges loaded;
   - rows returned;
   - rejection reason for failed planning.
3. Add CLI/debug rendering for diagnostics.
4. Add tests that assert diagnostics for indexed journal queries.

### Acceptance

A successful journal query can prove:

```text
plan: OrderedNodePropertyIndexScan
index: journal_entries_by_date
full_scan: false
edges_loaded: 0
```

## Phase IDX9: GQL Planner Parity

### Tasks

1. Add or complete GQL `ORDER BY` syntax/analysis if not already present.
2. Route compatible GQL match/order queries into the shared indexed planner.
3. Ensure GQL and structured API produce the same plan for equivalent query
   shapes.
4. Add GQL tests:

   ```gql
   MATCH (j:JournalEntry)
   RETURN j
   ORDER BY j.date
   FETCH FIRST 100 ROWS ONLY
   ```

### Acceptance

- Equivalent GQL and structured queries use the same ordered node property index.
- GQL does not silently full-scan when a required index is missing.

## Phase IDX10: Documentation and Operations

### Tasks

1. Update schema design docs with final GWL syntax.
2. Update query/API roadmap statuses after implementation.
3. Add operator docs for:
   - listing index build status;
   - rebuilding derived index data;
   - interpreting failed/stale/building indexes;
   - query failure behavior when indexes are missing.
4. Add release notes for any public API/SDK changes.

### Acceptance

- Docs describe how to declare, build, query, rebuild, and troubleshoot indexes.
- Roadmaps distinguish completed indexed execution from unsupported scan plans.

## Test Matrix

Minimum tests before calling the first tranche complete:

```sh
go test ./internal/schema/...
go test ./internal/graph/storage/...
go test ./internal/graph/service/...
go test ./internal/daemon/api/client/... -run Query
go test ./internal/query/...
make docs-check
git diff --check
```

Add an integration test that:

1. creates a strict schema without index;
2. inserts several `JournalEntry` nodes;
3. updates schema to add `journal_entries_by_date`;
4. waits for or triggers index build;
5. queries journal entries ordered by date;
6. asserts sorted results, index diagnostics, `edges_loaded=0`, and no full scan.

## Open Questions

- Should first implementation use foreground index builds or background builds?
- Should production query scan fallback be globally impossible or only disabled by
  default with an explicit debug option?
- Should index definitions live only in GWL source, or should the public schema
  API expose compiled index status separately?
- What physical index format should be used first: append-only segment, embedded
  key-value table, or existing storage abstraction extension?
- How should read-write transaction overlay merge be represented for ordered
  cursors when staged writes sort before/after committed page boundaries?
