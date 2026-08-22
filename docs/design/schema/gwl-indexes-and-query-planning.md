# GWL Index Declarations and Indexed Query Execution

## Status

Partially implemented. GWL index declarations, schema persistence/diffing, graph-storage node/edge indexes, foreground configure/backfill, synchronous maintenance, structured indexed `JournalEntry ORDER BY date`-style execution, query diagnostics, and equivalent GQL `ORDER BY` execution are implemented for the initial single-label ordered-property shape. General query planning, automatic schema-service-to-graph lifecycle wiring, broad GQL ordering, and operational rebuild/status commands remain follow-ups.

## Motivation

The current structured query executor can express queries such as "all
`JournalEntry` nodes ordered by `date`", but it answers them by reading all nodes
and edges in a domain and then filtering, sorting, limiting, and paging in daemon
memory. That execution model is not acceptable for a real database path.

mycel needs schema-declared indexes so the query planner can execute common graph
queries from deterministic storage/index structures instead of full-domain scans.
For example, this query shape:

```text
MATCH node label JournalEntry
ORDER BY properties.date ASC
LIMIT/PAGE 100
```

must become an ordered index scan over `JournalEntry.properties.date`, not a scan
of every node in the domain.

## Goals

- Add explicit index declarations to mycel GWL/domain schemas.
- Persist index definitions as part of the authoritative domain schema record.
- Persist index data in the graph subsystem as derived, rebuildable query state.
- Maintain index data synchronously on graph mutation apply.
- Teach structured query and GQL planning to require/use indexes for scalable
  node lookup, ordered property scans, and traversal starts.
- Fail closed when a production query requires an index that is missing or not
  ready.
- Preserve strong/read-index semantics and read-your-writes transaction overlay
  behavior.

## Non-goals

- Do not make query indexes authoritative graph data. Authoritative graph state
  remains nodes, edges, raft/WAL commands, and committed graph records.
- Do not silently fall back to full-domain scans for production query paths.
- Do not implement automatic restore or graph repair. Rebuilding an index only
  re-derives local index state from authoritative local graph records and schema
  definitions; it does not change graph data.
- Do not require generated ANTLR/internal generated code in this design.
- Do not standardize generic ISO GQL schema DDL. This is a mycel GWL extension.

## Terminology

| Term | Meaning |
|---|---|
| Index definition | Schema-owned declaration of an index: target type, field path, kind, ordering, uniqueness, and options. |
| Index data | Graph-subsystem derived data structure mapping encoded keys to node/edge IDs. |
| Indexed plan | Query plan that can answer a query from index reads and targeted entity fetches. |
| Fallback scan | Full-domain node/edge scan. This is rejected for production query execution. |
| Covering index | Future index that stores enough projected scalar values to avoid fetching every entity. Not required for the first implementation. |

## GWL Syntax

Index declarations are a mycel GWL extension. They should support both explicit
named declarations and compact field annotations.

### Explicit declaration

```gwl
schema "Knot PKM Content" version "1" mode strict

node JournalEntry labels [JournalEntry] {
  date: date required
  title: string
}

index journal_entries_by_date
  on node JournalEntry
  field properties.date
  ordered asc
```

The same declaration may be rendered on one line if the parser supports it:

```gwl
index journal_entries_by_date on node JournalEntry field properties.date ordered asc
```

### Field annotation shorthand

```gwl
node JournalEntry labels [JournalEntry] {
  date: date required indexed ordered asc
  title: string
}
```

The shorthand compiles to a named index definition. The compiler should generate
a stable index name, for example:

```text
node_JournalEntry_properties_date_ordered_asc
```

### Initial grammar concept

```text
indexDecl := "index" IDENT "on" ("node" | "edge") TYPE_NAME
             "field" FIELD_PATH indexOption*

indexOption := "ordered" ("asc" | "desc")?
             | "unique"
             | "required"
             | "include" FIELD_PATH_LIST

FIELD_PATH := ("properties" | "payload" | "meta") "." IDENT
```

Only `properties.<field>` should be required for the first tranche. `payload` and
`meta` can be enabled once field addressing and value normalization are settled.

## Canonical Schema Model

The GWL parser should compile index declarations into the canonical domain schema
model. Conceptually:

```go
type IndexDefinition struct {
    Name        string
    TargetKind  IndexTargetKind // node | edge
    TargetType  string          // schema node/edge type name
    Labels      []string        // resolved labels for planning
    Field       FieldPath       // properties.date, payload.text, meta.source
    Kind        IndexKind       // equality | ordered | full_text | semantic
    Direction   SortDirection   // asc | desc, for ordered indexes
    Unique      bool
    Required    bool
    Include     []FieldPath
}

type FieldPath struct {
    Namespace string // properties | payload | meta
    Name      string
}
```

`NodeType.Indexing`/`EdgeType.Indexing` can remain for broad policy, but explicit
query indexes should be represented as first-class named definitions so planners
can reason about exact coverage.

## Index Types

### Required first tranche

| Index | Purpose |
|---|---|
| Node label index | Find nodes by domain + label without scanning all nodes. May be system-owned/implicit. |
| Node property equality index | Find nodes by domain + label/type + field value. |
| Ordered node property index | Return nodes by domain + label/type + field value order. Required for `JournalEntry ORDER BY date`. |
| Edge adjacency index | Traverse from/to node by edge label. May use or extend the existing graph adjacency index. |

### Later tranches

| Index | Purpose |
|---|---|
| Edge property index | Filter traversed or matched edges by property. |
| Full-text index | Back text predicates over payload/properties. |
| Semantic search index | Back semantic similarity predicates. |
| Covering index | Store selected scalar values to reduce entity fetches. |
| Composite index | Support multi-field filters/orderings. |

## Persistence Model

### Index definitions

Index definitions are authoritative schema metadata:

- stored with the domain schema source/model;
- replicated through the schema subsystem's raft/WAL path when schema replication
  is enabled;
- versioned by schema source hash and schema revision;
- available to the query planner through the schema manager.

### Index data

Index data belongs to the graph subsystem, colocated with the graph partition that
owns the indexed graph records.

Index data is derived and rebuildable, but it must be durable enough for normal
startup and query performance. A possible physical layout:

```text
<space>/<domain>/graph/
  manifest.mycel
  segments/
    nodes-000001.kseg
    edges-000001.kseg
    txns-000001.kseg
  indexes/
    manifest.json
    label/
    node_property/
    edge_adjacency/
```

The graph store should track for each persisted index:

```text
index_name
schema_hash
field_path
target_type/labels
last_indexed_graph_revision
build_state: building | ready | failed | stale
key_encoding_version
```

A query may use an index only when its state is `ready` for the requested schema
hash and graph revision/read context.

## Schema Changes and Index Backfill

Schema changes can add, remove, or change index definitions after a domain
already contains data. Index lifecycle must be explicit because an index
definition becoming visible is not the same thing as index data being ready.

### Adding an index to an existing domain

Example: a domain starts with `JournalEntry` nodes and no date index. Later the
schema is updated to add:

```gwl
index journal_entries_by_date on node JournalEntry field properties.date ordered asc
```

The expected flow is:

1. `PutDomainSchema` validates and persists the new schema source/model through
   the schema subsystem.
2. The graph subsystem compares old and new index definitions and discovers the
   added `journal_entries_by_date` index.
3. It creates index metadata in `building` state for the target schema hash.
4. Queries that require `journal_entries_by_date` fail closed while the index is
   `building`:

   ```text
   FailedPrecondition: index journal_entries_by_date is building
   ```

5. The graph subsystem backfills the index by scanning authoritative committed
   graph records for the domain and inserting keys for matching records.
6. Writes that commit while the backfill is running must also maintain the new
   index, or be applied through a deterministic catch-up phase, so the final index
   includes both historical and concurrent writes.
7. When backfill and catch-up reach a consistent graph revision, the index state
   becomes `ready` with `last_indexed_graph_revision` recorded.
8. New queries can use the ordered index scan.

Backfill is not graph repair. It does not choose authoritative graph data or
modify nodes/edges. It only derives local index data from already authoritative
graph records plus the active schema index definition.

### Backfill consistency model

A safe first implementation can use one of these approaches:

| Approach | Behavior |
|---|---|
| Foreground schema update | `PutDomainSchema` blocks until required indexes are built, then returns. Simple but can make schema updates slow. |
| Background build with fail-closed queries | `PutDomainSchema` returns after schema/index metadata is persisted; queries requiring the new index fail until build is ready. Preferred for large domains. |
| Operator-triggered build | Schema update records the index as pending; an explicit build command constructs it. Useful if index builds need scheduling controls. |

The preferred production model is background build with fail-closed queries and
explicit diagnostics. The first implementation may choose foreground build for
simplicity if it is bounded and clearly documented.

### Concurrent writes during backfill

The graph subsystem must not lose writes that occur while an index is building.
Acceptable strategies include:

- maintain new index entries synchronously for all writes after the schema/index
  metadata is installed, while the backfill fills historical records idempotently;
- record a build start revision, backfill records up to that revision, then apply
  a catch-up pass for graph revisions after that point;
- temporarily reject writes for the affected domain during foreground builds.

The first two strategies are preferred. Temporary write rejection should be an
explicit operational tradeoff, not hidden behavior.

### Missing or invalid indexed values

For optional indexed fields, records without the field simply do not produce an
index entry.

For required schema fields, behavior depends on schema compatibility policy:

- if schema update validates existing graph data, the update fails before the
  index is created when existing records violate the required field/type;
- if existing data validation is deferred, the index build marks invalid records
  in diagnostics and the index remains `failed` until the data or schema is
  corrected and the index is rebuilt.

Silent omission of invalid required values is not allowed.

### Removing an index

When a schema update removes an index definition:

1. The index is removed from planner eligibility immediately.
2. Persisted index data is marked `retired` or deleted after active readers drain.
3. Graph writes stop maintaining that index.
4. Graph data is unchanged.

Queries that still require the removed index fail closed unless another suitable
index exists.

### Changing an index

Changing target type, field path, ordering, uniqueness, or key encoding is treated
as drop plus add:

1. old index becomes ineligible/retired;
2. new index starts in `building` state;
3. queries use the new index only after it becomes `ready`.

## Key Encoding

Ordered index keys must be deterministic and byte-sortable.

Conceptual key for `JournalEntry.properties.date`:

```text
domain_id | index_name | normalized_value | node_id
```

Example:

```text
content-domain | journal_entries_by_date | 2026-07-20 | 018f...node
```

The `node_id` suffix provides a stable tie-breaker for equal dates and makes page
cursors deterministic.

Value normalization must be type-aware:

| Field type | Encoding requirement |
|---|---|
| string | normalized UTF-8 bytes with escaping/length prefix |
| int | sortable signed integer encoding |
| float | sortable IEEE encoding with NaN policy |
| bool | `0`/`1` |
| date | canonical `YYYY-MM-DD` or sortable day number |
| datetime | UTC canonical timestamp or sortable epoch nanos |
| enum | normalized enum token |

Invalid or missing values for required indexed fields should fail graph mutation
validation in strict domains. Optional indexed fields should simply omit an index
entry when absent.

## Mutation Maintenance

Index maintenance must happen synchronously in the graph mutation apply path. A
successful graph commit must not leave index data stale.

### Insert node

When a new node is committed:

1. Validate labels and indexed fields against the active schema.
2. Persist/apply the node record through the graph store's authoritative path.
3. For each matching index definition:
   - compute the indexed field value;
   - normalize and encode the value;
   - insert index key -> node ID.
4. Insert implicit/system index entries, such as label index entries.
5. Commit succeeds only when graph record and index updates are durable or are
   covered by the same replayable graph command.

For a `JournalEntry`:

```text
node.labels = [JournalEntry]
node.properties.date = 2026-07-20
```

index entries include:

```text
label_index: domain | JournalEntry | node_id
journal_entries_by_date: domain | 2026-07-20 | node_id
```

### Update node

When labels or indexed fields change:

1. Load the previous committed node state.
2. Compute old index keys that applied to the old state.
3. Compute new index keys that apply to the new state.
4. Delete old keys that no longer apply.
5. Insert new keys.
6. Persist/apply the node update atomically with index changes.

Cases:

- `date` changes: remove old date key and insert new date key.
- `JournalEntry` label removed: remove label and ordered property index entries.
- `JournalEntry` label added: add label and ordered property index entries.
- required indexed field removed in strict mode: reject mutation.

### Delete node

When a node is deleted:

1. Load the previous committed node state.
2. Delete all index keys derived from that node.
3. Apply the node tombstone.

### Insert/update/delete edge

Edge indexes follow the same pattern. The first required edge index is adjacency:

```text
out: domain | from_node_id | edge_label | order/tie-break | edge_id
in:  domain | to_node_id   | edge_label | order/tie-break | edge_id
```

Traversal plans use adjacency entries instead of loading every edge in the
domain.

## Raft, WAL, Snapshots, and Recovery

Graph raft/WAL commands remain authoritative. Index updates are deterministic
side effects of applying those commands with the active schema's index
definitions.

Rules:

- Every replica applies the same committed graph mutation and derives the same
  index updates.
- Graph snapshots should include enough graph state and schema/index metadata to
  reload or rebuild indexes deterministically.
- Persisted index files may be included in backups as an optimization, but graph
  records plus schema definitions must be sufficient to rebuild them.
- If an index is missing, stale, or corrupt, the graph subsystem must mark that
  index unavailable. Queries requiring it fail closed until an explicit or
  startup-controlled rebuild completes.
- Rebuild does not repair graph data; it only re-derives local index data from
  authoritative graph records.

## Query Planning

The query planner should choose an indexed plan when query shape and schema
indexes align.

For journal entries ordered by date, the structured request says:

```text
start alias: j
labels: [JournalEntry]
order by: j.date ASC
returns: node j
no traversal
```

The planner resolves:

```text
j.date -> properties.date on node type JournalEntry
```

Then it requires:

```text
ordered node property index on JournalEntry.properties.date
```

The resulting plan:

```text
OrderedNodePropertyIndexScan(
  domain = <domain>,
  label/type = JournalEntry,
  field = properties.date,
  direction = ASC,
  cursor = <page token>,
  limit = min(query.limit, page_size, daemon cap)
)
```

Execution:

1. Acquire/verify strong read context.
2. Scan index keys in order from the cursor.
3. Fetch only node records referenced by returned index entries.
4. Apply transaction overlay merge if the transaction has staged writes.
5. Project rows.
6. Return a next-page token derived from the last index key.

The executor must not load edges for this node-only plan.

## Transaction Overlay

Read-write transactions require read-your-writes behavior. The indexed plan still
starts from committed index data, then overlays staged mutations:

- remove deleted nodes from results;
- remove nodes whose label or indexed field changed so they no longer match;
- insert staged created/updated nodes into the ordered stream;
- preserve the same ordering and node-ID tie-break semantics.

The overlay should be bounded to the transaction write set, not a full-domain
scan.

## Planner Failure Behavior

If a query needs scalable execution and no suitable index exists, fail closed:

```text
FailedPrecondition: no ordered index for JournalEntry.properties.date
```

Do not silently perform a full-domain scan.

Developer/operator-only debug modes may allow explicit scan execution, but scan
plans must be opt-in and visible in diagnostics.

## Query Diagnostics

Query responses should gain diagnostics sufficient to prove execution behavior:

```text
plan: OrderedNodePropertyIndexScan
index: journal_entries_by_date
full_scan: false
edges_loaded: 0
index_entries_scanned: 100
nodes_loaded: 100
rows_returned: 100
next_cursor_kind: index_key
```

Diagnostics should also identify rejected plans:

```text
rejected_reason: missing_ordered_index
required_index: JournalEntry.properties.date ASC
```

## GQL Impact

GQL and the structured query API should share the same planner/executor below
their surface syntax.

Textual GQL:

```gql
MATCH (j:JournalEntry)
RETURN j
ORDER BY j.date
FETCH FIRST 100 ROWS ONLY
```

Structured API:

```text
GraphQuery.match.start.labels = [JournalEntry]
GraphQuery.order_by = properties.date ASC
```

Both should compile to the same indexed plan when the same schema/index
definitions are active.

## Example End-to-End Flow

### Schema

```gwl
node JournalEntry labels [JournalEntry] {
  date: date required
  title: string
}

index journal_entries_by_date on node JournalEntry field properties.date ordered asc
```

### Insert

```text
CreateNode(labels=[JournalEntry], properties={date: 2026-07-20, title: "Today"})
```

Apply path:

```text
validate node
write node record
write label index entry
write journal_entries_by_date index entry
commit
```

### Query

```text
JournalEntry ORDER BY date ASC page_size=100
```

Plan and execute:

```text
OrderedNodePropertyIndexScan(journal_entries_by_date)
fetch returned node IDs only
project node rows
return index cursor
```

No full node scan. No edge load.

## Implementation Phases

### Phase 1: Schema/index declarations

- Extend schema model with first-class `IndexDefinition`.
- Extend GWL parser for explicit index declarations.
- Validate target type, field path, field type, and index options.
- Persist definitions with domain schema source/model.

### Phase 2: Graph index storage

- Add persisted local index storage under graph storage.
- Implement label and ordered node property index writers/readers.
- Add deterministic key encoding and cursor encoding.
- Add rebuild from authoritative node records.

### Phase 3: Synchronous maintenance

- Update node insert/update/delete apply paths to maintain indexes.
- Add tests for date changes, label changes, deletes, and rebuild parity.
- Fail writes when required indexed values are invalid.

### Phase 4: Indexed query planner

- Add a planner between query analysis and execution.
- Implement `OrderedNodePropertyIndexScan`.
- Reject missing-index production queries.
- Add diagnostics proving no full scan occurred.

### Phase 5: Traversal indexes

- Use adjacency index reads for outgoing/incoming traversals.
- Start traversals from indexed node candidates.
- Avoid loading all domain edges.

### Phase 6: GQL parity

- Route GQL `MATCH ... ORDER BY ...` through the shared indexed planner.
- Keep textual and structured query behavior aligned.

## Acceptance Criteria

The first production-ready tranche is complete when:

- GWL can declare an ordered index for `JournalEntry.properties.date`.
- The index definition is persisted with the domain schema.
- Inserting, updating, and deleting `JournalEntry` nodes synchronously updates
  index data.
- Index data survives restart or can be rebuilt from graph records and schema.
- A structured query for `JournalEntry ORDER BY date` uses an ordered index scan.
- The query does not load unrelated nodes.
- The query does not load edges.
- Pagination is based on an index cursor.
- Diagnostics show index use and scanned/loaded counts.
- Missing indexes cause a clear `FailedPrecondition` rather than a silent full
  scan.
