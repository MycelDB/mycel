# Graph Segment Store

KnotDB graph data is stored in local binary segment files under each space directory.

## Goals

- avoid rewriting whole graph files
- support targeted node/edge mutations
- support transaction recovery
- rebuild in-memory indexes at open
- keep dependencies low; only the Go standard library and existing UUID package are used

## Files

```text
graphs/<space_id>/
  manifest.knot
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
```

`manifest.knot` records the active segment files and the segment lists used for index rebuilds.

## Records

Each segment has a binary segment header. Each record has a fixed binary record header and optional binary payload.

Record kinds:

- `txn_begin`
- `txn_commit`
- `node_put`
- `node_tombstone`
- `edge_put`
- `edge_tombstone`

Node/edge payloads are fully binary encoded. They include UUID fields as 16-byte values, strings as length-prefixed UTF-8, and properties as typed binary values.

## Transactions

All writes use transactions:

```text
txn_begin(txn_id)
node_put(txn_id, ...)
edge_put(txn_id, ...)
txn_commit(txn_id)
```

Recovery rule:

```text
Only records whose txn_id has a committed transaction record are applied.
```

This prevents partially-written graph mutations from becoming visible after restart.

## Index rebuild

On open, the graph store enters `rebuilding_index` state and scans segments:

1. scan transaction segments to collect committed transaction IDs
2. scan node segments and apply committed node puts/tombstones
3. scan edge segments and apply committed edge puts/tombstones
4. rebuild hierarchy, template, and journal-day indexes
5. enter `ready` state

## Current indexes

- nodes by ID
- edges by ID
- node IDs by template ID
- `contains` children by parent ID, ordered by edge `Props["order"]`
- `contains` parent by child ID
- journal nodes by numeric `Props["journal_day"]`

## Deferred work

- persisted index snapshots
- segment compaction
- generalized property indexes
- full query planner integration
- multi-process writer coordination
