# Graph Storage

MycelDB stores graph data per space under:

```text
<data-root>/graphs/<space_id>/
```

Graph data uses custom append-only binary segment files. Metadata and indexes are rebuilt in memory from committed segment records at open time.

## Directory Tree

```text
graphs/
  <space_id>/
    .space
    manifest.mycel
    segments/
      txns-000001.kseg
      nodes-000001.kseg
      edges-000001.kseg

    embeddings/        # current embedding vector records; see semantic.md
      manifest.kemb
      segments/
        embeddings-000001.kvec

    semantic/          # proposed advanced semantic index storage; see semantic.md
      indexes.json
      index_state.json
      dirty_queue.json
      policy_decisions.json
      indexes/
        <semantic_index_id>/
          manifest.ksem
          records/
            embeddings-000001.kvec
          external_refs.json
```

## `.space`

`.space` is a marker file indicating that the directory belongs to a graph space.

It is not the authoritative source of space metadata. Space metadata lives in:

```text
meta/spaces.json
```

## `manifest.mycel`

The graph manifest is a small table of contents for graph segment files.

Current structure:

```json
{
  "format_version": 1,
  "node_segments": ["segments/nodes-000001.kseg"],
  "edge_segments": ["segments/edges-000001.kseg"],
  "txn_segments": ["segments/txns-000001.kseg"],
  "active_node_segment": "segments/nodes-000001.kseg",
  "active_edge_segment": "segments/edges-000001.kseg",
  "active_txn_segment": "segments/txns-000001.kseg"
}
```

Fields:

- `format_version`: graph storage format version.
- `node_segments`: ordered node segment files scanned during index rebuild.
- `edge_segments`: ordered edge segment files scanned during index rebuild.
- `txn_segments`: ordered transaction segment files scanned during recovery.
- `active_*_segment`: current append target for new records of each segment kind.

The manifest is not an index. It only identifies which segment files exist and which ones are active.

## Segment Files

Current segment files:

```text
segments/txns-000001.kseg
segments/nodes-000001.kseg
segments/edges-000001.kseg
```

Segment responsibilities:

- `txns-*.kseg`: transaction begin/commit records
- `nodes-*.kseg`: node puts and node tombstones
- `edges-*.kseg`: edge puts and edge tombstones

Detailed record layout is documented in [segment-store.md](segment-store.md).

## Transaction Model

All graph writes are committed through storage transactions.

Logical sequence:

```text
txn_begin(txn_id)
node_put(txn_id, ...)
edge_put(txn_id, ...)
txn_commit(txn_id)
```

Recovery rule:

```text
Only node/edge records whose txn_id has a committed transaction record are applied.
```

Uncommitted records are ignored after restart.

## Node Records

Node payloads are binary encoded by Mycel's graph codec.

A logical node contains fields such as:

```text
NodeID
DomainID
TemplateID
TemplateKey
Content
Props
BlobRef
CreatedAt/UpdatedAt metadata
```

Notes:

- UUIDs are encoded as 16-byte values.
- Strings are length-prefixed UTF-8.
- Properties are typed binary values.
- Nodes belong to exactly one domain.
- Blob nodes reference payloads stored under `blobs/<space_id>/`.

## Edge Records

Edge payloads are binary encoded by Mycel's graph codec.

A logical edge contains fields such as:

```text
EdgeID
FromID
ToID
Kind
Props
```

Containment edges represent hierarchy. Current domain rules require containment edges to stay inside a domain.

## In-Memory Index Rebuild

On open, the graph store enters `rebuilding_index` and scans segment files:

1. scan transaction segments and collect committed transaction IDs
2. scan node segments and apply committed node puts/tombstones
3. scan edge segments and apply committed edge puts/tombstones
4. rebuild in-memory indexes
5. enter `ready` state

Current in-memory indexes include:

- node ID to node record
- edge ID to edge record
- node IDs by template ID
- node IDs by domain ID
- `contains` parent to ordered child edges
- `contains` child to parent edge
- simple `journal_day` property index
- blob ID to referencing node IDs

Persisted index snapshots are intentionally deferred.

## Revisions and Optimistic Transactions

The graph store rebuilds an in-memory revision counter from committed transaction records.

Session-level transactions use this revision for optimistic conflict detection. A committed graph transaction increments the revision.

## Delete Behavior

Graph deletes append tombstones:

- deleting a node writes a node tombstone
- deleting a node also writes tombstones for affected incident edges
- recursive deletes write tombstones for the subtree and affected edges

Physical segment compaction is deferred.

## Relationship to Semantic Storage

Current embedding vector records live under:

```text
graphs/<space_id>/embeddings/
```

The proposed advanced semantic system lives under:

```text
graphs/<space_id>/semantic/
```

Graph writes should remain fast. Semantic maintenance work, such as embedding refresh, should be recorded as dirty work and processed asynchronously. See [semantic.md](semantic.md).
