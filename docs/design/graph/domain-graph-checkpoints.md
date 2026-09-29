# Domain graph checkpoints

## Summary

A domain graph checkpoint is a durable, compact representation of the latest
committed graph state for one domain graph store at a specific graph revision.

It is a local storage/recovery optimization. Its purpose is to avoid rebuilding a
domain graph store by replaying all historical `.kseg` segment records every time
the store is opened.

Phase 1 of advanced storage changes the physical graph store unit to:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
```

A checkpoint belongs to that domain store, for example:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
  manifest.mycel
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
  checkpoints/
    latest/
      manifest.json
      nodes.kchk
      edges.kchk
```

The exact file names are implementation details, but the conceptual split should
remain:

- checkpoint manifest: metadata, graph revision, segment offsets, checksums
- checkpoint payload: compact latest live nodes and edges

## Non-goals

A domain graph checkpoint is not:

- a replacement for Raft consensus
- a cluster-level backup
- a Raft snapshot, though Raft snapshot restore may create equivalent local graph state
- an index by itself
- a compaction mechanism that deletes old segments in its initial form
- a cross-domain transaction mechanism

It is also not intended to introduce historical MVCC snapshots. It represents the
latest committed domain graph state at one revision.

## Relationship to `.kseg` segments

Current graph stores use append-only segment files:

```text
segments/txns-000001.kseg
segments/nodes-000001.kseg
segments/edges-000001.kseg
```

The segments remain the authoritative mutation log for the store until later
compaction phases. A checkpoint records that graph state has already been
materialized from segment records up to specific safe positions.

Example manifest shape:

```json
{
  "format_version": 1,
  "space_id": "...",
  "domain_id": "...",
  "graph_revision": 123,
  "created_at": "2026-09-28T00:00:00Z",
  "node_count": 10000,
  "edge_count": 25000,
  "applied_segment_offsets": {
    "txns": [
      {"segment": "segments/txns-000001.kseg", "offset": 327680}
    ],
    "nodes": [
      {"segment": "segments/nodes-000001.kseg", "offset": 10485760}
    ],
    "edges": [
      {"segment": "segments/edges-000001.kseg", "offset": 7340032}
    ]
  },
  "checksum_algorithm": "graph-v1-sha256",
  "node_checksum": "...",
  "edge_checksum": "...",
  "graph_checksum": "..."
}
```

The applied segment offsets do not contain node or edge data. They say which
parts of the append-only mutation logs are already represented by the checkpoint.

The compact node and edge payload files contain the latest live records at the
checkpoint revision. Deleted historical records are not included in the checkpoint
payload.

## Open behavior

Without checkpoints, opening a domain store requires:

```text
1. scan transaction segments to find committed transaction IDs
2. scan node segments
3. scan edge segments
4. rebuild live node/edge maps and in-memory indexes
```

With checkpoints, opening should become:

```text
1. load latest valid checkpoint, if present
2. restore live nodes and edges from checkpoint payload
3. rebuild in-memory indexes from checkpoint payload
4. replay committed segment records after the checkpoint offsets
5. apply the tail to live state and indexes
```

If no checkpoint exists, the store falls back to the existing full segment replay
path.

If a checkpoint exists but is invalid, the preferred early behavior is:

```text
ignore checkpoint, full replay, log warning
```

Later production hardening may add strict modes where invalid checkpoint metadata
or checksums fail closed.

## Creation behavior

A checkpoint is created from a consistent view of one domain store's latest
committed state.

The storage engine should create it while holding the appropriate store lock or
under an equivalent quiesced/read-consistent state so that:

- live nodes and edges are copied from one graph revision
- segment offsets correspond to records included in that revision
- checksums match the payload
- the final manifest is published atomically

The safe write pattern should be:

```text
1. write checkpoint payload files to a temporary directory
2. fsync payload files/directories where applicable
3. write manifest to a temporary file
4. fsync manifest file
5. atomically rename temporary checkpoint directory or manifest into place
```

Readers should only use a checkpoint after its manifest is complete and valid.

## Leader vs follower creation

Checkpoints are local storage artifacts. They should not be created only by the
Raft leader/master for a domain.

Every pod that owns a local copy of the domain graph store may create its own
checkpoint after it has applied committed graph revisions locally.

Reasons:

- followers also need fast restart/open behavior
- leadership changes frequently during normal operations and disruption tests
- a checkpoint is not a consensus decision; Raft already decided the committed graph mutations
- checkpoint files are derived from local committed state and can be independently regenerated

Therefore the intended rule is:

```text
Any replica may checkpoint its local domain store after applying committed graph state.
```

A leader may trigger checkpointing as an implementation convenience, but the
checkpoint itself is still local and should not be considered authoritative for
other replicas.

If two replicas have applied the same graph revision, their checkpoints should be
logically equivalent, but byte-for-byte equality is not required unless the
encoding is intentionally canonicalized.

## Relationship to persistent indexes

Checkpoints are not specifically an index feature. They are a graph recovery
feature.

However, persistent indexes need a graph-revision baseline. A graph checkpoint
provides that baseline.

For example:

```text
graph checkpoint revision = 123
label index revision      = 123
adjacency index revision  = 123
```

means the index files are consistent with the checkpointed graph state.

If an index is behind:

```text
graph checkpoint revision = 123
property index revision   = 120
```

then the index must be updated by replaying graph changes from revision 121 to
123 or rebuilt.

The concepts should stay separate:

```text
GraphCheckpoint  -> canonical latest graph state baseline
IndexManifest    -> derived index state and indexed graph revision
```

## Relationship to Raft snapshots

Raft snapshots and graph checkpoints solve different problems.

A Raft snapshot is a consensus/state-machine artifact used to compact/catch up
Raft log state.

A domain graph checkpoint is a local storage artifact used to open a graph store
without replaying its entire historical segment log.

A Raft snapshot restore may rebuild a domain store and then write a checkpoint,
but the checkpoint is not itself the Raft snapshot.

## Triggering policy

Initial implementation should keep the trigger simple and explicit, for example:

```go
store.WriteCheckpoint(ctx)
```

Follow-up policies can add automatic checkpointing:

- after N committed revisions
- after N bytes appended to segments
- during clean shutdown
- during low-load background maintenance
- after Raft snapshot restore

Checkpoint frequency should balance:

- faster open/recovery
- write amplification
- disk usage while old checkpoint files are replaced
- CPU cost of serializing live graph state

## Acceptance criteria for an initial implementation

An initial implementation should prove:

1. a domain store can write a valid checkpoint
2. reopening with the checkpoint restores live nodes and edges
3. committed records after the checkpoint are replayed correctly
4. missing checkpoint falls back to full replay
5. corrupt/incomplete checkpoint is ignored or rejected according to policy
6. checkpoint metadata is domain-local and rejects mismatched space/domain IDs
7. checksums detect payload corruption
8. raft restart/disruption tests still converge

## Future phases

Later advanced storage phases can build on checkpoints to add:

- persistent label/tag/property/adjacency indexes
- index revision manifests
- segment compaction after safe checkpoint creation
- bounded in-memory caches
- explicit checkpoint/compaction metrics
- admin visibility into checkpoint age, graph revision, and replay-tail length
