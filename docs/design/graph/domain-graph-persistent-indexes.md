# Domain graph persistent indexes

## Summary

Domain graph persistent indexes are local, derived storage artifacts that make a
large domain graph store faster to open and query. They persist selected
in-memory graph indexes alongside the domain graph checkpoint baseline so a daemon
can avoid rebuilding every index from all live nodes/edges on each open.

The index unit is the domain graph store:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
```

The initial persistent index scope is intentionally conservative:

- label index
- tag index
- adjacency indexes for outgoing and incoming edges

Property, full-text, semantic, lexical, and schema-declared query indexes are
separate systems or later phases.

## Goals

- Keep the domain as the graph storage/cache/index management unit.
- Preserve `.kseg` segment format unchanged.
- Treat persistent indexes as derived, discardable artifacts.
- Tie every persistent index set to a domain graph checkpoint revision and graph
  checksum.
- Speed open/recovery for large domains by loading checkpointed graph state plus
  matching index files.
- Fall back safely to rebuilding indexes from graph checkpoint payloads and tail
  segment replay when index files are absent, stale, corrupt, or incompatible.
- Keep early implementation local-only; any replica may build indexes after
  applying committed graph state.

## Non-goals

Persistent graph indexes are not:

- authoritative graph storage
- Raft snapshots
- backups
- cross-domain query support
- segment compaction
- a replacement for lexical or semantic search indexes
- a public API compatibility boundary in the first phase

They should not change graph write correctness. If all persistent index files are
deleted, the domain store must still open from checkpoint plus `.kseg` segments,
or from full segment replay if the checkpoint is also missing.

## Relationship to graph checkpoints

A graph checkpoint is the baseline for persistent index validity.

Conceptually:

```text
graph checkpoint revision = 123
graph checkpoint checksum = abc...
index set graph revision  = 123
index set graph checksum  = abc...
```

means the index files describe the exact live graph state represented by the
checkpoint. On open, the store may load both the checkpoint payload and matching
index files, then replay tail `.kseg` records after the checkpoint offsets.

If the latest graph checkpoint is newer than the latest index set:

```text
graph checkpoint revision = 150
index set graph revision  = 123
```

then the index set is stale for that checkpoint and should not be loaded in the
initial implementation. The store should rebuild in-memory indexes from the graph
checkpoint payload and tail replay.

Later phases may support applying checkpoint-to-index deltas or retaining
multiple index generations, but phase 1 of persistent indexes should keep the
validity rule strict.

## Proposed layout

Within each domain graph store:

```text
graphs/<space_id>/domains/<domain_id>/
  manifest.mycel
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
  checkpoints/
    <checkpoint_id>/
      manifest.json
      nodes.kchk
      edges.kchk
    LATEST
  indexes/
    <index_set_id>/
      manifest.json
      labels.kidx
      tags.kidx
      adjacency-out.kidx
      adjacency-in.kidx
    LATEST
```

`indexes/LATEST` points to the latest published complete index set. As with graph
checkpoints, readers only use an index set after its manifest and payload files
are complete and valid.

The index set directory may include only files for index kinds implemented in
that format version. The manifest is authoritative for which files are expected.

## Manifest shape

Example:

```json
{
  "format_version": 1,
  "space_id": "...",
  "domain_id": "...",
  "index_set_id": "idx-...",
  "created_at": "2026-09-28T00:00:00Z",
  "graph_checkpoint_id": "chk-...",
  "graph_revision": 123,
  "graph_checksum": "...",
  "index_format": "domain-graph-index-v1",
  "checksum_algorithm": "domain-graph-index-v1-sha256",
  "indexes": {
    "labels": {
      "path": "labels.kidx",
      "entry_count": 1200,
      "checksum": "..."
    },
    "tags": {
      "path": "tags.kidx",
      "entry_count": 900,
      "checksum": "..."
    },
    "adjacency_out": {
      "path": "adjacency-out.kidx",
      "entry_count": 25000,
      "checksum": "..."
    },
    "adjacency_in": {
      "path": "adjacency-in.kidx",
      "entry_count": 25000,
      "checksum": "..."
    }
  }
}
```

The manifest should include enough information to reject indexes when:

- format version is unsupported
- space/domain IDs mismatch the local store
- graph revision does not match the loaded checkpoint revision
- graph checksum does not match the loaded checkpoint checksum
- required payload files are missing
- payload checksums fail
- index payloads contain invalid IDs or inconsistent sort/order encodings

## Index payload principles

Index files should be compact, deterministic, and easy to validate.

Recommended initial properties:

- binary payload with magic/version/kind header
- deterministic sorted keys and sorted ID lists
- per-file checksum recorded in manifest
- optional per-record checksums if needed for partial corruption diagnostics
- no embedded graph record bodies; store only keys and entity IDs

Suggested logical payloads:

```text
labels.kidx:
  label -> sorted node IDs

tags.kidx:
  tag -> sorted node IDs

adjacency-out.kidx:
  from node ID -> label/type bucket -> sorted edge IDs

adjacency-in.kidx:
  to node ID -> label/type bucket -> sorted edge IDs
```

The graph store remains the owner of node and edge records. Persistent indexes
refer to node/edge IDs only.

## Open behavior

Domain store open should follow this order:

```text
1. open manifest and segment handles
2. load latest valid graph checkpoint, if present
3. if a graph checkpoint was loaded, try to load latest matching index set
4. if matching index set loads, hydrate in-memory indexes from index files
5. if no matching index set loads, rebuild in-memory indexes from checkpoint payload
6. replay tail `.kseg` records after checkpoint offsets and update live state/indexes
7. if no graph checkpoint loads, use full segment replay and rebuild indexes as today
```

Invalid persistent indexes should initially be non-fatal:

```text
invalid/missing/stale index -> rebuild in-memory indexes + warning/status
```

Invalid graph checkpoint behavior remains governed by the checkpoint policy; the
index loader should not make graph checkpoint validity stricter.

## Write/update behavior

Persistent indexes should not be updated in place for every graph mutation in the
first implementation. The authoritative write path already updates in-memory
indexes synchronously as mutations apply.

Persistent index files should be generated as a point-in-time derived artifact,
usually after or alongside a graph checkpoint:

```text
1. create graph checkpoint at revision R
2. build persistent index payloads from the same live state at revision R
3. write index set to a temporary directory
4. fsync payloads/manifest where applicable
5. publish by atomically updating indexes/LATEST
```

This avoids introducing new per-commit fsync work on the graph write path.

## Consistency and recovery

The consistency contract is:

- `.kseg` segments remain authoritative mutation logs.
- graph checkpoints are derived latest-state baselines.
- persistent indexes are derived acceleration artifacts for a specific graph
  checkpoint baseline.
- tail segment replay after checkpoint load is responsible for bringing live
  state and in-memory indexes to the current revision.

If persistent indexes are absent or unusable, correctness is preserved by
rebuilding from graph state. Index corruption should affect startup latency, not
graph correctness.

## Replica behavior

Persistent indexes are local artifacts. Any replica may generate or replace them
after applying committed graph state and creating/loading a matching checkpoint.

There is no leader-only requirement. Byte-for-byte equality between replicas is
not required, but logical contents should be equivalent for the same graph
checkpoint revision/checksum.

## Observability

The store/admin status should eventually expose, per domain:

- whether a matching persistent index set is present
- index set ID
- graph revision/checksum covered by the index set
- index format version
- index load result: used/rebuilt/fallback
- fallback reason, if any
- index load duration
- index write duration
- last index write error
- entry counts by index kind

These can be added to the graph checkpoint/status admin surface or to a separate
future graph index status method.

## Retention

Initial retention can mirror checkpoints:

```text
keep latest complete index set, remove older index sets opportunistically
```

Future retention may keep multiple index sets if graph checkpoints retain
multiple generations.

## Future index kinds

After label/tag/adjacency indexes are stable, later phases can add:

- node property indexes
- edge property indexes
- hierarchy-specific indexes
- blob reference indexes
- schema-declared query indexes
- index manifests for GQL planning diagnostics

Property indexes need additional design for type normalization, collation,
range-order encoding, missing/null semantics, and schema evolution. They should
not be mixed into the first persistent-index payload unless the scope is kept
very small.
