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

## On-disk file structure

Persistent graph indexes live inside the domain graph store. The complete
advanced-storage domain directory is intended to look like this:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
  manifest.mycel
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
  checkpoints/
    LATEST
    chk-<uuid>/
      manifest.json
      nodes.kchk
      edges.kchk
  indexes/
    LATEST
    idx-<uuid>/
      manifest.json
      labels.kidx
      tags.kidx
      adjacency-out.kidx
      adjacency-in.kidx
```

### Existing graph store files

```text
manifest.mycel
segments/*.kseg
```

These remain the authoritative graph store manifest and append-only segment logs.
Persistent indexes must not require a `.kseg` format change.

### Checkpoint directory

```text
checkpoints/
  LATEST
  chk-<uuid>/
    manifest.json
    nodes.kchk
    edges.kchk
```

This is the phase 2 domain graph checkpoint layout. Persistent indexes are valid
only relative to a checkpoint baseline. The index manifest references the
checkpoint ID, graph revision, and graph checksum it was built from.

### Index root

```text
indexes/
```

The `indexes/` directory contains local derived persistent index sets for this
one domain store. It is safe to delete the whole directory: the domain store must
fall back to rebuilding in-memory indexes from graph state.

The index root contains:

```text
indexes/LATEST
indexes/idx-<uuid>/...
```

`indexes/LATEST` is a small text pointer file containing the latest published
index set directory name, for example:

```text
idx-4c5171a3-9d76-4f7c-a43f-24e3b105eae1
```

The pointer must name a child directory of `indexes/`; absolute paths, `..`, path
separators, whitespace-only names, and non-`idx-` names are invalid.

### Index set directory

Each complete index set is stored in one immutable directory:

```text
indexes/idx-<uuid>/
  manifest.json
  labels.kidx
  tags.kidx
  adjacency-out.kidx
  adjacency-in.kidx
```

The directory name is an implementation-generated ID and should be treated as
opaque. Initial implementations should use `idx-<uuid>` to make partial/manual
inspection simple and avoid collisions.

An index set directory is immutable after publication. To refresh indexes, write
a new `idx-<uuid>` directory and atomically update `indexes/LATEST`.

### Index manifest

```text
indexes/idx-<uuid>/manifest.json
```

The manifest is the authoritative description of the index set. It records:

- index format version
- space and domain IDs
- index set ID
- creation time
- checkpoint ID used as the graph baseline
- graph revision and graph checksum covered by the index set
- expected payload files
- per-payload entry counts and checksums

Readers should validate the manifest before opening payload files. A missing,
invalid, or unsupported manifest makes the whole index set unusable and triggers
fallback to in-memory rebuild.

### Index payload files

Initial payload files are:

```text
labels.kidx
```

Maps graph labels to sorted live node IDs.

```text
tags.kidx
```

Maps graph tags to sorted live node IDs.

```text
adjacency-out.kidx
```

Maps outgoing edge buckets to sorted live edge IDs. The logical key is expected
to include at least the source node ID and edge label/bucket used by the current
in-memory adjacency index.

```text
adjacency-in.kidx
```

Maps incoming edge buckets to sorted live edge IDs. The logical key is expected
to include at least the target node ID and edge label/bucket used by the current
in-memory adjacency index.

Payload files store index keys and entity IDs only. They must not duplicate full
node or edge records; authoritative record bodies remain in checkpoint payloads
and `.kseg` segment replay.

### Temporary write layout

Index creation should not publish partial files. A safe write uses temporary
paths under the index root, for example:

```text
indexes/.tmp-idx-<uuid>/
  manifest.json.tmp
  labels.kidx
  tags.kidx
  adjacency-out.kidx
  adjacency-in.kidx
indexes/LATEST.tmp
```

Recommended publish sequence:

1. create `indexes/.tmp-idx-<uuid>/`
2. write all payload files
3. fsync payload files and the temporary directory where supported
4. write and fsync `manifest.json.tmp`
5. rename `manifest.json.tmp` to `manifest.json` inside the temporary directory
6. rename `.tmp-idx-<uuid>` to `idx-<uuid>`
7. write `indexes/LATEST.tmp` containing `idx-<uuid>\n`
8. rename `LATEST.tmp` to `LATEST`
9. fsync `indexes/` where supported

Readers must ignore `.tmp-*` directories and `LATEST.tmp`.

### Retention and cleanup

The initial retention policy should keep only the latest complete index set:

```text
keep:    indexes/LATEST target
remove:  older indexes/idx-* directories
ignore:  transient indexes/.tmp-* until cleanup
```

Cleanup is opportunistic. Failure to delete an old index set must not make the
new index set invalid.

### File ownership and locality

All files under `indexes/` are local to one daemon replica. They are not copied
through Raft, are not authoritative cluster state, and can be regenerated by any
replica that has applied the corresponding graph state.

Backups may include them as derived artifacts, but restore correctness must not
depend on their presence.

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

## In-memory index structures

The initial persistent-index phase should keep the current in-memory index model:
hash maps and ID sets optimized for fast mutation and lookup while a domain store
is loaded. The persistent files are a deterministic serialization of those
logical indexes, not a replacement in-memory data structure.

In other words, phase 5 should not replace loaded indexes with B-trees or a
page-cache-backed index engine. B-tree-like or LSM-like structures may become
useful in a later bounded-memory phase, but they add more write-path,
cache-management, and recovery complexity than is needed for the first
checkpoint-aligned persistent indexes.

### Authoritative live maps

The loaded domain store keeps authoritative live records in memory:

```text
node_records:
  node_id -> Node

edge_records:
  edge_id -> Edge
```

Persistent indexes must not duplicate full records. They refer back to these maps
by node ID or edge ID.

### Label index

Logical shape:

```text
labels:
  domain_id -> label -> set(node_id)
```

Purpose:

- accelerate label scans
- support query planning/execution paths that filter nodes by label

Persistent payload shape:

```text
label -> sorted node IDs
```

On load, sorted node ID lists from `labels.kidx` are hydrated back into the
in-memory set/map representation. Tail replay then applies node puts/deletes to
that map as normal.

### Tag index

Logical shape:

```text
tags:
  domain_id -> tag -> set(node_id)
```

Purpose:

- accelerate tag scans
- avoid scanning all live nodes for tag predicates

Persistent payload shape:

```text
tag -> sorted node IDs
```

As with labels, the on-disk sorted representation is for deterministic storage
and validation. The loaded form remains map/set based.

### Adjacency indexes

Logical shape:

```text
adjacency_out:
  domain_id -> from_node_id -> edge_bucket -> set(edge_id)

adjacency_in:
  domain_id -> to_node_id -> edge_bucket -> set(edge_id)
```

The exact edge bucket key should match the current in-memory adjacency semantics.
For the initial design, it is enough to support the buckets needed by traversal,
hierarchy validation, and label/type-scoped edge lookups.

Purpose:

- accelerate outgoing traversal from a node
- accelerate incoming traversal to a node
- avoid full edge scans in hierarchy and reference-heavy workloads

Persistent payload shape:

```text
from_node_id -> edge_bucket -> sorted edge IDs
to_node_id   -> edge_bucket -> sorted edge IDs
```

The persisted sorted lists hydrate the in-memory adjacency sets. Tail replay then
updates adjacency maps for edge puts/deletes as normal.

### Hierarchy helper indexes

The graph store also maintains hierarchy-oriented helper structures, for example:

```text
contains_parent:
  child_node_id -> edge_id

contains_children:
  parent_node_id -> ordered/sorted edge IDs
```

These are derived from live hierarchy edges. They may be rebuilt from the loaded
adjacency indexes or from live edges during open. They are not part of the first
persistent payload set unless implementation proves that persisting them provides
a clear additional open-time win.

### Property and other indexes

The store has or may gain additional in-memory indexes, such as:

```text
node_property_index
edge_property_index
journal_day
blob_refs
configured schema index metadata
```

These are out of scope for the first persistent-index phase. Property indexes in
particular need separate design for type normalization, collation, null/missing
semantics, and schema evolution.

### Map/set vs B-tree decision

Phase 5 should keep map/set in-memory indexes for these reasons:

- current lookup/update paths are already expressed around map/set operations
- graph commits update in-memory indexes synchronously and cheaply
- the first persistent-index goal is faster open, not lower steady-state memory
- sorted persistent payloads already provide deterministic validation without
  imposing sorted-tree mutation costs on the loaded store
- 10k–100k+ nodes per user/domain are still practical with map/set indexes if
  domain caches are bounded at a higher layer

A future bounded-memory storage phase can introduce B-tree-like structures if we
need indexes that are partially resident, memory-mapped, or updated directly on
disk. That future design should be explicit about page/cache ownership,
transactional updates, crash recovery, and interaction with `.kseg` segments.

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
