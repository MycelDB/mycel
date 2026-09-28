# Domain graph persistent query indexes

## Summary

Domain graph persistent query indexes extend the existing domain graph checkpoint
and persistent index foundation to schema-declared query indexes. They persist
local derived property index maps so a daemon can open a large domain and answer
indexed structured/GQL queries without rebuilding every configured query index
from checkpoint records or full `.kseg` replay.

This design covers graph-storage query indexes backed by schema declarations:

- ordered node property indexes
- ordered edge property indexes
- equality node/edge property indexes when they are added to storage/planning

It does not cover lexical search, semantic/vector search, full-text search,
Raft snapshots, backups, or cross-domain query execution.

## Context

Mycel already has two related systems:

1. **Schema-declared query indexes** documented in
   [GWL index declarations and indexed query execution](../schema/gwl-indexes-and-query-planning.md).
   These define authoritative schema metadata and current in-memory graph-storage
   property index maps.
2. **Domain graph persistent indexes** documented in
   [Domain graph persistent indexes](domain-graph-persistent-indexes.md). These
   persist checkpoint-aligned label, tag, and adjacency indexes as local derived
   artifacts under the domain store `indexes/` directory.

Persistent query indexes build on both:

- schema/index definitions remain authoritative metadata;
- graph records and checkpoints remain authoritative graph data;
- persisted index payloads remain local, discardable, derived artifacts;
- validity is tied to one domain graph checkpoint revision/checksum and schema
  hash.

## Goals

- Persist configured query index maps per domain graph store.
- Avoid query-index rebuild work during domain open when a valid index set is
  present.
- Preserve fail-closed query semantics: an index may be used only when its
  definition, schema hash, key encoding, and graph baseline match.
- Keep persistent query indexes local-only and rebuildable.
- Keep `.kseg` segment format unchanged.
- Reuse the existing `indexes/idx-<uuid>/manifest.json` generation and status
  model where practical.
- Support tail replay after checkpoint/index load so post-checkpoint graph
  mutations update query indexes normally.

## Non-goals

- Do not make query indexes authoritative graph state.
- Do not introduce cross-domain GQL execution.
- Do not silently scan the full domain when a production query requires a missing
  or stale index.
- Do not add per-commit persistent-index fsync work.
- Do not replace lexical, semantic, or vector search storage.
- Do not implement bounded-memory/page-cache-backed B-tree or LSM indexes in this
  phase.

## Current in-memory structures

Current graph storage maintains query index state in maps:

```go
configuredIndexes map[graph.DomainID][]schema.IndexDefinition
indexMetadata     map[string]IndexMetadata
nodePropertyIndex map[string]map[string]nodePropertyIndexEntry
edgePropertyIndex map[string]map[string]edgePropertyIndexEntry
```

where `indexIdentity(domainID, indexName)` identifies one configured index.

`IndexMetadata` records:

```go
Name
DomainID
SchemaHash
TargetKind
TargetType
Labels
Field
Kind
Direction
BuildState
LastIndexedGraphRevision
KeyEncodingVersion
Error
```

Query scans use encoded ordered keys:

```go
nodePropertyIndex[identity][encodedKey] = nodePropertyIndexEntry{NodeID, Value, Key}
edgePropertyIndex[identity][encodedKey] = edgePropertyIndexEntry{EdgeID, Value, Key}
```

The persistent format should hydrate these same structures in Phase 1. Bounded
memory alternatives are deferred.

## Validity model

A persistent query index set is valid only when all of the following match:

- domain graph checkpoint ID/revision/checksum;
- domain ID and space ID;
- schema hash for each configured index;
- index definition identity and structural fingerprint;
- index target kind, labels, field path, kind, direction;
- key encoding version;
- payload checksum.

A mismatch means the persisted query-index payload is not usable. The store must
fall back to rebuilding the affected query indexes from checkpoint records and
tail replay, and query surfaces must continue to report non-ready indexes as
unavailable when appropriate.

## On-disk layout

Persistent query indexes should live in the same index set generation as
label/tag/adjacency indexes:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
  indexes/
    LATEST
    idx-<uuid>/
      manifest.json
      labels.kidx
      tags.kidx
      adjacency-out.kidx
      adjacency-in.kidx
      query-node-property.kidx
      query-edge-property.kidx
      query-index-metadata.kidx
```

The current binary `.kidx` format can be extended by adding new payload kinds.
The initial query-index payloads should remain deterministic and sorted.

## Manifest extensions

The existing index set manifest can keep one `indexes` map for payload files and
add query-index metadata either inline or in `query-index-metadata.kidx`.

Recommended approach:

```json
{
  "format_version": 1,
  "index_format": "domain-graph-index-v1-binary",
  "graph_checkpoint_id": "chk-...",
  "graph_revision": 123,
  "graph_checksum": "...",
  "indexes": {
    "labels": {"path": "labels.kidx", "entry_count": 1200, "checksum": "..."},
    "query_node_property": {"path": "query-node-property.kidx", "entry_count": 90000, "checksum": "..."},
    "query_edge_property": {"path": "query-edge-property.kidx", "entry_count": 25000, "checksum": "..."},
    "query_metadata": {"path": "query-index-metadata.kidx", "entry_count": 12, "checksum": "..."}
  }
}
```

`query_metadata` describes every configured query index included in the payloads.
It should include enough information to reject stale or incompatible payloads
without consulting external mutable state beyond the currently configured schema
hash/definitions:

- index identity;
- domain ID;
- schema hash;
- definition fingerprint;
- target kind and target type;
- labels;
- field namespace/name;
- index kind and direction;
- key encoding version;
- build state at checkpoint time;
- last indexed graph revision.

Only ready indexes should be written as usable persistent query indexes.
Building, failed, stale, or retired indexes should be represented in metadata if
needed for status, but their payload entries must not be loaded as ready query
state.

## Definition fingerprints

Schema hash alone is usually sufficient for a domain schema generation, but
query-index loading should also store a per-index definition fingerprint. This
makes local validation robust when partial metadata is inspected or when future
schema lifecycle code supports selective index changes.

The fingerprint should be deterministic over:

- index name;
- target kind;
- target type;
- labels, sorted/canonicalized;
- field namespace and name;
- index kind;
- sort direction;
- required/unique flags;
- key encoding version.

## Payload principles

Persistent query-index payloads should store encoded index keys and entity IDs,
not full node/edge records.

For ordered property indexes, the payload can store:

```text
identity
encoded key
entity id
encoded scalar value, optional
```

The current in-memory entry stores `Value any` for returning query/index scan
results. The persistent payload therefore needs either:

1. enough type-tagged scalar value data to reconstruct `Value`, or
2. a rule that scans fetch the entity record to recover the value.

Phase 1 should store type-tagged scalar values to preserve current scan behavior
without extra entity fetches. Supported value tags should initially cover the
scalar types already accepted by ordered key encoding:

- string
- bool
- integer
- float
- time/date encoded string if currently represented as string
- null only when explicitly allowed by index semantics

Unsupported values must not be persisted as ready entries.

## Open path

The desired open path is:

1. Load and validate graph checkpoint manifest/payloads.
2. Load configured schema index definitions from `index_manifest.mycel` or the
   current graph index manifest.
3. Detect a matching persistent index set.
4. Hydrate authoritative node/edge records and non-query helper indexes from the
   checkpoint.
5. Load persistent label/tag/adjacency/query indexes when valid.
6. If query-index payloads are invalid, rebuild query indexes from checkpoint
   records while preserving safe fallback for non-query indexes.
7. Replay checkpoint tail `.kseg` records; normal mutation apply paths update all
   loaded/rebuilt query indexes.
8. Mark index metadata ready only when schema hash, graph revision, and key
   encoding are consistent.

## Failure policy

Persistent query-index files are local derived artifacts. Failure policy:

```text
missing/stale/corrupt query index payload -> rebuild local query index or mark unavailable; never return wrong query results
```

If the authoritative schema says a query index should exist but the persistent
payload is invalid, the store may rebuild it synchronously during open for the
initial implementation. Later phases may defer rebuild and expose `building` or
`stale` status if open latency becomes too high.

Queries must continue to fail closed when the required index is not `ready`.

## Status and observability

The existing `graph-checkpoint status` nested `persistent_index` object should be
extended or complemented with query-index details:

- query index payload present;
- loaded/rebuilt/fallback result;
- fallback reason;
- number of query indexes included;
- per-index entry counts;
- schema hash;
- key encoding version;
- unavailable index names and reasons.

A later explicit `graph-index status` command can provide detailed per-index
lifecycle output if checkpoint status becomes too crowded.

## Interaction with schema lifecycle

Automatic schema-service-to-graph lifecycle wiring remains a follow-up from the
GWL index implementation plan. Persistent query indexes should not assume that
schema changes are already fully automated.

When schema lifecycle wiring is added:

- adding an index creates metadata in `building` state;
- backfill derives entries from authoritative graph records;
- ready metadata records graph revision and schema hash;
- checkpoint/index writing includes only ready indexes aligned with the
  checkpoint graph revision;
- removing/changing an index retires or removes its persistent payload from the
  next generation.

## Security and locality

Persistent query indexes may contain user property values. They must follow the
same local storage security policy as graph checkpoints and other derived graph
artifacts, including encryption-at-rest requirements when enabled.

Persistent query indexes are local to a replica. They are not Raft replicated,
not backups, and not consensus decisions.

## Open questions

- Should invalid query-index payloads trigger synchronous rebuild during open or
  mark indexes unavailable for asynchronous rebuild?
- Should query-index status remain nested under checkpoint status, or should a
  dedicated `graph-index status` admin method be added before implementation?
- Should equality indexes be included in the first persistence tranche even
  though current implemented scans are ordered-property focused?
- Should metadata and entries share one binary payload or separate metadata,
  node-property, and edge-property payloads?
- What benchmark threshold defines success for 100k+ node domains?
