# Lexical Search Design

## Status

Proposed design. Implementation should not begin until this design and the related implementation plan have been reviewed.

## Summary

Lexical search is a first-class MycelDB search mechanism for exact/token-based full-text retrieval. It is a peer to semantic/vector search and metadata/tag/property search, not a replacement for either.

The first version provides per-space/domain lexical indexing over user-authored string content, Lucene-style query syntax, BM25-style scoring, and a dedicated Search API. Indexing is eventually consistent, like semantic indexing, and slightly stale results are acceptable when freshness is reported.

Future search orchestration can combine lexical, semantic, and metadata signals for hybrid retrieval and reranking.

## Goals

1. Add a general MycelDB lexical search subsystem usable by any application, including Knot PKM.
2. Index user-authored strings from graph node payloads and properties.
3. Exclude system metadata/internal bookkeeping from the lexical index.
4. Keep indexes scoped per space/domain for access-control and operational simplicity.
5. Support a Lucene-style query syntax subset in v1.
6. Rank matches with BM25-style relevance scoring.
7. Update indexes asynchronously/eventually, similar to semantic indexing.
8. Make the subsystem Raft-safe by deriving local physical indexes from committed graph changes rather than directly replicating index files.
9. Expose lexical search through a dedicated Search API, not as only a GQL or structured-query feature.
10. Report index freshness/staleness and diagnostics to callers.

## Non-goals for v1

- Import/export portability for other PKMs.
- Semantic reranking implementation. The design must leave room for it, but lexical search v1 can return lexical scores only.
- Blob binary/text extraction. Blob text indexing can be added later after extractors and content-type handling are designed.
- Stemming or language-specific analyzers.
- Fuzzy search and wildcard/prefix search.
- Snippet/highlight generation.
- External search engines such as Elasticsearch/OpenSearch/Lucene/Solr.
- Replicating physical index segments through Raft.

## Relationship to existing search mechanisms

MycelDB should have three complementary search families:

| Search family | Purpose | Examples |
| --- | --- | --- |
| Metadata/tag/property search | Structured filtering over explicit fields | `tag=project`, `status=active`, date ranges |
| Semantic search | Meaning/similarity search using embeddings/vector stores | "notes like this idea" |
| Lexical search | Token/phrase/boolean full-text search | `"vector database" AND raft` |

Lexical search should be independently useful, but the API should not prevent future hybrid search plans that combine:

- lexical BM25 score;
- semantic/vector similarity score;
- metadata filters and boosts;
- recency or graph-structure boosts;
- reranking diagnostics.

## Indexed content

### Included in v1

Lexical search indexes user-authored string content from graph nodes:

- string values in node payloads;
- string values in node properties;
- string values nested inside arrays/objects in payloads/properties, using deterministic traversal;
- optional field paths for diagnostics and future fielded scoring, e.g. `payload.text`, `properties.title`.

### Excluded in v1

Lexical search does not index:

- node metadata/internal fields such as IDs, revision counters, timestamps, raft/WAL state, or system bookkeeping;
- edge metadata;
- system metadata catalogs;
- inference credentials/secrets;
- raw binary blob bytes;
- generated embedding vectors.

Tags and structured properties remain available through metadata search/filtering. Lexical indexing may index string property values, but structured filtering semantics should remain owned by metadata/tag/property search.

## Scope and partitioning

Indexes are scoped by:

```text
space_id + domain_id
```

A search request must specify the space/domain scope. This keeps access control simple and avoids cross-tenant/global index leakage.

Physical index files should live under the daemon data directory:

```text
<MYCELD_DATA_DIR>/search/lexical/<space-id>/<domain-id>/
```

The path is daemon-owned derived state and must be safe for backup/restore procedures.

## Physical file format

V1 should use a Mycel-native, Lucene-inspired segment format:

```text
<MYCELD_DATA_DIR>/search/lexical/<space-id>/<domain-id>/
├── manifest.json
├── cursor.json
├── segments/
│   ├── seg_000001/
│   │   ├── segment.json
│   │   ├── terms.idx
│   │   ├── postings.bin
│   │   ├── docs.bin
│   │   └── checksum.sha256
│   └── seg_000002/
└── locks/
```

Use JSON for small control/diagnostic files and binary varint-oriented files for high-volume index data.

### `manifest.json`

`manifest.json` is the atomically-written index manifest. It records the format/analyzer versions and the active immutable segment list:

```json
{
  "format_version": 1,
  "analyzer_version": "lexical-analyzer-v1",
  "space_id": "...",
  "domain_id": "...",
  "segments": ["seg_000001", "seg_000002"],
  "created_at": "...",
  "updated_at": "..."
}
```

Manifest updates must use write-temp/fsync/rename semantics where practical so a crash never publishes a partial segment list.

### `cursor.json`

`cursor.json` mirrors local freshness/progress for diagnostics and startup:

```json
{
  "indexed_graph_revision": 12345,
  "updated_at": "...",
  "last_error": ""
}
```

The authoritative progress model must still be cluster-safe. The local cursor file is not a substitute for Raft/WAL-owned progress where that is required for failover correctness.

### Immutable segment directories

Each indexing batch writes a complete immutable segment under `segments/seg_<n>/`, validates checksums, then atomically publishes it by updating `manifest.json`. This avoids a single large mutable index file and simplifies crash recovery.

`segment.json` records per-segment metadata:

```json
{
  "segment_id": "seg_000001",
  "format_version": 1,
  "doc_count": 10000,
  "deleted_count": 42,
  "term_count": 50000,
  "avg_doc_length": 153.2,
  "min_graph_revision": 100,
  "max_graph_revision": 12345
}
```

`terms.idx` is a sorted term dictionary mapping each normalized term to its postings offset, postings length, and document frequency.

`postings.bin` stores varint-encoded postings lists. Each posting should include enough data for BM25 and phrase search:

```text
doc_id_delta
term_frequency
positions_delta[]
field_ids_or_field_mask
```

`docs.bin` maps internal index document IDs to graph nodes and scoring stats:

```text
doc_id -> node_id, graph_revision, token_count, field lengths
```

Do not store full source node text in the lexical index in v1. The graph store remains the source of truth for full content.

Compaction can merge immutable segments and tombstones later without changing query semantics.

## Consistency model

Lexical indexes are eventually consistent. Slightly stale results are acceptable.

The Search API should return freshness metadata such as:

- indexed graph revision;
- latest known graph revision if available;
- staleness status: `fresh`, `stale`, `rebuilding`, `unavailable`, or similar;
- lag, if known.

Committed graph/query reads remain authoritative. Lexical search results are discovery candidates with freshness diagnostics, not proof that a node still exists at the latest committed revision.

Callers that require strict current state should re-read returned node IDs through graph APIs after search.

## Raft and clustering model

Raft should own committed graph mutations and durable indexing progress, not physical inverted-index files.

Recommended model:

1. Graph writes commit through existing graph/Raft ownership.
2. Graph change events become the source of truth for index updates.
3. The current leader/master for a space/domain is the only v1 component that advances authoritative lexical indexing progress for that scope.
4. Followers do not independently advance authoritative lexical cursors.
5. A durable per-space/domain lexical cursor records the highest graph revision indexed.
6. Cursor/progress state must be cluster-safe and recoverable.
7. On startup, leadership change, or snapshot restore, the current owner resumes indexing from the last durable cursor or rebuilds if the physical index is missing/corrupt.

This mirrors semantic indexing at the consistency level: asynchronously update derived search state from committed graph data, report staleness, and avoid treating derived indexes as authoritative cluster state. It differs from broad replica-local indexing by making v1 progress advancement owner-only.

### Leader/ownership behavior

V1 indexing should be owned by the authoritative leader/master for the space/domain. In the current Raft placement model, that means the node that owns the relevant space partition for committed writes and domain-scoped work.

The leader/master should:

- consume committed graph changes for its owned space/domain scopes;
- update the lexical index and durable lexical indexing cursor;
- serve lexical Search API requests for those scopes, or receive forwarded requests from followers;
- resume indexing from the durable cursor after leadership changes, restart, or recovery.

Followers should not independently advance authoritative lexical indexing progress in v1. A follower that receives a lexical search request should route/forward it to the current owner, or fail closed with a clear unavailable/routing diagnostic if no safe owner is known.

Replica-local read indexes can be added later as an optimization, but they should be derived caches with explicit freshness diagnostics, not independent sources of authoritative indexing progress.

### Distributed request routing

Search requests are scoped by `space_id + domain_id`. In clustered mode:

1. The receiving node authenticates and authorizes the request enough to decide whether forwarding is allowed.
2. The receiving node resolves the current owner/leader for the target space/domain.
3. If the receiver is the owner, it executes lexical search against its local derived index.
4. If the receiver is not the owner, it forwards the request to the owner using authenticated backend routing.
5. If no safe owner is known, the request fails closed with an unavailable/routing diagnostic.

The owner returns index freshness with the result. A follower must not silently serve from a stale local cache in v1 unless a future API explicitly models replica-cache reads and freshness constraints.

### Leadership changes and failover

When leadership changes:

- the old leader stops advancing lexical indexing progress for scopes it no longer owns;
- the old leader stops serving authoritative lexical search for those scopes and forwards or returns unavailable;
- the new leader validates its local index manifest/analyzer version/cursor;
- if the index is valid but behind, the new leader catches up from committed graph changes;
- if the index is missing, corrupt, or incompatible, the new leader rebuilds from graph state;
- while rebuilding, the Search API returns `rebuilding`/`unavailable`, or returns stale results only when explicitly allowed and clearly reported.

Physical index files are never the source of truth. Losing them should affect search availability/freshness, not graph correctness.

## Query syntax

The user-facing syntax should be Lucene-style but deliberately scoped for v1.

### v1 supported syntax

The implicit default operator is `AND`.

- term search:

  ```text
  database
  ```

- phrase search:

  ```text
  "vector database"
  ```

- boolean operators:

  ```text
  graph AND raft
  graph OR semantic
  graph NOT draft
  graph -draft
  ```

- grouping:

  ```text
  (graph OR semantic) AND search
  ```

- implicit default operator, likely `AND` unless product review chooses `OR`:

  ```text
  graph search
  ```

### Out of scope for v1

- wildcard/prefix terms:

  ```text
  graph*
  ```

- fuzzy terms:

  ```text
  graph~1
  ```

- full fielded lexical scoring:

  ```text
  title:graph
  ```

- range expressions in lexical syntax:

  ```text
  created:[2026-01-01 TO 2026-12-31]
  ```

Fielded and range filters should be handled by metadata/filter request fields in v1. The parser may reserve these forms and return clear unsupported-syntax diagnostics.

## Tokenization and normalization

V1 should use simple language-neutral token normalization:

- Unicode-aware token boundaries;
- lowercase/case-folded terms;
- punctuation splitting;
- preserve token positions for phrase search;
- no stemming;
- no stop-word removal by default;
- deterministic behavior across platforms.

The analyzer configuration should be versioned. Index files should record analyzer version so future analyzer changes can trigger rebuilds.

## Inverted index model

The physical index should be implemented in Go as daemon-owned storage.

Suggested logical structures:

- term dictionary;
- posting lists per term;
- per-document field/token length stats;
- document table mapping index document IDs to graph node IDs and revisions;
- deleted/tombstone records for node deletion/update handling;
- per-domain index metadata and cursor.

A document corresponds to one graph node version visible to a domain. Updates can be implemented as delete-old/add-new, with compaction later.

## Ranking

V1 should use BM25-style scoring.

Required scoring inputs:

- term frequency in document;
- document length;
- average document length in the index scope;
- document frequency per term;
- total document count.

Phrase matches may receive a deterministic boost over independent term matches. Exact boost constants should be documented and versioned.

The API should return both score and diagnostic components if practical, for example:

- final score;
- matched terms;
- phrase matched true/false;
- analyzer/index version;
- indexed revision.

## Dedicated Search API

Lexical search should be exposed through a dedicated Search API rather than only through GQL or `QueryService.ExecuteQuery`.

A future protobuf surface might look conceptually like:

```proto
service SearchService {
  rpc Search(SearchRequest) returns (SearchResponse);
}

message SearchRequest {
  string space_id = 1;
  string domain_id = 2;
  SearchMode mode = 3;
  string query = 4;
  SearchFilters filters = 5;
  int32 page_size = 6;
  string page_token = 7;
  bool include_diagnostics = 8;
}
```

V1 can support `mode=LEXICAL` only. The API should leave room for future modes such as semantic, metadata, hybrid, or reranked search.

### Response requirements

Search responses should include:

- ordered results;
- node ID and domain/space identifiers;
- lexical score;
- optional matched fields/terms diagnostics;
- pagination token;
- index freshness/staleness metadata;
- warnings for stale/rebuilding/unsupported syntax cases.

The API should avoid returning full node payloads by default. Callers can fetch nodes through graph APIs if they need complete current state.

## Authorization model

Lexical search must enforce the same access boundaries as graph reads.

Minimum requirements:

- request principal must have read access to the target space/domain;
- results must never include nodes outside the authorized space/domain;
- future cross-domain search must explicitly model authorization and result partitioning;
- diagnostics must not leak unauthorized terms, node counts, or existence metadata across spaces/domains.

Per-space/domain indexes simplify this requirement, but request authorization remains mandatory.

## Operations and maintenance

Operators and applications need visibility into indexing state.

Recommended diagnostics:

- list lexical index status by space/domain;
- current indexed revision;
- queued/backlog count if available;
- last indexing error;
- rebuild in progress;
- analyzer/index version;
- disk usage estimate;
- manual rebuild request for a space/domain.

Backup/restore behavior must be documented. Because lexical indexes are derived, a restore can either include the physical index for faster startup or rebuild it from graph state. The implementation plan should choose and test one default behavior.

## Failure behavior

Lexical search should fail safely:

- invalid query syntax returns `InvalidArgument` with useful diagnostics;
- index missing/corrupt returns `FailedPrecondition`/`Unavailable` or triggers rebuild, depending on request mode;
- stale indexes return results only with freshness warnings;
- unsafe Raft/local state must fail closed rather than serving from untrusted state;
- partial internal failures must not leak unauthorized data.

## Future hybrid reranking

A later search orchestration layer can combine lexical, semantic, and metadata signals.

The lexical subsystem should therefore expose enough stable result data for reranking:

- node IDs;
- lexical score;
- score explanation or matched terms where practical;
- indexed revision/freshness;
- candidate limit and pagination behavior.

Hybrid search can then perform:

1. lexical candidate generation;
2. metadata filtering/boosting;
3. semantic candidate generation or semantic reranking;
4. final score normalization and explanation.

This should be designed as an orchestrator above individual search mechanisms, not by entangling BM25 internals with vector search internals.

## Testing strategy

Implementation should include tests for:

- tokenizer/analyzer determinism;
- parser support for terms, phrases, boolean operators, and grouping;
- BM25 ranking order and tie stability;
- per-space/domain isolation;
- authorization boundaries;
- graph create/update/delete indexing behavior;
- eventual indexing cursor/freshness reporting;
- startup/rebuild behavior after missing/corrupt index files;
- Raft-mode fail-closed behavior and replicated graph-change replay;
- Search API pagination and diagnostics.

## LS0 implementation decisions

The following decisions are fixed for the initial implementation tranche:

1. API package and services:
   - client search API: `mycel.client.v1.SearchService` in `mycel/client/v1/search.proto`;
   - admin maintenance API: `mycel.admin.v1.AdminLexicalMaintenanceService` in `mycel/admin/v1/lexical_maintenance.proto`.
2. Client API exposes `Search` and `GetLexicalIndexStatus`; manual rebuild is admin-only.
3. V1 `SearchRequest.mode` supports `SEARCH_MODE_LEXICAL`; unspecified mode defaults to lexical while lexical is the only supported mode.
4. The implicit boolean operator is `AND`.
5. Stale results are opt-in through `allow_stale`; callers may also set `max_revision_lag` when the latest graph revision is known.
6. Followers use authenticated backend forwarding to the current space/domain owner. Unknown or unsafe owner routing fails closed.
7. Clustered cursor/progress is Raft/WAL-owned per space/domain. Local `cursor.json` is diagnostic/cache state only and cannot be used by stale followers to publish authoritative progress.
8. BM25 defaults are `k1=1.2` and `b=0.75`. Phrase matches receive a deterministic multiplier boost of `1.25` after base term scoring.
9. Logical/export-style backups do not need to include derived lexical index files. Data-directory backups may include them, but restore must validate manifest/analyzer/cursor state and rebuild when invalid, missing, stale, or unsafe.
10. Automatic segment compaction is deferred. V1 must define compaction interfaces and tombstone behavior, and manual rebuild is the operational cleanup path.
