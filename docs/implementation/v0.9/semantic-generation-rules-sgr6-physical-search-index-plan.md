# SGR6 Semantic Generation Rules Physical Search Index Plan

## Status

Planned. This tranche follows SGR5 rule/binding source assembly and embedding
generation.

## Goal

Replace semantic search's dependence on scanning append-only historical vector
records with a rule/binding-aware physical latest-live search index.

SGR6 should make semantic search fast and bounded by maintaining rebuildable
per-rule/per-binding indexes derived from vector records and tombstones.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Vector store physical index abstraction, mycel-file latest-live index, search-index state updates, vectorstore/search tests. |
| `mycel-api` | Out of scope unless SGR0 search-index status fields are insufficient. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Primary files

```text
internal/semantic/vectorstore/types.go
internal/semantic/vectorstore/mycel_file.go
internal/semantic/vectorstore/mycel_file_test.go
internal/semantic/search/planner.go
internal/semantic/search/types.go
internal/semantic/storage/interface.go
internal/semantic/storage/file_store.go
internal/semantic/model/semantic.go
```

Likely new files:

```text
internal/semantic/vectorstore/search_index.go
internal/semantic/vectorstore/search_index_test.go
```

## Current behavior

Current `mycel-file` search reads all vector records for a semantic index,
reduces them to latest records in memory, and scores those records:

```text
read append-only records -> latest map -> cosine scan -> top N
```

That is acceptable as transitional behavior, but it is not the target model.
Search must not require unbounded scans of historical records.

## Target behavior

For each rule/binding/vector-space, maintain a physical latest-live search index:

```text
space_id + domain_id + semantic_rule_id + embedding_binding_key + vector_space_key
```

Minimum physical index implementation:

- latest live record metadata map;
- dense vector array or compact JSON/binary segment for current live records;
- tombstone application;
- bounded rebuild from append-only records;
- cosine search over the current live set only.

This physical index is derived state. It may be rebuilt from vector records, but
search should not silently fall back to unbounded historical scans.

## Search index files

Recommended initial mycel-file layout:

```text
graphs/<space_id>/semantic/search_indexes/<semantic_rule_id>/<embedding_binding_key>/
  state.json
  latest.json
  vectors-000001.kvix
```

Where:

- `state.json` stores `SemanticSearchIndexState`-compatible status;
- `latest.json` stores record IDs and target metadata for current live records;
- `vectors-000001.kvix` stores vectors for current live records.

If a binary vector file is too much for SGR6, `latest.json` may include vectors
for the initial implementation, as long as it is latest-live only and not
historical.

## New abstractions

Add a search-index abstraction in `internal/semantic/vectorstore`:

```go
type SearchIndexKey struct {
    SpaceID             domainspace.SpaceID
    DomainID            graph.DomainID
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    VectorStoreID       domainsemantic.VectorStoreID
    VectorSpaceKey      string
}

type LiveVectorRecord struct {
    RecordID            domainsemantic.SemanticRecordID
    TargetNodeID        graph.NodeID
    NodeID              graph.NodeID
    SourceHash          string
    SourceMode          string
    ModelEndpointID     domainsemantic.ModelEndpointID
    ModelID             domainsemantic.InferenceModelID
    CapabilityID        domainsemantic.ModelEndpointCapabilityID
    CredentialGrantID   domainsemantic.CredentialGrantID
    Vector              []float64
    CreatedAt           time.Time
}

type PhysicalSearchIndex interface {
    UpsertLive(ctx context.Context, key SearchIndexKey, rec domainsemantic.AdvancedEmbeddingRecord) error
    Tombstone(ctx context.Context, key SearchIndexKey, rec domainsemantic.AdvancedEmbeddingRecord) error
    Search(ctx context.Context, key SearchIndexKey, query []float64, limit int, minScore float64) ([]SearchResult, error)
    Rebuild(ctx context.Context, key SearchIndexKey, records []domainsemantic.AdvancedEmbeddingRecord, limit RebuildLimit) (domainsemantic.SemanticSearchIndexState, error)
    State(ctx context.Context, key SearchIndexKey) (domainsemantic.SemanticSearchIndexState, error)
}
```

`RebuildLimit` should prevent accidental unbounded rebuilds:

```go
type RebuildLimit struct {
    MaxRecords int
    MaxBytes   int64
}
```

## Vector write/delete integration

Update `MycelFileBackend.Upsert`:

1. append vector record as today;
2. derive `SearchIndexKey` from record rule/binding/vector-space fields;
3. update latest-live physical index for the key;
4. update `SemanticSearchIndexState` to ready/degraded.

Update `MycelFileBackend.Delete`:

1. append tombstone record as today;
2. apply tombstone to physical index for the same rule/binding/key;
3. update state.

If physical index update fails after append succeeds, preserve append-only record
as authoritative and mark search-index state degraded/error. Do not lose the
record.

## Search behavior

Update `MycelFileBackend.Search` so rule-native searches use the physical index.

Search input should carry rule/binding fields:

```go
type SearchInput struct {
    SpaceID             domainspace.SpaceID
    DomainID            graph.DomainID
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    SemanticIndexID     domainsemantic.SemanticIndexID // transitional
    VectorStoreID       domainsemantic.VectorStoreID
    VectorSpaceKey      string
    Query               []float64
    Limit               int
    MinScore            float64
}
```

Rules:

- If `SemanticRuleID` is present, use physical index only.
- If physical index is missing, either perform bounded rebuild or return a clear
  fail-closed error.
- If transitional `SemanticIndexID` search is used, keep old behavior only for
  legacy paths, and mark it for SGR7 removal.
- Never do an unbounded historical record scan for rule-native search.

## Bounded rebuild

A rebuild is allowed only when explicitly requested or when bounded by safe
limits.

Recommended default limits:

```text
max_records = 100_000
max_bytes   = configurable/future, default conservative
```

Rebuild algorithm:

1. read append-only records for the rule/index directory;
2. filter by rule ID, binding key, domain, vector store, vector space;
3. sort/apply by creation time and record ID;
4. maintain latest per:

   ```text
   target_node_id + source_mode + vector_space_key
   ```

5. remove tombstoned targets;
6. write physical index atomically;
7. update `SemanticSearchIndexState`.

If limits are exceeded, set state degraded/error and fail closed.

## Search-index state

Use existing SGR2 storage method:

```go
UpsertSearchIndexState(ctx, domainsemantic.SemanticSearchIndexState)
```

For pure vectorstore tests without a `SpaceManager`, mycel-file may persist a
local `state.json` and later service layers can mirror it into semantic storage.

State values:

```text
ready
building
degraded
missing
error
```

State should include:

- semantic rule ID;
- embedding binding key;
- live record count;
- last rebuild time;
- last error;
- updated time.

## Search planner integration boundary

Full planner rewrite is SGR7. SGR6 should expose the fast index through
vectorstore APIs and update tests there.

Minimal planner change allowed in SGR6:

- when a result/record carries rule/binding fields, preserve them in search
  result structs;
- avoid adding new legacy index assumptions.

Do not fully replace semantic search API behavior in SGR6 unless small and safe.

## Tests

### Vectorstore tests

Add/update tests in `internal/semantic/vectorstore`:

1. upsert creates latest-live search index for rule/binding;
2. second record for same target/source replaces latest record;
3. tombstone removes latest live record;
4. same target in different binding remains independent;
5. same target in different vector space remains independent;
6. search uses physical latest-live index and does not include historical records;
7. missing physical index fails closed for rule-native search;
8. bounded rebuild reconstructs latest-live index from append-only records;
9. rebuild limit exceeded returns actionable error and degraded/error state;
10. legacy `SemanticIndexID` search still passes existing transitional tests.

### Storage/state tests

Add/update tests in `internal/semantic/storage` if state mirroring is implemented:

- search-index state upserts by rule/binding;
- degraded/error state preserves last error;
- live record count updates.

### Search tests

If planner is touched, add focused tests in `internal/semantic/search` for
rule/binding result attribution only.

## Validation commands

Minimum:

```sh
go test ./internal/semantic/vectorstore ./internal/semantic/storage -count=1
git diff --check
```

Preferred:

```sh
go test ./internal/semantic/... -count=1
make docs-check
```

Do not run destructive Compose/K3s cluster tests for SGR6.

## Implementation sequence

1. Extend `vectorstore.SearchInput` with rule/binding/vector-space fields.
2. Add physical search-index key and latest-live data structures.
3. Implement mycel-file latest-live index persistence.
4. Update `Upsert` to append record then update physical index.
5. Update `Delete` to append tombstone then remove live record from physical
   index.
6. Implement bounded rebuild.
7. Update rule-native `Search` to use physical index or fail closed.
8. Preserve transitional legacy search behavior for `SemanticIndexID` only.
9. Add vectorstore/state tests.
10. Run validation and mark this plan implemented.

## Acceptance criteria

SGR6 is complete when:

- rule-native vector upserts maintain a per-rule/per-binding latest-live physical
  index;
- tombstones remove records from that physical index;
- rule-native search uses the physical latest-live index, not historical scans;
- missing/stale indexes fail closed or rebuild within explicit bounds;
- physical index state is observable through `SemanticSearchIndexState` or local
  state files;
- tests prove binding isolation, tombstone behavior, rebuild behavior, and
  no-historical-result search behavior;
- legacy scan behavior remains only for explicitly transitional semantic-index
  paths.

## Risks and follow-ups

- Atomicity: append-only vector record writes and physical index updates are not
  a single transaction. If index update fails, appended records remain
  authoritative and state must become degraded/error.
- Performance: initial latest-live implementation may still use a simple vector
  array. ANN or mmap optimizations can come later as long as the search input is
  latest-live and bounded.
- Planner SGR7 must route semantic search by rule/binding and remove legacy
  semantic-index scan fallback.
- Raft mode treats vector/search index state as rebuildable derived state; do not
  make it authoritative over append-only vector records.
