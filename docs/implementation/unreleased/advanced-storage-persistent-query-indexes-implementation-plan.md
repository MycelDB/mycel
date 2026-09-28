# Advanced storage phase 6: persistent query indexes implementation plan

## Status

Partially implemented on the `advanced_storage` branch. The first implementation
persists and loads checkpoint-aligned binary payloads for the current ordered
node and edge property index maps, with fallback to checkpoint rebuild when query
index payloads are missing, stale, or corrupt. `graph-checkpoint status` JSON now
includes per-query-index details under `persistent_index.query_indexes[]`. Larger
query-index benchmarks and restart validation remain follow-ups.

Design reference:

- [Domain graph persistent query indexes](../../design/graph/domain-graph-persistent-query-indexes.md)

This phase builds on:

- domain-scoped graph stores;
- domain graph checkpoints;
- automatic/manual checkpoint operations;
- persistent binary label/tag/adjacency index sets;
- schema-declared GWL/query index definitions and current in-memory property
  index maps.

## Goal

Persist schema-declared query index maps as checkpoint-aligned local derived
artifacts so large domains can open and immediately serve indexed structured/GQL
queries without rebuilding every configured property index from checkpoint
records or full segment replay.

Initial implementation should focus on currently implemented graph-storage query
indexes:

1. ordered node property indexes;
2. ordered edge property indexes.

Equality indexes should be added only if they are implemented in the in-memory
query index path before or during this phase.

## Constraints

- `.kseg` segment format remains unchanged.
- Query index payloads are derived local artifacts, not authoritative graph data.
- Schema index definitions and graph records remain authoritative.
- Query index payloads must be checkpoint-aligned by graph revision/checksum.
- Query index payloads must also be schema-aligned by schema hash and definition
  fingerprint.
- Corrupt/stale payloads must never return wrong query results.
- Production indexed queries must continue to fail closed when required indexes
  are unavailable.
- No per-commit persistent-index fsync work.
- Keep map-based in-memory indexes in this phase; bounded-memory B-tree/LSM
  storage remains future work.

## Phase 6.1 — format design and metadata helpers

### Tasks

1. Add internal metadata structs for persisted query indexes:
   - index identity;
   - domain ID;
   - schema hash;
   - target kind/type;
   - labels;
   - field path;
   - index kind/direction;
   - key encoding version;
   - build state;
   - last indexed graph revision;
   - definition fingerprint;
   - entry count.
2. Add deterministic definition fingerprint helper for `schema.IndexDefinition`.
3. Add type-tagged scalar encoding helpers for stored property values.
4. Decide final payload layout names. Proposed:

   ```text
   query-index-metadata.kidx
   query-node-property.kidx
   query-edge-property.kidx
   ```

5. Extend persistent index manifest validation to recognize optional query-index
   payloads while keeping older label/tag/adjacency-only index sets valid.

### Tests

- fingerprint is stable across process runs;
- label ordering in definitions does not create false mismatches after
  canonicalization;
- changed field/path/direction/schema hash changes validation outcome;
- scalar value binary round trips for supported types;
- unsupported values are rejected or skipped according to chosen policy.

### Acceptance

- Format helpers compile and are covered by unit tests.
- No open-path behavior changes yet.

## Phase 6.2 — deterministic export/import helpers

### Tasks

1. Export ready `indexMetadata` entries for configured query indexes.
2. Export `nodePropertyIndex` entries grouped by index identity.
3. Export `edgePropertyIndex` entries grouped by index identity.
4. Add binary marshal/unmarshal helpers for metadata, node-property payloads,
   and edge-property payloads.
5. Import helpers hydrate:

   ```go
   indexMetadata
   nodePropertyIndex
   edgePropertyIndex
   configuredIndexes // only from authoritative graph index manifest, not from payload
   ```

6. Validate imported entries against live checkpointed `nodeRecords` and
   `edgeRecords` to prevent dangling entity IDs.

### Tests

- round trip node property indexes;
- round trip edge property indexes;
- deterministic output independent of map iteration order;
- reject dangling node/edge IDs;
- reject stale schema hash;
- reject key encoding mismatch;
- reject index metadata for missing configured index definitions.

### Acceptance

- Export/import tests pass without modifying checkpoint open behavior.

## Phase 6.3 — writer integration

### Tasks

1. Extend `writeIndexSetLocked` to include query-index payloads when ready query
   indexes exist.
2. Include query payload entries in `indexes/idx-<uuid>/manifest.json`:

   ```json
   {
     "query_metadata": {"path": "query-index-metadata.kidx", "entry_count": 12, "checksum": "..."},
     "query_node_property": {"path": "query-node-property.kidx", "entry_count": 90000, "checksum": "..."},
     "query_edge_property": {"path": "query-edge-property.kidx", "entry_count": 25000, "checksum": "..."}
   }
   ```

3. Write query-index payloads in the same temporary directory and atomic publish
   sequence as label/tag/adjacency payloads.
4. Exclude indexes whose metadata is not `ready` or whose
   `LastIndexedGraphRevision` is behind the checkpoint revision unless the
   implementation can prove tail replay will bring them current.
5. Preserve writer success for domains with no configured query indexes.

### Tests

- checkpoint creation writes query-index payloads for ready configured indexes;
- no query payload is written when no configured query indexes exist;
- building/failed/stale/retired indexes are not published as ready payloads;
- manifest checksums detect payload corruption;
- older index sets are cleaned as before.

### Acceptance

- Manual/admin checkpoint creation produces query-index artifacts when configured
  indexes are ready.
- Graph checkpoint remains valid even if query-index payload writing fails; index
  write failure must not invalidate authoritative graph checkpoint creation.

## Phase 6.4 — open-path loader and fallback

### Tasks

1. During checkpoint open, detect whether the latest index set includes query
   payloads matching the loaded checkpoint and configured schema definitions.
2. Hydrate checkpoint records without rebuilding query-property indexes when a
   valid query-index candidate exists.
3. Load query-index metadata and property payloads.
4. If loading succeeds, install `indexMetadata`, `nodePropertyIndex`, and
   `edgePropertyIndex` maps before tail replay.
5. Tail replay uses existing mutation apply paths to update loaded query indexes
   for post-checkpoint changes.
6. If query-index loading fails:
   - rebuild query indexes from checkpoint records synchronously for initial
     implementation; or
   - mark affected indexes unavailable if review chooses async rebuild.
7. Keep label/tag/adjacency persistent index behavior unchanged.

### Tests

- open with matching query-index payload uses persisted query indexes;
- indexed node-property scan works immediately after open;
- indexed edge-property scan works immediately after open;
- corrupt query payload falls back and does not return wrong results;
- stale schema hash falls back or marks unavailable;
- tail replay after query-index load updates node and edge property indexes;
- deletion tombstones after checkpoint remove query-index entries during tail
  replay.

### Acceptance

- Domain open is backward-compatible with old index sets that lack query payloads.
- Query correctness matches checkpoint-only rebuild behavior.

## Phase 6.5 — status/admin visibility

### Tasks

Implemented by extending the existing `graph-checkpoint status` nested
`persistent_index` object rather than adding a new admin method. JSON output now
includes `persistent_index.query_indexes[]` entries with:

- index identity and name;
- domain ID;
- schema hash;
- definition fingerprint;
- target kind/type, labels, field, index kind, and direction;
- build state;
- last indexed graph revision;
- key encoding version;
- entry count;
- load result inherited from the persistent index set.

The parent persistent-index status still reports query payload presence via
manifest entries, set-level load result, and fallback reason.

### Tests

- storage status reports query index details before and after reopen;
- admin service maps query index details to protobuf responses;
- corrupt query payload fallback continues to report set-level fallback reason.

### Acceptance

- Operators can tell whether configured query indexes were included in a
  checkpoint-aligned persistent index set and whether that set was used on open
  or is present-but-not-loaded.

## Phase 6.6 — benchmarks and validation

### Tasks

Add benchmarks that compare:

1. checkpoint-only rebuild of query indexes;
2. checkpoint + persistent label/tag/adjacency only;
3. checkpoint + persistent label/tag/adjacency + query indexes.

Implemented storage benchmarks:

```bash
go test ./internal/graph/storage -run '^$' -bench BenchmarkLocalStoreQueryIndexOpenAndScan -benchmem -count=1
go test ./internal/graph/storage -run '^$' -bench BenchmarkLocalStoreQueryIndexOpenPhases -benchmem -count=1
MYCEL_GRAPH_BENCH_LARGE=1 go test ./internal/graph/storage -run '^$' -bench BenchmarkLocalStoreQueryIndexOpenPhasesLarge -benchmem -benchtime=1x -count=1 -timeout=30m
```

The default benchmark fixture currently uses:

- 10k live nodes / 20k live edges;
- 3 historical node update rounds;
- 1 ordered node property index on `BenchmarkNode.properties.ordinal`;
- 1 ordered edge property index on `REFERENCES.properties.ordinal`;
- immediate ordered node-property and edge-property scans after open.

Initial Apple M4 Max combined open + node scan + edge scan baseline with
`-benchtime=1x`:

```text
BenchmarkLocalStoreQueryIndexOpenAndScan/full-replay-query-index-rebuild          ~197 ms/op
BenchmarkLocalStoreQueryIndexOpenAndScan/checkpoint-query-index-rebuild            ~95 ms/op
BenchmarkLocalStoreQueryIndexOpenAndScan/checkpoint-persistent-query-indexes       ~97 ms/op
```

Separated phase baseline with `-benchtime=1x`:

```text
BenchmarkLocalStoreQueryIndexOpenPhases/full-replay-query-index-rebuild/open-only                 ~183 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/full-replay-query-index-rebuild/open-first-node-scan      ~191 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/full-replay-query-index-rebuild/open-first-edge-scan      ~188 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-query-index-rebuild/open-only                   ~88 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-query-index-rebuild/open-first-node-scan        ~94 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-query-index-rebuild/open-first-edge-scan        ~97 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-persistent-query-indexes/open-only              ~94 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-persistent-query-indexes/open-first-node-scan   ~93 ms/op
BenchmarkLocalStoreQueryIndexOpenPhases/checkpoint-persistent-query-indexes/open-first-edge-scan   ~92 ms/op
```

Interpretation: checkpoints materially reduce query-index rebuild cost versus
full replay. Persistent query indexes are roughly at parity with checkpoint-side
rebuild for this 10k/20k fixture. The separated phase benchmark suggests
persistent query payload loading currently shifts cost into open allocations but
has comparable immediate first-scan latency. Larger configured-index domains or
multiple property indexes are the next benchmark target before treating
persistent query indexes as a clear performance win.

Large opt-in benchmark fixture currently uses:

- 100k live nodes / 250k live edges;
- 3 historical node update rounds;
- 3 ordered node property indexes;
- 2 ordered edge property indexes;
- checkpoint-only rebuild versus checkpoint + persistent query indexes only
  (full replay is intentionally omitted from this large manual benchmark).

Large Apple M4 Max baseline with `-benchtime=1x`:

```text
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-only                    ~1.44 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-first-node-scans        ~1.52 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-first-edge-scans        ~1.57 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-only               ~1.67 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-first-node-scans   ~1.64 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-first-edge-scans   ~1.64 s/op
```

After pre-sizing hydrated query maps and reusing decoded metadata from the open
load path:

```text
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-only                    ~1.42 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-first-node-scans        ~1.48 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-query-index-rebuild/open-first-edge-scans        ~1.54 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-only               ~1.50 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-first-node-scans   ~1.54 s/op
BenchmarkLocalStoreQueryIndexOpenPhasesLarge/checkpoint-persistent-query-indexes/open-first-edge-scans   ~1.57 s/op
```

Interpretation: the current persistent query-index map loader is still not a
clear performance win at the 100k/250k multi-index scale, but the first load-path
optimization narrowed the gap materially and reduced allocations. Further wins
likely require reducing query payload read/decode overhead rather than only map
pre-sizing.

Suggested future benchmark dimensions:

- edge-heavy domain with multiple edge property indexes beyond the current large
  fixture;
- tail replay after checkpoint with updates/deletes affecting indexed values.

### Acceptance

- Benchmarks cover checkpoint rebuild versus checkpoint + persistent query
  indexes.
- Results are documented in this plan or a follow-up validation report.

## Rollout strategy

1. Land metadata/fingerprint/scalar encoding helpers. — implemented for ordered
   node/edge property indexes.
2. Land export/import helpers behind tests. — implemented.
3. Extend index writer to produce optional query payloads. — implemented.
4. Extend open loader with strict fallback. — implemented.
5. Add detailed query-index status visibility. — implemented under
   `persistent_index.query_indexes[]`.
6. Run larger query-index benchmarks and restart validation. — follow-up.
7. Only then consider enabling any automatic policy beyond checkpoint-coupled
   best-effort writes.

## Failure policy

Initial policy:

```text
missing query payload -> rebuild query indexes from checkpoint records
stale/corrupt query payload -> rebuild query indexes or mark unavailable, never use stale entries
required query index unavailable -> query fails closed
```

If synchronous rebuild makes open too slow for very large domains, a later phase
can switch to async rebuild with explicit `building` status.

## Validation commands

Run at minimum:

```bash
go test ./internal/graph/storage ./internal/graph/service ./internal/daemon/api/admin ./internal/cli/cmd -count=1
go test ./internal/graph/storage -run '^$' -bench BenchmarkLocalStoreOpen -benchmem -count=1
make docs-check
git diff --check
make test
```

Manual/destructive validation should include restart/reopen tests for a domain
with configured node and edge property indexes and at least one checkpoint/index
set written before restart.

## Open review questions

- Should invalid persisted query indexes rebuild synchronously during open, or
  mark affected indexes unavailable for async rebuild?
- Should query-index status remain inside graph checkpoint status, or should this
  phase introduce `graph-index status`?
- Should equality indexes be included in Phase 6 if current query execution is
  primarily ordered-property based?
- Should edge property indexes be included immediately or after node property
  indexes prove the format?
- What 100k-node benchmark threshold defines success?
