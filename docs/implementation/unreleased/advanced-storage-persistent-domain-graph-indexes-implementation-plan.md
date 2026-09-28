# Advanced storage phase 5: persistent domain graph indexes implementation plan

## Status

Reviewed and partially implemented on the `advanced_storage` branch. The first
implemented tranche writes checkpoint-aligned persistent label/tag/adjacency
index sets and loads matching sets on checkpoint open with fallback to in-memory
rebuild.

This phase builds on:

- phase 1 domain-scoped graph stores
- phase 2 domain graph checkpoints
- phase 3 manual graph checkpoint admin operations
- phase 4 automatic graph checkpoint policy and checkpoint status

Primary design doc:

- [Domain graph persistent indexes](../../design/graph/domain-graph-persistent-indexes.md)

## Goal

Persist selected derived graph indexes per domain so large domain stores can open
without rebuilding all in-memory indexes from live graph records every time.

The first implementation should persist only indexes that already exist or are
simple to derive safely:

1. label index
2. tag index
3. adjacency-out index
4. adjacency-in index

Property indexes and schema-declared query indexes are intentionally deferred.

## Scope and constraints

- The graph management unit remains the domain.
- `.kseg` segment format remains unchanged.
- Persistent indexes are derived local artifacts, not authoritative storage.
- Persistent indexes are tied to a graph checkpoint revision/checksum.
- Any replica may generate indexes locally after applying committed state.
- Invalid/stale/missing indexes must fall back to in-memory rebuild without graph
  correctness loss.
- Do not add per-commit persistent-index fsync work in this phase.

## Proposed layout

```text
graphs/<space_id>/domains/<domain_id>/
  indexes/
    <index_set_id>/
      manifest.json
      labels.kidx
      tags.kidx
      adjacency-out.kidx
      adjacency-in.kidx
    LATEST
```

The index set manifest references the loaded graph checkpoint baseline:

```json
{
  "format_version": 1,
  "space_id": "...",
  "domain_id": "...",
  "index_set_id": "idx-...",
  "created_at": "...",
  "graph_checkpoint_id": "chk-...",
  "graph_revision": 123,
  "graph_checksum": "...",
  "index_format": "domain-graph-index-v1-json",
  "checksum_algorithm": "domain-graph-index-v1-sha256",
  "indexes": {
    "labels": {"path": "labels.kidx", "entry_count": 1200, "checksum": "..."},
    "tags": {"path": "tags.kidx", "entry_count": 900, "checksum": "..."},
    "adjacency_out": {"path": "adjacency-out.kidx", "entry_count": 25000, "checksum": "..."},
    "adjacency_in": {"path": "adjacency-in.kidx", "entry_count": 25000, "checksum": "..."}
  }
}
```

## Phase 5.1 — storage format and in-memory export/import helpers

### Tasks

- Add internal storage types for index manifests and payload file metadata.
- Add deterministic export helpers for current in-memory indexes:
  - labels: `label -> []nodeID`
  - tags: `tag -> []nodeID`
  - adjacency out: `fromID -> label bucket -> []edgeID`
  - adjacency in: `toID -> label bucket -> []edgeID`
- Add import helpers that hydrate existing in-memory index structures from
  validated payloads.
- Add checksum helpers and deterministic sorting.
- Keep payload readers/writers package-private until format has stabilized.

### Tests

- deterministic output independent of insertion order
- round-trip export/import equivalence
- checksum changes when index contents change
- reject unsupported format versions
- reject malformed/unsafe paths in manifests

### Acceptance

- No graph open path behavior changes yet.
- Existing tests pass.

## Phase 5.2 — index set writer

### Tasks

- Add `LocalStore.WriteIndexSet(ctx, checkpointManifest)` or equivalent internal
  writer that writes index files for the current live state.
- Writer must publish atomically via temporary directory + manifest + `indexes/LATEST`.
- Writer must include graph checkpoint revision/checksum in manifest.
- Writer must remove older `idx-*` directories opportunistically after publishing
  the latest complete index set.
- Writer should require a matching graph checkpoint baseline or a status object
  returned by `WriteCheckpoint`/checkpoint status.

### Tests

- writer creates expected layout
- manifest references graph checkpoint revision/checksum
- older index sets are cleaned up
- partial failed writes are not published as latest
- deleted entities are absent from index payloads

### Acceptance

- Manual writer can produce index artifacts in unit tests.
- No automatic loading yet unless explicitly exercised by tests.

## Phase 5.3 — index set loader and open-path integration

### Tasks

- During domain store open, after loading a valid graph checkpoint, attempt to
  load latest matching index set.
- Validate index set before use:
  - format version
  - space/domain IDs
  - graph revision equals checkpoint revision
  - graph checksum equals checkpoint graph checksum
  - required files and checksums
- If validation succeeds, hydrate in-memory indexes from persistent index files.
- If validation fails, rebuild in-memory indexes from checkpoint payload and tail
  replay as today.
- Record local load status/fallback reason for admin/status surfacing.

### Tests

- open with matching index set uses persistent indexes
- open with stale revision falls back to rebuild
- open with checksum mismatch falls back to rebuild
- corrupt payload falls back to rebuild
- tail replay after checkpoint updates indexes correctly

### Acceptance

- Persistent index corruption cannot make valid graph data unavailable.
- Open behavior is backward-compatible with stores that have no `indexes/` directory.

## Phase 5.4 — checkpoint/index coordination

### Tasks

- Decide whether `WriteCheckpoint(ctx)` should optionally write index sets or
  whether a separate method is clearer, for example:

  ```go
  LocalStore.WriteCheckpoint(ctx)
  LocalStore.WriteIndexSetForLatestCheckpoint(ctx)
  ```

- Wire automatic checkpoint policy to optionally write index sets after successful
  checkpoint creation.
- Add config gate for automatic index writing if needed, for example:

  ```text
  MYCELD_GRAPH_INDEX_AUTO_ENABLED=false
  ```

  This can be deferred if the first phase keeps index writing manual/internal.

### Tests

- automatic checkpoint can create matching index set when enabled
- checkpoint success with index write failure records index error but does not
  invalidate checkpoint
- index writing is not on the foreground graph commit path

### Acceptance

- Operators can enable/disable automatic derived index writing independently if
  needed.

## Phase 5.5 — admin/status and CLI visibility

### Tasks

Expose local index status, either by extending graph checkpoint status or adding a
new admin method. Suggested status fields:

- index present
- index set ID
- index format version
- indexed graph revision
- indexed graph checksum
- load result: `used`, `rebuilt`, `fallback`, `missing`
- fallback reason
- last index write duration/error
- entry counts by kind

Possible CLI shape:

```bash
mycel cluster graph-index status --space-id <space> --domain-id <domain>
mycel cluster graph-index write --space-id <space> --domain-id <domain>
```

The write command should remain local-only and derived-artifact-only, mirroring
graph checkpoint behavior.

### Tests

- admin status maps storage status correctly
- CLI JSON shape is stable
- write command requires auth and returns local status

### Acceptance

- Operators can tell whether a domain is using persistent indexes and why it fell
  back if not.

## Phase 5.6 — benchmarks and validation

### Tasks

Add benchmarks or integration tests for large domains:

1. full segment replay only
2. checkpoint only
3. checkpoint + persistent indexes

Suggested initial benchmark dimensions:

- 10k nodes / 25k edges
- 100k nodes / 250k edges, if feasible outside normal `make test`
- label/tag-heavy graph
- hierarchy/reference-heavy graph

### Acceptance

- Benchmarks demonstrate materially faster open/index hydration for checkpoint +
  persistent indexes.
- Results are documented in the implementation note or follow-up report.

## Rollout strategy

1. Land storage format and writer behind tests.
2. Land loader fallback behavior without automatic writing.
3. Add manual/admin visibility.
4. Enable automatic index writing only after fallback behavior is validated.
5. Run restart/open performance tests and restart soaks.

## Failure policy

Initial policy should be forgiving:

```text
missing/stale/corrupt index -> rebuild in-memory indexes + warning/status
```

Do not fail domain open solely because a derived persistent index is invalid.

Potential later strict mode can fail closed for operators who prefer early
corruption detection over availability, but that is out of scope for phase 5.

## Open review questions

- Should index generation be part of graph checkpoint creation by default, or a
  separate explicit operation?
- Should automatic index writing have a separate config gate from automatic graph
  checkpointing?
- Should phase 5 include only label/tag indexes first, with adjacency in a second
  tranche, or include adjacency immediately because it is already central to graph
  traversal performance?
- Should index status live under `graph-checkpoint status` or a new
  `graph-index status` command?
- What open-time benchmark threshold should define success for large domains?

## Validation commands

After implementation begins, run at minimum:

```bash
go test ./internal/graph/storage ./internal/graph/service ./internal/daemon/api/admin ./internal/cli/cmd -count=1
make docs-check
git diff --check
make test
```

Manual/destructive validation should include restart/open tests against large
fixture domains before merging phase 5 into a release-bound branch.
