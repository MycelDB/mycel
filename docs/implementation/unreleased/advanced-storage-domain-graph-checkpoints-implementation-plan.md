# Advanced storage phase 2: domain graph checkpoints implementation plan

## Status

Planned for the `advanced_storage` branch.

This plan implements domain graph checkpoints as described in
[`docs/design/graph/domain-graph-checkpoints.md`](../../design/graph/domain-graph-checkpoints.md).

The implementation targets fresh advanced-storage deployments only. No migration
from pre-advanced-storage graph layouts is required.

## Goal

Add durable per-domain graph checkpoints so opening a domain graph store can use a
compact latest-state baseline instead of replaying every historical `.kseg`
record forever.

Phase 2 must keep the system runnable and preserve the existing `.kseg` segment
format.

## Non-goals

Do not implement these in this phase:

- persistent label/tag/property/adjacency indexes
- segment compaction/deletion
- bounded graph cache eviction
- Raft repartitioning by domain
- cross-domain query or transaction support
- production migration from the old space-level graph layout

## Target layout

For one domain graph store:

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

Implementation may use temporary directories/files during checkpoint creation:

```text
checkpoints/.tmp-<id>/
```

and atomically publish them as:

```text
checkpoints/latest/
```

## Checkpoint data model

Add checkpoint model types in `internal/graph/storage`, likely a new file:

```text
internal/graph/storage/checkpoint.go
```

Suggested types:

```go
type CheckpointManifest struct {
    FormatVersion         int                    `json:"format_version"`
    SpaceID               string                 `json:"space_id,omitempty"`
    DomainID              string                 `json:"domain_id,omitempty"`
    GraphRevision         uint64                 `json:"graph_revision"`
    CreatedAt             time.Time              `json:"created_at"`
    NodeCount             int                    `json:"node_count"`
    EdgeCount             int                    `json:"edge_count"`
    AppliedSegmentOffsets CheckpointSegmentState `json:"applied_segment_offsets"`
    ChecksumAlgorithm     string                 `json:"checksum_algorithm"`
    NodeChecksum          string                 `json:"node_checksum"`
    EdgeChecksum          string                 `json:"edge_checksum"`
    GraphChecksum         string                 `json:"graph_checksum"`
}

type CheckpointSegmentState struct {
    Txns  []CheckpointSegmentOffset `json:"txns"`
    Nodes []CheckpointSegmentOffset `json:"nodes"`
    Edges []CheckpointSegmentOffset `json:"edges"`
}

type CheckpointSegmentOffset struct {
    Segment string `json:"segment"`
    Offset  int64  `json:"offset"`
}
```

The manifest records what log positions are included in the compact payload, but
it does not contain live graph records itself.

Payload files contain compact latest-state records:

```text
nodes.kchk
edges.kchk
```

Use deterministic encoding where practical. JSON Lines is acceptable for the
first implementation if the code validates checksums and can evolve the format,
but a length-prefixed binary/protobuf-like format may be better long term.

Minimum requirement:

- payload files can be streamed
- corruption is detectable
- format has a version boundary

## Checkpoint checksums

Use or mirror the deterministic graph checksum behavior from graph consistency
code where possible:

```text
graph-v1-sha256
```

The checkpoint should store:

- node checksum
- edge checksum
- combined graph checksum
- counts

On load, verify:

- manifest format version is supported
- manifest domain metadata matches the domain store if included
- payload counts match manifest counts
- checksums match payload contents

Initial policy:

```text
invalid checkpoint -> ignore checkpoint, full replay, log warning
```

Do not silently use a partially valid checkpoint.

## Store API additions

Add methods to `graphstorage.LocalStore`:

```go
func (s *LocalStore) WriteCheckpoint(ctx context.Context) error
func (s *LocalStore) LoadCheckpoint(ctx context.Context) (bool, error)
```

or keep load private:

```go
func (s *LocalStore) loadCheckpoint(ctx context.Context) (bool, error)
```

`WriteCheckpoint` should be exported because service/admin/test code may need to
trigger it explicitly.

Add optional checkpoint metadata inspection later if useful:

```go
type CheckpointInfo struct { ... }
func (s *LocalStore) CheckpointInfo(ctx context.Context) (*CheckpointInfo, error)
```

This can be deferred if not needed for Phase 2 tests.

## Open path changes

Current `LocalStore.open` path:

```text
load manifest
open active segments
rebuildIndexes(ctx)
```

Target path:

```text
load manifest
open active segments
try load valid checkpoint
if checkpoint loaded:
    initialize live node/edge state from checkpoint payload
    rebuild in-memory indexes from checkpoint live state
    replay committed tail after checkpoint offsets
else:
    full rebuildIndexes(ctx)
```

For the first implementation, it is acceptable to split existing rebuild logic
into reusable pieces before adding tail replay.

Suggested refactor:

```go
func (s *LocalStore) rebuildIndexes(ctx context.Context) error
func (s *LocalStore) rebuildFromSegments(ctx context.Context, from *CheckpointSegmentState) error
func (s *LocalStore) applyScannedNodeRecord(...)
func (s *LocalStore) applyScannedEdgeRecord(...)
func (s *LocalStore) collectCommittedTransactions(...)
```

However, be careful: current rebuild first scans txn segments to know committed
transaction IDs. Tail replay must only apply node/edge records whose transaction
commit is present after combining checkpoint state and tail transaction records.

Initial implementation may choose a simpler safe approach:

1. Load checkpoint live state.
2. Scan transaction segments from checkpoint txn offsets to collect committed tail txns.
3. Scan node/edge segments from checkpoint offsets.
4. Apply only records whose txn ID appears in committed tail txns.

This requires recording enough segment offsets to distinguish checkpointed and
post-checkpoint records.

## Capturing segment offsets

The checkpoint must record safe offsets for txn/node/edge segments.

For active segment files, use current file size at the time the checkpoint is
created while the store lock prevents concurrent appends.

For older immutable segments, record their full size.

Because current segment rotation is minimal, Phase 2 can support the current
single active segment pattern but should model offsets generally so future
rotation/compaction can reuse it.

Add helper APIs in `segment.go` if needed:

```go
func (s *segment) size() (int64, error)
```

and/or helpers to scan from an offset:

```go
func scanSegmentFrom(path string, kind SegmentKind, enc *encryption.Service, offset int64, visit func(scannedRecord) error) error
```

`scanSegmentFrom` must validate the segment header if offset is after the header.

## Writing checkpoints

`LocalStore.WriteCheckpoint(ctx)` should:

1. Check store state is ready.
2. Acquire store lock.
3. Snapshot current revision, live nodes, live edges, manifest segment lists, and segment offsets.
4. Compute counts/checksums.
5. Write payload files into a temp checkpoint directory.
6. Write manifest into the temp directory.
7. fsync files/directories where feasible.
8. Atomically publish the temp checkpoint as `checkpoints/latest`.
9. Remove stale temp checkpoint directories best-effort.

The store lock may be held throughout the initial implementation for simplicity
and correctness. Later optimization can reduce lock duration by copying live
state under lock and writing files after unlock.

## Atomic publish strategy

Preferred strategy:

```text
checkpoints/.tmp-<uuid>/...
rename checkpoints/latest -> checkpoints/previous-<uuid> or remove
rename checkpoints/.tmp-<uuid> -> checkpoints/latest
remove old latest/previous best-effort
```

Because replacing a non-empty directory atomically can be platform-dependent,
implementation may instead use versioned checkpoint directories plus an atomic
pointer file:

```text
checkpoints/<checkpoint_id>/manifest.json
checkpoints/LATEST
```

where `LATEST` contains the checkpoint directory name and is replaced by atomic
file rename.

Recommended for portability:

```text
checkpoints/<checkpoint_id>/...
checkpoints/LATEST.tmp
rename LATEST.tmp -> LATEST
```

The loader reads `checkpoints/LATEST`, validates the referenced directory, then
loads that checkpoint. Old checkpoint directories can be cleaned up best-effort.

## Initial trigger policy

Add explicit trigger only:

```go
store.WriteCheckpoint(ctx)
```

Do not enable automatic checkpointing yet unless tests require it.

Follow-up issues can add:

- checkpoint every N revisions
- checkpoint every N segment bytes
- checkpoint on clean shutdown
- checkpoint after Raft snapshot restore
- background maintenance worker
- admin/CLI trigger and metrics

## Service integration

Phase 2 does not need public API changes.

Graph service integration should be minimal:

- local store open automatically tries checkpoint load
- tests may trigger checkpoint directly through service-private access or storage package tests
- raft snapshot restore may optionally call `WriteCheckpoint`, but this is not required for Phase 2

Do not make checkpoint creation leader-only. Each pod/replica may checkpoint its
local domain store after applying committed graph state.

## Tests

### Storage unit tests

Add tests under `internal/graph/storage`:

1. `TestLocalStoreWriteCheckpointAndOpen`
   - create store
   - commit nodes/edges
   - write checkpoint
   - close/reopen
   - verify records and revision

2. `TestLocalStoreCheckpointReplaysTail`
   - commit revision 1
   - write checkpoint
   - commit revision 2
   - close/reopen
   - verify both checkpointed and tail records exist
   - verify revision 2

3. `TestLocalStoreMissingCheckpointFallsBackToFullReplay`
   - current behavior remains valid

4. `TestLocalStoreCorruptCheckpointFallsBackToFullReplay`
   - corrupt manifest or payload
   - reopen succeeds via full replay
   - records intact

5. `TestLocalStoreCheckpointRejectsDomainMismatch` if manifest includes domain
   metadata and store options carry expected domain identity.

6. `TestCheckpointDoesNotIncludeDeletedEntities`
   - create node
   - delete node
   - write checkpoint
   - inspect/reopen and ensure deleted node absent

### Graph service tests

Add or update tests under `internal/graph/service`:

1. domain A checkpoint does not affect domain B
2. domain store path remains `graphs/<space>/domains/<domain>`
3. consistency stats after checkpoint/reopen match pre-checkpoint stats

### Raft/daemon tests

Run existing raft graph tests to confirm no regression:

```bash
go test ./internal/graph/service ./internal/daemon/app ./internal/clustering/... -count=1
```

## Validation commands

At minimum:

```bash
go test ./internal/graph/storage ./internal/graph/service -count=1
go test ./internal/daemon/api/client ./internal/daemon/app ./internal/clustering/... -count=1
make docs-check
git diff --check
make test
```

Before merging advanced storage back to `develop`, also rerun destructive soak
validation from fresh data.

## Risks and decisions

### Risk: incorrect tail replay

If checkpoint offsets are wrong, opening could miss committed records or replay
records twice.

Mitigation:

- lock store while capturing checkpoint offsets and live state
- add tail replay tests
- verify revision and checksums

### Risk: incomplete checkpoint publish

A crash during checkpoint writing could leave partial files.

Mitigation:

- write into versioned checkpoint directory
- publish with atomic `LATEST` pointer file rename
- loader only trusts complete manifest + checksum-valid payload

### Risk: over-coupling checkpoints to indexes

Checkpoint should remain graph-state focused.

Mitigation:

- keep index metadata out of graph checkpoint manifest for Phase 2
- future index manifests reference graph revision separately

### Decision: fallback vs fail-closed

Initial policy should be:

```text
invalid checkpoint -> full replay + warning
```

Future hardening can add strict mode.

## Suggested implementation sequence

1. Add checkpoint model/types and payload encode/decode helpers.
2. Add segment size and scan-from-offset helpers.
3. Refactor full replay code into reusable committed-record apply helpers.
4. Implement `WriteCheckpoint` with versioned directory + `LATEST` pointer.
5. Implement checkpoint load on open, without tail replay at first if tests are scoped accordingly.
6. Add tail replay using applied segment offsets.
7. Add corruption/fallback tests.
8. Add service-level domain isolation tests around checkpoints.
9. Run full validation.
