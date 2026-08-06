# Write-Ahead Log Implementation Plan

## Status

Proposed.

This plan implements [Write-Ahead Log Design](../../design/persistence/write-ahead-log.md). The first milestone is single-node crash recovery and a durable mutation pipeline. The design should be kept replication-ready, but leader election, read replicas, and quorum commit are intentionally out of scope for this plan.

## Acceptance criteria

- `myceld` has a daemon-owned WAL under the data directory.
- WAL records have monotonic LSNs, checksums, schema versions, and stable record types.
- Startup validates WAL segments and replays committed records after the last applied LSN.
- At least one bounded context is converted to the command -> WAL record -> applier -> durable state path.
- Converted mutation paths do not directly mutate durable state outside the WAL applier.
- Crash after WAL fsync but before local apply is recovered by replay.
- Reapplying records at or below `applied_lsn` is skipped or idempotent.
- Write responses internally carry `commit_lsn`; public API exposure may be deferred.
- Tests cover frame encoding, segment rotation, torn final frames, corruption, recovery, applier ordering, and the first converted bounded context.
- Documentation is updated with actual paths, config keys, and any design deviations.

## Phase 0: Inventory and cut-line decisions

Status: completed initial inventory; the old phase inventory document was removed after the WAL migration completed.

### Goals

Identify all durable mutation paths and choose the first bounded context to convert.

### Tasks

1. Inventory writes in these areas:
   - `internal/graph/storage`
   - `internal/blob/storage`
   - `internal/space/storage`
   - `internal/identity/storage`
   - `internal/semantic/storage`
   - `internal/semantic/maintenance`
   - `internal/embedding/store`
   - daemon metadata under `internal/daemon`
2. For each path, document:
   - mutation entrypoint
   - durable files touched
   - current locking/transaction behavior
   - whether state and `applied_lsn` can be updated atomically
   - candidate WAL record types
3. Choose the first conversion target. Prefer a narrow file-backed store with simple CRUD semantics, such as a space/domain/ACL metadata store, before graph segment commits. Decision: use space metadata, starting with `CreateSpace` as an aggregate record touching spaces, domains, and ACL metadata.
4. Decide whether the initial applied-LSN model is:
   - global daemon applied LSN, or
   - per bounded-context/store applied LSN.
   Decision: start with daemon-level `meta/wal/progress.json`, with idempotent appliers for multi-file records.
5. Decide frame payload encoding. Recommended initial choice: binary frame envelope with protobuf or deterministic JSON payloads. If protobuf payloads are used, define internal-only messages first unless public APIs need them. Decision: binary frame envelope with deterministic JSON payloads for Phase 1-4.
6. Decide exact WAL directory layout under `MYCELD_DATA_DIR`. Decision: segments under `<data_dir>/wal/`, progress/checkpoint metadata under `<data_dir>/meta/wal/`.

### Deliverables

- Update this plan or add a short inventory appendix with the first conversion target.
- Confirm selected encoding and applied-LSN strategy.

### Acceptance

No runtime behavior yet. The implementation scope and first conversion target are explicit.

## Phase 1: WAL core package

### Goals

Implement append-only segmented WAL storage independent of domain records.

### Files

```text
internal/wal/lsn.go
internal/wal/record.go
internal/wal/codec.go
internal/wal/segment.go
internal/wal/manager.go
internal/wal/reader.go
internal/wal/errors.go
internal/wal/*_test.go
```

### Tasks

1. Add `type LSN uint64` with helpers for zero/invalid values and formatting.
2. Add record envelope types:
   - `LSN`
   - `RecordType`
   - `SchemaVersion`
   - `Timestamp`
   - `Payload`
   - `Checksum`
3. Implement frame encode/decode with:
   - magic/version
   - frame length
   - LSN
   - record type
   - schema version
   - payload encoding
   - payload bytes
   - CRC32C
4. Implement append-only segment writer.
5. Implement segment reader and iterator from a requested LSN.
6. Implement size-based segment rotation.
7. Implement WAL manager APIs:

```go
Append(ctx context.Context, rec PendingRecord) (wal.LSN, error)
Sync(ctx context.Context, lsn wal.LSN) error
ReadFrom(ctx context.Context, lsn wal.LSN) (Iterator, error)
LastCommittedLSN() wal.LSN
Close() error
```

8. Implement startup scan:
   - discover segments
   - validate ordering
   - validate checksums
   - find last committed LSN
   - handle torn final frame according to commit rules
   - fail on mid-log corruption
9. Add initial config defaults:

```text
MYCELD_WAL_ENABLED=true
MYCELD_WAL_DIR=<data_dir>/wal
MYCELD_WAL_SEGMENT_BYTES=67108864
MYCELD_WAL_SYNC_POLICY=always
```

### Unit tests

- LSNs are monotonic and never reused after reopen.
- Records round-trip exactly.
- CRC mismatch is detected.
- Segment rotation preserves read order.
- `ReadFrom` starts at the requested LSN.
- Torn final frame is handled safely.
- Corruption before the final frame stops startup.
- `LastCommittedLSN` is restored after reopen.

### Acceptance

```sh
go test ./internal/wal
```

## Phase 2: Record registry and applier framework

### Goals

Create the daemon-level mechanism that maps committed WAL records to deterministic state transitions.

### Files

```text
internal/wal/applier.go
internal/wal/registry.go
internal/wal/recovery.go
internal/wal/progress.go
internal/daemon/modules/wal.go        # if module wiring follows existing daemon module pattern
```

### Tasks

1. Define applier interfaces:

```go
type Applier interface {
    ApplyWAL(ctx context.Context, rec wal.Record) error
}

type AppliedLSNStore interface {
    AppliedLSN(ctx context.Context) (wal.LSN, error)
    SetAppliedLSN(ctx context.Context, lsn wal.LSN) error
}
```

2. Add record type registry so domain packages can register codecs and appliers without import cycles.
3. Add recovery runner:

```text
read applied_lsn
iterate WAL from applied_lsn + 1
for each record:
  dispatch to applier
  persist applied_lsn
```

4. Add `WaitUntilApplied(lsn)` support for future read freshness.
5. Add metrics/logging hooks:
   - last committed LSN
   - last applied LSN
   - replay records/sec
   - recovery duration
   - replay failure record type/LSN
6. Define behavior for unknown record type:
   - fail startup for committed records by default
   - allow explicit compatibility exceptions only if documented

### Unit tests

- Registered applier receives records in LSN order.
- Unknown record type fails recovery.
- Recovery starts at `applied_lsn + 1`.
- Records at or below `applied_lsn` are skipped.
- `WaitUntilApplied` unblocks when target LSN is applied.
- Replay stops and reports the failing LSN on applier error.

### Acceptance

```sh
go test ./internal/wal
```

## Phase 3: Daemon integration and startup recovery

### Goals

Make WAL lifecycle daemon-owned and ensure recovery runs before services accept mutating traffic.

### Candidate files

```text
internal/daemon/app/app.go
internal/daemon/runtime/runtime.go
internal/daemon/modules/*
internal/daemon/config/*
```

### Tasks

1. Add WAL config to daemon config loading.
2. Initialize the WAL manager after the data directory is known and before bounded-context services start accepting requests.
3. Register appliers before recovery runs.
4. Run WAL recovery during daemon startup.
5. Block API ingress until recovery completes successfully.
6. On recovery failure, fail daemon startup with an actionable error.
7. Ensure daemon shutdown closes the WAL manager after writers are stopped.
8. Add structured logs for:
   - WAL directory
   - last committed LSN
   - starting applied LSN
   - ending applied LSN
   - recovery duration
9. Integrate with quiesce/backup status where useful, but do not require backup changes in this phase.

### Tests

- Daemon starts with empty WAL.
- Daemon starts with existing WAL and no pending records.
- Daemon replays pending records before accepting requests.
- Daemon startup fails on WAL corruption.
- Shutdown closes the WAL cleanly.

### Acceptance

```sh
go test ./internal/daemon/...
go test ./internal/wal
```

## Phase 4: Convert first bounded context

### Goals

Prove the WAL-first mutation path with a small real store.

### Recommended target

Pick one of the file-backed metadata stores after Phase 0 inventory, for example:

```text
internal/space/storage/spaces
internal/space/storage/domains
internal/space/storage/acl
```

### Tasks

1. Define domain WAL record payloads for the selected context.
2. Add deterministic codecs and record type constants.
3. Refactor mutation command handlers so they:
   - validate request and state preconditions
   - resolve timestamps/IDs/defaults
   - append and sync a WAL record
   - apply through the applier
   - return `commit_lsn` internally
4. Move durable write logic into the applier or a function called only by the applier.
5. Add applied-LSN persistence for the selected store.
6. Ensure direct store mutation APIs are either:
   - made private to the applier path, or
   - clearly marked test/recovery-only and not used by request handlers.
7. Add crash-recovery tests using a WAL with committed-but-unapplied records.
8. Add idempotency tests for replay after partial apply when applicable.

### Tests

- Existing store tests continue to pass.
- New mutation writes a WAL record before state changes.
- Recovery applies committed records after restart.
- Replaying already-applied records is safe/skipped.
- Invalid command is rejected before WAL append.
- Valid committed record is not revalidated against current request-time policy during replay.

### Acceptance

```sh
go test ./internal/space/...
go test ./internal/wal
go test ./internal/daemon/...
```

Adjust package paths if Phase 0 chooses a different target.

## Phase 5: Commit LSN propagation

### Goals

Make LSN visible inside the daemon and prepare public/API-level exposure later.

### Tasks

1. Add internal write result structs that include `CommitLSN`.
2. Propagate `CommitLSN` through service boundaries for the converted context.
3. Add request-context or response metadata helpers for future gRPC exposure.
4. Add `min_lsn`/`WaitUntilApplied` plumbing internally without changing public API semantics yet.
5. Document where public proto/API changes would be made later.

### Tests

- Converted write path returns non-zero `CommitLSN` internally.
- `CommitLSN` equals the WAL record LSN.
- `WaitUntilApplied(commit_lsn)` succeeds after write response.

### Acceptance

Package tests for the converted service and `internal/wal` pass.

## Phase 6: Convert remaining mutation paths

### Goals

Move all durable daemon mutation paths behind WAL-first application.

### Order recommendation

1. Space/domain/ACL metadata, if not already done.
2. Identity users and sessions.
3. Blob metadata, not necessarily blob object bytes.
4. Graph commits and change stream records.
5. Template catalogs.
6. Semantic maintenance/work state.
7. Embedding/vector metadata.
8. Daemon metadata.

### Tasks per context

For each context:

1. Define record types and payload versions.
2. Ensure records contain deterministic timestamps, IDs, defaults, and expected versions.
3. Register codec and applier.
4. Refactor request handlers to append/sync/apply.
5. Add or integrate applied-LSN storage.
6. Remove or restrict direct durable mutation paths.
7. Add recovery and idempotency tests.
8. Update inventory status.

### Tests

Run targeted tests for each converted context plus:

```sh
go test ./internal/...
```

### Acceptance

All durable daemon mutations are represented by committed WAL records, or documented as explicit exceptions with rationale.

## Phase 7: Checkpoints, snapshots, and retention

### Goals

Prevent unbounded WAL growth and align WAL retention with backup/quiesce behavior.

### Files

```text
internal/wal/checkpoint.go
internal/wal/retention.go
internal/wal/checkpoint_test.go
```

### Tasks

1. Define checkpoint metadata format, for example:

```text
meta/wal/checkpoint.json
```

2. Record:
   - checkpoint LSN
   - timestamp
   - WAL segment boundary
   - snapshot/backup identifier if applicable
3. Ensure checkpoint only advances after all relevant stores have applied through that LSN.
4. Integrate with quiesce/backup so backups include a consistent checkpoint.
5. Implement retention that deletes WAL older than the oldest required checkpoint/backup boundary.
6. Add safety checks to prevent deleting WAL needed for recovery.

### Tests

- Checkpoint does not advance past applied LSN.
- Recovery from checkpoint plus remaining WAL works.
- Retention keeps needed WAL and deletes only safe segments.
- Backup snapshot includes checkpoint metadata.

### Acceptance

```sh
go test ./internal/wal ./internal/backup/... ./internal/daemon/quiesce/...
```

## Phase 8: Replication-ready APIs, no replicas yet

### Goals

Expose stable internal APIs needed by future read replicas while keeping runtime single-node.

### Tasks

1. Harden `ReadFrom(lsn)` for long-running streaming use.
2. Add optional blocking wait for new committed records after the iterator reaches EOF.
3. Add lightweight WAL status API inside daemon:
   - committed LSN
   - applied LSN
   - oldest retained LSN
   - current segment
4. Add tests for concurrent append/read.
5. Document how a future replica will:
   - request records from LSN
   - append to local WAL
   - apply in order
   - report lag

### Acceptance

No public distributed behavior yet, but future replication work should not need to redesign WAL storage.

## Phase 9: Documentation and operational guide

Status: completed. See the write-ahead log design and operational notes in `docs/design/write-ahead-log.md`.

### Goals

Document the actual implementation for operators and future developers.

### Tasks

1. Update [Write-Ahead Log Design](../../design/persistence/write-ahead-log.md) if implementation differs.
2. Add operational documentation covering:
   - WAL directory layout
   - durability policy
   - disk-full behavior
   - corruption behavior
   - backup/checkpoint interaction
   - safe restore expectations
3. Add developer documentation covering:
   - how to add a new WAL record type
   - deterministic payload rules
   - applier requirements
   - testing checklist
4. Add release notes for any migration or data directory changes.

### Acceptance

Docs describe the implemented behavior, not just the intended design.

## Migration and compatibility notes

- Existing data directories without WAL should start with an initial checkpoint or baseline applied LSN so old state is not replayed from nonexistent history.
- The first WAL-enabled release should write a metadata marker indicating WAL is active for that data directory.
- Downgrade behavior must be explicit. Once WAL-backed mutations occur, older daemons may not understand the data directory state.
- Record schema versions must be backward readable by newer daemons.
- Removing or changing a record type requires a migration plan.

## Risks and mitigations

- **Partial apply duplicate effects**: persist state and `applied_lsn` atomically where possible; otherwise make records idempotent.
- **Hidden direct writes**: add inventory, code review checklist, and tests that assert WAL append occurs for converted handlers.
- **Replay rejects historical records**: keep replay validation separate from command validation.
- **External side effects during replay**: use outbox pattern and never call providers directly from appliers.
- **Disk full during append/sync**: fail writes before state mutation; surface clear transient/persistent errors.
- **Corruption handling ambiguity**: define torn-final-frame versus mid-log-corruption behavior in tests.

## Suggested PR breakdown

1. WAL core types, codec, and segment tests.
2. WAL manager append/read/sync/reopen.
3. Applier registry and recovery framework.
4. Daemon config/lifecycle integration.
5. First bounded-context conversion.
6. Commit LSN propagation helpers.
7. Additional bounded-context conversions, one PR per context.
8. Checkpoint and retention.
9. Replication-ready iterator/status APIs.
10. Documentation and migration notes.
