# Write-Ahead Log Design

## Status

Proposed.

This document defines the initial write-ahead log (WAL) architecture for Mycel. The WAL is intended to become the authoritative mutation stream for crash recovery first, and for leader/replica distribution later.

## Goals

- Ensure no durable mutation is applied unless it is represented by a committed WAL record.
- Make committed mutations replayable in deterministic LSN order.
- Support crash recovery by replaying records after the last applied LSN.
- Provide a clean future replication stream for read replicas.
- Support snapshots/checkpoints so nodes do not need to replay history from genesis forever.
- Return a commit LSN from write operations so clients and future replicas can reason about read freshness.

## Non-goals

- Multi-leader writes.
- Automatic leader election or Raft in the first WAL implementation.
- Cross-node consensus durability in the first WAL implementation.
- Physical page-level logging tied to one storage engine layout.
- Replaying external side effects such as webhooks, emails, or provider calls directly from WAL recovery.

## Core invariant

The central invariant is:

> No durable mutation happens unless it is represented by a committed WAL record, and the only durable-state update path is applying committed WAL records in LSN order.

Mutation handlers must not update durable stores directly. They must validate commands, build deterministic WAL records, append and commit those records, then apply them through the WAL applier.

## High-level architecture

```text
Write request
    |
    v
Command handler
    | validates request and current-state preconditions
    | builds deterministic logical WAL record
    v
WAL manager
    | assigns LSN
    | appends record frame
    | fsyncs according to durability policy
    v
WAL applier
    | applies records in LSN order
    | updates durable state and applied_lsn atomically where possible
    v
Storage/indexes/caches
```

Startup recovery:

```text
1. Open storage and read applied_lsn.
2. Open WAL and validate frames/checksums.
3. Replay records where record.lsn > applied_lsn.
4. Finish startup only after replay succeeds or corruption is reported.
```

## Write path

Initial implementation should use a synchronous local apply path:

```text
1. Receive mutation command.
2. Validate request shape, authorization, and state-dependent preconditions.
3. Resolve all non-deterministic values before logging:
   - generated IDs
   - timestamps
   - defaults
   - caller identity
   - expected versions
4. Build a logical WAL record containing all data needed for replay.
5. Assign the next LSN.
6. Append the record to the WAL.
7. Sync the WAL according to the configured durability policy.
8. Apply the record locally through the WAL applier.
9. Persist applied_lsn with the state update where possible.
10. Return success with commit_lsn.
```

Returning after WAL commit but before local apply is intentionally deferred. It can improve latency later, but it complicates read-your-writes behavior and turns apply errors into post-commit failures.

## WAL record model

Use a logical WAL, not a physical page WAL. Records should describe domain mutations such as creating spaces, updating ACLs, writing graph changes, storing blob metadata, or updating semantic maintenance state.

Every record should include:

- `lsn`: monotonically increasing log sequence number.
- `record_type`: stable operation identifier.
- `schema_version`: payload version for compatibility.
- `timestamp`: resolved at command time, never during replay.
- `actor` or service identity when relevant for audit/debugging.
- `payload`: deterministic operation data.
- `checksum`: frame integrity check.

Future distributed fields should be reserved or easy to add:

- `cluster_id`
- `node_id`
- `leader_epoch` / `term`
- `prev_lsn`
- `idempotency_key`

Example logical record shape:

```text
WalRecord {
  lsn: 42,
  schema_version: 1,
  record_type: GraphCommit,
  timestamp: 2026-07-13T12:00:00Z,
  actor: user:abc,
  payload: GraphCommitPayload { ... },
  checksum: ...,
}
```

## Determinism requirements

Replay must produce the same durable state every time. WAL records must therefore contain the resolved result of any non-deterministic decision.

Do not do this during replay:

- call `now()` to set record timestamps
- generate random IDs
- re-evaluate defaults from current config
- call external providers
- perform network side effects

Instead, resolve those values before WAL append and include them in the record payload.

## Validation model

Validation is split into two layers.

Before appending:

- authenticate and authorize the caller
- validate request shape
- check state-dependent preconditions such as uniqueness or expected version
- resolve non-deterministic values

During replay:

- trust committed WAL records as accepted commands
- apply deterministically
- treat malformed or impossible committed records as WAL corruption or a software bug

The applier should not reject valid historical records merely because current code would reject a similar new command.

## Applied LSN and idempotency

Each durable store must know the highest WAL LSN that has been fully applied to it, or the daemon must maintain equivalent per-store progress.

Preferred behavior:

```text
storage transaction:
  apply mutation
  update applied_lsn
commit
```

If a store cannot atomically update state and `applied_lsn`, applier operations must be idempotent or must include operation IDs that make repeated application safe.

On replay:

```text
if record.lsn <= applied_lsn:
    skip
else:
    apply(record)
```

## WAL file format

The first implementation can use an append-only segmented binary format.

Recommended frame:

```text
[magic/version]
[frame_length]
[lsn]
[record_type]
[payload_encoding]
[payload_bytes]
[crc32c]
```

Recommended segment naming:

```text
wal/
  0000000000000001.wal
  0000000000100000.wal
  0000000000200000.wal
```

A segment filename identifies the first LSN in that segment. Segment rotation should be size-based initially.

Startup should tolerate a torn final frame by truncating only if the frame was never committed. A checksum or length failure in the middle of a segment is corruption and should stop startup.

## Durability policy

Start with a conservative default:

```text
append record -> fsync WAL -> apply locally -> return success
```

Expose durability policy later if needed:

- `always`: fsync every committed write.
- `batch`: group fsyncs for throughput, with bounded loss window.
- `none/dev`: no fsync, only for tests or local development.

For production and future replication, `always` should be the default until stronger operational experience exists.

## Snapshots and checkpoints

WAL should not grow forever. A checkpoint records that durable state is complete through a specific LSN.

Snapshot/checkpoint flow:

```text
1. Quiesce or otherwise coordinate writers as needed.
2. Ensure all records through checkpoint_lsn are applied.
3. Persist snapshot/checkpoint metadata.
4. Retain WAL from checkpoint_lsn + 1 onward.
5. Delete older WAL only after backup/retention policy allows it.
```

New replicas will eventually bootstrap by restoring a snapshot, then replaying WAL after the snapshot LSN.

This design should integrate with the existing quiesce/backup design rather than inventing a separate global pause mechanism.

## Replication readiness

Although the first implementation is single-node recovery, the WAL API should be shaped so replication can use it later.

Future leader/replica model:

```text
leader WAL commit -> stream records by LSN -> replica WAL append -> replica applier
```

Useful APIs to design early:

- `Append(record) -> lsn`
- `Sync(lsn)`
- `ReadFrom(lsn) -> iterator`
- `LastCommittedLSN()`
- `AppliedLSN()`
- `WaitUntilApplied(lsn)`

Client write responses should include `commit_lsn`. Future read requests can optionally require `min_lsn` for read-your-writes semantics.

## External side effects

WAL replay must not directly trigger non-idempotent external effects. If a committed mutation needs a side effect, use an outbox-style pattern:

```text
WAL record -> durable state update + durable outbox item -> async dispatcher
```

The dispatcher tracks delivery separately and can retry safely.

## Suggested package structure

Potential daemon-internal packages:

```text
internal/wal
  manager.go        // append, sync, segment lifecycle
  record.go         // envelope, LSN, record type registry
  codec.go          // frame encode/decode
  reader.go         // read/iterate by LSN
  recovery.go       // startup validation/truncation/replay helpers
  applier.go        // applier interfaces
```

Domain packages should own their record payload definitions and apply logic, while `internal/wal` owns ordering, persistence, framing, and recovery mechanics.

## Implementation phases

### Phase 1: WAL foundation

- Add LSN type and WAL manager.
- Add segmented append-only WAL storage.
- Add frame checksums and startup validation.
- Add append, sync, read-from, and last-LSN APIs.
- Add tests for torn final frames, checksum failures, segment rotation, and replay ordering.

### Phase 2: Applier framework

- Add applier registry keyed by record type.
- Add recovery loop from `applied_lsn + 1`.
- Add durable applied-LSN tracking.
- Add `WaitUntilApplied(lsn)` for future read freshness.

### Phase 3: Convert one bounded context

- Pick a narrow mutation path.
- Convert handler to command -> WAL record -> applier -> state.
- Ensure no direct durable mutation remains in that path.
- Return `commit_lsn` internally, even if public APIs do not expose it yet.

### Phase 4: Convert all durable mutation paths

- Graph, blob metadata, spaces/domains/ACLs, identity/session, templates, semantic state, and daemon metadata.
- Add tests proving recovery after crash between WAL commit and local apply.

### Phase 5: Checkpoints and retention

- Add checkpoint metadata.
- Integrate with backup/quiesce.
- Add safe WAL retention policy.

### Phase 6: Replication protocol

- Stream WAL records from leader by LSN.
- Have replicas append to their local WAL and apply in order.
- Add replica lag metrics and `min_lsn` read routing behavior.

## Testing requirements

Minimum tests:

- LSNs are monotonic and never reused.
- Appended records can be read back exactly.
- Replay applies records in strict LSN order.
- Recovery skips records at or below `applied_lsn`.
- Crash after WAL fsync but before apply is recovered by replay.
- Crash after apply but before applied-LSN update is safe or detected.
- Torn final frame is handled according to commit rules.
- Mid-log corruption stops startup.
- Record codecs remain backward compatible across schema versions.

## Open questions

- Which bounded context should be converted first?
- Should `applied_lsn` be global or per bounded context/store?
- What is the first public API surface that should expose `commit_lsn`?
- Should the first frame encoding use protobuf, msgpack, or a custom binary envelope with protobuf payloads?
- What snapshot metadata format should be shared with backup/restore?
