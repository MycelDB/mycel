# WAL Snapshot Catch-up and Retention Safety Design

## Status

Design proposal.

This document describes the next correctness layer after the WAL propagation MVP: detecting when a follower can no longer catch up from retained WAL alone, surfacing that state clearly, and preparing for snapshot/checkpoint transfer.

The first implementation should focus on **gap detection and explicit snapshot-required status**, not full snapshot transfer.

## Background

The current WAL propagation MVP supports:

- static primary authority
- primary-only client/operator writes
- primary `StreamWal` backend RPC
- follower receive log
- follower replication progress
- follower WAL apply through existing WAL appliers
- role-aware replication/WAL status in CLI and `mycel-admin`

This works when the follower asks for WAL records that the primary still retains.

However, followers may fall behind:

- a follower joins after many writes
- a follower is offline for a long time
- primary checkpoint/retention removes old WAL segments
- follower progress is reset or lost
- receive log is truncated or corrupt

In those cases, the follower may request an LSN older than the primary's retained WAL range. The system must not fail ambiguously or pretend replication is healthy.

## Goals

- Detect when follower WAL catch-up cannot proceed from retained primary WAL.
- Return a stable, machine-detectable `snapshot_required` error from `StreamWal`.
- Persist follower status explaining that snapshot catch-up/resync is required.
- Expose WAL retained range/checkpoint state through status.
- Update CLI and `mycel-admin` to show clear operator guidance.
- Define the future snapshot transfer model at a high level.

## Non-goals

- Implement snapshot transfer in the first phase.
- Implement follower automatic reinitialization.
- Implement manual promotion/fencing.
- Implement quorum/election.
- Implement retention coordination that blocks primary truncation based on follower lag.
- Implement incremental/per-space snapshots.

## Problem statement

A follower tracks primary progress like:

```json
{
  "applied_lsn": 10
}
```

The primary may have already retained only WAL records beginning at LSN 200:

```text
primary first retained LSN = 200
primary last committed LSN = 450
latest checkpoint LSN      = 199
```

If the follower asks:

```text
StreamWal(after_lsn=10)
```

then the primary cannot stream records 11–199. The follower needs a snapshot or full resync before it can resume at an LSN still retained by the primary.

The correct behavior is:

```text
StreamWal -> FailedPrecondition(snapshot_required)
follower progress -> last_error=snapshot_required, catchup_state=snapshot_required
CLI/UI -> follower requires snapshot/resync
```

## Definitions

| Term | Meaning |
| --- | --- |
| `last_committed_lsn` | Highest WAL LSN committed on the primary. |
| `first_retained_lsn` | Lowest WAL LSN still available for streaming from the primary. |
| `checkpoint_lsn` | LSN through which durable state is represented by checkpointed materialized files. |
| `requested_after_lsn` | Follower's requested stream offset. Primary should stream records with LSN greater than this. |
| `snapshot_required` | Follower cannot catch up from retained WAL alone and needs snapshot/resync. |
| `snapshot_base_lsn` | LSN represented by an installed snapshot; follower resumes WAL from this LSN. |

## WAL retained range API

Add WAL status APIs that expose retained range.

Suggested WAL manager API:

```go
type RetainedRange struct {
    FirstRetainedLSN wal.LSN
    LastCommittedLSN wal.LSN
}

func (m *Manager) RetainedRange(ctx context.Context) (RetainedRange, error)
```

Behavior:

- `LastCommittedLSN` is current committed high-water mark.
- `FirstRetainedLSN` is the first LSN available from retained WAL segments.
- Empty WAL may return both as zero.
- If segments exist but no record can be read, return an explicit error.

If current segment metadata only exposes segment start LSNs, the first implementation can approximate `FirstRetainedLSN` as the first segment start LSN. A later implementation can scan the first segment for the exact first record LSN.

## StreamWal gap detection

Before streaming, the primary should evaluate:

```text
next_requested_lsn = requested_after_lsn + 1
first_retained_lsn = WAL retained range first
last_committed_lsn = WAL retained range last
```

If:

```text
next_requested_lsn < first_retained_lsn
```

then return `FailedPrecondition` with a structured snapshot-required detail.

If:

```text
requested_after_lsn >= last_committed_lsn
```

then no catch-up gap exists; the stream can block/tail waiting for future records.

If WAL is empty:

```text
first_retained_lsn = 0
last_committed_lsn = 0
```

then stream can block/tail.

## Error shape

Use a stable gRPC error so CLI/SDK/UI can detect it.

Recommended:

```text
gRPC code: FailedPrecondition
message:   follower requires snapshot catch-up
```

Use `google.rpc.ErrorInfo` details:

```text
reason: MYCEL_WAL_SNAPSHOT_REQUIRED
domain: mycel.replication
metadata:
  requested_after_lsn: "10"
  next_requested_lsn: "11"
  first_retained_lsn: "200"
  last_committed_lsn: "450"
  checkpoint_lsn: "199"
  primary_node_id: "node_a"
  authority_epoch: "1"
```

The exact metadata keys should be constants in a shared daemon/clustering package.

## Follower behavior

When follower receives `snapshot_required` from `StreamWal`:

1. Do not advance `received_lsn` or `applied_lsn`.
2. Persist replication progress `last_error` with a concise message.
3. Persist a machine-readable catch-up state.
4. Stop retrying aggressively; use slower retry/backoff or wait for operator action.
5. Expose status through admin/CLI/UI.

Extend progress model:

```json
{
  "catchup_state": "snapshot_required",
  "snapshot_required": {
    "requested_after_lsn": 10,
    "next_requested_lsn": 11,
    "first_retained_lsn": 200,
    "last_committed_lsn": 450,
    "checkpoint_lsn": 199,
    "primary_node_id": "node_a",
    "authority_epoch": 1
  }
}
```

Suggested catch-up states:

| State | Meaning |
| --- | --- |
| `unknown` | No replication attempt has completed yet. |
| `streaming` | Connected to primary stream. |
| `caught_up` | Applied through known primary last LSN. Best effort until heartbeats exist. |
| `retrying` | Temporary stream/connect/apply error; retrying. |
| `snapshot_required` | WAL gap prevents catch-up without snapshot/resync. |
| `error` | Non-retryable or repeated error requiring operator attention. |

The MVP can start with `retrying` and `snapshot_required` only.

## Status API changes

Extend `ClusterReplicationStatus` with WAL range and catch-up fields.

Conceptual shape:

```proto
enum ClusterReplicationCatchupState {
  CLUSTER_REPLICATION_CATCHUP_STATE_UNSPECIFIED = 0;
  CLUSTER_REPLICATION_CATCHUP_STATE_UNKNOWN = 1;
  CLUSTER_REPLICATION_CATCHUP_STATE_STREAMING = 2;
  CLUSTER_REPLICATION_CATCHUP_STATE_CAUGHT_UP = 3;
  CLUSTER_REPLICATION_CATCHUP_STATE_RETRYING = 4;
  CLUSTER_REPLICATION_CATCHUP_STATE_SNAPSHOT_REQUIRED = 5;
  CLUSTER_REPLICATION_CATCHUP_STATE_ERROR = 6;
}

message ClusterReplicationStatus {
  // existing fields...
  ClusterReplicationCatchupState catchup_state = N;
  uint64 first_retained_lsn = N+1;
  uint64 checkpoint_lsn = N+2;
  SnapshotRequiredInfo snapshot_required = N+3;
}

message SnapshotRequiredInfo {
  uint64 requested_after_lsn = 1;
  uint64 next_requested_lsn = 2;
  uint64 first_retained_lsn = 3;
  uint64 last_committed_lsn = 4;
  uint64 checkpoint_lsn = 5;
  string primary_node_id = 6;
  int64 authority_epoch = 7;
}
```

Primary status should show:

- `primary_last_lsn`
- `first_retained_lsn`
- `checkpoint_lsn`

Follower status should show:

- current applied/received LSN
- catch-up state
- snapshot-required detail when relevant

## CLI behavior

`mycel cluster status` should render:

Primary:

```text
role=primary wal_last_lsn=450 first_retained_lsn=200 checkpoint_lsn=199
```

Healthy follower:

```text
role=follower replication_applied_lsn=450 replication_lag=0 catchup=caught_up
```

Snapshot-required follower:

```text
role=follower replication_applied_lsn=10 catchup=snapshot_required first_retained_lsn=200 checkpoint_lsn=199
follower requires snapshot/resync before WAL replication can continue
```

JSON output should include all structured fields.

## mycel-admin behavior

Cluster General tab should display:

Primary WAL card:

```text
WAL
Last committed LSN: 450
First retained LSN: 200
Checkpoint LSN: 199
```

Follower replication card:

```text
Replication
Applied LSN: 10
Received LSN: 10
State: Snapshot required
Primary retained from LSN 200
Checkpoint LSN: 199
Action required: resync this follower from snapshot
```

Use warning styling for `snapshot_required` and `error`.

## Snapshot transfer future design

Full snapshot transfer is deferred, but the gap detection work should prepare for it.

Future snapshot catch-up flow:

1. Primary quiesces or reaches a safe checkpoint boundary.
2. Primary creates or selects a checkpoint/snapshot at `snapshot_base_lsn`.
3. Follower downloads snapshot manifest and files.
4. Follower stops replication apply.
5. Follower installs snapshot into a staging directory.
6. Follower verifies manifest/checksums.
7. Follower atomically swaps durable state or performs module-specific install.
8. Follower sets:

```text
applied_lsn = snapshot_base_lsn
received_lsn = snapshot_base_lsn
```

9. Follower resumes `StreamWal(after_lsn=snapshot_base_lsn)`.

### Snapshot content

Initial likely snapshot scope is whole-node durable state under the data directory, excluding volatile/runtime paths.

Candidate included paths:

- materialized metadata stores under `meta/`
- graph stores
- blob metadata and/or blob content, depending on durability model
- semantic metadata/index state if authoritative
- template stores

Candidate excluded paths:

- active WAL directory
- replication receive log/progress
- process logs
- local session/runtime caches, depending on auth model
- node identity and cluster admission files, unless explicitly part of rebootstrap flow

This needs a separate snapshot design before implementation.

## Retention policy implications

Current WAL retention/checkpoint policy should eventually become replication-aware.

Future primary retention should consider:

- minimum applied LSN across admitted followers
- configured maximum lag window
- storage pressure limits
- operator override for force-truncation

Before retention coordination exists, primary may truncate WAL that a follower still needs. That is acceptable only if the follower clearly enters `snapshot_required` and operators have a resync path.

## Implementation phases

### Phase 1: WAL retained range API

- Add retained-range method to WAL manager.
- Add tests for empty WAL, normal WAL, and post-retention/checkpoint cases where possible.

### Phase 2: Snapshot-required error type

- Add constants/helper for `MYCEL_WAL_SNAPSHOT_REQUIRED`.
- Include structured `ErrorInfo` details.
- Add client-side detection helpers later if needed.

### Phase 3: StreamWal gap detection

- Before tailing, check retained range.
- Return snapshot-required error if requested LSN is too old.
- Add backend tests.

### Phase 4: Follower progress/status

- Extend replication progress with catch-up state and snapshot-required info.
- Detect error in follower worker.
- Persist structured status.

### Phase 5: Admin API/CLI/UI

- Extend admin proto/status.
- Show primary retained range and checkpoint LSN.
- Show follower snapshot-required warnings.

### Phase 6: Documentation and validation

- Add validation scenario for synthetic old follower progress once retention APIs can simulate a gap.
- Update operator guide.

## Acceptance criteria

This work is complete when:

- primary can report WAL first retained LSN and last committed LSN
- `StreamWal` rejects too-old follower offsets with structured `snapshot_required`
- follower persists `snapshot_required` state without advancing applied LSN
- admin status exposes retained range and catch-up state
- CLI and `mycel-admin` show clear action-required messages
- tests cover gap detection and follower state handling

## Deferred work

- Actual snapshot transfer.
- Snapshot manifest format.
- Atomic snapshot install.
- Retention coordination with follower lag.
- Operator-driven follower resync command.
