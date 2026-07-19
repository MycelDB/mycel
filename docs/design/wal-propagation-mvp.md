# WAL Propagation MVP Design

## Blob payload replication

Blob WAL records carry metadata plus a payload descriptor (space ID, blob ID, size, sha256 checksum), not raw payload bytes. On followers, blob metadata WAL records have a pre-apply dependency: the follower fetches the payload from the current primary using the internal `GetBlobPayload` backend RPC, validates size/checksum, installs the content-addressed object locally, and only then applies metadata.

This preserves the invariant that if blob metadata is visible on a follower, the corresponding blob payload is readable locally. Payload fetch failures block applied-LSN advancement and surface as replication lag/errors until retry succeeds or an operator resyncs the follower.

## Status

MVP implemented; hardening in progress.

Implemented behavior includes internal `StreamWal`, follower receive log/progress store, follower apply through WAL appliers, daemon worker integration, role-aware status, CLI/UI visibility, and local validation script. Current hardening adds long-lived tailing streams and focused replication/backend tests.

This document defines the first WAL propagation model for Mycel clustering. It builds on the current static-primary authority model, WAL-first mutation foundation, explicit membership/admission, and primary-only write guardrails.

The design intentionally starts with a narrow **primary-to-follower WAL tail replication MVP**. It does not introduce election, quorum commit, transparent forwarding, or automatic failover.

## Goals

- Replicate committed WAL records from the static primary to admitted followers.
- Let followers apply primary-originated WAL records without accepting client writes.
- Preserve primary WAL LSN semantics on followers.
- Keep the existing local WAL implementation stable during the first replication stage.
- Expose follower replication health and lag through daemon status/admin APIs.
- Prepare for future read consistency, catch-up, manual promotion, and election work.

## Non-goals

- Automatic leader election.
- Quorum writes or consensus commit.
- Transparent follower write forwarding.
- Manual promotion/fencing implementation.
- Snapshot transfer/checkpoint catch-up in the first MVP.
- Strong/linearizable reads from followers.
- Cross-cluster or multi-primary replication.
- Conflict resolution between independently-written WALs.

## Key decision: separate follower receive log

The MVP should not append replicated primary records directly into the follower's normal local WAL.

Instead, followers store primary-originated WAL records in a dedicated receive log under clustering metadata:

```text
<data_dir>/meta/clustering/replication/receive-log/
```

and track replication progress in:

```text
<data_dir>/meta/clustering/replication/progress.json
```

Rationale:

- Primary LSNs remain primary LSNs on followers.
- Follower progress directly corresponds to primary WAL position.
- Existing local WAL writer semantics do not need externally-assigned LSN support yet.
- Recovery can replay received primary records independently from any future local WAL use.
- Future read-after-LSN semantics are easier because the follower can report primary-applied LSN.

A later phase may merge or unify local WAL and replicated receive-log storage, but the MVP should keep them separate.

## Replication model

There is exactly one authority primary at a time.

```text
primary:
  accepts client/operator cluster writes
  appends/syncs/applies local WAL
  serves committed WAL records to followers

follower:
  rejects client/operator cluster writes
  connects to primary
  receives committed primary WAL records
  stores records in receive log
  applies records through existing WAL appliers
  advances primary-applied progress
```

The important distinction is:

```text
client/operator write path -> requires standalone or primary
replication apply path     -> allowed on followers
```

Follower mutation is allowed only when it is caused by primary-originated WAL replay or local operational state such as progress/topology/session files.

## Authority behavior

Replication runs only when all of these are true:

- local node lifecycle is `clustered`
- local node is admitted
- local role is `follower`
- authority is known
- authority primary is not the local node
- primary endpoint is known or discoverable from topology/membership

Replication must stop or pause when:

- local node becomes primary
- local node becomes standalone/unadmitted
- authority epoch changes and the current stream is stale
- primary identity changes
- protocol validation fails

The first MVP does not need automatic failover. If the primary is unavailable, followers should retry and expose replication error state.

## Backend API

Add an internal daemon-to-daemon streaming RPC to `mycel.cluster.v1.ClusterBackendService`.

Conceptual proto shape:

```proto
rpc StreamWal(StreamWalRequest) returns (stream WalRecord);

message StreamWalRequest {
  ClusterProtocolVersion protocol_version = 1;
  string cluster_id = 2;
  string follower_node_id = 3;
  int64 after_lsn = 4;
  int64 authority_epoch = 5;
}

message WalRecord {
  int64 lsn = 1;
  string type = 2;
  uint32 schema_version = 3;
  string timestamp = 4;
  string encoding = 5;
  bytes payload = 6;
}
```

Validation rules:

- protocol version must be supported
- cluster ID must match primary local cluster ID
- serving node must be primary for the requested authority epoch
- follower node ID should be an admitted active member, once membership is available
- `after_lsn` must be non-negative

Response semantics:

- stream committed WAL records with `lsn > after_lsn`
- records are sent in ascending LSN order
- the stream may remain open and tail new commits, or may end after current records in the first implementation
- followers reconnect using their last applied primary LSN

The MVP now uses long-lived tailing streams: after catch-up records are sent, the stream waits for newly committed WAL records and sends them as they appear. Followers reconnect from their last applied LSN if the stream is interrupted.

## WAL record mapping

The backend `WalRecord` should be a logical transport representation of the existing WAL record:

| Transport field | Source |
| --- | --- |
| `lsn` | primary committed WAL LSN |
| `type` | WAL record type string |
| `schema_version` | WAL schema version |
| `timestamp` | record timestamp, RFC3339Nano |
| `encoding` | payload encoding string/enum |
| `payload` | serialized WAL payload bytes |

The transport should not expose plaintext secrets. Existing WAL rules still apply: sensitive values must not be placed into WAL payloads in plaintext.

## Receive log

Implement a receive log package, likely under:

```text
mycel/internal/clustering/replication
```

Suggested interfaces:

```go
type ReceiveLog interface {
    Put(ctx context.Context, rec Record) error
    Get(ctx context.Context, lsn wal.LSN) (Record, error)
    ScanAfter(ctx context.Context, after wal.LSN) ([]Record, error)
    TruncateBefore(ctx context.Context, lsn wal.LSN) error
}
```

MVP storage can be simple file-based JSON or length-delimited binary records. Prefer correctness and inspectability over compactness initially.

Example layout:

```text
receive-log/
  00000000000000000001.json
  00000000000000000002.json
  ...
```

Each file stores exactly one primary WAL record and its primary LSN.

Idempotency:

- `Put` for an already-present identical LSN may succeed.
- `Put` for an already-present LSN with different contents must fail.
- The follower should skip records whose LSN is `<= applied_lsn`.
- A gap where `record.lsn != applied_lsn + 1` should pause replication and report an error.

## Progress store

Persist follower replication progress at:

```text
<data_dir>/meta/clustering/replication/progress.json
```

Suggested shape:

```json
{
  "version": 1,
  "cluster_id": "cluster_...",
  "primary_node_id": "node_...",
  "authority_epoch": 1,
  "received_lsn": 42,
  "applied_lsn": 42,
  "last_record_at": "2026-07-13T12:34:56Z",
  "last_error": "",
  "updated_at": "2026-07-13T12:34:56Z"
}
```

Definitions:

| Field | Meaning |
| --- | --- |
| `received_lsn` | highest primary LSN durably stored in receive log |
| `applied_lsn` | highest primary LSN successfully applied through WAL appliers |
| `authority_epoch` | authority epoch for the primary being followed |
| `last_record_at` | timestamp when the latest record was received/applied |
| `last_error` | last replication error, if any |

On authority primary/epoch change, the follower should either reset or branch progress in a clearly defined way. For the MVP, if the primary node ID or epoch changes, pause replication and require explicit future handling unless `applied_lsn` is zero. Manual promotion/fencing will define safe epoch transitions later.

## Applying records on followers

Follower replication should use the existing WAL applier registry, but it must not call public module mutation methods.

Apply flow:

1. receive transport record from primary
2. validate LSN order and cluster/epoch
3. durably store in receive log
4. convert transport record to local `wal.Record` shape
5. invoke registered applier for `record.type`
6. advance `applied_lsn`
7. update progress store

Appliers must be idempotent or replication must guarantee each LSN is applied once. The MVP should enforce apply-once using progress:

```text
if record.lsn <= applied_lsn: skip
if record.lsn != applied_lsn + 1: report gap and stop
apply record
applied_lsn = record.lsn
```

Recovery on follower startup:

1. load progress
2. scan receive log after `applied_lsn`
3. apply contiguous records
4. then connect to primary for more records

## Replication worker

Add a follower replication worker owned by daemon runtime or clustering manager assembly.

Suggested package:

```text
mycel/internal/clustering/replication
```

Suggested responsibilities:

```go
type Follower struct {
    manager      *clustering.Manager
    backendDial  BackendDialer
    receiveLog   ReceiveLog
    progress     ProgressStore
    walRegistry  *wal.Registry
    logger       *slog.Logger
}
```

Loop behavior:

```text
while daemon running:
  if local role is not follower:
    sleep/backoff
    continue

  resolve primary endpoint
  load progress
  stream records after applied_lsn
  for each record:
    store receive log
    apply record
    update progress
  on error:
    save last_error
    backoff and retry
```

Backoff should be bounded and observable. The first implementation can use a simple fixed interval or exponential backoff with max delay.

## Primary streaming implementation

The primary side can initially use existing WAL reader/follow APIs.

Requirements:

- read committed records after requested LSN
- send them in order
- do not send uncommitted/partial records
- stop or fail if node is no longer primary for the requested epoch
- respect context cancellation

If long-lived tailing is simple with existing `wal.Follow`, use it. Otherwise, start with catch-up-only streams and have followers poll/reconnect.

## Status and observability

Expose replication state through daemon/admin cluster status.

Suggested public admin shape:

```proto
message ClusterReplicationStatus {
  string primary_node_id = 1;
  string primary_node_name = 2;
  string primary_backend_advertise_addr = 3;
  int64 authority_epoch = 4;
  int64 received_lsn = 5;
  int64 applied_lsn = 6;
  int64 primary_last_lsn = 7;
  int64 lag_records = 8;
  bool connected = 9;
  string last_error = 10;
  string updated_at = 11;
}
```

For the MVP, `primary_last_lsn` and `lag_records` may be omitted or best-effort unless the primary exposes its last committed LSN in status.

CLI/UI should show:

```text
role=follower primary=node-a applied_lsn=42 lag=0 connected=true
```

`mycel-admin` should add replication state to the Cluster General tab and, later, per-node detail pages.

## Failure handling

Expected failures and MVP behavior:

| Failure | Behavior |
| --- | --- |
| primary unavailable | follower records `last_error`, retries with backoff |
| stream interrupted | reconnect from `applied_lsn` |
| duplicate record | skip if `lsn <= applied_lsn`; idempotent receive-log put if same content |
| LSN gap | stop applying, record error, retry/catch-up |
| authority epoch mismatch | stop stream, refresh authority, retry only if safe |
| local applier error | stop applying, record error, do not advance `applied_lsn` |
| receive-log fsync/write error | stop, record error if possible, do not apply record |

## Security

- StreamWal is internal daemon-to-daemon API only.
- The caller must be an admitted cluster member when membership is available.
- Future production internode mTLS should authenticate node identity.
- WAL payloads must continue to avoid plaintext secrets.
- Join token hashes and provider secrets must not be exposed through replication status or logs.

## Implementation phases

### Phase 1: Design and proto

- Add this design doc.
- Extend internal backend proto with `StreamWal` and WAL transport messages.
- Regenerate internal/backend generated code.
- Add conversion helpers between `wal.Record` and backend proto `WalRecord`.

### Phase 2: Receive log and progress store

- Implement file-backed receive log.
- Implement file-backed replication progress store.
- Add idempotency/gap tests.

### Phase 3: Primary StreamWal service

- Implement backend `StreamWal` on primary.
- Validate protocol, cluster ID, role, authority epoch, and follower identity.
- Stream committed records after `after_lsn`.
- Add service tests.

### Phase 4: Follower apply/recovery path

- Implement conversion from transport record to `wal.Record`.
- Apply received records through WAL registry.
- Track applied primary LSN.
- On startup, replay receive-log records after applied LSN before connecting.
- Add tests with fake appliers.

### Phase 5: Follower replication worker

- Add worker lifecycle to daemon assembly.
- Run only on admitted followers.
- Resolve primary endpoint from authority/topology/membership.
- Retry with backoff and persist `last_error`.
- Add integration-style tests with in-memory/fake backend stream.

### Phase 6: Status/API/CLI/UI

- Add replication status to daemon/admin cluster status.
- Display replication state in CLI `cluster status`.
- Display replication state in `mycel-admin` Cluster General tab.
- Optionally add per-node replication detail.

### Phase 7: E2E validation

Create a dev validation script that:

1. starts node-a as primary
2. starts node-b as follower
3. creates a space or other guarded write on primary
4. waits for follower applied LSN to advance
5. verifies follower read sees replicated state
6. verifies follower still rejects client writes

## Acceptance criteria

The MVP is complete when:

- primary can stream committed WAL records after a requested LSN
- follower durably stores primary records in receive log
- follower applies received records through WAL appliers
- follower persists received/applied primary LSN progress
- follower restart replays unapplied receive-log records
- follower rejects client writes but applies primary WAL records
- replication status is visible through admin status/CLI/UI
- e2e validation proves a primary write becomes visible on follower

## Deferred work

The following are intentionally deferred:

- snapshot/checkpoint transfer
- WAL compaction/retention coordination with follower lag
- read-after-LSN API
- linearizable reads
- transparent write forwarding
- manual promotion/fencing
- automatic election/quorum
- multi-primary conflict handling
