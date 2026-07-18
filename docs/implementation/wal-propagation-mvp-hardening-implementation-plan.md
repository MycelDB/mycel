# WAL Propagation MVP Hardening Implementation Plan

## Status

Implementation plan.

This plan follows the initial WAL propagation MVP and focuses on correctness, operator clarity, and lower-latency replication before moving on to promotion/election work.

## Goals

- Make replication/WAL status clear and role-aware in API, CLI, and `mycel-admin`.
- Add focused unit tests for receive log, progress store, follower applier/worker, and backend `StreamWal`.
- Replace finite polling catch-up with long-lived tailing streams.
- Harden local validation scripts so they avoid port conflicts and print useful diagnostics.
- Update docs to accurately reflect implemented behavior and limitations.

## Non-goals

- Automatic leader election.
- Manual promotion/fencing.
- Snapshot transfer.
- WAL retention coordination with follower lag.
- Transparent write forwarding.
- Strong/linearizable follower reads.

## Phase 1: Role-aware WAL/replication status semantics

### Problem

The current replication card can show `Applied LSN 0` on a primary. That value is follower replication progress, so it is misleading when the connected daemon is the primary.

### Desired semantics

Primary nodes should report local WAL state, not follower progress:

```text
role=primary
wal_last_committed_lsn=N
replication=not_following
```

Follower nodes should report replication progress:

```text
role=follower
received_lsn=N
applied_lsn=N
lag_records=M
connected=true|false
last_error="..."
```

Standalone nodes should report no replication status or an explicit `not_applicable` state.

### API changes

Update public admin proto:

```text
mycel-api/api/proto/mycel/admin/v1/cluster.proto
```

Extend `ClusterReplicationStatus` with role-aware fields:

```proto
enum ClusterReplicationRole {
  CLUSTER_REPLICATION_ROLE_UNSPECIFIED = 0;
  CLUSTER_REPLICATION_ROLE_NOT_APPLICABLE = 1;
  CLUSTER_REPLICATION_ROLE_PRIMARY = 2;
  CLUSTER_REPLICATION_ROLE_FOLLOWER = 3;
}

message ClusterReplicationStatus {
  ClusterReplicationRole role = 1;
  string primary_node_id = 2;
  string primary_node_name = 3;
  string primary_backend_advertise_addr = 4;
  int64 authority_epoch = 5;
  uint64 received_lsn = 6;
  uint64 applied_lsn = 7;
  uint64 primary_last_lsn = 8;
  uint64 lag_records = 9;
  bool connected = 10;
  string last_error = 11;
  string updated_at = 12;
}
```

If preserving field numbers is preferred, append only:

```proto
ClusterReplicationRole role = 12;
```

Recommendation: because this is pre-release/internal-adjacent API, choose clarity but avoid unnecessary churn if generated SDK compatibility matters.

### Daemon service changes

Update:

```text
mycel/internal/daemon/api/admin/cluster_service.go
```

Behavior:

- standalone:
  - `role=NOT_APPLICABLE`
  - no LSNs unless useful
- primary:
  - `role=PRIMARY`
  - `primary_last_lsn = rt.WAL.LastCommittedLSN()` or injected WAL status provider
  - `received_lsn/applied_lsn` can be zero or omitted; UI must ignore them for primary
  - `connected=false`
- follower:
  - `role=FOLLOWER`
  - load `replication/progress.json`
  - `received_lsn`, `applied_lsn`, `connected`, `last_error`
  - `lag_records = primary_last_lsn - applied_lsn` when primary last LSN is known; otherwise best-effort `received_lsn - applied_lsn`

Add a small dependency to `AdminClusterService` for primary WAL last committed LSN. Options:

1. Pass `*wal.Manager` into `NewAdminClusterService`/`WithReplication`.
2. Pass an interface:

```go
type WALStatusProvider interface {
    LastCommittedLSN() wal.LSN
}
```

Prefer interface for tests.

### CLI changes

Update:

```text
mycel/internal/cli/cmd/cluster.go
```

Primary text output:

```text
node=... role=primary primary=node-a epoch=1 wal_last_lsn=42
```

Follower text output:

```text
node=... role=follower primary=node-a epoch=1 replication_applied_lsn=42 replication_lag=0 connected=true
```

JSON output should include the replication role and LSN fields.

### mycel-admin changes

Update:

```text
mycel-admin/src/types/cluster.ts
mycel-admin/src-tauri/src/commands/cluster.rs
mycel-admin/src/features/cluster/pages/ClusterPage.tsx
```

UI behavior:

- Primary:
  - card title: `WAL`
  - show `Last committed LSN`
  - show `Replication: primary / not following`
- Follower:
  - card title: `Replication`
  - show `Applied LSN`, `Received LSN`, `Lag`, `Connected`, `Last error`
- Standalone:
  - hide replication card or show `Replication not applicable`

### Tests

Add/update:

- admin cluster service status tests for primary/follower/standalone status semantics
- CLI cluster status rendering tests
- ClusterPage test for primary WAL card and follower replication card

### Acceptance

- Primary no longer appears to have `Applied LSN 0` as a replication state.
- Follower still shows applied/received replication progress.
- Full API/SDK/UI validation passes.

## Phase 2: Replication package unit tests

### Receive log tests

File:

```text
mycel/internal/clustering/replication/receive_log_test.go
```

Test cases:

- `Put` then `Get` round-trips a record
- duplicate identical `Put` succeeds
- duplicate conflicting LSN fails
- `ScanAfter` returns ascending records only after requested LSN
- `TruncateBefore` removes old records and keeps newer records
- corrupt JSON file returns an error
- missing receive-log directory scans as empty

### Progress store tests

File:

```text
mycel/internal/clustering/replication/progress_test.go
```

Test cases:

- missing progress file returns zero/default progress
- save/load round trip
- save rejects `applied_lsn > received_lsn`
- `UpdateError` preserves LSN fields and sets/clears `last_error`
- save creates parent directories

### Applier tests

File:

```text
mycel/internal/clustering/replication/applier_test.go
```

Use fake WAL appliers registered into `wal.Registry`.

Test cases:

- applies next LSN and advances received/applied progress
- skips already applied LSN
- detects LSN gap
- does not advance applied LSN when applier fails
- replay applies unapplied receive-log records after restart
- replay stops on gap/error
- authority identity/epoch mismatch is rejected

### Follower worker tests

File:

```text
mycel/internal/clustering/replication/follower_test.go
```

Use fake manager/streamer/applier if possible. If current `Follower` depends directly on `*clustering.Manager`, consider extracting a smaller interface to make this testable.

Test cases:

- no-op when standalone
- no-op when primary
- no-op when unadmitted
- records error when primary endpoint is unknown
- sends `StreamWalRequest` with `after_lsn=progress.applied_lsn`
- applies received records
- records stream error in progress
- stop cancels the loop cleanly

### Acceptance

```bash
cd mycel
go test ./internal/clustering/replication
```

passes with meaningful coverage.

## Phase 3: Backend StreamWal tests

Add tests under:

```text
mycel/internal/clustering/backend/service_wal_test.go
```

Use fake WAL reader/iterator if possible. If `wal.Iterator` is hard to fake because it is concrete, create a small adapter interface or use a real temporary `wal.Manager` and append records.

Test cases:

- missing WAL reader returns `Unavailable`
- unsupported protocol rejected
- wrong cluster ID rejected
- unadmitted local node rejected
- non-primary rejected with `MYCEL_CLUSTER_NOT_PRIMARY` details
- authority epoch mismatch rejected
- empty follower node ID rejected
- inactive/unknown follower rejected when membership exists
- streams records strictly after `after_lsn`
- preserves LSN/type/schema/timestamp/encoding/payload
- streams records in ascending order

### Acceptance

```bash
cd mycel
go test ./internal/clustering/backend
```

passes.

## Phase 4: Long-lived tailing StreamWal

### Current behavior

The MVP follower periodically reconnects and receives a finite catch-up stream.

### Desired behavior

`StreamWal` should remain open and stream new committed WAL records as they arrive.

### Primary implementation

Update:

```text
mycel/internal/clustering/backend/service_wal.go
```

Behavior:

1. Validate request as today.
2. Set `next := wal.LSN(req.after_lsn).Next()`.
3. Loop until context cancellation:
   - call `ReadNextBlocking(ctx, next)`
   - if record returned, send it
   - set `next = rec.LSN.Next()`
   - before/after blocking, verify node is still primary for requested epoch
4. Return cleanly on context cancellation.

Pseudo-code:

```go
next := wal.LSN(req.GetAfterLsn()).Next()
for {
    if err := s.ensureStillPrimary(req); err != nil { return err }
    rec, ok, err := s.WAL.ReadNextBlocking(stream.Context(), next)
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) { return nil }
    if err != nil { return err }
    if !ok { continue }
    send rec
    next = rec.LSN.Next()
}
```

### Follower behavior

The follower can keep the same reconnect loop. A healthy stream now stays open; on interruption it reconnects from last applied LSN.

### Tests

Add backend test that:

- starts stream after current last LSN
- appends a WAL record after stream starts
- verifies stream receives it without reconnect
- cancels context and verifies stream exits

### Acceptance

- Replication latency no longer depends on polling interval in the healthy case.
- Existing e2e script still passes.

## Phase 5: Validation script hardening

### Current issue

Scripts can conflict with existing local daemons on default ports.

### Changes

Update:

```text
mycel/scripts/validateShortTermClusterAuthority.sh
mycel/scripts/validateWALPropagation.sh
```

Add helpers:

```bash
find_free_port() { ... }
```

Behavior:

- choose random/free ports by default
- allow override through `NODE_A_ADDR`/`NODE_B_ADDR`
- use unique data dirs under `/tmp`, e.g. `/tmp/mycel-wal-prop-${RUN_ID}-node-a`
- use unique token files and log dirs
- on failure, print:
  - node-a/node-b logs tail
  - follower progress file
  - command stdout/stderr
- cleanup all child processes reliably
- optionally preserve daemons/logs with `KEEP_CLUSTER_VALIDATION=1`

### Acceptance

Scripts can be run repeatedly without manual port cleanup:

```bash
cd mycel
./scripts/validateShortTermClusterAuthority.sh
./scripts/validateWALPropagation.sh
```

## Phase 6: Documentation status update

Update:

```text
mycel/docs/design/wal-propagation-mvp.md
mycel/docs/implementation/wal-propagation-mvp-implementation-plan.md
mycel/docs/design/clustering-architecture-evolution.md
```

Document:

- implemented receive-log path
- implemented progress path
- role-aware status semantics
- long-lived tailing behavior, once implemented
- validation scripts and usage
- known limitations:
  - no snapshot transfer
  - no follower lag based retention
  - no manual promotion/fencing
  - no election/quorum
  - follower reads are stale/eventual

### Acceptance

Docs match current behavior and do not imply election/snapshot/strong consistency exists.

## Validation commands

Run after backend/daemon changes:

```bash
cd mycel
go test ./internal/clustering/replication ./internal/clustering/backend ./internal/daemon/api/admin ./internal/cli/cmd
go test ./internal/...
```

Run after proto changes:

```bash
cd mycel
./scripts/generate-proto.sh

cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
```

Run after SDK/UI changes:

```bash
cd mycel-go-sdk
./scripts/generate-proto.sh
go test ./...

cd ../mycel-rust-sdk
cargo check -p mycel-proto
cargo check -p mycel-sdk

cd ../mycel-admin/src-tauri
cargo check

cd ..
npm test -- --runInBand
npm run build
```

Run e2e:

```bash
cd mycel
./scripts/validateWALPropagation.sh
```

## Acceptance criteria

This hardening plan is complete when:

- primary status shows WAL last committed LSN instead of misleading follower applied LSN
- follower status shows replication applied/received/lag/error clearly
- `mycel-admin` renders primary and follower states differently
- replication package has focused unit tests
- backend `StreamWal` has focused tests
- `StreamWal` tails new committed records over a long-lived stream
- validation scripts are repeatable without port conflicts
- docs reflect actual behavior and limitations
- full validation and WAL propagation e2e pass
