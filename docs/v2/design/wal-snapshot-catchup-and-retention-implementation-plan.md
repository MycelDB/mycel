# WAL Snapshot Catch-up and Retention Safety Implementation Plan

## Status

Implementation plan for `wal-snapshot-catchup-and-retention.md`.

## Objective

Add the next correctness layer after WAL propagation MVP: detect when a follower cannot catch up from retained primary WAL, persist a clear `snapshot_required` follower state, and expose WAL retained range/checkpoint information in daemon status, CLI, and `mycel-admin`.

This plan does **not** implement snapshot transfer. It makes the missing snapshot/resync requirement explicit and observable.

## Scope

In scope:

- WAL retained range API.
- Snapshot-required structured error.
- `StreamWal` gap detection.
- Follower catch-up state persistence.
- Admin API status fields.
- CLI and `mycel-admin` display.
- Tests and docs updates.

Out of scope:

- Snapshot manifest/transfer/install.
- Automatic follower reinitialization.
- Retention coordination based on follower lag.
- Election/failover/promotion.
- Strong follower reads.

## Phase 1: WAL retained range API

### Files

Primary files:

```text
mycel/internal/wal/manager.go
mycel/internal/wal/reader.go
mycel/internal/wal/manager_test.go
```

### API

Add:

```go
type RetainedRange struct {
    FirstRetainedLSN wal.LSN
    LastCommittedLSN wal.LSN
}

func (m *Manager) RetainedRange(ctx context.Context) (RetainedRange, error)
```

Because this is inside package `wal`, the actual type should be:

```go
type RetainedRange struct {
    FirstRetainedLSN LSN
    LastCommittedLSN LSN
}
```

### Behavior

- Empty WAL:

```text
FirstRetainedLSN = 0
LastCommittedLSN = 0
```

- Non-empty WAL:
  - `LastCommittedLSN = m.LastCommittedLSN()`
  - `FirstRetainedLSN` should be the first readable committed record still retained.

### Implementation approach

1. Use existing segment listing logic.
2. Open the first retained segment.
3. Read until the first valid record is found.
4. Return that record's LSN as `FirstRetainedLSN`.
5. If no readable records exist but `LastCommittedLSN == 0`, return zero range.
6. If `LastCommittedLSN > 0` but no record can be read, return an error.

If current segment internals make exact first-record scan awkward, a temporary approximation using first segment start LSN is acceptable, but tests should document this. Prefer exact first readable record.

### Tests

Add tests for:

- empty WAL range is zero/zero
- after one append, first and last are one
- after multiple appends, first is one and last is N
- after retention/checkpoint truncation, first retained advances if existing retention helpers allow this to be tested

### Acceptance

```bash
cd mycel
go test ./internal/wal
```

passes.

## Phase 2: Snapshot-required error helper

### Files

Suggested package/file:

```text
mycel/internal/clustering/replication/errors.go
```

or, if backend-only at first:

```text
mycel/internal/clustering/backend/wal_errors.go
```

Prefer `replication/errors.go` so follower code can parse the same error reason.

### Constants

Add:

```go
const SnapshotRequiredReason = "MYCEL_WAL_SNAPSHOT_REQUIRED"

const (
    SnapshotRequestedAfterLSNKey = "requested_after_lsn"
    SnapshotNextRequestedLSNKey  = "next_requested_lsn"
    SnapshotFirstRetainedLSNKey  = "first_retained_lsn"
    SnapshotLastCommittedLSNKey  = "last_committed_lsn"
    SnapshotCheckpointLSNKey     = "checkpoint_lsn"
    SnapshotPrimaryNodeIDKey     = "primary_node_id"
    SnapshotAuthorityEpochKey    = "authority_epoch"
)
```

### Types

```go
type SnapshotRequiredInfo struct {
    RequestedAfterLSN wal.LSN
    NextRequestedLSN  wal.LSN
    FirstRetainedLSN  wal.LSN
    LastCommittedLSN  wal.LSN
    CheckpointLSN     wal.LSN
    PrimaryNodeID     string
    AuthorityEpoch    int64
}
```

### Helpers

```go
func SnapshotRequiredError(info SnapshotRequiredInfo) error
func SnapshotRequiredInfoFromError(err error) (SnapshotRequiredInfo, bool)
```

Error shape:

```text
code:    FailedPrecondition
message: follower requires snapshot catch-up
reason:  MYCEL_WAL_SNAPSHOT_REQUIRED
```

Use `google.rpc.ErrorInfo` details.

### Tests

- helper returns `codes.FailedPrecondition`
- helper includes expected reason/domain
- parser extracts all numeric/string fields
- parser returns false for unrelated errors

### Acceptance

```bash
go test ./internal/clustering/replication
```

passes.

## Phase 3: Checkpoint LSN provider

### Problem

Snapshot-required error details should include `checkpoint_lsn` when known.

### Existing foundation

There is a WAL checkpoint store under:

```text
mycel/internal/wal/checkpoint.go
```

### API

Add a small interface in backend or replication package:

```go
type CheckpointProvider interface {
    CheckpointLSN(ctx context.Context) (wal.LSN, error)
}
```

or use existing checkpoint store directly if it already exposes `Load(ctx)` with LSN.

### Backend service wiring

Extend backend service:

```go
type Service struct {
    ...
    WAL WALReader
    Checkpoint CheckpointProvider
}

func (s *Service) WithCheckpoint(provider CheckpointProvider) *Service
```

Wire in daemon app when constructing/wiring backend:

```go
clusterManager.SetBackendWAL(walManager)
clusterManager.SetBackendCheckpoint(rt.WALCheckpoint)
```

If checkpoint store does not implement the interface directly, add adapter.

### Tests

- checkpoint absent -> checkpoint LSN zero
- checkpoint present -> included in snapshot-required error

## Phase 4: StreamWal retained-range gap detection

### Files

```text
mycel/internal/clustering/backend/service_wal.go
mycel/internal/clustering/backend/service_wal_test.go
```

### Logic

Before entering tail loop:

```go
requestedAfter := wal.LSN(req.GetAfterLsn())
nextRequested := requestedAfter.Next()
range, err := s.WAL.RetainedRange(ctx)

if range.FirstRetainedLSN != wal.ZeroLSN && nextRequested < range.FirstRetainedLSN {
    return replication.SnapshotRequiredError(replication.SnapshotRequiredInfo{
        RequestedAfterLSN: requestedAfter,
        NextRequestedLSN: nextRequested,
        FirstRetainedLSN: range.FirstRetainedLSN,
        LastCommittedLSN: range.LastCommittedLSN,
        CheckpointLSN: checkpointLSN,
        PrimaryNodeID: s.Identity.NodeID,
        AuthorityEpoch: s.Authority.GetAuthorityEpoch(),
    })
}
```

Important cases:

- Empty WAL: no gap; stream can wait.
- Request after current last LSN: no gap; stream can tail.
- Request exactly before first retained:
  - if `nextRequested == firstRetained`, OK.
- Request too old:
  - if `nextRequested < firstRetained`, snapshot required.

### Interface update

Current backend `WALReader` may need to become:

```go
type WALReader interface {
    ReadFrom(ctx context.Context, lsn wal.LSN) (*wal.Iterator, error) // optional if still used
    ReadNextBlocking(ctx context.Context, lsn wal.LSN) (wal.Record, bool, error)
    RetainedRange(ctx context.Context) (wal.RetainedRange, error)
}
```

### Tests

Add fake or real WAL test for:

- request too old returns snapshot-required error
- parser extracts requested/first/last/checkpoint
- request at `first_retained - 1` is OK
- empty WAL does not return snapshot-required

If simulating retention with real WAL is hard, use a fake WAL reader implementing `RetainedRange` and `ReadNextBlocking`.

### Acceptance

```bash
go test ./internal/clustering/backend
```

passes.

## Phase 5: Follower progress catch-up state

### Files

```text
mycel/internal/clustering/replication/progress.go
mycel/internal/clustering/replication/follower.go
mycel/internal/clustering/replication/progress_test.go
mycel/internal/clustering/replication/follower_test.go
```

### Model additions

Add:

```go
type CatchupState string

const (
    CatchupStateUnknown          CatchupState = "unknown"
    CatchupStateStreaming        CatchupState = "streaming"
    CatchupStateCaughtUp         CatchupState = "caught_up"
    CatchupStateRetrying         CatchupState = "retrying"
    CatchupStateSnapshotRequired CatchupState = "snapshot_required"
    CatchupStateError            CatchupState = "error"
)

type Progress struct {
    ...
    CatchupState     CatchupState          `json:"catchup_state,omitempty"`
    SnapshotRequired *SnapshotRequiredInfo `json:"snapshot_required,omitempty"`
}
```

### Store helpers

Add:

```go
func (s *ProgressStore) UpdateCatchupState(ctx context.Context, state CatchupState, err error) error
func (s *ProgressStore) UpdateSnapshotRequired(ctx context.Context, info SnapshotRequiredInfo) error
```

### Follower behavior

In `Follower.runOnce`:

- before/while stream is active:

```go
progress.CatchupState = streaming
```

- on snapshot-required error:

```go
info, ok := SnapshotRequiredInfoFromError(err)
if ok {
    progress.CatchupState = snapshot_required
    progress.SnapshotRequired = &info
    progress.LastError = "follower requires snapshot catch-up"
    save progress
    return err
}
```

- on normal transient stream error:

```go
CatchupState = retrying
LastError = err.Error()
```

- after clean stream end, if retained range/last LSN indicate caught up:

```go
CatchupState = caught_up
```

For long-lived streams, caught-up may be best-effort. It is acceptable to remain `streaming` while connected.

### Tests

- snapshot-required error sets state and structured info
- generic stream error sets retrying/error state
- successful stream clears last error and sets streaming/caught-up appropriately

### Acceptance

```bash
go test ./internal/clustering/replication
```

passes.

## Phase 6: Admin proto/status fields

### File

```text
mycel-api/api/proto/mycel/admin/v1/cluster.proto
```

### Proto additions

Add catch-up enum:

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
```

Extend `ClusterReplicationStatus`:

```proto
ClusterReplicationCatchupState catchup_state = 13;
uint64 first_retained_lsn = 14;
uint64 checkpoint_lsn = 15;
SnapshotRequiredInfo snapshot_required = 16;
```

Add:

```proto
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

Regenerate:

```bash
cd mycel
./scripts/generate-proto.sh
```

### Daemon status population

Update:

```text
mycel/internal/daemon/api/admin/cluster_service.go
```

For primary:

- `primary_last_lsn`
- `first_retained_lsn`
- `checkpoint_lsn`
- `catchup_state = caught_up` or `not applicable`; choose primary-specific display in UI by role

For follower:

- map progress `CatchupState`
- map `SnapshotRequiredInfo`
- include primary retained/checkpoint fields if available from progress snapshot-required info

### Tests

- primary status includes retained range and last LSN
- follower status maps snapshot-required progress info

## Phase 7: CLI display

### File

```text
mycel/internal/cli/cmd/cluster.go
```

### Output examples

Primary:

```text
node=... role=primary wal_last_lsn=450 first_retained_lsn=200 checkpoint_lsn=199
```

Follower healthy:

```text
node=... role=follower replication_applied_lsn=450 replication_lag=0 catchup=streaming connected=true
```

Follower snapshot required:

```text
node=... role=follower replication_applied_lsn=10 catchup=snapshot_required first_retained_lsn=200 checkpoint_lsn=199
follower requires snapshot/resync before WAL replication can continue
```

### JSON

Include:

- `catchup_state`
- `first_retained_lsn`
- `checkpoint_lsn`
- `snapshot_required`

### Tests

Update CLI cluster status tests for primary/follower/snapshot-required rendering if existing fake response tests are available.

## Phase 8: mycel-admin display

### Files

```text
mycel-admin/src/types/cluster.ts
mycel-admin/src-tauri/src/commands/cluster.rs
mycel-admin/src/features/cluster/pages/ClusterPage.tsx
mycel-admin/src/features/cluster/pages/ClusterPage.test.tsx
```

### UI behavior

Primary WAL card:

- Last committed LSN
- First retained LSN
- Checkpoint LSN

Follower replication card:

- Applied LSN
- Received LSN
- Lag
- Connected/disconnected
- Catch-up state

Snapshot-required warning:

```text
Snapshot required
This follower can no longer catch up from retained WAL. Resync from a snapshot is required.
Requested next LSN: 11
Primary first retained LSN: 200
Checkpoint LSN: 199
```

### Tests

- renders primary WAL retained range
- renders follower catch-up state
- renders snapshot-required warning

## Phase 9: SDK helpers

### Go SDK

File:

```text
mycel-go-sdk/cluster_errors.go
```

Add:

```go
func IsSnapshotRequiredError(err error) bool
func SnapshotRequiredInfoFromError(err error) (SnapshotRequiredInfo, bool)
```

### Rust SDK

File:

```text
mycel-rust-sdk/crates/mycel-sdk/src/error.rs
```

Add:

```rust
Error::is_snapshot_required()
Error::snapshot_required_info()
```

This can mirror existing not-primary `ErrorInfo` parsing.

## Phase 10: Validation scenario

Add or extend a script only if gap can be simulated reliably.

Suggested script:

```text
mycel/scripts/validateWALSnapshotRequired.sh
```

Possible strategy:

1. start primary/follower
2. stop follower
3. create writes on primary
4. force WAL retention/checkpoint if API/script exists
5. restart follower
6. verify follower enters `snapshot_required`

If forced retention is not currently exposed, skip script and rely on unit tests until retention control exists.

## Phase 11: Documentation updates

Update:

```text
mycel/docs/v2/design/wal-snapshot-catchup-and-retention.md
mycel/docs/v2/design/wal-propagation-mvp.md
mycel/docs/v2/design/write-ahead-log-operational-guide.md
```

Document:

- how to interpret `snapshot_required`
- current lack of automatic resync
- how primary WAL retained range is shown
- operator next step: rebuild/resync follower manually until snapshot transfer exists

## Validation commands

Run after Go changes:

```bash
cd mycel
go test ./internal/wal ./internal/clustering/replication ./internal/clustering/backend ./internal/daemon/api/admin ./internal/cli/cmd
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

Run existing e2e:

```bash
cd mycel
./scripts/validateWALPropagation.sh
```

## Acceptance criteria

This implementation is complete when:

- WAL manager exposes retained range.
- Primary `StreamWal` detects too-old follower offsets.
- Snapshot-required errors are structured and parseable.
- Followers persist `catchup_state=snapshot_required` and structured gap info.
- Admin status exposes retained range/checkpoint/catch-up state.
- CLI and `mycel-admin` clearly warn when snapshot/resync is required.
- Unit tests cover WAL range, gap detection, and follower state handling.
- Full validation passes.

## Suggested chunking

1. WAL retained range + tests.
2. Snapshot-required error helper + tests.
3. Backend `StreamWal` gap detection + tests.
4. Follower progress/catch-up state + tests.
5. Admin proto/status + CLI/UI.
6. SDK helpers + docs.
