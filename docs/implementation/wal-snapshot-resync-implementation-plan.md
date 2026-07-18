# WAL Snapshot Resync Implementation Plan

## Status

Implementation plan for `wal-snapshot-resync.md`.

## Objective

Implement operator-driven follower resync for followers that cannot catch up from retained primary WAL and enter `snapshot_required` state.

Primary command:

```bash
mycel resync node-b
```

The target must be an active follower. The primary creates a quiesced/checkpointed backup-based snapshot, transfers it to the follower over internal RPC, the follower installs it while preserving node identity/admission/authority, resets replication progress to the snapshot base LSN, and resumes WAL propagation.

## Scope

In scope:

- primary-side resync target validation
- internal daemon-to-daemon snapshot install RPC
- backup/quiesce/checkpoint-based snapshot creation
- snapshot transfer over RPC chunks
- follower staging/verification/install skeleton
- preserving clustering identity/admission/authority
- resetting follower replication progress
- CLI command `mycel resync NODE`
- `mycel-admin` snapshot-required UX and optional resync action/manual instructions
- tests and e2e validation

Out of scope for first implementation:

- automatic resync without operator action
- election/failover/promotion
- incremental snapshots
- per-space snapshots
- retention coordination with follower lag
- production internode mTLS, beyond existing internal RPC assumptions

## Phase 1: Snapshot/resync proto

### Files

```text
mycel/internal/clustering/proto/mycel/cluster/v1/backend.proto
```

### Add RPC

Prefer a primary-initiated follower install stream for MVP:

```proto
rpc InstallSnapshot(stream SnapshotChunk) returns (InstallSnapshotResponse);
```

Optionally add a unary status/probe RPC later.

### Add messages

```proto
message SnapshotDescriptor {
  string operation_id = 1;
  string cluster_id = 2;
  string primary_node_id = 3;
  string target_node_id = 4;
  int64 authority_epoch = 5;
  uint64 snapshot_base_lsn = 6;
  string manifest_json = 7;
  uint64 total_bytes = 8;
  string checksum = 9;
}

message SnapshotChunk {
  oneof payload {
    SnapshotDescriptor descriptor = 1;
    bytes data = 2;
  }
}

message InstallSnapshotResponse {
  bool installed = 1;
  uint64 applied_lsn = 2;
  string message = 3;
}
```

### Generate

```bash
cd mycel
./scripts/generate-proto.sh
```

### Acceptance

- generated Go/Rust proto code compiles
- `buf lint` passes for `mycel-api` if proto is mirrored there

## Phase 2: Resync domain model and errors

### Package

Create or extend:

```text
mycel/internal/clustering/replication
```

### Types

```go
type ResyncTarget struct {
    NodeID string
    NodeName string
    BackendAdvertiseAddr string
}

type SnapshotDescriptor struct {
    OperationID string
    ClusterID string
    PrimaryNodeID string
    TargetNodeID string
    AuthorityEpoch int64
    SnapshotBaseLSN wal.LSN
    ManifestJSON string
    TotalBytes uint64
    Checksum string
}
```

### Errors

Add typed/sentinel errors or stable gRPC helpers for:

- target not found
- target is not active
- target is not follower
- connected daemon is not primary
- primary endpoint unavailable
- snapshot validation failed
- snapshot install failed

### Acceptance

- unit tests for target validation helpers

## Phase 3: Primary target validation

### Location

Likely primary coordinator package:

```text
mycel/internal/clustering/replication/resync_coordinator.go
```

or daemon admin cluster service if orchestration remains daemon-facing initially.

### Validation rules

Before quiesce/snapshot:

- local node role is primary
- target node name/ID resolves to one active membership member
- target node ID is not local node ID
- target node is not authority primary
- target backend address is known from membership or topology
- target node is active/admitted

### Function

```go
func ResolveFollowerTarget(ctx context.Context, manager *clustering.Manager, target string) (ResyncTarget, error)
```

### Tests

- target by node name
- target by node ID
- unknown target
- pending target rejected
- primary target rejected
- missing backend address rejected

## Phase 4: Backup-based snapshot creation

### Goal

Reuse backup infrastructure to create a snapshot archive/manifest under quiesce/checkpoint.

### Files to inspect/use

```text
mycel/internal/backup
mycel/internal/daemon/modules/backup
mycel/internal/daemon/quiesce
mycel/internal/wal/checkpoint.go
```

### Desired API

Expose a daemon/runtime-level snapshot creator that can be used by resync:

```go
type SnapshotResult struct {
    OperationID string
    BaseLSN wal.LSN
    Manifest backup.Manifest
    ArchivePath string
    TotalBytes uint64
    Checksum string
}

func CreateResyncSnapshot(ctx context.Context, input CreateResyncSnapshotInput) (SnapshotResult, error)
```

### Consistency

First implementation may hold quiesce through archive creation for correctness.

Flow:

1. enter quiesce
2. ensure WAL applied/checkpointed
3. create backup/snapshot archive
4. record snapshot base LSN from checkpoint
5. release quiesce

### Snapshot excludes

Ensure snapshot archive does not blindly install/overwrite follower-local cluster files. Either:

- exclude them from archive up front, or
- include them but follower installer explicitly ignores/preserves them.

Preserve/exclude:

```text
meta/clustering/node.json
meta/clustering/local_state.json
meta/clustering/authority.json
meta/clustering/peers.json
meta/clustering/replication/
wal/
logs/
```

### Tests

- snapshot creation returns base LSN
- quiesce is released on error
- manifest/archive exists
- excluded files are not part of install set or are marked preserve

## Phase 5: Primary-to-follower snapshot transfer client

### Backend client

Extend:

```text
mycel/internal/clustering/backend/client.go
```

Add:

```go
func (c Client) InstallSnapshot(ctx context.Context, addr string, descriptor SnapshotDescriptor, archive io.Reader) (InstallSnapshotResult, error)
```

Implementation:

1. dial follower backend
2. open `InstallSnapshot` stream
3. send descriptor chunk first
4. stream data chunks, e.g. 1 MiB
5. close and receive response

### Tests

Use bufconn or fake stream if existing patterns support it.

- descriptor sent first
- data chunking works
- stream error propagates
- response mapped correctly

## Phase 6: Follower InstallSnapshot service

### Location

```text
mycel/internal/clustering/backend/service_snapshot.go
```

### Dependencies

Backend service needs access to an installer interface:

```go
type SnapshotInstaller interface {
    InstallSnapshot(ctx context.Context, desc replication.SnapshotDescriptor, r io.Reader) (wal.LSN, error)
}

func (s *Service) WithSnapshotInstaller(installer SnapshotInstaller) *Service
```

Daemon app wires this on followers and primaries; handler validates local target.

### Handler behavior

1. Receive first chunk; must be descriptor.
2. Validate:
   - cluster ID matches
   - target node ID equals local identity node ID
   - local node is admitted follower
   - authority epoch is not stale
   - primary node ID matches current authority primary
3. Stream data into installer/staging.
4. Return installed/applied LSN.

### Tests

- missing descriptor rejected
- wrong cluster rejected
- wrong target rejected
- primary/local non-follower rejected
- installer error returned
- successful install returns applied LSN

## Phase 7: Follower snapshot installer

### Package

```text
mycel/internal/clustering/replication/snapshot_installer.go
```

### Staging layout

```text
<data_dir>/meta/clustering/replication/snapshot-staging/<operation_id>/
  snapshot.archive
  descriptor.json
```

### Install steps

1. Create staging dir `0700`.
2. Write archive to staging file `0600` while hashing.
3. Verify descriptor checksum and byte count.
4. Verify backup manifest/checksums if archive format supports it.
5. Stop/pause follower replication apply.
6. Preserve clustering identity/admission/authority files.
7. Install materialized data.
8. Clear receive log.
9. Set progress:

```text
received_lsn = snapshot_base_lsn
applied_lsn = snapshot_base_lsn
catchup_state = caught_up
snapshot_required = nil
last_error = ""
```

10. Cleanup staging on success; preserve on failure only if debug flag is set.

### MVP install strategy

Because safe whole-data-dir replacement is sensitive, implement a conservative first version:

- install only backup archive paths that are explicitly included in manifest
- never overwrite `meta/clustering/**`
- never overwrite `wal/**`
- never overwrite logs
- use temp directory and atomic rename for replaceable subtrees where possible

If backup restore utilities already exist, reuse them with an exclusion/preserve list.

### Tests

- validates checksum/size
- wrong target rejected
- preserves clustering identity files
- progress reset to base LSN
- receive log cleared
- failed checksum leaves existing data untouched

## Phase 8: Primary resync coordinator

### Package/API

```go
type ResyncCoordinator struct {
    Manager *clustering.Manager
    BackupSnapshotCreator SnapshotCreator
    BackendClient SnapshotInstallClient
    Logger *slog.Logger
}

func (c *ResyncCoordinator) ResyncFollower(ctx context.Context, target string) (ResyncResult, error)
```

### Flow

1. Validate primary role.
2. Resolve follower target.
3. Create snapshot under quiesce/checkpoint.
4. Stream snapshot to follower.
5. Return result.

### Result

```go
type ResyncResult struct {
    TargetNodeID string
    TargetNodeName string
    SnapshotBaseLSN wal.LSN
    TotalBytes uint64
    Message string
}
```

### Tests

Use fakes for snapshot creator and backend client.

- non-primary rejected
- non-follower rejected
- snapshot creation failure aborts before transfer
- transfer failure returned
- success returns base LSN/bytes

## Phase 9: Public admin API and CLI command

### Admin proto

Add to admin cluster service:

```proto
rpc ResyncClusterNode(ResyncClusterNodeRequest) returns (ResyncClusterNodeResponse);

message ResyncClusterNodeRequest {
  string node = 1;
}

message ResyncClusterNodeResponse {
  string node_id = 1;
  string node_name = 2;
  uint64 snapshot_base_lsn = 3;
  uint64 total_bytes = 4;
  string message = 5;
}
```

### Daemon admin service

In:

```text
mycel/internal/daemon/api/admin/cluster_service.go
```

- require cluster manage capability
- require local primary
- call resync coordinator
- map errors to stable gRPC codes

### CLI

Add top-level command:

```bash
mycel resync NODE
```

Implementation file candidate:

```text
mycel/internal/cli/cmd/resync.go
```

Output:

```text
resynced node-b at snapshot LSN 1234 (42 MiB transferred)
```

If not primary, reuse primary hint formatting.

### Tests

- CLI command calls admin API
- invalid target message
- not-primary message

## Phase 10: mycel-admin UX

### Initial UI

In Cluster page/node detail page:

- If follower catch-up state is `snapshot_required`, show warning.
- If connected daemon is primary, show button:

```text
Resync follower
```

- If connected daemon is follower, show manual instruction:

```text
Connect to the primary and run: mycel resync node-b
```

### Tauri command

Add command:

```rust
admin_resync_cluster_node(node: String) -> ResyncClusterNodeResult
```

### React behavior

- confirm modal before resync
- show progress as pending/spinner
- refresh cluster status/membership after completion

### Tests

- snapshot-required warning renders
- button enabled on primary
- manual instruction shown on follower
- command error shown clearly

## Phase 11: E2E validation

### Script

Create:

```text
mycel/scripts/validateWALSnapshotResync.sh
```

### Scenario

If forced WAL retention can be controlled:

1. start node-a/node-b
2. stop node-b
3. create writes on node-a
4. force checkpoint/retention so node-b is behind retained range
5. restart node-b
6. verify node-b enters `snapshot_required`
7. run `mycel resync node-b` against node-a
8. verify node-b progress becomes snapshot base LSN
9. verify node-b catches up via WAL
10. verify reads on node-b see data

If forced retention is not available, use a test hook/dev-only helper to simulate retained-range gap, or limit e2e to coordinator/install flow until retention controls exist.

## Phase 12: Documentation updates

Update:

```text
mycel/docs/design/wal-snapshot-resync.md
mycel/docs/design/wal-snapshot-catchup-and-retention.md
mycel/docs/design/wal-propagation-mvp.md
mycel/docs/design/write-ahead-log-operational-guide.md
```

Document:

- command usage
- prerequisites
- failure modes
- what is preserved on follower
- current limitations
- troubleshooting

## Validation commands

Go:

```bash
cd mycel
go test ./internal/clustering/replication ./internal/clustering/backend ./internal/daemon/api/admin ./internal/cli/cmd
go test ./internal/...
```

Proto/API:

```bash
cd mycel
./scripts/generate-proto.sh

cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
```

SDK/UI:

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

E2E:

```bash
cd mycel
./scripts/validateWALSnapshotResync.sh
```

## Acceptance criteria

Implementation is complete when:

- `mycel resync node-b` exists.
- command must be run against the primary.
- target must be an active follower; other targets are rejected clearly.
- primary creates a quiesced/checkpointed backup-based snapshot.
- primary transfers snapshot to follower over internal RPC.
- follower validates, stages, verifies, and installs snapshot.
- follower preserves node identity/admission/authority.
- follower resets replication progress to snapshot base LSN.
- follower resumes WAL propagation and catches up.
- CLI and `mycel-admin` provide clear operator UX.
- tests and e2e validation pass.
