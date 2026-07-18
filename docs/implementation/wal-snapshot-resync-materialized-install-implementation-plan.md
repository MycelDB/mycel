# WAL Snapshot Resync Materialized Install Implementation Plan

## Status

Focused implementation plan.

This plan fills the main missing piece after the snapshot resync foundation: creating a real backup-based snapshot on the primary and safely installing its materialized durable state on the follower.

## Objective

Implement the durable data portion of snapshot resync:

```text
primary quiesce + checkpoint + backup snapshot archive
  -> transfer archive over InstallSnapshot RPC
  -> follower stages/verifies/unpacks archive
  -> follower preserves local cluster identity/admission/authority
  -> follower installs materialized durable state
  -> follower resets replication progress to snapshot_base_lsn
  -> follower resumes WAL propagation
```

This plan intentionally focuses on snapshot creation/install. Public `mycel resync NODE` orchestration can be completed after this foundation is safe.

## Current foundation

Already implemented:

- internal `InstallSnapshot(stream SnapshotChunk)` backend RPC
- snapshot descriptor/chunk/response proto
- primary-to-follower snapshot transfer client
- follower `InstallSnapshot` service validation
- follower snapshot installer staging/checksum/byte-count validation
- receive-log clear
- replication progress reset to snapshot base LSN

Missing:

- primary backup-based snapshot creation under quiesce/checkpoint
- explicit snapshot archive format/path inclusion rules
- follower archive unpack/manifest verification
- materialized state replacement
- preservation of local-only cluster files during install
- safety tests for install correctness/corruption prevention

## Non-goals

- automatic resync trigger
- public CLI/admin `resync` orchestration, except interfaces needed by this work
- incremental snapshots
- per-space snapshots
- election/failover
- retention coordination
- production-grade transport security

## Phase 1: Audit backup archive and restore capabilities

### Files to inspect

```text
mycel/internal/backup
mycel/internal/daemon/modules/backup
mycel/internal/backup/manifest.go
mycel/internal/daemon/modules/backup/checkpoint.go
mycel/internal/daemon/modules/backup/module.go
```

### Questions to answer

- Does the backup mechanism create a directory snapshot, archive file, or manifest + files?
- Does it already support restore/unpack?
- Are checksums already computed per file or archive?
- Does backup already coordinate with quiesce/checkpoint?
- Which paths does backup currently include/exclude?
- Does backup include WAL? logs? clustering metadata? replication metadata?

### Deliverable

Add implementation notes to this plan or a short status doc section with:

- actual backup artifact format
- usable APIs
- missing APIs to add

## Phase 2: Define resync snapshot inclusion/exclusion policy

### Include materialized cluster-authoritative state

Initial included paths should be the durable materialized state needed for reads after WAL replay resumes. Candidate paths:

```text
meta/admin*
meta/users*
meta/spaces*
meta/domains*
meta/acl*
meta/semantic*
meta/embedding*
graph*/
templates/
blobs/ or blob content store path
```

Use actual project paths discovered in Phase 1.

### Always preserve/exclude follower-local state

The snapshot install must never overwrite:

```text
meta/clustering/node.json
meta/clustering/local_state.json
meta/clustering/authority.json
meta/clustering/peers.json
meta/clustering/membership.json        # decide: likely preserve follower local view unless snapshot membership is authoritative
meta/clustering/replication/**
wal/**
logs/**
```

Also preserve any node-local credential/TLS material.

### Membership note

Membership is cluster-authoritative, but overwriting follower-local membership during install can be risky if the snapshot contains stale local paths or conflicts. First implementation should preserve local clustering metadata and rely on backend membership/topology refresh. If membership snapshot install is needed later, handle it explicitly.

### Deliverable

Create a function or policy object:

```go
type SnapshotPathPolicy struct {
    Include []string
    Preserve []string
    Exclude []string
}

func DefaultResyncSnapshotPathPolicy() SnapshotPathPolicy
```

or equivalent helpers:

```go
func IsResyncSnapshotIncluded(path string) bool
func IsResyncSnapshotPreserved(path string) bool
```

## Phase 3: Primary resync snapshot creator

### Package/API

Likely package:

```text
mycel/internal/clustering/replication
```

or daemon backup module if it needs direct access to quiesce/backup internals.

Suggested API:

```go
type SnapshotCreator struct {
    DataDir    string
    Quiesce    *quiesce.Coordinator
    WAL        *wal.Manager
    Progress   wal.AppliedLSNStore
    Checkpoint *wal.CheckpointStore
    Backup     SnapshotBackupAdapter
    Logger     *slog.Logger
}

type SnapshotResult struct {
    OperationID string
    BaseLSN     wal.LSN
    ManifestJSON string
    ArchivePath string
    TotalBytes uint64
    Checksum string
}

func (c *SnapshotCreator) Create(ctx context.Context) (SnapshotResult, error)
```

### Snapshot flow

1. Enter quiesce.
2. Ensure WAL is fully applied through current committed LSN.
3. Create checkpoint and get `base_lsn`.
4. Create backup archive/snapshot using resync path policy.
5. Compute archive checksum and total bytes.
6. Release quiesce.
7. Return `SnapshotResult`.

### Quiesce safety

Use `defer release` immediately after entering quiesce. Tests must verify quiesce is released on failure.

### Base LSN

`base_lsn` should be checkpoint LSN. If checkpoint cannot advance to last committed because not all WAL is applied, either wait for apply or fail clearly.

### Tests

- creates snapshot with nonzero operation ID
- base LSN equals checkpoint LSN
- archive exists
- checksum and bytes match archive
- quiesce release on backup failure
- excluded paths are absent from archive, or marked preserve for installer

## Phase 4: Archive manifest model for install

### Goal

Follower installer needs a manifest that allows safe verification and install.

If backup manifest already exists, extend/reuse it. Otherwise add resync-specific wrapper:

```go
type ResyncSnapshotManifest struct {
    Version int `json:"version"`
    ClusterID string `json:"cluster_id"`
    PrimaryNodeID string `json:"primary_node_id"`
    AuthorityEpoch int64 `json:"authority_epoch"`
    SnapshotBaseLSN wal.LSN `json:"snapshot_base_lsn"`
    Files []SnapshotFile `json:"files"`
}

type SnapshotFile struct {
    Path string `json:"path"`
    Size int64 `json:"size"`
    Checksum string `json:"checksum"`
}
```

### Rules

- Paths must be relative.
- Reject absolute paths.
- Reject `..` traversal.
- Reject paths matching preserve/exclude list.
- Verify file checksum after unpack.

### Tests

- manifest rejects unsafe paths
- manifest rejects preserve-list paths
- checksum mismatch detected

## Phase 5: Follower archive staging and unpack

### Existing staging

Current staging path:

```text
<data_dir>/meta/clustering/replication/snapshot-staging/<operation_id>/
  snapshot.archive
  descriptor.json
```

Extend with:

```text
  unpacked/
  manifest.json
```

### Unpack behavior

1. Verify archive checksum/size from descriptor.
2. Unpack archive into `unpacked/`.
3. Load manifest.
4. Validate all paths against policy.
5. Verify per-file checksums/sizes.
6. Only after successful validation proceed to install.

### Archive format

Prefer the backup mechanism's existing archive format. If none exists, use tar or tar.gz with safe extraction helpers.

### Safe extraction requirements

- no absolute paths
- no `..`
- no symlink traversal in first implementation; either reject symlinks or ignore them
- create files with safe permissions
- directories `0700` or original safe mode as appropriate

### Tests

- valid archive unpacks
- path traversal rejected
- symlink rejected or ignored
- checksum mismatch fails
- corrupt archive fails

## Phase 6: Materialized state install

### Install strategy

Use staged replacement with preserve list.

Recommended first implementation:

1. Stop/pause replication worker/apply.
2. Validate staged snapshot completely.
3. For each top-level materialized store subtree included in snapshot:
   - move current subtree to backup path under install operation dir
   - move staged subtree into place
4. Preserve excluded paths by never touching them.
5. On failure before commit, leave current state untouched.
6. On failure after partial commit, attempt rollback from backup path and return error.

### Atomicity level

Full data-dir atomic swap is not acceptable because node identity/clustering metadata must be preserved. Instead, use per-subtree atomic renames.

### Store quiescence

Follower should not serve writes already due to authority guards. Reads may be in progress. First implementation can perform install during follower-local quiesce or service pause if available. If no local read quiesce exists, document that install is an operational maintenance action and may briefly disrupt reads.

### Receive log/progress

Only after materialized install succeeds:

```go
receiveLog.Clear(ctx)
progress.Save(Progress{
    ClusterID: desc.ClusterID,
    PrimaryNodeID: desc.PrimaryNodeID,
    AuthorityEpoch: desc.AuthorityEpoch,
    ReceivedLSN: desc.SnapshotBaseLSN,
    AppliedLSN: desc.SnapshotBaseLSN,
    CatchupState: CatchupStateCaughtUp,
})
```

If install fails, do not reset progress.

### Tests

- materialized file appears after install
- old materialized file removed/replaced
- clustering identity file preserved
- authority file preserved
- replication progress reset only after success
- receive log cleared only after success
- install failure preserves old data and progress

## Phase 7: Backend installer integration

Update current:

```text
mycel/internal/clustering/backend/service_snapshot.go
mycel/internal/clustering/replication/snapshot_installer.go
```

The backend service already validates descriptor and streams payload. Replace current "stage and reset only" behavior with full installer behavior from phases 5–6.

### Tests

- `InstallSnapshot` success installs materialized file and resets progress
- wrong target rejected
- wrong primary/epoch rejected
- checksum mismatch rejected and progress unchanged

## Phase 8: Primary coordinator adapter preparation

Even before public `mycel resync`, add interfaces needed by the future coordinator:

```go
type SnapshotCreateService interface {
    Create(ctx context.Context) (SnapshotResult, error)
}

type SnapshotInstallClient interface {
    InstallSnapshot(ctx context.Context, addr string, desc replsnapshot.SnapshotDescriptor, r io.Reader) (replsnapshot.InstallSnapshotResult, error)
}
```

This keeps phase 8 of the broader resync plan straightforward.

## Phase 9: Documentation updates

Update:

```text
mycel/docs/design/wal-snapshot-resync.md
mycel/docs/implementation/wal-snapshot-resync-implementation-plan.md
mycel/docs/design/wal-snapshot-catchup-and-retention.md
```

Document:

- actual archive format
- actual include/preserve policy
- current install atomicity guarantees
- known limitations

## Validation commands

Go focused:

```bash
cd mycel
go test ./internal/clustering/replication ./internal/clustering/backend ./internal/backup ./internal/daemon/modules/backup
```

Go full:

```bash
cd mycel
go test ./internal/...
```

Proto/SDK/UI if any proto surfaces change:

```bash
cd mycel
./scripts/generate-proto.sh
cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
cd ../mycel-go-sdk
./scripts/generate-proto.sh && go test ./...
cd ../mycel-rust-sdk
cargo check -p mycel-proto && cargo check -p mycel-sdk
cd ../mycel-admin/src-tauri
cargo check
```

## Acceptance criteria

This focused implementation is complete when:

- primary can create a backup-based resync snapshot under quiesce/checkpoint
- snapshot descriptor includes base LSN, manifest, size, checksum
- follower verifies archive size/checksum and manifest
- unsafe paths are rejected
- materialized durable stores are installed from snapshot
- follower cluster identity/admission/authority are preserved
- receive log and progress are reset only after successful install
- install failures do not corrupt existing state
- tests cover checksum, path safety, preservation, progress reset, and failure behavior
