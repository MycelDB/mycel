# Cluster System Backup Raft Freeze Implementation Plan

## Status

Complete through RF6 in the current tranche. Coordinated cluster backup now
acquires a short raft freeze/checkpoint lease before local archive creation,
records freeze evidence in `backup-set.json`, validates destructive K3s offline
restore, and documents the release-gate production path.

Parent design: [cluster system backup design](../../design/backup-restore/cluster-system-backup.md).

## Problem

The coordinated cluster backup flow can quiesce user-visible writes and collect
raft barriers, but raft itself may still mutate local implementation storage
while the archive is being created. Examples include term changes, votes,
`hard_state.pb`, `entries.pb`, and `conf_state.pb` updates. A tar archive created
while those files are changing can have valid checksums but restore into a raft
cluster with inconsistent terms/logs across ordinals.

The destructive K3s restore test exposed this failure mode: archives were
created and restored to matching PVCs, but the restored cluster could enter raft
term churn and one pod stopped answering daemon CLI requests.

## Constraints

- Restore remains offline/operator-driven; no live restore and no automatic PVC
  repair.
- No automatic divergent PVC repair or authoritative pod selection.
- System raft remains authoritative for cluster metadata and backup lifecycle.
- The coordinator must not write system raft records while raft storage is
  frozen.
- Freeze must be lease/TTL based so coordinator death cannot leave raft frozen
  indefinitely.
- Each tranche must leave the system functional.
- Internal backend RPCs remain daemon/internal only unless a public API review
  approves otherwise.

## Desired end state

A successful full-system cluster backup has this safety sequence:

1. record backup request and expected membership in system raft;
2. fail-closed precheck before quiesce;
3. cluster-wide daemon write quiesce;
4. raft barrier/read-index collection and applied-index wait;
5. acquire raft freeze leases on every expected node;
6. while frozen, flush/fsync local raft storage and create each local archive;
7. release all raft freeze leases;
8. record node results, validate/write `backup-set.json`, and commit terminal
   state through system raft;
9. release daemon write quiesce.

## Phase RF0 — Document and gate current behavior

Status: complete.

Goal: make the current limitation explicit and prevent accidental release claims.

Tasks:

- Update design/operations docs to state that raft-storage-safe backup requires
  a freeze/checkpoint window.
- Update the implementation plan status to identify the destructive K3s restore
  failure mode.
- Add test/log notes describing that current archive creation validates artifact
  creation, but destructive restore is not considered proven until this plan is
  complete.

Validation:

```sh
make docs-check
git diff --check
```

## Phase RF1 — Raft storage freeze abstraction

Status: complete for the initial group-level storage mutation freeze.

Goal: add an internal abstraction that can pause raft storage mutation for local
raft groups without exposing public APIs.

Proposed interfaces:

```go
type RaftBackupFreezer interface {
    AcquireBackupFreeze(ctx context.Context, in RaftBackupFreezeInput) (*RaftBackupFreezeLease, error)
}

type RaftBackupFreezeInput struct {
    BackupSetID string
    Reason string
    Groups []consensus.GroupID
    Barriers map[string]uint64
    TTL time.Duration
}

type RaftBackupFreezeLease struct {
    ID string
    ExpiresAt time.Time
    Checkpoint RaftBackupCheckpoint
    Release func(context.Context) error
}
```

Initial implementation options:

- pause raft ticking/network advancement for local groups while holding a mutex
  around raft storage writes; or
- add a storage-level backup lock in consensus storage and require all write
  paths to hold an exclusive mutation lock.

The implementation must:

- reject freeze if barriers are missing, duplicate, stale, or below local
  applied indexes required by the run;
- wait for every local group to apply the recorded barrier before freezing;
- flush/fsync raft storage files before returning the lease;
- expose checkpoint metadata: covered groups, applied indexes, term, commit
  index, storage last index, snapshot index, lease ID, and expiry.

Validation:

```sh
go test ./internal/clustering/consensus ./internal/backup/service
```

## Phase RF2 — Backend RPCs for freeze acquire/release

Status: complete for internal backend RPC/client/provider wiring.

Goal: let the coordinator acquire/release raft freeze leases on every expected
node.

Add internal backend RPCs in `internal/clustering/proto/mycel/cluster/v1/backend.proto`:

- `AcquireLocalRaftBackupFreeze`
- `ReleaseLocalRaftBackupFreeze`

These RPCs are daemon/internal only and should be backend-token protected. They
must be quiesce-exempt because they run during backup quiesce.

Provider methods should validate:

- cluster ID;
- backup set ID;
- pod/node/ordinal/raft node mapping;
- recorded active backup membership;
- recorded barriers;
- local admitted identity;
- TTL bounds.

Validation:

```sh
go test ./internal/clustering/backend ./internal/daemon/server ./internal/backup/service
./scripts/check-daemon-only.sh
./scripts/check-public-surface.sh
```

## Phase RF3 — Coordinator freeze window

Status: complete for the initial coordinator integration.

Goal: integrate freeze into `TriggerClusterBackup` without deadlocking system
raft.

Sequence changes:

1. keep request/precheck/quiesce/barrier records as system-raft writes before
   freeze;
2. acquire freeze leases from every expected node;
3. call archive RPCs while leases are held;
4. release freeze leases in reverse order with `defer`/best effort;
5. only after all releases complete, commit node-result records through system
   raft;
6. then validate/write manifest and commit success/failure.

Failure rules:

- If acquire fails on any node, release acquired freezes and then record failure.
- If archive fails, release freezes and then record failure.
- If release fails, wait for TTL expiry or mark failure with explicit release
  warnings; do not record success while any freeze is known active.
- Coordinator should use bounded contexts for freeze/archives and surface
  actionable errors.

Validation:

```sh
go test ./internal/backup/service ./internal/clustering/backend
```

## Phase RF4 — Manifest/checkpoint evidence

Status: complete for backup-set manifest evidence and restore-mode validation.

Goal: make backup-set validation prove that archives were created under the raft
freeze checkpoint.

Extend internal manifest model with freeze evidence, for example:

```json
{
  "raft_freeze": {
    "lease_id": "...",
    "acquired_at": "...",
    "released_at": "...",
    "expires_at": "...",
    "groups": {
      "system": {
        "barrier_index": 123,
        "applied_index": 123,
        "term": 4,
        "commit_index": 123,
        "last_index": 123,
        "snapshot_index": 0
      }
    }
  }
}
```

Validation should reject a successful restore-mode manifest if:

- any expected node lacks freeze evidence;
- freeze evidence does not cover all recorded raft barriers;
- applied/commit/last indexes are below recorded barriers;
- lease timestamps are missing or impossible;
- archive creation time is outside the freeze lease window, if recorded.

Validation:

```sh
go test ./internal/backup/cluster ./internal/backup/service
```

## Phase RF5 — Destructive K3s proof

Status: implemented in the script expectations; destructive execution must pass
before release sign-off.

Goal: prove the offline restore path with coherent raft metadata/storage.

Update `scripts/testK3sSystemBackupRestore.sh` expectations:

- require raft freeze evidence in `backup-set.json`;
- validate the backup set before wiping PVCs;
- restore each ordinal archive to matching PVC;
- verify all pods answer cluster status/health after restore;
- verify login, graph data, and blob payloads through every pod.

Validation:

```sh
make test-k3s-system-backup-restore
```

Acceptance:

- The test ends with `K3s system backup/restore validation passed`.
- The restored cluster is healthy from every pod.
- No pod hangs on daemon CLI calls after restore.

## Phase RF6 — Operations docs and release gate

Status: complete for operations docs and release-gate documentation.

Goal: make raft-storage-safe backup the documented production path.

Tasks:

- Update backup/restore operations docs to describe the freeze/checkpoint window.
- Document failure handling and TTL self-release semantics.
- Add the destructive K3s restore test to release-gate expectations only after
  RF5 passes reliably.
- Document residual risks and explicitly state whether raw raft storage remains
  the restore mechanism or whether a future snapshot/bootstrap restore will
  supersede it.

Validation:

```sh
make docs-check
git diff --check
```

## Open questions

- Should future implementations move from the current group-level mutation lock
  to a lower-level storage lock or raft transport/tick pause?
- Should the freeze TTL become operator-configurable or renewable for very large
  PVCs and slow backup volumes?
- Should archive creation record start/end timestamps in each per-node manifest
  for stronger freeze-window validation?
- Should the backup command require all raft groups to have leaders on the local
  expected node set before freeze, or is one verified linearizable read per group
  sufficient?
- Should production restore eventually move away from raw raft-log restore to a
  subsystem snapshot plus system-raft bootstrap format?
