# Cluster System Backup Implementation Plan

## Status

Complete for the initial coordinated full-cluster backup implementation. This
plan implements the coordinated full-cluster backup design in
[Cluster system backup](../../design/backup-restore/cluster-system-backup.md)
and the target operator workflow in
[Cluster system backup and restore](../../operations/procedures/cluster-system-backup-restore.md).

The implementation replaces manual per-pod orchestration with a
system-raft-backed cluster backup coordinator, a backup-set manifest, and a
raft freeze/checkpoint archive window.

## Goals

- Add a single operator command that creates one complete backup set for the
  cluster.
- Fail closed unless all expected nodes are healthy, admitted, reachable, and
  caught up.
- Coordinate backup intent, phase transitions, quiesce, barriers, node results,
  and terminal state through system raft.
- Quiesce the whole cluster before local pod archives are created.
- Produce one archive and per-pod manifest per pod/PVC, plus one backup-set
  manifest.
- Include UTC date and pod name in each backup archive filename.
- Validate and restore from explicit pod ordinal mappings.
- Keep destructive restore offline and operator-driven.

## Non-goals

- No degraded or partial backup success state in the initial release.
- No automatic divergent PVC repair, merge, overwrite, or rebalance.
- No live in-place restore.
- No automatic secret export.
- No generated public SDK/API code commits unless explicitly approved.

## Current state

Already available:

- `mycel admin backup policy get/set`
- `mycel admin backup trigger`
- `mycel admin backup status/list/delete`
- local daemon archive creation outside `MYCELD_DATA_DIR`
- per-node daemon backup manifests and checksums
- daemon quiesce coordinator
- system raft metadata and cluster diagnostics
- K3s destructive restore mechanics in `make test-k3s-system-backup-restore`

Current limitation:

- `admin backup trigger` is node-local.
- A complete three-pod restore currently requires an external script to invoke
  backup once per pod and preserve ordinal mapping.
- There is no cluster-wide point-in-time backup barrier or backup-set manifest.

## Proposed API shape

This will likely require `mycel-api` changes. Do not commit generated SDK/API
code in this repo unless explicitly approved.

Candidate Admin Backup API additions:

```proto
service AdminBackupService {
  rpc TriggerClusterBackup(TriggerClusterBackupRequest) returns (TriggerClusterBackupResponse);
  rpc GetClusterBackupStatus(GetClusterBackupStatusRequest) returns (GetClusterBackupStatusResponse);
  rpc ListClusterBackups(ListClusterBackupsRequest) returns (ListClusterBackupsResponse);
  rpc ValidateClusterBackupSet(ValidateClusterBackupSetRequest) returns (ValidateClusterBackupSetResponse);
}
```

Candidate request fields:

- `reason`
- `output_dir`
- `archive_format`
- `quiesce_timeout_seconds`
- `backup_timeout_seconds`
- `barrier_timeout_seconds`
- `node_timeout_seconds`

Candidate response fields:

- `backup_set_id`
- `state`
- `cluster_id`
- `expected_nodes`
- `created_at`
- `completed_at`
- `backup_set_manifest_uri`
- per-node archive summaries
- failed phase and error details

CLI command shape:

```sh
mycel admin backup cluster trigger \
  --reason "before upgrade" \
  --output-dir /mnt/mycel-backups \
  --archive-format tar.zst

mycel admin backup cluster status BACKUP_SET_ID
mycel admin backup cluster list
mycel admin backup cluster validate --backup-set /mnt/mycel-backups/backup-set-...
```

## Raft-owned model

The system raft state machine owns cluster backup coordination records.

Suggested raft record types:

- `daemon.backup.cluster.request.v1`
- `daemon.backup.cluster.precheck.v1`
- `daemon.backup.cluster.quiesce.v1`
- `daemon.backup.cluster.barrier.v1`
- `daemon.backup.cluster.node-result.v1`
- `daemon.backup.cluster.complete.v1`
- `daemon.backup.cluster.fail.v1`
- `daemon.backup.cluster.abort.v1`

Suggested persisted run state:

```text
requested -> prechecking -> quiescing -> barrier_wait -> archiving -> validating -> committing_manifest -> succeeded
                                                           \-> failed
                                                           \-> aborted
```

System raft must enforce:

- only one active cluster backup at a time;
- membership/ordinal set is fixed at request time;
- terminal states are durable;
- failed/aborted state releases quiesce;
- node archive results are tied to one backup set ID and pod ordinal.

## Backup-set manifest V1

Create `internal/backup/cluster/manifest.go` or equivalent with V1 structures.

Required fields:

- `version`
- `backup_set_id`
- `created_at`
- `completed_at`
- `cluster_id`
- `complete`
- `state`
- `reason`
- `namespace`
- `statefulset`
- `expected_nodes`
- `data_dir`
- `archive_format`
- `image` / version metadata when available
- raft barriers per group
- per-node entries:
  - pod name
  - node ID
  - ordinal
  - archive name
  - archive URI/path
  - manifest name
  - manifest URI/path
  - size bytes
  - SHA-256 checksum
  - applied indexes at archive time

Do not embed Kubernetes Secret values, plaintext passwords, active tokens, or
application credentials. Record non-sensitive fingerprints or names only.

Archive filename format:

```text
mycel-system-<utc_timestamp>-<pod_name>-<backup_set_id>.<archive_ext>
mycel-system-<utc_timestamp>-<pod_name>-<backup_set_id>.manifest.json
```

## Implementation phases

### Phase 0 — Inventory and exact API decision

Status: complete for the first implementation tranche.

Decisions:

- Future public RPCs should live on the existing `AdminBackupService` under the
  `admin backup cluster` CLI namespace unless API review requires a split.
- Restore remains offline/operator-driven and out of daemon RPC scope.
- Backup-set manifest V1 is implemented internally with per-pod artifact
  URI/path fields so archives may live on per-pod backup mounts or object-store
  gateway paths.
- No generated public SDK/API code is committed in this tranche.

Deliverables:

- Confirm `mycel-api` proto additions and package naming.
- Confirm whether cluster backup APIs live on `AdminBackupService` or a new
  admin service.
- Confirm backup-set manifest V1 JSON schema.
- Confirm local output destination semantics:
  - shared RWX mount;
  - per-pod backup mount;
  - object-store gateway path;
  - local path copied by operator automation.
- Confirm restore remains offline and explicitly out of daemon RPC scope.

Validation:

```sh
make docs-check
git diff --check
```

### Phase 1 — Backup-set manifest package

Status: complete for V1 internal manifest validation.

Implemented files:

```text
internal/backup/cluster/manifest.go
internal/backup/cluster/manifest_test.go
```

Add a focused package for backup-set metadata and validation.

Suggested files:

```text
internal/backup/cluster/manifest.go
internal/backup/cluster/manifest_test.go
```

Implement:

- deterministic JSON encode/decode;
- checksum validation;
- ordinal/pod uniqueness checks;
- archive filename validation;
- complete/incomplete state validation;
- path/URI fields without assuming all artifacts share one filesystem.

Acceptance criteria:

- Invalid manifests reject missing pod entries, duplicate ordinals, checksum
  mismatches, missing pod names in filenames, and `complete=false` for restore.
- Tests do not require Kubernetes.

Validation:

```sh
go test ./internal/backup/cluster
```

### Phase 2 — Cluster backup raft state machine records

Status: complete for internal lifecycle records and snapshot/replay state.

Implemented files:

```text
internal/backup/service/cluster_backup.go
internal/backup/service/cluster_backup_test.go
internal/backup/service/raft.go
internal/backup/service/raft_snapshot.go
internal/backup/service/raft_snapshot_test.go
internal/backup/service/wal.go
```

Extend the backup subsystem's raft integration to track cluster backup runs.

Implement:

- raft record types for cluster backup lifecycle;
- active-run guard;
- terminal state persistence;
- replay from raft log/snapshot;
- snapshot/restore coverage for active and historical cluster backup records.

Acceptance criteria:

- Only one active cluster backup is allowed.
- Replayed raft state preserves terminal status and active-run lock.
- Snapshot restore preserves enough state to avoid duplicate active backups.

Validation:

```sh
go test ./internal/backup/service
make test-phase-d
```

### Phase 3 — Cluster precheck collector

Status: complete for the initial fail-closed precheck evaluator. Later phases
will wire this to concrete backend peer RPCs and coordinator execution.

Implemented files:

```text
internal/backup/service/cluster_precheck.go
internal/backup/service/cluster_backup_test.go
```

Add a coordinator precheck that fails before quiesce unless the cluster is safe.

Inputs:

- authoritative system raft metadata;
- cluster manager readiness/health;
- raft group status;
- backend peer reachability;
- local backup destination checks from every expected node.

Preconditions:

- all expected nodes present and reachable;
- all nodes Ready/admitted;
- same cluster ID everywhere;
- every raft group has quorum;
- every expected replica can reach the requested barrier;
- backup destination mounted, writable, and outside data dir;
- no active backup/quiesce/recovery conflict.

Acceptance criteria:

- Any missing/stale/unhealthy node prevents quiesce.
- Error messages identify the failing node/precondition.
- Unit tests cover each fail-closed condition.

Validation:

```sh
go test ./internal/backup/service ./internal/clustering/...
make test-phase-g
```

### Phase 4 — Cluster-wide quiesce epoch

Status: complete for backend/local archive execution. Coordinator fan-out and
public operator entrypoints are in later phases.

Implemented files:

```text
internal/backup/service/cluster_execution.go
internal/backup/service/module.go
internal/clustering/backend/service_backup.go
```

Extend quiesce handling so a cluster backup can create one raft-recorded quiesce
epoch across all nodes.

Implement:

- backup set ID as quiesce reason/epoch;
- no new writes admitted cluster-wide;
- in-flight writes drain;
- background mutators pause:
  - semantic maintenance;
  - automation side effects;
  - blob cleanup;
  - scheduled backup jobs;
  - other mutating subsystem workers;
- safe release on success, failure, timeout, or abort.

Acceptance criteria:

- New writes fail with actionable retryable errors during backup quiesce.
- Quiesce releases even if one node archive fails.
- Existing per-daemon quiesce tests still pass.

Validation:

```sh
go test ./internal/runtime/quiesce ./internal/daemon/runtime ./internal/backup/service
make test-phase-f
```

### Phase 5 — Raft backup barrier

Status: complete for local barrier collection/wait helpers and backend archive
request enforcement. Coordinator fan-out and failure-state wiring are in Phase 7.

Implemented files:

```text
internal/backup/service/cluster_execution.go
internal/backup/service/cluster_backup.go
internal/backup/cluster/manifest.go
```

After quiesce, record target raft indexes and wait until every expected node has
applied the required barrier.

Implement:

- barrier collection per raft group;
- wait/apply checks per expected node;
- timeout and failure state;
- barrier details in backup-set manifest.

Acceptance criteria:

- Backups fail if a node cannot reach the barrier.
- Barrier indexes appear in final backup-set manifest.
- Tests cover lagging follower and timeout paths.

Validation:

```sh
go test ./internal/clustering/consensus ./internal/backup/service
make test-phase-d
make test-phase-f
```

### Phase 6 — Backend RPC for local archive creation

Status: complete for the internal backend RPC/client/provider surface and local
archive creation implementation.

Implemented files:

```text
internal/clustering/proto/mycel/cluster/v1/backend.proto
internal/clustering/backend/service_backup.go
internal/clustering/backend/client_backup.go
internal/backup/service/cluster_execution.go
internal/backup/manager.go
```

Add an internal/backend RPC or equivalent peer call that asks each node to create
its local archive for a backup set.

The request should include:

- backup set ID;
- UTC timestamp;
- pod name / expected ordinal;
- output directory/URI;
- archive format;
- timeout;
- expected barrier indexes.

The response should include:

- pod name;
- node ID;
- ordinal;
- archive path/URI/name;
- manifest path/URI/name;
- size;
- checksum;
- applied indexes;
- warnings/errors.

Acceptance criteria:

- Archive output filenames include UTC timestamp and pod name.
- Local archive destination is outside `MYCELD_DATA_DIR`.
- Peer call rejects wrong backup set ID, wrong pod/ordinal, unsafe paths, and
  stale barriers.

Validation:

```sh
go test ./internal/clustering/backend ./internal/backup/...
```

### Phase 7 — Coordinator execution and backup-set manifest write

Status: complete for the initial daemon coordinator.

Implemented files:

```text
internal/backup/service/cluster_coordinator.go
internal/backup/service/cluster_backup.go
internal/backup/cluster/manifest.go
```

Implement coordinator orchestration:

1. commit request;
2. precheck;
3. quiesce;
4. barrier wait;
5. dispatch per-node archive calls;
6. validate node results;
7. write `backup-set.json`;
8. commit success;
9. release quiesce.

Failure path:

1. commit failed/aborted state;
2. preserve partial artifacts as failed evidence;
3. release quiesce;
4. return actionable error.

Acceptance criteria:

- One command creates a complete backup set when all nodes are healthy.
- Any missing node or failed archive makes the backup set failed, not partial
  success.
- `backup-set.json` includes every expected pod and checksum.

Validation:

```sh
go test ./internal/backup/service ./internal/daemon/api/admin ./internal/cli/cmd
make test-phase-g
```

### Phase 8 — Admin API and CLI

Status: complete for the initial AdminBackupService RPCs and CLI commands.

Implemented files:

```text
mycel-api/api/proto/mycel/admin/v1/backup.proto
internal/daemon/api/admin/backup_service.go
internal/cli/cmd/backup.go
```

Add operator-facing command support.

CLI commands:

```sh
mycel admin backup cluster trigger
mycel admin backup cluster status BACKUP_SET_ID
mycel admin backup cluster list
mycel admin backup cluster validate --backup-set PATH_OR_URI
```

Acceptance criteria:

- Trigger works from any healthy node and routes/coordinators correctly.
- JSON output includes backup set ID, state, cluster ID, and node artifact list.
- Human output explains where artifacts were written.
- Validate rejects incomplete or mismatched backup sets.

Validation:

```sh
go test ./internal/daemon/api/admin ./internal/cli/cmd
make docs-check
```

### Phase 9 — K3s destructive validation update

Status: complete for the script path; destructive execution remains operator-run.

Implemented file:

```text
scripts/testK3sSystemBackupRestore.sh
```

Update `scripts/testK3sSystemBackupRestore.sh` to use one coordinated cluster
backup command instead of invoking local backup on each pod.

Test must still:

1. create a three-pod cluster;
2. create user/space/domain graph data;
3. create blob-backed graph nodes;
4. run one cluster backup trigger;
5. verify `backup-set.json`, one archive per pod, and checksums;
6. wipe namespace and PVCs;
7. restore archives to matching ordinal PVCs;
8. restart StatefulSet;
9. verify login, graph data, blob payloads, cluster identity, and health.

Acceptance criteria:

- Final success line remains:

```text
K3s system backup/restore validation passed
```

Validation:

```sh
make test-k3s-system-backup-restore
```

### Phase 10 — Documentation and operational polish

Status: complete for command/procedure updates in this tranche.

Update:

- [Cluster system backup design](../../design/backup-restore/cluster-system-backup.md)
- [Cluster system backup and restore procedure](../../operations/procedures/cluster-system-backup-restore.md)
- [Backup and restore procedures](../../operations/procedures/backup-restore.md)
- [K3s cluster validation](../../operations/procedures/k3s-cluster-validation.md)
- [Raft cluster test matrix](../../operations/procedures/raft-cluster-test-matrix.md)
- CLI docs under `docs/operations/cli/`

Validation:

```sh
make docs-check
git diff --check
```

## Acceptance criteria

- A single operator command creates a complete backup set for a healthy cluster.
- The command fails before quiesce if any expected node is unavailable or unsafe.
- The whole cluster is quiesced before filesystem archive creation.
- Raft barriers are recorded and all expected nodes reach them before archive
  creation.
- Raft freeze/checkpoint leases are acquired before local archive creation,
  recorded in `backup-set.json`, and released before node results or terminal
  state are committed through system raft.
- Backup archive filenames include UTC date and pod name.
- `backup-set.json` records pod/ordinal/archive/checksum mapping.
- Restore validation rejects missing, duplicated, mismatched, or incomplete
  backup sets.
- Destructive K3s system backup/restore validation passes using the coordinated
  command.
- No degraded backup mode is exposed by default.
- No automatic divergent PVC repair or live restore is introduced.

## Open questions

- Should cluster backup API live on `AdminBackupService` or a new
  `AdminClusterBackupService`?
- Should backup-set artifacts be written by pods directly to final storage, or
  should the coordinator collect/copy them after local creation?
- Do we need explicit object-store support now, or is file/mounted path support
  enough for the first release?
- How should image digest/version be discovered reliably from inside the daemon?
- Should `backup-set.json` be committed into system raft as full JSON or only as
  a checksum/URI plus structured summary?
- What is the maximum retained history for cluster backup runs in raft state?

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Cluster stays quiesced after failure | Commit terminal failed/aborted state and always release quiesce in deferred cleanup. Add failure-path tests. |
| Backup set spans inconsistent raft indexes | Establish raft barriers after quiesce and require every expected node to apply them. |
| Raw raft storage mutates while archives are created | Acquire TTL-bound raft freeze/checkpoint leases, flush local raft storage, archive while frozen, and reject restore-mode manifests without freeze evidence. |
| One pod writes archive to the wrong destination | Validate backup destination per pod and record artifact URI/checksum in manifest. |
| Operator restores archives to wrong ordinals | Include pod name in filenames and validate ordinal mapping before restore. |
| Partial backup mistaken for complete | No degraded mode; `complete=true` only after all expected node results validate. |
| Secrets leak into backup-set manifest | Store only non-sensitive names/fingerprints; document secret-manager requirement. |
| Generated API drift | Keep generated public SDK/API code out of this repo unless explicitly approved. |
