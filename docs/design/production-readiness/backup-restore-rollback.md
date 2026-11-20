# Backup, restore, and rollback safety

Status: draft  
Last updated: 2026-11-20

## Production requirement

Backup, restore, and rollback safety is the top hard requirement before
CommonFolio stores real customer production data in Mycel.

A bad upgrade, operator error, data corruption event, or infrastructure failure
must not become unrecoverable customer data loss. Operators need an easy,
trustworthy way to create backups, verify them, restore them into a clean
cluster, and decide whether rollback means a binary rollback or a
restore-from-backup operation.

## Mandatory first-cut capabilities

Before CommonFolio production, Mycel should provide:

- on-demand backup before upgrades;
- scheduled backup to durable storage, likely S3;
- a backup manifest with checksums, format version, source cluster identity,
  included subsystems, and committed state markers;
- restore into a new clean cluster;
- backup verification;
- at least one successful restore drill from a release-candidate backup;
- a clear rollback runbook:
  - binary rollback when the older daemon can safely read the current state;
  - restore-from-backup when state has crossed a downgrade boundary.

## Data to protect

The backup boundary must include every durable subsystem needed to restore a
functional cluster:

- system/cluster metadata;
- Raft state and snapshots required for committed state recovery;
- graph state;
- identity and auth/session metadata needed for controlled recovery;
- space/domain metadata;
- attachment metadata and required object payload references;
- usage accounting counters or enough committed state to recompute them safely;
- projection metadata and readiness fences.

Derived state such as lexical and semantic indexes may be backed up or rebuilt,
but the restore manifest must say which path is expected.

## Quiescence assumption

The first production-ready backup design assumes the cluster is quiesced before
capturing backup artifacts. This means Mycel stops admitting new writes, drains
active writes, pauses background mutators, establishes the necessary committed
state/checkpoint barriers, and only then captures the backup set.

Reads may be allowed during the quiesced window only through a conservative
read-only allowlist. Backup and quiesce status/control APIs must remain
available. The initial design may keep the cluster quiesced for the full capture
and upload window; later work can shorten the write outage by creating an
immutable local/PVC snapshot under quiescence, releasing quiescence, and then
uploading that snapshot asynchronously.

## Backup repository model

A successful production backup should not live on the pods it protects. Pod-local
files are allowed only as temporary scratch space. The authoritative backup is a
complete backup set in a common, externally accessible backup repository.

The initial repository abstraction should be S3-compatible object storage:

- AWS/CommonFolio production uses Amazon S3.
- Non-AWS, self-hosted, local, and Mycel Lab deployments may use MinIO or
  another S3-compatible object store.

Backup correctness should rely on Mycel-authored manifests, explicit SHA256
checksums, and a final completion marker, not on backend-specific directory
semantics or object ETags. Restore should ignore incomplete backup sets and
accept only sets with a valid final manifest, all expected per-node artifacts,
matching checksums, and a completion marker.

The logical layout should group all per-pod artifacts under one backup-set prefix
such as:

```text
s3://<bucket>/<prefix>/clusters/<cluster_id>/backup-sets/<backup_set_id>/
  backup-set.json
  COMPLETE
  nodes/
    myceld-0/archive.tar.zst
    myceld-0/manifest.json
    myceld-1/archive.tar.zst
    myceld-1/manifest.json
```

For a 20-pod cluster, the backup set may contain 20 per-pod artifacts, but all
artifacts must be discoverable from one repository URI and one backup-set
manifest.

## Admin API direction

This section is design guidance only; it is not an immediate protobuf change.
The existing `mycel.admin.v1.AdminBackupService` is the right ownership boundary,
but it should be reviewed before implementation against this production backup
model.

The production-facing admin API should model:

- a configured backup repository, not a pod-local output directory;
- S3-compatible repository settings that cover both AWS S3 and MinIO;
- cluster backup as the primary production backup operation;
- cluster restore as a first-class operation, not an out-of-band procedure;
- validation of repositories and backup sets;
- quiesce/checkpoint/capture/upload/validation/finalization phases;
- a shared backup-set manifest with per-node artifacts;
- restore acceptance based on manifest, checksums, expected-node coverage, and a
  completion marker;
- credential references rather than raw object-store credentials in API
  messages.

Current path-oriented concepts such as `backup_dir`, `output_dir`, and
`backup_set_path` should be revisited. The future API should prefer repository
identifiers, logical `s3://` backup-set URIs, and repository configuration
resolved from deployment secrets or identity such as IRSA. Raw S3/MinIO access
keys should not be supplied in ordinary backup requests.

Potential admin operations to review include:

- configure/get/list backup repositories;
- validate backup repository access;
- start/get/list/cancel cluster backup;
- validate cluster backup set;
- start/get/list/cancel cluster restore;
- delete or expire a backup set according to retention policy.

The single-daemon backup API may remain useful for development, diagnostics, or
single-node deployments, but CommonFolio production should treat cluster backup
to a shared S3-compatible repository as the normative path.

## Validation expectations

A backup should not be considered production-safe until it can be restored and
verified in isolation. Minimum validation should include:

- manifest checksum validation;
- graph/domain identity checks;
- attachment manifest/object-reference checks;
- Raft/log consistency checks;
- projection/index rebuild requirement checks;
- usage counter availability or reconciliation checks;
- smoke tests for auth, graph read/write, and search after restore.

## Launch gate

CommonFolio production launch should require:

1. scheduled and on-demand backups working in the target AWS deployment;
2. restore into a clean environment;
3. documented and rehearsed rollback procedure;
4. restore verification evidence from at least one current release candidate;
5. alerts for backup failure or stale backup state.

## Follow-up design work

- Define the exact backup manifest schema.
- Decide which indexes are backed up versus rebuilt.
- Decide whether first production supports restore into a new cluster only or
  also in-place restore over an existing cluster.
- Define encryption, retention, and IAM defaults for S3-backed backups.
- Add Mycel Lab release-gate coverage for backup/restore and rollback drills.
