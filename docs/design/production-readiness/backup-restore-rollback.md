# Backup, restore, and rollback safety

Status: draft  
Last updated: 2026-11-19

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
