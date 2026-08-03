# Phase B2 — Subsystem Snapshot Inventory

## Status

B2.0 inventory complete for the current V1 raft composition. This inventory classifies every raft-owned durable subsystem that participates in the daemon system or partition composite state machines before broad production snapshot/compaction is enabled.

Classification values:

- `snapshot-capable-now` — implements `consensus.StateMachineSnapshotter` and `consensus.StateMachineSnapshotRestorer` with raft snapshot install/catch-up tests.
- `snapshot-capable-initial` — implements partition-scoped snapshot/restore with focused direct restore tests, but broader production compaction remains blocked by other composite children or follow-up hardening.
- `needs-restore-api` — raft-owned durable state exists, but subsystem-specific snapshot/restore is not implemented yet.
- `derived/rebuildable` — state can be reconstructed from other authoritative raft-owned state and should not block compaction if explicitly documented.
- `unsafe-for-compaction` — snapshot/restore or external artifact semantics are not safe enough for production compaction.

## Current composite groups

### System group

Created in `internal/daemon/app/app.go` through `compositeSystemStateMachine`:

- `consensus.SystemStateMachine`
- `identityservice.UserRaftStateMachine`
- `identityservice.AdminRaftStateMachine`
- `backupservice.RaftStateMachine`
- `daemonsemantic.RaftStateMachine` for system/global semantic records

### Partition groups

Created in `internal/daemon/app/app.go` through `compositePartitionStateMachine`:

- `spaceservice.RaftStateMachine`
- `schemaservice.RaftStateMachine`
- `graphservice.RaftStateMachine`
- `blobservice.RaftStateMachine`
- `daemonsemantic.RaftStateMachine` for partition semantic records

## Inventory table

| Subsystem state machine | Group | Raft records / scope | Durable local state touched by apply | Existing reload/snapshot-like APIs | Dedupe/apply state | Classification | Proposed snapshot contents | Gaps / tests needed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `consensus.SystemStateMachine` | System | `system.cluster.bootstrap_metadata`, `system.cluster.register_node` | In-memory system metadata derived from raft log/snapshot. | `Snapshot()` and `RestoreSnapshot([]byte)` already exist. | No separate applied-command dedupe. | `snapshot-capable-now` | Versioned system metadata, nodes, placement. | Already covered by snapshot-only restart and lagging-follower snapshot install tests. |
| `identityservice.UserRaftStateMachine` | System | `identity.user.put.v1`, `identity.user.session.put.v1` | User store and user refresh/session stores under identity data dirs. | System-scoped raw JSON snapshot/restore exists for user and refresh-session stores. | Raft applied command IDs are included. | `snapshot-capable-initial` | Users including password hashes, refresh sessions, applied command IDs. | Need stronger atomic restore and login/session snapshot catch-up tests before production compaction. |
| `identityservice.AdminRaftStateMachine` | System | `identity.admin.put.v1`, `identity.admin.session.put.v1` | Admin/operator store and admin session store. | System-scoped raw JSON snapshot/restore exists for admin and refresh-session stores. | Raft applied command IDs are included. | `snapshot-capable-initial` | Operators/admins including password hashes/grants, admin sessions, applied command IDs. | Need stronger atomic restore and admin login/session snapshot catch-up tests before production compaction. |
| `backupservice.RaftStateMachine` | System | `daemon.backup.policy.update.v1`, `daemon.backup.delete.v1` | Backup policy/config/audit metadata; execution state may be local. | System-scoped JSON snapshot/restore exists for backup policy. Runtime executions and completed archives are explicitly excluded as local artifacts. | No separate persisted raft command dedupe. | `snapshot-capable-initial` | Backup policy/config only. | Completed archive inventory remains local/non-authoritative. Restore must not resurrect in-progress local executions as complete. |
| `daemonsemantic.RaftStateMachine` | System | `semantic.global.mutation.v1`, `semantic.accounting.mutation.v1` | Global semantic config/accounting/checkpoint stores. | System-scoped JSON snapshot/restore exists for global semantic metadata and accounting events. Vector index payloads remain derived/rebuildable. | Semantic raft applied command IDs are included by system command prefix. | `snapshot-capable-initial` plus `derived/rebuildable` vector state | Authoritative semantic config/accounting metadata; derived-state marker for vector indexes. | Need stronger freshness/degraded diagnostics after restore before production compaction. |
| `spaceservice.RaftStateMachine` | Partition | `space.create_with_default_domain.v1`, `space.domain.create.v1`, `space.domain.update.v1`, `space.domain.delete.v1`, `space.acl.grant.v1`, `space.delete.v1` | Spaces, domains, ACL/access stores. | Partition-scoped JSON raft snapshot/restore now exists for spaces, domains, ACL rules, and create-space idempotency results. | Create-space raft idempotency maps are included for matching partition spaces. | `snapshot-capable-initial` | Spaces, domains, ACL grants, create-space idempotency results per partition. | Need lagging-follower raft install coverage and stronger atomic partition replace before production compaction. |
| `schemaservice.RaftStateMachine` | Partition | `schema.put.v1`, `schema.delete.v1` | Domain schema store and schema metadata/checksums. | Partition-scoped JSON raft snapshot/restore now exists for domain schemas. | No separate persisted command-ID dedupe identified for schema. | `snapshot-capable-initial` | Domain schemas per partition; restore rebuilds validation cache through normal apply path. | Need corrupt/unknown-version restore tests and lagging-follower raft install coverage. |
| `graphservice.RaftStateMachine` | Partition | `graph.commit.v1` | Graph segment store: committed nodes, edges, revisions, payload metadata. | Partition-scoped JSON raft snapshot/restore now exists for live committed nodes/edges and revision padding. | Graph raft applied command IDs are merged from snapshot payload. | `snapshot-capable-initial` | Per-space committed graph latest state, revision, nodes, edges, and applied command IDs. | V1 snapshot is latest-state only; tombstone/history is not preserved. Need lagging-follower raft install coverage, stronger atomic restore, and Phase G consistency-report-after-catch-up tests. |
| `blobservice.RaftStateMachine` | Partition | `blob.meta.put.v1`, `blob.meta.delete.v1` | Blob metadata store and references to blob payloads. | Partition-scoped JSON snapshot/restore exists for blob metadata. Restore prevalidates blob IDs/digests and payload availability, fetching from configured raft peers when possible; missing payloads fail closed before metadata publish. | Partition command IDs are included only when the command ID references a snapshotted space. | `snapshot-capable-initial` | Blob metadata and payload safety descriptors; payload bytes remain content-addressed local/shared artifacts, not embedded in raft snapshots. | Need lagging-follower snapshot install tests with peer payload fetch before production compaction. |
| `daemonsemantic.RaftStateMachine` | Partition | `semantic.space.mutation.v1`, `semantic.maintenance.mutation.v1` | Space/domain semantic config, maintenance metadata/checkpoints. Vector indexes are derived/local. | Partition-scoped JSON snapshot/restore exists for semantic indexes, grants, policies, index states, policy decisions, dirty events, checkpoints, and dirty work items. Running work is reset to pending on restore. | Partition command IDs are included only when the command ID references a snapshotted space. | `snapshot-capable-initial` plus `derived/rebuildable` vector state | Authoritative semantic config/checkpoints/maintenance metadata; derived-state marker for vector indexes. | Need explicit freshness/degraded diagnostics and lagging-follower snapshot install tests before production compaction. |
| Automation service | System/partition future | Durable automation definitions/invocations/audit records are Phase D classified, but not currently in the raft composite shown above. | Automation durable stores. | No raft snapshot contract. | Automation raft ownership/fail-closed behavior exists in Phase D scope. | `needs-restore-api` when included in composite | Definitions, durable invocation/audit records, applied command IDs/checkpoints. | Must not duplicate in-flight worker-local executions. |
| Change stream service | Partition future | Change-stream durable records are Phase D classified, but not currently in the raft composite shown above. | Change-stream event/checkpoint stores. | No raft snapshot contract. | Change-stream raft ownership/fail-closed behavior exists in Phase D scope. | `needs-restore-api` when included in composite | Durable event/checkpoint metadata, applied command IDs/checkpoints. | Restore must not duplicate delivery without checkpoint semantics. |

## B2.0 conclusions

- System metadata is `snapshot-capable-now`.
- Current system and partition composite children are `snapshot-capable-now` or `snapshot-capable-initial`.
- Space, schema, graph, blob, identity, backup, and semantic snapshots have focused direct restore tests; broader lagging-follower snapshot-install and atomic-restore hardening remain before production auto-compaction.
- Graph snapshot V1 is latest-state plus revision, not full historical/tombstone preservation.
- Blob payload bytes remain outside raft snapshots; restore validates/fetches payloads and fails closed before publishing healthy metadata when payloads are missing.
- Semantic vector indexes remain derived/rebuildable unless a future design makes index payloads authoritative.
- Automation and change-stream snapshot work should follow their final raft composite inclusion point; they are not in the current composite.

## B2.1 impact

The composite state-machine snapshot contract must default to fail-closed:

- composite snapshots can only be created when every non-neutral child supports `Snapshot()`;
- composite restores require every non-neutral child to have a matching payload and `RestoreSnapshot([]byte)`;
- unknown, duplicate, missing, or checksum-mismatched child payloads fail restore;
- snapshot-neutral children must explicitly implement a marker interface and must not own durable raft state.

All current composite children now expose snapshot/restore, but production automatic compaction remains disabled until lagging-follower snapshot-install coverage, atomic-restore hardening, and soak gates are complete.
