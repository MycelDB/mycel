# Phase B2 — Subsystem Snapshot Recovery Implementation Plan

## Status

In progress. B2.0 inventory is complete in `phase-b2-subsystem-snapshot-inventory.md`. B2.1 composite snapshot envelope/fail-closed contract is implemented for the current daemon composite state machines. B2.2 has initial partition-scoped space/schema snapshot/restore support with focused direct restore tests. B2.3 has initial partition-scoped graph latest-state snapshot/restore support with focused direct restore tests. B2.4 has initial blob metadata snapshot/restore with fail-closed payload availability validation. B2.5 has initial identity, semantic, and backup snapshots; automation/change-stream remain out of the current composite and fail closed/local until raft-owned. B2.6 adds conservative/off-by-default compaction configuration without enabling an automatic compaction loop. B2.7 extends soak flags but refuses destructive forced snapshot/PVC replacement until a safe admin harness exists. Phase B V1 now has durable raft storage, generic snapshot plumbing for snapshot-capable state machines, system metadata snapshot-only restart/catch-up tests, and restart/rejoin gates. Phase B2 closes the remaining production hardening gap: every durable raft-owned subsystem in production composite state machines must have explicit snapshot/restore semantics before automatic raft log compaction and snapshot-only partition catch-up are enabled broadly.

This phase must preserve the current fail-closed safety model. A group may only create production snapshots when all durable child state machines in that composite group can either restore from the snapshot payload or are explicitly documented as derived/rebuildable from included raft-owned state.

## Goals

- Define snapshot contracts for raft-owned durable subsystems.
- Implement subsystem snapshot/restore for production system and partition composites.
- Enable safe composite snapshots only when all children are snapshot-capable or explicitly snapshot-neutral.
- Add compaction/catch-up tests proving a follower can recover from snapshot-only evidence.
- Keep automatic destructive repair, merge, rebalance, and divergent-PVC reuse out of scope.

## Non-goals

- No automatic repair of already divergent PVCs.
- No automatic merge of conflicting graph/PVC exports.
- No historical graph diff or common-revision repair beyond Phase G V1 latest-state tooling.
- No blob payload transport redesign unless explicitly required by the blob snapshot contract.
- No production auto-compaction until subsystem snapshot contracts and tests are complete.

## Current baseline

Implemented before this phase:

- `consensus.StateMachineSnapshotter` and `consensus.StateMachineSnapshotRestorer` optional interfaces.
- `Group.CreateSnapshot(index, compact)` for snapshot-capable state machines.
- Startup restore-then-replay path:
  - restore persisted raft snapshot data;
  - replay only committed entries after the restored snapshot index;
  - seed `raft.Config.Applied` and local applied index.
- `Ready` snapshot installation restores state-machine payload and marks applied.
- Non-empty snapshots fail closed when a state machine cannot restore them.
- System metadata snapshot tests cover snapshot-only restart and lagging-follower snapshot installation.

Remaining gap:

- Most production subsystem raft state machines are apply-only.
- Composite state machines now expose snapshot/restore, but fail closed until every non-neutral durable child is snapshot-capable.
- Partition subsystem payload formats are not defined.
- No production automatic snapshot/compaction policy exists.

## Snapshot contract principles

Every snapshot-capable subsystem must document:

1. **Scope** — system group or partition group, and which records are covered.
2. **Payload format** — versioned JSON or protobuf envelope; stable field names; deterministic ordering where possible.
3. **Restore semantics** — replace-vs-merge behavior, validation, and fail-closed errors.
4. **Dedupe/apply state** — whether applied command IDs or raft indexes are included or safely derivable.
5. **External artifacts** — file/blob/vector payloads referenced by metadata and how missing artifacts are handled.
6. **Compatibility** — behavior when loading unknown versions or missing fields.
7. **Atomicity** — how restore avoids partially applied local state.
8. **Observability** — snapshot version, item counts, checksums, and restore errors in logs/diagnostics.

Default rule: restore should be **replace-current materialized state with snapshot state**, not merge, unless a subsystem proves merge is safe and deterministic.

## Phase B2.0 — Inventory and classification

### Status

Complete. See `phase-b2-subsystem-snapshot-inventory.md`.

### Tasks

Create `docs/implementation/phase-b2-subsystem-snapshot-inventory.md` with one row per raft state machine:

- system metadata;
- user identity;
- admin identity;
- backup metadata;
- semantic system metadata;
- space/domain;
- schema;
- graph;
- blob metadata;
- semantic partition metadata/checkpoints;
- automation/change-stream if/when included in composite raft groups.

For each subsystem, record:

- current raft command record types;
- durable local stores mutated by `ApplyCommand`;
- existing reload/snapshot-like APIs;
- whether applied-command dedupe exists and where it is stored;
- whether state is authoritative, derived, or externally backed;
- proposed snapshot payload contents;
- missing restore primitives;
- tests needed.

### Acceptance

- Inventory proves no raft-owned durable subsystem is omitted.
- Each subsystem has a proposed classification:
  - `snapshot-capable-now`;
  - `needs-restore-api`;
  - `derived/rebuildable`;
  - `unsafe-for-compaction`.

## Phase B2.1 — Snapshot envelope and composite contract

### Status

Implemented for the current daemon composite state machines. Composite snapshots use a versioned JSON envelope with child names, payload bytes, and SHA-256 checksums. Snapshot creation and restore fail closed for unsupported durable children, duplicate children, missing children, unknown children, checksum mismatch, and group-kind mismatch. Snapshot-neutral children require an explicit marker interface.

### Tasks

- Add a small versioned snapshot envelope type in the relevant package, likely `internal/clustering/consensus` or `internal/daemon/app`:

  ```json
  {
    "version": 1,
    "group_kind": "system|partition",
    "partition_id": 0,
    "children": [
      {"name": "graph", "version": 1, "payload": {...}, "checksum": "..."}
    ]
  }
  ```

- Define composite child interface expectations:
  - child name is stable;
  - child snapshot payload is versioned;
  - child restore fails on unknown incompatible version;
  - child restore is idempotent for the same payload.
- Implement composite snapshot/restore only when every non-nil child supports required interfaces or explicitly declares itself snapshot-neutral.
- Keep composite snapshot creation fail-closed if any durable child is not snapshot-capable.

### Acceptance

- Unit tests prove composite snapshot refuses unsupported durable children.
- Unit tests prove composite restore dispatches by stable child name and fails on missing/unknown child payloads.
- No production partition group can accidentally compact with incomplete subsystem coverage.

## Phase B2.2 — Space and schema subsystem snapshots

### Status

Initial implementation complete. Space and schema raft state machines now implement partition-scoped JSON snapshot/restore. Focused tests cover direct snapshot restore for spaces/domains/ACL grants/create-space idempotency and domain schemas/cache rebuild. Lagging-follower raft snapshot install coverage and stronger atomic partition replacement remain follow-up hardening before production compaction.

Start with space and schema because they are relatively bounded and core to graph validation.

### Tasks

- Add snapshot/restore APIs to the space subsystem raft state machine:
  - spaces;
  - domains;
  - access/ACL data if partition-owned in raft mode;
  - applied-command dedupe if required.
- Add snapshot/restore APIs to schema subsystem raft state machine:
  - domain schemas;
  - schema versions/checksums;
  - validation metadata;
  - applied-command dedupe if required.
- Prefer subsystem-owned snapshot methods over daemon-level file copying.
- Restore into a temp/staging location or memory structure first; atomically replace durable state only after validation.

### Tests

- Single-node snapshot-only restart for space/schema.
- Three-node lagging follower receives snapshot after leader compaction.
- Existing Phase D multi-subsystem restart still passes.
- Restore rejects corrupt/unknown snapshot versions.

### Acceptance

- Space/schema can catch up from snapshot-only evidence.
- Composite partition snapshots remain disabled unless every other durable child is covered or explicitly excluded in tests.

## Phase B2.3 — Graph subsystem snapshots

### Status

Initial implementation complete. Graph raft state machine now implements partition-scoped JSON snapshot/restore for latest committed nodes/edges, per-space revision, and merged raft applied command IDs. Focused tests cover direct snapshot restore and Phase G local consistency stats after restore. V1 graph snapshots are latest-state only; tombstone/history preservation, lagging-follower raft snapshot install coverage, stronger atomic restore, and consistency-report-after-catch-up tests remain follow-up hardening before production compaction.

Graph is the highest-value partition subsystem and must be handled carefully.

### Tasks

- Define graph snapshot payload:
  - space ID;
  - domain ID;
  - committed revision;
  - node records;
  - edge records;
  - tombstone/history policy for V1;
  - deterministic checksums compatible with Phase G where possible.
- Decide whether snapshot data embeds graph entities directly or references a sidecar snapshot artifact.
- Include enough metadata to validate:
  - node/edge counts;
  - graph checksum;
  - domain/schema compatibility marker;
  - raft partition ID.
- Restore must replace committed graph state for the covered partition/domain set atomically.
- Ensure in-flight session/transaction overlays remain excluded; they are home-node local and fail on home-node loss in V1.

### Tests

- Snapshot-only restart restores committed graph data.
- Lagging follower installs graph snapshot after compaction and then applies post-snapshot entries.
- Phase G consistency report returns `consistent` after snapshot catch-up.
- Corrupt/mismatched graph snapshot fails closed.
- Read-index strong reads work after snapshot restore.

### Acceptance

- A follower missing compacted graph log entries can rejoin and serve consistent committed reads after snapshot installation.
- Graph snapshot/restore does not create local-only divergent state.

## Phase B2.4 — Blob metadata and payload safety

### Status

Initial implementation complete. Blob raft snapshots are partition-scoped and contain metadata only. Restore validates space partition, blob ID/digest/size, and payload availability before publishing metadata. Missing local-only payloads fail closed; configured raft peers may be used to fetch payloads through the existing backend payload path. Payload bytes remain outside raft snapshots.

Blob metadata is raft-owned, but payload availability may depend on local/shared/object storage.

### Tasks

- Define blob snapshot payload for metadata only.
- Explicitly validate payload references on restore:
  - shared/object-backed payloads may be referenced directly;
  - local-only payloads require presence checks or fail not-ready until fetched/rebuilt;
  - missing payloads must not silently produce healthy metadata.
- Decide whether blob payload checksums belong in this phase or a follow-up blob integrity phase.

### Tests

- Metadata snapshot restore with available payloads succeeds.
- Restore with missing required local payload fails closed or marks subsystem degraded/not-ready.
- Graph blob-node references remain safe after snapshot catch-up.

### Acceptance

- Snapshot catch-up cannot create blob metadata that points to silently missing payloads.

## Phase B2.5 — Semantic, backup, automation, and change-stream snapshots

### Status

Initial implementation complete for current composite children. Identity user/admin snapshots were also added because they participate in the system composite. Semantic snapshots cover system global/accounting metadata and partition semantic/maintenance metadata, with vector indexes explicitly marked derived/rebuildable and running work reset to pending on restore. Backup snapshots cover policy/config only and explicitly exclude running executions and completed local archives. Automation and change-stream are not current raft composite children; their durable worker/local state remains outside raft snapshots until future raft-owned inclusion.

### Tasks

- Semantic:
  - snapshot authoritative config/checkpoints/accounting metadata;
  - keep vector indexes derived/rebuildable unless explicitly made authoritative;
  - expose freshness/rebuild state after restore.
- Backup:
  - snapshot policy/config/audit metadata as raft-owned state;
  - avoid resuming in-progress local executions as if they were cluster-complete.
- Automation/change-stream:
  - snapshot definitions, durable invocation/audit records, and checkpoints if raft-owned;
  - keep worker-local execution state derived/local.

### Tests

- Snapshot-only restart for each authoritative metadata set.
- Restore does not duplicate in-progress jobs or change-stream deliveries.
- Derived indexes/checkpoints report rebuild/freshness state explicitly.

### Acceptance

- Non-graph subsystems have clear authoritative-vs-derived restore behavior.

## Phase B2.6 — Production compaction policy

### Status

Initial configuration boundary complete. `MYCELD_CLUSTER_RAFT_COMPACTION_MODE` defaults to `off`; threshold knobs are parsed/validated but no automatic production compaction loop is enabled. This preserves the conservative boundary until lagging-follower install tests, atomic restore hardening, and soak gates pass.

Only after subsystem coverage is complete.

### Tasks

- Add configurable raft snapshot/compaction thresholds:
  - entries since snapshot;
  - elapsed time;
  - max persisted raft log bytes;
  - minimum retain entries after snapshot.
- Default conservative/off until release gates prove safe.
- Expose snapshot/compaction diagnostics in admin APIs/CLI:
  - last snapshot index/term/time;
  - last compaction index/time;
  - snapshot failures;
  - restore failures;
  - per-group snapshot capability status.
- Ensure compaction never advances beyond the latest successfully restored and durable snapshot.

### Tests

- Unit tests for threshold decisions.
- Multi-node test where follower is offline past compaction and catches up by snapshot.
- Restart after compaction uses snapshot plus post-snapshot entries.
- Read-index diagnostics remain healthy after compaction.

### Acceptance

- Production groups can compact logs without sacrificing follower recovery.

## Phase B2.7 — Destructive/soak validation

### Status

Initial soak flag plumbing complete. `testClusterSoak.sh` accepts `MYCEL_CLUSTER_SOAK_WRITES`, `MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS`, and `MYCEL_CLUSTER_SOAK_REPLACE_PVC`; forced snapshots and PVC replacement currently fail explicitly instead of silently doing nothing or mutating volumes without a safe harness.

### Tasks

Extend optional soak, not default CI at first:

```sh
make test-cluster-soak
```

Add mode/env flags to force snapshot/compaction:

```sh
MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS=true
MYCEL_CLUSTER_SOAK_WRITES=...
MYCEL_CLUSTER_SOAK_RESTART_EVERY=...
MYCEL_CLUSTER_SOAK_REPLACE_PVC=true
```

Workload:

- create many spaces/domains/schemas/graph entities;
- force snapshot/compaction;
- hold one follower offline beyond retained entries;
- rejoin follower and require snapshot catch-up;
- run Phase G consistency report;
- run strong read/query checks through every pod.

### Acceptance

- Optional soak proves snapshot-only catch-up in Compose/K3s before enabling production compaction by default.

## Phase B2.8 — Documentation and operator guidance

### Status

In progress with the snapshot inventory and this implementation plan updated. Operator docs now need the final command examples and troubleshooting guidance before a release that enables automatic compaction.

### Tasks

Update:

- `docs/design/clustering-replication-reliability.md`;
- `docs/implementation/phase-b-durable-raft-runtime-audit.md`;
- `docs/operations/raft-cluster-operations.md`;
- `docs/operations/raft-cluster-test-matrix.md`;
- `docs/makefile_commands.md`.

Document:

- snapshot capability matrix;
- compaction defaults;
- how to inspect snapshot/restore diagnostics;
- what to do after restore failure;
- what remains unsupported for known-divergent PVCs.

### Acceptance

- Operators can distinguish normal snapshot catch-up from divergent-PVC repair.
- Docs keep Phase G manual repair boundaries intact.

## Release gates

Before enabling production auto-compaction:

```sh
make test-phase-d
make test-phase-f
make test-phase-g
go test ./...
git diff --check
make test-compose-cluster
make test-k3s-cluster
```

Before major clustering release:

```sh
make test-cluster-release-gate
MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS=true make test-cluster-soak
```

## Risk register

| Risk | Mitigation |
| --- | --- |
| Snapshot payload omits durable state. | Inventory every raft record type and durable store; composite snapshots fail closed until all children are covered. |
| Snapshot data and metadata index mismatch. | `Group.CreateSnapshot` snapshots only current applied state. Keep this invariant in tests. |
| Replaying entries already included in snapshot duplicates effects. | Startup seeds raft applied index and replays only entries after snapshot index; tests cover non-idempotent counter state machine. |
| Blob metadata restores without payloads. | Blob restore validates payload availability or marks subsystem degraded/not-ready. |
| Derived semantic/vector state appears authoritative. | Snapshot only authoritative metadata/checkpoints; expose rebuild/freshness state. |
| Restore partially mutates local files. | Restore through staging/atomic replacement and validate before publish. |
| Automatic compaction enabled too early. | Default off/conservative until subsystem gates and soak pass. |
| Existing divergent PVCs mistaken for snapshot catch-up. | Keep Phase G manual repair docs explicit: snapshot catch-up is for admitted replicas in one authoritative cluster, not split-brain merge. |

## Suggested implementation order

1. B2.0 inventory.
2. B2.1 composite envelope and fail-closed composite snapshot contract.
3. B2.2 space/schema snapshots.
4. B2.3 graph snapshots.
5. B2.4 blob metadata/payload safety.
6. B2.5 semantic/backup/automation/change-stream metadata.
7. B2.6 production compaction policy.
8. B2.7 soak validation.
9. B2.8 docs/operator guidance.
