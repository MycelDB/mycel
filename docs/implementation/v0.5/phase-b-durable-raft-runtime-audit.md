# Phase B — Durable Raft Runtime Audit

## Status

Phase B is **partially complete for V1, with generic snapshot restore implemented and initial Phase B2 subsystem snapshot contracts now available for current composite state machines**.

Implemented and validated:

- daemon raft startup wires file-backed raft storage under `<data-dir>/meta/raft`;
- the raft `Ready` loop persists snapshots, hard state, and entries before sending messages/applying committed entries;
- system and partition groups restart from persisted raft state when present;
- committed log entries are replayed into fresh state machines on restart;
- snapshot-capable state machines can now create and restore raft snapshot payloads;
- current daemon composite children have initial snapshot/restore implementations for system metadata, identity user/admin, backup policy, space, schema, graph latest state, blob metadata with payload validation, and semantic metadata;
- startup restores a persisted raft snapshot before replaying post-snapshot committed entries;
- raft `Ready` snapshot installation restores state-machine snapshot data and advances the applied index;
- non-empty snapshot data fails closed when the state machine cannot restore it;
- persistent storage tests cover hard state, entries, conf state, snapshots, compaction, and reload;
- in-process restart tests cover multi-subsystem convergence after restart;
- system metadata tests cover snapshot-only restart and lagging-follower snapshot installation;
- destructive Compose/K3s gates validate real pod data-plane behavior across restart/rejoin scenarios, including K3s single-PVC replacement.

Remaining Phase B gap:

- initial subsystem snapshot formats/restore hooks exist for current composite children, but production automatic compaction should stay disabled until lagging-follower snapshot-install coverage, stronger atomic restore behavior, and destructive forced-snapshot soak gates prove the contracts under real catch-up conditions.

## Audit table

| Phase B requirement | Status | Evidence | Remaining risk |
| --- | --- | --- | --- |
| Wire persistent raft storage into daemon group startup. | Complete when `DataDir` is set. | `internal/daemon/app/raft_experimental.go` builds `storageDir = <data-dir>/meta/raft` and passes it to `consensus.StartMultiGroup`. `internal/clustering/consensus/multigroup.go` creates persistent storage for system and partition groups from `StorageDir`. | If `DataDir` is blank, storage falls back to in-memory. Normal daemon config should provide a data dir; keep this as a dev/test-only fallback. |
| Persist hard state, entries, and snapshots per group. | Complete for raft storage. | `internal/clustering/consensus/storage.go` persists `hard_state.pb`, `entries.pb`, `snapshot.pb`, and `conf_state.pb`. `internal/clustering/consensus/group.go` persists `Ready` snapshot/hard-state/entries before sending/applying. `internal/clustering/consensus/storage_test.go` covers reload and snapshot behavior. | Snapshot files are persisted, but state-machine data inside snapshots is not generally consumed by subsystem state machines. |
| Restart existing raft groups from persisted state. | Complete for un-compacted log replay. | `StartGroup` chooses `raft.RestartNode` when storage has hard state or entries and can replay committed entries before restart. `StartMultiGroup` enables committed-entry replay for system and persistent partition groups. | If entries needed to rebuild state are compacted below `FirstIndex`, replay does not restore state-machine data from snapshot. |
| Snapshot creation/restoration for system and partition state machines. | Initial composite coverage. | `StateMachineSnapshotter`/`StateMachineSnapshotRestorer` define the optional contract. `Group.CreateSnapshot` serializes snapshot-capable state machines. Startup and raft `Ready` snapshot install restore non-empty snapshot data before continuing. System metadata tests cover snapshot-only restart and lagging-follower install; B2 direct restore tests cover current subsystem contracts. | Production compaction remains disabled until lagging-follower install and atomic-restore hardening cover all current subsystem contracts. |
| Recover graph/space/blob/schema/semantic state from raft log/snapshot. | Initial subsystem snapshot contracts exist. | Daemon wires composite partition state machines for space, schema, graph, blob, and semantic. Phase D integration tests restart a persistent multi-subsystem cluster and verify convergence. B2 direct restore tests cover space/schema/graph/blob/semantic snapshots. | Blob payload bytes remain outside raft snapshots and must be available/fetched before metadata publish; graph snapshots are latest-state V1; vector indexes remain derived/rebuildable. |
| Restart tests proving no graph divergence after restart. | Substantial coverage. | `TestPhaseDMultiSubsystemRaftConvergesAndRestarts` verifies graph state across all nodes after restart. Compose gate restarts all myceld containers and revalidates pre-existing data through every pod/service. K3s gate validates rolling restart. | Coverage is representative, not exhaustive for arbitrary crash timing or every domain. Destructive gates are pre-release/manual, not default unit CI. |
| PVC loss for one follower triggers rejoin/catch-up or safe failure. | Partial. | `scripts/testK3sCluster.sh` scales down the last pod, deletes its PVC, scales back up, then validates cluster identity and existing data-plane state. | No focused in-process follower-PVC-loss test. Catch-up is not proved for compacted logs/snapshot-only recovery. |

## Current V1 conclusion

Phase B should be recorded as **V1 partially complete** rather than missing:

- persistent raft storage wiring is implemented;
- restart/rejoin validation exists;
- the old claim that daemon raft groups are memory-only is stale;
- generic snapshot restore now exists for snapshot-capable state machines;
- subsystem-specific partition snapshot formats now exist at an initial level, but forced snapshot-only follower recovery remains the real unresolved production correctness boundary.

This is acceptable for the current V1 release gate only with the existing constraints:

- do not roll a fixed image across known-divergent PVCs expecting repair;
- use Phase G forensic/export workflows for existing divergence;
- keep destructive Compose/K3s validation in the release process;
- treat subsystem-wide snapshot/compaction-based recovery as future hardening before advertising arbitrary long-running production clusters as complete.

## Recommended follow-up tranche

Create a future Phase B2 / subsystem snapshot recovery tranche. Detailed plan: `phase-b2-subsystem-snapshot-recovery-implementation-plan.md`.

1. Define snapshot payload format/versioning for each partition-owned subsystem.
2. Implement snapshot/restore for composite production state machines only when all included durable child state machines have safe contracts.
3. Add an automatic snapshot creation/compaction policy for production groups.
4. Add tests for graph/space/blob/schema/semantic follower catch-up after log compaction and for empty-PVC follower rejoin from snapshot-only evidence.
5. Extend destructive soak to force enough writes/compaction to require subsystem snapshot recovery.
