# Static-Primary Clustering Artifact Inventory

## Status

Phase 0 inventory for the space-partitioned Raft clustering refactor.

The artifacts below belong to the current static-primary/WAL-propagation design. They should remain in place until the phased Raft implementation replaces their behavior. Once equivalent Raft paths are live and validated, they should be removed, rewritten, or marked legacy as noted.

## Static authority and primary operations

Candidate artifacts:

- `internal/clustering/authority.go`
- `internal/clustering/switchover_intent.go`
- `internal/clustering/replication/switchover.go`
- `internal/clustering/replication/failover.go`
- `internal/clustering/backend/service_authority.go`
- `internal/clustering/backend/authority_client.go`
- authority-related CLI paths in `internal/cli/cmd/cluster.go`
- public admin APIs for planned primary switch and emergency promote
- `mycel-admin` primary switch/promote UI

Raft replacement:

- per-partition Raft leadership
- system Raft group metadata
- partition leader distribution/status APIs
- no cluster-wide primary
- no normal manual promote/switchover flow

Removal phase:

- Phase 14 updates public API/CLI/SDK/Admin UI.
- Phase 15 removes obsolete static-primary implementation artifacts.

## WAL propagation and follower receive log

Candidate artifacts:

- `internal/clustering/replication/follower.go`
- `internal/clustering/replication/receive_log.go`
- `internal/clustering/replication/progress.go` where tied to follower WAL apply
- `internal/clustering/backend/service_wal.go`
- `internal/clustering/backend/stream_wal_client.go`
- `internal/clustering/backend/wal_reader.go`
- `internal/clustering/backend/wal_convert.go`
- follower catch-up/status fields in cluster health APIs

Raft replacement:

- `etcd/raft` logs are the replication and ordering source
- per-group applied index/term
- Raft-native catch-up and snapshots

Removal phase:

- Phase 10 replaces blob payload assumptions tied to WAL streaming.
- Phase 15 removes old WAL propagation artifacts.

## Snapshot resync and materialized install

Candidate artifacts:

- `internal/clustering/replication/resync*.go`
- `internal/clustering/replication/snapshot_creator.go`
- `internal/clustering/replication/snapshot_installer.go`
- `internal/clustering/replication/snapshot_install_materialized.go`
- `internal/clustering/replication/install_materialized.go`
- `internal/clustering/replsnapshot/*` where used only for cluster resync
- `internal/clustering/backend/service_snapshot.go`
- `internal/clustering/backend/snapshot_client.go`
- public `cluster node resync` API/CLI/UI flows

Raft replacement:

- per-Raft-group internal snapshots for compaction and replica catch-up
- backup/restore remains a separate operator archive feature

Removal phase:

- Phase 13 refactors backup/restore separation.
- Phase 15 removes old cluster resync artifacts.

## Membership/admission artifacts

Candidate artifacts:

- existing join-token admission flows
- remove/rename node flows
- seed peer topology semantics
- current membership lifecycle fields tied to static-primary mode

Raft replacement:

- fixed bootstrap-configured node set for MVP
- system Raft group owns node registry and placement metadata
- dynamic membership deferred

Removal phase:

- Phase 5 introduces system Raft bootstrap metadata.
- Phase 14 updates APIs/UI.
- Phase 15 removes obsolete legacy paths.

## Validation scripts to replace

Current static-primary/WAL scripts:

- `scripts/validateShortTermClusterAuthority.sh`
- `scripts/validateWALPropagation.sh`
- `scripts/validateWALSnapshotResync.sh`
- `scripts/validateClusterPrimarySwitchover.sh`
- `scripts/validateBlobPayloadReplication.sh`

Raft-aware replacements planned:

- `scripts/validateRaftClusterBootstrap.sh`
- `scripts/validateRaftLeaderFailover.sh`
- `scripts/validateRaftPartitionRouting.sh`
- `scripts/validateRaftBlobPayloadReplication.sh`
- `scripts/validateRaftBackupRestore.sh`

## Documentation to rewrite or retire

Static-primary documents that will need replacement after Raft cutover:

- `docs/design/cluster-leadership-authority.md`
- `docs/design/clustering-short-term-authority-and-client-behavior.md`
- `docs/design/cluster-safe-planned-switchover.md`
- `docs/design/wal-propagation-mvp.md`
- `docs/design/wal-snapshot-resync.md`
- cluster replication sections of `docs/design/write-ahead-log-operational-guide.md`

Current Raft documents:

- `docs/design/space-partitioned-raft-clustering.md`
- `docs/implementation/space-partitioned-raft-clustering-implementation-plan.md`

## Phase 0 rule

Do not remove runtime artifacts in Phase 0. This phase only records the inventory and adds configuration placeholders. Runtime behavior must remain unchanged.
