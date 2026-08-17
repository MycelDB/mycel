# Space-Partitioned Raft Clustering Implementation Plan

## Status

Planning document for implementing the design in `docs/design/space-partitioned-raft-clustering.md`.

The implementation must proceed in incremental phases. At the end of every phase:

- `mycel` must build and pass the relevant test suite.
- Standalone mode must remain functional.
- Existing non-Raft development flows used by `knot_pkm` must remain functional until the phase explicitly switches those flows to the new Raft path.
- Any removed static-primary artifact must either be unused by the current phase or replaced by an equivalent Raft-based capability.

Backward compatibility with existing static-primary cluster data directories is not required.

## Target architecture summary

- Raft library: `etcd/raft`.
- Bootstrap-configurable constants:
  - default node count: `3`
  - default partition count: `64`
  - default replica factor: `3`
- Raft groups:
  - one system Raft group
  - `partition_count` space partition Raft groups
- Partition key: `space_id`.
- Default partitioning:
  - `partition_id = hash(space_id) % partition_count`
- Reads: leader-only.
- Writes: return after quorum commit and leader apply.
- Request routing: daemon-side forwarding.
- Sessions: cluster-wide, long-lived, system-Raft-backed refresh/session metadata.
- Internode auth MVP: required shared cluster token in Raft cluster mode.
- Idempotency: SDK-generated idempotency keys carried as `RaftCommand.CommandID`.
- Raft snapshots: internal catch-up/compaction only.
- Backups: separate operator archive feature.
- Restore MVP:
  - full restore offline into empty/new cluster
  - single-space restore into running cluster as new `space_id`

## Validation baseline

Unless a phase states otherwise, validate with:

```bash
cd mycel && go test ./internal/...
cd mycel-api && go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
cd mycel-go-sdk && ./scripts/generate-proto.sh && go test ./...
cd mycel-rust-sdk && cargo check -p mycel-proto && cargo check -p mycel-sdk
cd mycel-console/src-tauri && cargo check
cd mycel-console && npm test -- --runInBand && npm run build
```

For `knot_pkm`, maintain a smoke test at each phase using standalone Mycel until Raft cluster mode is activated for application flows. The smoke test should verify at least:

- server starts
- connects/logs in to Mycel
- loads spaces/data needed by the UI
- can create/read/update a representative record

If a dedicated script does not exist yet, add one early, for example:

```text
scripts/validateKnotPkmMycelSmoke.sh
```

or a project-local equivalent.

## Phase 0 — Document, inventory, and feature flag boundary

### Goals

- Land design and implementation plans.
- Inventory current static-primary/WAL-replication artifacts.
- Add a clear configuration boundary for the future Raft mode without changing runtime behavior.
- Keep all existing standalone and current dev flows working.

### Work

1. Keep/update design docs:
   - `docs/design/space-partitioned-raft-clustering.md`
   - this implementation plan
2. Inventory artifacts to remove or replace later:
   - static authority files/APIs
   - manual promotion/switchover flows
   - follower receive log and WAL streaming
   - snapshot resync coordinator and RPCs
   - old cluster health fields tied to static primary/follower roles
3. Add config placeholders, not yet active:
   - cluster engine: `static` or `raft`, default current behavior until cutover
   - bootstrap node count default `3`
   - bootstrap partition count default `64`
   - bootstrap replica factor default `3`
4. Add documentation note that `raft` mode is experimental until later phases.

### Tests

- Existing full validation baseline.
- Add unit tests for config parsing/defaults.

### Knot PKM compatibility

No behavior changes. `knot_pkm` continues to use current standalone/static behavior.

## Phase 1 — Partitioning and routing primitives, no Raft yet

Status: implemented initial primitives in `internal/clustering/partitioning` and `internal/clustering/routing`. Runtime behavior remains local-only.

### Goals

- Introduce stable space partitioning primitives.
- Make service code able to express routing keys without changing execution.
- Keep behavior identical by routing everything locally.

### Work

1. Add package, for example:

```text
internal/clustering/partitioning
internal/clustering/routing
```

2. Implement:

```go
type PartitionID uint32

type Config struct {
    PartitionCount uint32
}

func PartitionForSpace(spaceID string, partitionCount uint32) (PartitionID, error)
```

3. Use a stable hash, documented and tested. Avoid Go map/hash randomness.
4. Add `PartitionExecutor` interface with local-only implementation:

```go
type PartitionExecutor interface {
    ForSpace(ctx context.Context, spaceID string, fn func(context.Context) error) error
}
```

5. Start threading explicit `space_id` routing through key service paths without forwarding yet.

### Tests

- Unit tests for hash stability and distribution.
- Unit tests for invalid/missing `space_id`.
- Existing full validation baseline.

### Knot PKM compatibility

No external behavior change. All requests still execute locally.

## Phase 2 — Raft command envelope and applier bridge

Status: implemented initial `internal/clustering/consensus` command envelope, codec, validation, WAL applier state-machine bridge, and in-memory test state machine. Runtime behavior remains unchanged.

### Goals

- Define the command format that will be committed through Raft.
- Refactor existing WAL appliers behind a reusable state-machine apply interface.
- Do not switch production write paths to Raft yet.

### Work

1. Add consensus command package, for example:

```text
internal/clustering/consensus/command.go
```

2. Define versioned command envelope:

```go
type CommandScope string

const (
    CommandScopeSystem CommandScope = "system"
    CommandScopeSpacePartition CommandScope = "space_partition"
)

type RaftCommand struct {
    Version     uint32
    Scope       CommandScope
    PartitionID uint32
    SpaceID     string
    RecordType  string
    Payload     []byte
    CommandID   string
    CommandHash []byte
}
```

3. Add encode/decode tests and validation rules.
4. Refactor WAL appliers so they can apply from either:
   - current local WAL path
   - future Raft state machine path
5. Add an in-memory fake state machine for tests.

### Tests

- Command validation/codec tests.
- Applier bridge tests.
- Existing WAL tests continue passing.
- Existing full validation baseline.

### Knot PKM compatibility

No external behavior change. Existing local WAL path remains active.

## Phase 3 — Minimal embedded etcd/raft runtime skeleton

Status: implemented initial `etcd/raft`-backed `consensus.Group` skeleton with in-memory Raft storage/transport test coverage for leader election, quorum commit with leader apply, one-node failover, and quorum loss. Runtime daemon behavior remains unchanged.

### Goals

- Introduce an internal Raft runtime that can run in tests.
- Start with in-memory transport/storage for deterministic unit tests.
- Do not make daemon depend on it for normal operations yet.

### Work

1. Add dependency on `go.etcd.io/raft/v3`.
2. Add package, for example:

```text
internal/clustering/consensus
```

3. Implement core abstractions:

```go
type GroupID string
type NodeID uint64

type Group struct { ... }
type Storage interface { ... }
type Transport interface { ... }
type StateMachine interface { Apply(ctx context.Context, cmd RaftCommand) error }
```

4. Implement in-memory three-node test cluster.
5. Implement proposal flow returning only after commit + leader apply.
6. Implement leader detection and basic leadership transfer/test controls.

### Tests

- In-memory Raft group elects a leader.
- Proposal commits on quorum.
- Proposal returns after leader apply.
- One node stopped: new leader elected, proposals continue.
- Two nodes stopped: proposals fail/unavailable.

### Knot PKM compatibility

No daemon runtime behavior change. `knot_pkm` still uses existing standalone/static path.

## Phase 4 — Persistent Raft storage and snapshots skeleton

Status: implemented initial file-backed `PersistentStorage` for Raft hard state, entries, and snapshots, with recovery, snapshot creation, snapshot install, and compaction tests. Runtime daemon behavior remains unchanged.

### Goals

- Add durable Raft log/state/snapshot storage.
- Implement Raft-native snapshot hooks, not current full-node resync.
- Keep disabled from production paths until later phases.

### Work

1. Add storage layout draft:

```text
<data_dir>/meta/raft/system/
<data_dir>/meta/raft/partitions/<partition_id>/
```

2. Implement persistent storage for:
   - HardState
   - Entries
   - Snapshots
3. Implement snapshot create/install interfaces per group.
4. Implement compaction policy placeholders.
5. Keep current backup archives separate from Raft snapshots.

### Tests

- Restart recovers HardState/log entries.
- Snapshot install restores state machine state in tests.
- Compaction does not remove needed entries before snapshot is durable.
- Existing full validation baseline.

### Knot PKM compatibility

No production behavior change.

## Phase 5 — System Raft group for bootstrap metadata

Status: implemented initial system metadata state machine, bootstrap metadata command, deterministic partition placement, snapshot/restore helpers, and in-memory Raft commit coverage. Runtime daemon behavior remains unchanged.

### Goals

- Introduce system metadata state machine.
- Persist bootstrap-configured node count, partition count, and replica factor through system Raft.
- Keep existing user/admin/session paths unchanged until later phases.

### Work

1. Define system metadata records:
   - cluster ID
   - node registry
   - partition count
   - replica factor
   - partition placement
2. Add bootstrap flow for experimental Raft clusters:
   - exactly configured node set for MVP
   - default node count `3`
   - default partition count `64`
   - default replica factor `3`
3. Add system group state machine and snapshot.
4. Add CLI/dev scripts for starting a local three-node Raft cluster in experimental mode.
5. Keep current static-primary cluster commands marked legacy or hidden for raft mode.

### Tests

- System group bootstraps metadata consistently across three nodes.
- Restart preserves metadata.
- Invalid bootstrap values rejected.
- Full validation baseline.

### Knot PKM compatibility

Default mode remains compatible. Experimental Raft mode may be validated separately without requiring `knot_pkm` to switch yet.

## Phase 6 — Start all partition groups and deterministic leader distribution

Status: implemented initial `MultiGroup` skeleton that starts system + all partition groups, computes deterministic round-robin preferred leaders, exposes internal status, and tests multi-group in-memory leader election. Runtime daemon behavior remains unchanged.

### Goals

- Start all configured partition groups on daemon startup in experimental Raft mode.
- Implement round-robin preferred leaders.
- Still avoid routing real application data through Raft until phase 7.

### Work

1. Start:

```text
1 system group
partition_count partition groups
```

2. Use preferred leader mapping:

```text
partition_id % node_count
```

3. Add group status APIs internal to daemon.
4. Add cluster health/status updates for Raft groups.
5. Add leader drift metrics and debug output.

### Tests

- All groups start.
- Expected leader distribution for default 3/64 profile.
- One node failure redistributes leaders after election.
- Restart catches up groups.

### Knot PKM compatibility

Still not switched by default. Existing app behavior remains functional.

## Phase 7 — Space create/list/get through partition Raft groups

Status: implemented for experimental Raft mode. Implemented service-path integration by threading the `PartitionExecutor` through the space module and routing `CreateSpace`/`GetSpace` through explicit `space_id` partition boundaries. Phase 7a added experimental daemon Raft group startup behind `MYCELD_CLUSTER_ENGINE=raft`. Phase 7b added a Raft message envelope, routed transport abstraction, and local router test harness. Phase 7c added a Raft-aware executor abstraction. Phase 7d added create-space Raft command build/apply helpers. Phase 7e added partition leader resolution from `MultiGroup` and a leader-routed `GetSpace` helper with tests. Phase 7f added an in-memory three-replica create-space commit/apply/failover harness. Phase 7g added the backend `DeliverRaftMessages` RPC, a backend `RaftMessageSender`, and local daemon router registration for experimental Raft groups. Phase 7h starts experimental Raft groups after the space module is initialized, wires partition groups to the space Raft state machine, swaps the space module to a Raft executor in Raft mode, and routes `CreateSpaceWithResult` through a partition Raft proposal. Phase 7i adds experimental numeric Raft node ID to backend address mapping via `MYCELD_CLUSTER_RAFT_NODE_ADDRS` and uses backend `RaftMessageSender` for remote Raft messages. Phase 7j propagates admin CreateSpace idempotency headers (`idempotency-key` / `x-idempotency-key`) into the Raft command ID when present. Phase 7k adds an in-memory Raft CreateSpace idempotency cache for repeated command IDs during the experimental runtime. Phase 7m adds an experimental backend `GetRaftSpace` forwarding RPC/client and wires the daemon backend service to the space module. Phase 7n hooks the Raft executor forwarder to `GetRaftSpace` using `MYCELD_CLUSTER_RAFT_NODE_ADDRS`, and backend `GetRaftSpace` now uses a local-only space read to avoid forwarding loops. Phase 7o adds experimental `ListRaftSpaces` backend RPC/client and `ListSpaces` partition-leader aggregation with de-duplication in Raft mode. Multi-daemon validation was performed with a temporary script that started three Raft-mode daemons, created a space through node 1, and verified admin list/get from all three nodes; the script passed and was removed.

### Goals

- Move the first real space-scoped durable path to Raft in experimental mode.
- Keep standalone/local WAL behavior working.

### Work

1. Modify `CreateSpace` in Raft mode:
   - daemon generates `space_id`
   - compute partition
   - route to partition leader
   - propose `CreateSpace` command
   - return after leader apply
2. Move `GetSpace`/`ListSpaces` reads in Raft mode:
   - leader-only reads
   - daemon-side forwarding
3. Implement service-layer `PartitionExecutor.ForSpace` for real forwarding.
4. Add idempotency key support for `CreateSpace` in API/SDK where applicable.
5. Add internal forwarding RPC for space service operations or a generic routed request mechanism.

### Tests

- Create space through any node; stored on owning partition.
- Read space through any node; forwarded to leader if needed.
- Kill partition leader; after election, reads/writes continue.
- Idempotent retry of `CreateSpace` does not duplicate.
- Standalone mode tests still pass.

### Knot PKM compatibility

Run `knot_pkm` smoke test against standalone mode and, if feasible, against experimental Raft mode for space create/list/get only.

## Phase 8 — Migrate space ACLs, domains, and templates to partition Raft

Status: implemented for experimental Raft mode. ACL grants, domain create/update/delete, and template create/update/archive/delete/import now propose partition Raft metadata commands when `MYCELD_CLUSTER_ENGINE=raft`. Admin domain/template read paths are safe in Raft mode and use local materialized state after Raft apply; admin template list/get was validated across a three-daemon experimental Raft cluster. A temporary Phase 8 validator started three Raft-mode daemons, created a space/user, exercised domain and template mutation flows, verified admin template list from all nodes, passed, and was removed. Remaining hardening is durable/broader idempotency and future system-Raft-backed identity/session work.

### Goals

- Move remaining foundational space-scoped metadata to partition Raft.
- Ensure admin template APIs remain functional.

### Work

1. Move space grants/ACL mutations to partition Raft.
2. Move domain create/update/delete metadata to partition Raft.
3. Move template create/update/archive/delete/import metadata to partition Raft.
4. Ensure reads are leader-routed.
5. Add/propagate idempotency keys for mutations.
6. Update `AdminTemplateService` implementation to read from partition leader in Raft mode.

### Tests

- ACL/domain/template CRUD through any node.
- Leader failover during domain/template operation.
- Admin template list/get still works.
- `mycel-console` Spaces detail tabs still work.
- Full frontend/Tauri validation.

### Knot PKM compatibility

Smoke test must verify normal space/domain/template usage required by `knot_pkm`.

## Phase 9 — Migrate graph mutations and reads to partition Raft

Status: in progress. Graph commit records (`graph.commit.v1`) now have a partition Raft command builder and graph Raft state machine. Experimental Raft startup wires a composite partition state machine so space metadata and graph commits are applied by the same partition groups. `CommitTransactionGraph` proposes commits through partition Raft when Raft mode is enabled, while static/WAL behavior remains unchanged. Leader-only graph read forwarding is implemented for node/edge/list/parent/children reads via an internal backend RPC. Coverage includes command build/apply, single-node Raft-enabled commit, in-process three-node replication of a graph commit to all replicas, durable command dedupe across module restart, payload/command space mismatch rejection, and in-process leader-failover write validation. Remaining work: daemon-level multi-node validation via public APIs and `knot_pkm` smoke testing.

### Goals

- Move primary application data path to Raft.
- This is the main phase where `knot_pkm` should become functional against Raft mode.

### Work

1. Move graph node/edge/file metadata mutations to partition Raft.
2. Route graph reads/queries to partition leaders.
3. Reject cross-space queries/writes explicitly.
4. Ensure transaction/session semantics are single-space only in Raft mode.
5. Add SDK idempotency for graph mutations.
6. Implement command dedupe in graph state machine.

### Tests

- Create/read/update/delete graph records through any node.
- Query single-space data through any node.
- Cross-space query/write rejected with clear error.
- Kill leader; after election app reads data from new leader.
- Unknown-commit retry does not duplicate graph mutation.

### Knot PKM compatibility

Required phase gate: `knot_pkm` server must be smoke-tested against Raft mode and successfully:

- connect to any node
- load existing data
- create/update/read representative records
- reconnect after killing a node and continue loading data after election

## Phase 10 — Blob metadata and payload replication under Raft

Status: started. Blob metadata put/delete records can now be encoded as partition Raft commands and applied by a blob Raft state machine. Experimental Raft startup wires blob metadata apply into the composite partition state machine alongside space and graph commands. `UploadBlob` and `DeleteBlob` propose blob metadata through partition Raft in Raft mode while preserving static/WAL behavior otherwise. Initial tests cover direct blob metadata Raft apply, a single-node Raft-mode upload/open path, missing-payload rejection, remote payload fetch from a backend peer, in-process three-node Raft metadata replication with follower payload fetch before visibility, and blob upload after partition leader failover. Remaining work: replacing old WAL payload replication paths after static/WAL migration cleanup.

### Goals

- Preserve blob invariant under partition Raft.
- Replace old WAL propagation blob pre-apply path with Raft state-machine-safe payload handling.

### Work

1. Store blob metadata as partition Raft commands.
2. Ensure payload availability before metadata apply on every replica.
3. Design payload transfer path between partition replicas.
4. Integrate payload transfer with Raft snapshot install.
5. Remove or replace old `StreamWal`-based blob payload replication code.

### Tests

- If blob metadata is visible on a node, payload is locally readable.
- Leader failover with blob writes.
- Snapshot catch-up includes/fetches required blob payloads before metadata visibility.
- Existing blob validation script replaced with Raft-aware version.

### Knot PKM compatibility

Smoke test any attachment/blob functionality used by `knot_pkm`.

## Phase 11 — Semantic/index metadata and maintenance records through partition Raft

Status: in progress. Added a semantic partition Raft state machine for space-scoped semantic mutations (`semantic.space.mutation.v1`) and maintenance mutations (`semantic.maintenance.mutation.v1`), and wired it into the experimental composite partition state machine. Space semantic managers and maintenance managers now use existing mutation wrappers when Raft mode is enabled, causing semantic index/grant/policy mutations and durable maintenance records to propose through the owning space partition while static/WAL behavior remains unchanged. Coverage includes direct semantic space and maintenance mutation apply, single-node Raft-mode `UpsertSemanticIndex` and dirty-event append paths, three-node semantic index replication, leader-failover mutation validation, durable command dedupe, and space-mismatch rejection.

### Goals

- Move semantic durable metadata to partition Raft.
- Keep maintenance workers node-local where appropriate but durable records partition-owned.

### Work

1. Move semantic index metadata to partition Raft.
2. Move accounting/maintenance records to partition Raft where space-scoped.
3. Keep worker execution state local/ephemeral.
4. Ensure `mycel-console` semantic tabs use leader-routed reads.

### Tests

- Semantic metadata CRUD through any node.
- Maintenance records survive leader failover.
- Admin semantic UI tests/build pass.

### Knot PKM compatibility

Smoke semantic-dependent flows if used by `knot_pkm`.

## Phase 12 — Cluster-wide users/admins/sessions through system Raft

Status: in progress. Added system-Raft support for user put records (`identity.user.put.v1`) and admin/operator put records (`identity.admin.put.v1`) and wired the experimental system Raft group with a composite state machine that applies system metadata, user commands, and admin commands. User and operator create/update/enable/disable/delete/password paths now persist by proposing put records through system Raft when Raft mode is enabled while static/WAL behavior remains unchanged. Coverage includes direct user/admin Raft apply, single-node Raft-mode create, durable user command dedupe across module restart, in-process three-node user and admin replication, and system-leader failover validation for user/admin creation. User auth refresh-session records (`identity.user.session.put.v1`) and operator refresh-session records (`identity.admin.session.put.v1`) now also flow through system Raft for create, refresh/rotate, expiry marking, and revocation paths. Public multi-daemon login/session validation is covered by `scripts/validateRaftPhase12Public.sh`, which starts three Raft-mode daemons, creates a user on one node, logs in on another, and verifies authenticated session visibility on a third. Phase 9–11 executable validators were recreated as `scripts/validateRaftPhase9.sh`, `scripts/validateRaftPhase10.sh`, and `scripts/validateRaftPhase11.sh` and passed against the current Raft test coverage.

### Goals

- Move global identity/auth metadata into system Raft.
- Make long-lived sessions valid across nodes.

### Work

1. Move admins/operators to system Raft.
2. Move users to system Raft.
3. Move refresh/session metadata to system Raft.
4. Use signed access tokens verifiable on all nodes.
5. Avoid per-request session writes.
6. Implement token refresh/revoke/logout as system Raft writes.
7. Update SDK reconnect behavior to use multiple bootstrap addresses.

### Tests

- Login on node A, use token on node B/C.
- Kill node A, reconnect to node B without re-login.
- Refresh token after failover.
- Revoke session propagates cluster-wide.
- No `last_used_at` write on ordinary requests.

### Knot PKM compatibility

Required phase gate: `knot_pkm` remains logged in/reconnects across node failure or can refresh transparently without user-visible failure.

## Phase 13 — Backup/restore refactor for Raft architecture

### Goals

- Separate backups from Raft snapshots cleanly.
- Update restore semantics for partitioned Raft.

### Work

1. Refactor backup manifest to include:
   - cluster format/version
   - partition count
   - per-partition applied index/term where relevant
   - included spaces
   - blob payload references/checksums
2. Full restore:
   - offline only
   - empty/new cluster/data dir only
3. Single-space restore:
   - running cluster allowed
   - creates new `space_id` by default
   - routes restored data to new partition
4. Ensure sessions are not restored as active sessions.
5. Remove dependency on old cluster snapshot/resync implementation.

### Tests

- Full offline restore into empty data dirs.
- Single-space restore as new space into running Raft cluster.
- Blob payloads included/restored.
- In-place full restore rejected.
- In-place overwrite space restore rejected for MVP.

### Knot PKM compatibility

Smoke backup/restore only if `knot_pkm` depends on it. Otherwise verify normal server operations after backup changes.

## Phase 14 — Public API, CLI, SDK, and Admin UI updates

### Goals

- Replace static-primary cluster UX with Raft cluster UX.
- Update SDKs for bootstrap addresses and idempotency.

### Work

1. Update public admin cluster API:
   - Raft group health
   - partition leader distribution
   - node health
   - system group status
   - partition status summaries
2. Remove/deprecate commands that no longer apply:
   - manual primary promote
   - planned primary switch
   - node resync based on old snapshots
   - old follower WAL status commands
3. Add/adjust commands:
   - cluster raft status
   - partition status
   - leader distribution
   - bootstrap config display
4. Go SDK:
   - multiple bootstrap addresses
   - automatic idempotency keys for mutations
   - reconnect/failover behavior
5. Rust SDK:
   - same as Go where applicable
6. `mycel-console`:
   - cluster console shows Raft health instead of static primary/follower state
   - remove obsolete primary promotion/switchover UI

### Tests

- CLI cluster status tests.
- SDK retry/idempotency tests.
- Admin UI tests updated.
- Full validation baseline.

### Knot PKM compatibility

Smoke with updated SDK/client connection configuration using multiple bootstrap addresses.

## Phase 15 — Remove obsolete static-primary/WAL-replication artifacts

### Goals

- Remove code and docs from the old design that are no longer used.
- Ensure no stale public workflows remain.

### Candidate removals/replacements

Remove or replace, after equivalent Raft paths are live:

```text
internal/clustering/authority.go
internal/clustering/switchover_intent.go
internal/clustering/replication/follower.go
internal/clustering/replication/receive_log.go
internal/clustering/replication/switchover.go
internal/clustering/replication/failover.go
internal/clustering/replication/resync*.go
internal/clustering/replication/snapshot_* old full-node resync paths
internal/clustering/backend/service_wal.go
internal/clustering/backend/stream_wal_client.go
internal/clustering/backend/service_snapshot.go old install snapshot RPCs
internal/clustering/backend/service_authority.go
```

Remove or rewrite scripts:

```text
scripts/validateShortTermClusterAuthority.sh
scripts/validateWALPropagation.sh
scripts/validateWALSnapshotResync.sh
scripts/validateClusterPrimarySwitchover.sh
scripts/validateBlobPayloadReplication.sh
```

Replace with Raft-aware validations:

```text
scripts/validateRaftClusterBootstrap.sh
scripts/validateRaftLeaderFailover.sh
scripts/validateRaftPartitionRouting.sh
scripts/validateRaftBlobPayloadReplication.sh
scripts/validateRaftBackupRestore.sh
```

Remove or replace docs that describe old static-primary behavior. The legacy authority, switchover, WAL-propagation, and snapshot-resync design docs were removed during static-primary cleanup; current clustered operation is documented by the Raft design and implementation plan.

Update docs index:

```text
docs/README.md
docs/makefile_commands.md
```

### Tests

- `go test ./internal/...` with removed packages absent.
- Public surface check scripts updated.
- Docs references checked for removed command names:
  - `primary promote`
  - `primary switch`
  - `node resync`
  - `StreamWal`
  - `authority_epoch`

### Knot PKM compatibility

This phase must happen only after `knot_pkm` is validated against the Raft path. Run full smoke/e2e before and after removals.

## Phase 16 — Make Raft cluster mode the default clustered mode

### Goals

- Promote Raft mode from experimental to default for clustered deployments.
- Keep standalone mode stable.

### Work

1. Default `clustered` mode to Raft engine.
2. Remove or hide legacy static-primary configuration.
3. Update quickstart/dev scripts:
   - standalone node
   - three-node Raft cluster
4. Update operator docs and troubleshooting.
5. Ensure failure behavior is documented:
   - one node down: temporary election interruption only
   - two nodes down: unavailable
   - minority partition: unavailable

### Tests

- Full validation baseline.
- Raft cluster e2e scripts.
- `knot_pkm` Raft smoke/e2e.
- Manual failover demo: kill one node, app reconnects and loads data.

## Rollback strategy by phase

Before Phase 16, Raft work should be guarded behind explicit config so standalone/current flows remain usable. If a phase breaks application compatibility, disable Raft mode by default and fix forward.

After Phase 16, rollback means returning to the previous release or using backup/export/import into a known-good build; in-place compatibility with old static-primary data is not guaranteed.

## Phase gates

A phase is complete only when:

1. Code builds.
2. Relevant unit/integration tests pass.
3. Docs affected by the phase are updated.
4. Obsolete artifacts introduced by earlier designs are either still needed or explicitly removed/replaced.
5. `knot_pkm` compatibility smoke test passes for the mode expected at that phase.
6. Any public API/CLI/SDK change is reflected in Go SDK, Rust SDK, and `mycel-console` where applicable.

## Risks

- Multi-Raft operational overhead with 65 groups per node.
- Service-level forwarding coverage gaps.
- Idempotency retrofitting across all mutation APIs.
- Blob payload ordering during Raft apply/snapshot install.
- Session migration to system Raft creating unexpected write pressure.
- Removing old cluster artifacts too early before all replacement paths are validated.

## Recommended first implementation PRs

1. Docs + config placeholders + artifact inventory.
2. Partitioning package and local-only `PartitionExecutor`.
3. Raft command envelope and applier bridge.
4. In-memory etcd/raft group tests.
5. Persistent Raft storage skeleton.

These PRs keep behavior stable while creating the foundation for the later cutover phases.
