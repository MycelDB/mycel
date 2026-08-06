# Space-Partitioned Raft Clustering

## Status

Design proposal for replacing the current static-primary clustering model with a space-partitioned multi-Raft architecture.

This design intentionally does **not** preserve backward compatibility with the current pre-product clustering storage layout. Existing static-primary cluster data directories are not expected to be migrated in-place.

## Goals

- Automatic failover when one node is lost.
- Multiple concurrent writers by distributing spaces across Raft groups.
- Keep the application/SDK ergonomics simple: clients may connect to any daemon.
- Preserve read-after-write behavior for UI-facing workloads.
- Avoid cross-space transaction/query complexity in the first implementation.
- Reuse the existing logical WAL record/applier model as much as practical.
- Use a standard Raft implementation: `etcd/raft`.

## Non-goals

- Cross-space writes or cross-space queries.
- Dynamic node add/remove in the first Raft implementation.
- Automatic migration from the current static-primary clustering storage format.
- Follower/stale reads in the MVP.
- Production-grade internode identity such as mTLS in the MVP.
- In-place full-cluster restore into a running cluster.

## High-level model

The physical Mycel cluster defaults to a three-node deployment for the MVP, but cluster sizing constants must be externalized so deployments can choose appropriate values at bootstrap. On top of those physical nodes, Mycel runs multiple logical Raft groups:

- one **system Raft group** for cluster/control-plane metadata
- sixty-four **space partition Raft groups** for user data

```text
physical nodes: node-a, node-b, node-c

Raft groups per node:
  system
  space-partition-0
  space-partition-1
  ...
  space-partition-63
```

Every partition group defaults to replica factor `3`; in the MVP default deployment, each partition is replicated to all three nodes. The replica factor is a bootstrap-time cluster setting and should be persisted in system metadata.

## Partitioning

User data is partitioned by `space_id`:

```text
partition_id = hash(space_id) % 64
```

The partition count is configured at cluster bootstrap and defaults to:

```text
partition_count = 64
```

The partition count must be externalized as a bootstrap-time setting and persisted in system metadata. It is immutable after bootstrap for the MVP. Normal space creation uses daemon-generated `space_id` values only. Caller-provided `space_id` may be reserved for future privileged restore/import flows.

Domain-level partitioning is explicitly out of scope. Domains live inside their owning space's partition.

## Raft groups

### System group

The system Raft group owns cluster-wide metadata:

- cluster ID
- node registry
- admitted nodes
- node names
- advertise addresses
- partition count
- replica factor
- partition placement map
- global admin/operator accounts
- global users
- global backup policy/configuration
- global embedding/provider/inference configuration when cluster-wide
- cluster-wide session/refresh metadata

The system group uses the bootstrap-configured node set. The default MVP node count is three.

### Space partition groups

Each space partition group owns all durable state scoped to spaces assigned to that partition:

- space records
- space ACLs/grants
- domains in those spaces
- templates in those spaces
- graph nodes/edges/files for those spaces
- blob metadata for those spaces
- semantic indexes/metadata for those spaces
- semantic maintenance/accounting records for those spaces

Blob payload files are materialized replicated data associated with the owning partition. Metadata must not become visible unless the corresponding payload is locally readable.

## Node-local state

The following state remains node-local:

- local health observations
- caches
- temporary jobs
- in-flight worker state
- local backup artifacts
- Raft logs/snapshots on disk
- materialized blob payload files

Sessions are **not** node-local in this design. Long-lived cluster sessions are stored through the system Raft group so clients can reconnect to any surviving node after failure.

## Leadership

All 64 partition groups are started at daemon startup.

Initial preferred partition leadership is deterministic round-robin:

```text
preferred_leader(partition_id) = partition_id % 3
```

For the default three-node deployment this yields approximately:

```text
node-a: 22 partition leaders
node-b: 21 partition leaders
node-c: 21 partition leaders
```

The system group initially prefers `node-a` as leader.

`etcd/raft` does not provide permanent preferred leaders by itself. The implementation may use startup sequencing, campaign control, or leadership transfer once nodes are ready. A future background leader balancer can correct drift after failures/restarts.

## Request routing

Clients may connect to any daemon. The receiving daemon is the ingress node.

For space-scoped APIs, the service layer extracts `space_id`, computes the partition, and uses a `PartitionExecutor`-style abstraction:

```go
executor.ForSpace(ctx, spaceID, func(ctx context.Context) (*Response, error) {
    return service.executeLocal(ctx, req)
})
```

The executor handles:

- local execution when this node is leader for the partition
- internode forwarding when another node is leader
- one internal retry on safe leader-change races where appropriate

Forwarding is service-aware. Service methods remain explicit about routing keys so authorization, session handling, streaming behavior, and API-specific semantics remain auditable.

## Reads

MVP reads are leader-only:

```text
client -> any daemon -> owning partition leader -> local applied state
```

Follower reads are not supported initially. Future consistency modes may include:

- strong/default: leader read
- stale: local follower read allowed
- bounded staleness: follower read allowed if applied index is sufficiently current

## Writes

A write returns success only after:

```text
Raft quorum commit + applied on partition leader
```

The flow is:

```text
client/SDK
  -> any daemon
  -> partition leader via forwarding if needed
  -> leader proposes RaftCommand
  -> quorum commits
  -> leader applies to materialized state
  -> success returned
```

Followers apply asynchronously after commit. The write path does not wait for every replica's state machine to apply.

This preserves UI-friendly read-after-write behavior when subsequent reads route to the same partition leader.

## Raft command envelope

The Raft log is the durability and ordering source for clustered mode. Existing logical WAL payloads should be reused inside a Raft command envelope rather than appended to a second local WAL.

Conceptual command shape:

```go
type RaftCommand struct {
    Scope       CommandScope // system or space_partition
    PartitionID uint32       // 0-63 for space partitions
    SpaceID     string       // required for space-scoped commands
    RecordType  string       // existing logical WAL record type
    Payload     []byte       // existing logical WAL payload
    CommandID   string       // idempotency key
    CommandHash []byte       // optional hash for idempotency conflict detection
}
```

For system operations, `Scope=system` and `PartitionID`/`SpaceID` are omitted.

For space operations, `Scope=space_partition`, `SpaceID` is set, and the implementation validates:

```text
PartitionID == hash(SpaceID) % 64
```

The existing WAL appliers should be refactored behind Raft state machines. Clustered Raft mode should not perform a second local WAL append for Raft-managed data.

Standalone mode may keep a local WAL path during transition if useful.

## Idempotency

SDKs generate idempotency keys automatically for high-level mutations. Public write APIs should accept an explicit `idempotency_key` for advanced callers.

`RaftCommand.CommandID` is required internally.

This handles the failure case where a command commits but the client connection dies before the response:

```text
client sends command_id=abc
leader commits/applies
connection dies
client retries command_id=abc against another node
state machine detects abc was already applied
no duplicate side effect
```

State machines should retain applied command IDs per owning scope/partition. Dedupe records should include at least:

- principal ID/type
- idempotency key
- command hash
- applied Raft index
- result reference or enough metadata to safely derive/re-read the result
- expiration/retention metadata

A duplicate with the same command hash should be treated as already applied. A duplicate idempotency key with a different command hash should be rejected as an idempotency conflict.

Initial retention may be time-bounded, for example 24 hours, with configurability later.

## Sessions and authentication

Sessions are cluster-wide and long-lived.

Recommended model:

- access tokens are signed and verifiable by every node
- refresh/session metadata is stored in the system Raft group
- login/refresh/revoke/logout are system Raft writes
- normal API requests do not write session state
- `last_used_at` is not updated per request

On node failure, clients can reconnect to another node and continue using the same access token or refresh through replicated session metadata.

For daemon-side forwarding:

- ingress node validates public credentials/session
- ingress forwards trusted principal context to the partition leader over authenticated internode RPC
- public client/admin RPCs must never accept forwarded-principal metadata
- forwarded principal metadata is accepted only on internode-authenticated endpoints

## Internode authentication

For the MVP, internode RPC uses a required shared cluster token in Raft cluster mode.

Requirements:

- the token is required for all internode/Raft/forwarding RPCs
- the token is never logged
- the token is not written to Raft logs
- the token is not included in backups
- public client/admin RPCs do not use this token
- forwarded principal context is accepted only when the internode token is valid

mTLS or signed node identity is deferred to a future production hardening phase.

## Failure semantics

### One node dies

For partitions where the dead node was leader:

```text
election occurs among the two surviving replicas
partition unavailable briefly
new leader serves reads/writes after election
```

For partitions where the dead node was follower:

```text
no partition-level interruption
```

If an app was connected to the dead node, the SDK reconnects to another configured bootstrap address.

### Two nodes die

With replica factor 3, quorum is lost. Since all MVP partitions are replicated to all three nodes, the cluster is unavailable for leader reads and writes.

### Network partition 1 vs 2

The two-node side retains quorum and continues. The one-node minority cannot serve leader reads or writes.

### Old node rejoins

A restarted/rejoined node catches up through normal Raft log replay or Raft snapshot installation. Manual promote/resync is not part of normal operation.

## Membership

MVP membership is fixed at bootstrap. The default and recommended initial deployment is:

```text
node_count = 3
no add/remove after bootstrap
```

`node_count` must be externalized as a bootstrap-time setting, with `3` as the sensible HA default. Node restart/rejoin is supported. Dynamic membership, learners, node replacement, drain, and rebalance are future work.

## Snapshots

Raft snapshots are internal replication artifacts only. They are used for:

- log compaction
- replica catch-up

Each Raft group has its own snapshot stream/storage:

- system group snapshot: compact system/control-plane metadata
- partition group snapshot: materialized state for spaces in that partition, plus the information needed to make blob payload availability safe

The current full-node cluster resync/snapshot mechanism should not be the normal Raft catch-up path. It may be removed, deprecated, or retained temporarily only as an operator disaster-recovery transition tool.

## Backups and restore

Backups are separate operator/user archives, not Raft snapshots.

Backup archives are created from consistent applied state and include blob payloads referenced by included metadata.

MVP restore semantics:

- full backup restore is offline only
- full backup restore targets an empty/new cluster/data directory
- single-space restore into a running cluster is allowed
- single-space restore creates a new `space_id` by default
- in-place overwrite restore is not supported initially
- sessions are not backed up/restored as active sessions

A future backup manifest should be Raft-aware and record system/partition applied indexes and terms where relevant, but backup archives remain distinct from internal Raft snapshots.

## Space creation

Normal `CreateSpace` flow:

```text
receiving daemon generates space_id
partition_id = hash(space_id) % 64
request routes to partition leader
CreateSpace command commits/applies in that partition
created space is returned
```

Caller-provided `space_id` is not allowed for normal public create operations.

## Cross-space behavior

Cross-space writes and cross-space queries are unsupported in the MVP. APIs that attempt to mutate or query multiple spaces should reject the request unless they can be decomposed into independent single-space operations with explicitly non-transactional semantics.

Future distributed transactions would require a separate design, likely involving transaction coordinators and multi-partition commit protocols.

## Implementation implications

This design implies replacing the current static-primary concepts with Raft-derived leadership:

- no cluster-wide primary
- no manual planned switchover as a normal operation
- no emergency manual promotion as a normal operation
- not-primary hints are replaced or supplemented by partition-leader routing errors/hints
- follower WAL receive logs are replaced by Raft logs
- manual snapshot resync is replaced by Raft log/snapshot catch-up

The daemon remains the subsystem assembler, but clustering/consensus code should live under `mycel/internal/clustering`, likely with a new consensus package, for example:

```text
mycel/internal/clustering/consensus
mycel/internal/clustering/partitioning
mycel/internal/clustering/routing
```

## Open future work

- Supported bootstrap profiles beyond the default 3-node/64-partition profile.
- Dynamic membership and node replacement.
- Learner catch-up and promotion.
- Partition placement on clusters larger than three nodes.
- Partition movement and rebalancing.
- Leader balancing after failures/restarts.
- Follower/stale reads with explicit consistency modes.
- SDK-side partition routing cache for latency optimization.
- Production internode security with mTLS or signed node identity.
- Cross-space transactions/queries, if ever required.
- Online full-cluster restore or in-place restore workflows.
