# Authoritative System Raft Cluster Metadata

## Status

Implemented through compose and local K3s validation on the `improved_clustering` branch.

This document describes making the **system Raft group** the source of truth for MycelDB cluster identity, membership, and placement metadata. It intentionally skips the simpler interim approach of configuring a static `MYCELD_CLUSTER_ID`; the long-term replicated metadata model is implemented directly.

Operator procedures are documented in `docs/operations/raft-cluster-operations.md`.

## Motivation

The current multi-node raft deployment can start three pods that are configured as one raft cluster but that each create a different local `cluster_id`.

Reproduction notes are in:

```text
docs/implementation/clustering-problem-1-cluster-identity-reproduction.md
```

Observed behavior from a fresh three-node compose deployment:

```text
myceld-a: cluster_ff1dabd9-8284-4ab8-b475-0dbe82e4644d
myceld-b: cluster_81f9d623-e357-42c6-93da-5bbec33b0b91
myceld-c: cluster_5e06f2c2-bf03-45e8-ba7e-710c65fc9de9
```

Each daemon reports itself as a healthy one-member cluster while the experimental raft runtime separately starts raft groups with node IDs `1..3`.

This is a split-brain bootstrap problem. A replicated database must have one authoritative cluster identity and one membership/placement model.

## Goals

1. The system Raft group owns the canonical cluster identity.
2. A multi-node raft deployment cannot silently create multiple independent `cluster_id`s.
3. Local node identity is validated against system Raft metadata before the node is client-ready.
4. Cluster status/health/readiness reflect the authoritative raft metadata, not only local self-membership files.
5. The design works for Kubernetes StatefulSets and local compose.
6. The design supports future membership changes, replacement nodes, and partition placement.

## Non-goals

- This document does not complete all graph replication correctness work.
- This document does not implement follower reads, read-index, or session routing.
- This document does not solve all raft durability work, though durable system Raft storage is a prerequisite for correctness.
- This document does not define arbitrary dynamic membership for V1; initial V1 may support static configured members only.

## Core idea

A MycelDB raft cluster has one authoritative system metadata record replicated through the system Raft group.

That metadata contains at minimum:

```text
cluster_id
cluster_name
bootstrap_epoch
node_count
partition_count
replica_factor
nodes[]
placement[]
created_at
updated_at
```

Every node must eventually apply the same system metadata. Local files may cache node identity and last-seen metadata, but they are not authoritative in raft mode.

## Terminology

### Bootstrap config

The static configuration needed to form the initial system Raft group. It comes from environment/config:

```text
MYCELD_CLUSTER_RAFT_NODE_COUNT
MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID
MYCELD_CLUSTER_RAFT_NODE_ADDRS
MYCELD_CLUSTER_RAFT_PARTITION_COUNT
MYCELD_CLUSTER_RAFT_REPLICA_FACTOR
MYCELD_CLUSTER_NAME
```

Bootstrap config is not the final authority. It is only the input that allows the first system Raft group to form and commit the authoritative metadata.

### System metadata

The authoritative metadata stored in the system Raft state machine. It defines the cluster identity and member set.

### Local identity cache

A local file under the daemon data directory that records the node's persisted identity and the last accepted cluster ID. In raft mode this cache is validated against system metadata. It must not invent the cluster ID independently.

### Backend readiness vs client readiness

A node may need to serve internal raft/backend RPCs before it is safe for clients. Kubernetes deployment should distinguish:

- **backend serving**: gRPC is up and can receive raft messages.
- **client readiness**: node has validated cluster metadata and can safely serve or route client/admin API calls.

## Target metadata model

Extend or formalize the existing `SystemMetadata` model in `internal/clustering/consensus/system_state.go`.

Suggested shape:

```go
type SystemMetadata struct {
    ClusterID      string
    ClusterName    string
    BootstrapEpoch string
    NodeCount      int
    PartitionCount int
    ReplicaFactor  int
    Nodes          map[string]SystemNode
    Placement      map[uint32]PartitionPlacement
    CreatedAt      time.Time
    UpdatedAt      time.Time
}
```

Suggested `SystemNode`:

```go
type SystemNode struct {
    NodeID               string
    RaftNodeID           uint64
    NodeName             string
    ClientAdvertiseAddr  string
    BackendAdvertiseAddr string
    State                string // active, joining, draining, removed
    Ordinal              int    // optional Kubernetes-friendly hint
}
```

V1 can use deterministic node IDs derived from raft node IDs or configured node names:

```text
node_1, node_2, node_3
```

or:

```text
myceld-0, myceld-1, myceld-2
```

The key design requirement is stability. Node identity must not be random on every fresh PVC if the node is expected to rejoin as the same raft member.

## Bootstrap protocol

### Initial static-cluster V1

V1 assumes a static configured cluster. Dynamic membership can be added later.

#### Step 1 — Start backend server

Each node starts enough gRPC/backend infrastructure to receive raft messages.

It does **not** become client-ready yet.

#### Step 2 — Start durable system Raft group

Each node starts the system Raft group using the static bootstrap config:

```text
local raft node ID
peer raft node IDs
peer backend addresses
```

The system group must use durable raft storage. In-memory system raft is not acceptable for authoritative cluster metadata.

#### Step 3 — Bootstrap coordinator proposes metadata

Exactly one node is allowed to propose the initial cluster metadata when the system metadata is empty.

For V1, use deterministic bootstrap coordinator:

```text
raft node ID 1
```

The coordinator builds metadata from the bootstrap config:

```text
cluster_id: generated once by node 1
cluster_name: MYCELD_CLUSTER_NAME
node_count: configured node count
partition_count: configured partition count
replica_factor: configured replica factor
nodes: one node per configured raft node/address
placement: deterministic placement for all partitions
```

It proposes a `system.cluster.bootstrap_metadata` command to the system group.

The metadata becomes authoritative only after quorum commit and apply.

#### Step 4 — Non-coordinator nodes wait

Other nodes do not create their own `cluster_id`. They wait for the system metadata to be committed and applied.

#### Step 5 — Validate local identity cache

After applying metadata, each node validates:

- local raft node ID is present in metadata;
- configured backend address matches metadata or is an allowed update;
- local cached cluster ID is empty or equal to metadata cluster ID;
- local cached node ID is empty or equal to metadata node identity;
- partition count and replica factor match metadata.

If validation passes, the node writes/updates its local identity cache.

If validation fails, the node must fail startup or remain NotReady.

#### Step 6 — Client readiness

A node becomes client-ready only after:

- system metadata has been applied;
- local identity has been validated;
- required raft groups are started;
- the node can identify leaders or safely route/fail client operations;
- no cluster ID conflict exists.

## Restart protocol

On restart with the same PVC:

1. Load local identity cache.
2. Start backend server.
3. Start durable system Raft group from stored raft state.
4. Apply/restore system metadata.
5. Validate local cache against system metadata.
6. Become client-ready only if validation succeeds.

The node must not generate a new cluster ID on restart.

## Fresh node / PVC loss protocol

If a node loses its PVC but has the same StatefulSet ordinal and raft node ID:

1. It starts with empty local identity cache.
2. It joins the system raft group as the same configured raft node ID.
3. It catches up from raft log or snapshot.
4. It validates applied metadata.
5. It writes local identity cache from metadata.
6. It becomes client-ready only after catch-up is sufficient.

If catch-up is impossible, it stays NotReady and requires operator repair.

## Misconfiguration behavior

Fail closed in all of these cases:

- node applies system metadata with a different cluster ID than local cache;
- local raft node ID is not in metadata;
- configured partition count differs from metadata;
- configured replica factor differs from metadata;
- configured peer count differs from metadata in static V1;
- bootstrap coordinator detects existing incompatible local identity;
- non-coordinator cannot obtain system metadata;
- a node tries to self-bootstrap an independent cluster while `node_count > 1`.

## Kubernetes design implications

### StatefulSet

StatefulSet pods provide stable ordinal identity:

```text
myceld-0 -> raft node ID 1
myceld-1 -> raft node ID 2
myceld-2 -> raft node ID 3
```

This maps well to static-cluster V1.

### Services

Use separate concerns:

- headless service for raft/backend peer addresses;
- client service only selects client-ready pods;
- later, a leader/router service can be introduced for writes.

A TCP port-open probe is insufficient for client readiness.

### Readiness

Client readiness must include cluster metadata validation.

A node with a mismatched or missing cluster identity must not be selected by the client service. The current Kubernetes manifests use an exec readiness probe that requires the local identity cache to contain an authoritative cluster ID, `cluster_admitted=true`, and local state `clustered`. This is intentionally stricter than a TCP-open probe, though a future gRPC readiness endpoint should expose the full readiness blocker model directly.

### Parallel startup

Parallel startup is acceptable only if the bootstrap protocol is safe:

- only node 1 proposes initial metadata;
- other nodes wait;
- metadata is committed by quorum;
- no node locally self-bootstraps a different cluster ID.

If this is difficult to guarantee initially, use ordered startup as a deployment requirement until tests prove parallel startup safe.

## Relationship to existing local membership files

Existing files under `meta/clustering` should become caches and diagnostics in raft mode, not authority.

Current problematic behavior:

```text
fresh data dir -> random cluster_id -> cluster_admitted=true -> cluster_bootstrap=true
```

New raft-mode behavior:

```text
fresh data dir -> pending local identity -> wait for system metadata -> validate -> cache authoritative metadata
```

For standalone mode, local identity can remain self-owned.

For raft mode, local identity creation must not generate a cluster ID unless it is copying the committed system metadata.

## Health and status model

Cluster status should report both local and authoritative metadata.

Suggested fields:

```text
local_node_id
local_raft_node_id
local_cached_cluster_id
authoritative_cluster_id
cluster_name
metadata_applied
metadata_validated
node_count
active_member_count
expected_member_count
partition_count
replica_factor
system_group_leader_id
system_group_term
system_group_commit_index
system_group_applied_index
client_ready
readiness_blockers[]
```

Health must not report `healthy` for a configured three-node raft cluster where membership shows only one active member.

## Interaction with partition raft groups

The system metadata must be available before partition groups are considered authoritative.

Possible sequencing:

1. Start system group.
2. Commit/apply system metadata.
3. Build partition placement from metadata.
4. Start partition groups using metadata-defined membership/placement.
5. Enable client graph/space/schema APIs.

If partition groups must start before metadata for implementation reasons, they must not accept client writes until metadata validation is complete.

## Durable raft storage prerequisite

Authoritative system metadata requires durable raft storage. The implementation persists hard state, entries, snapshots, and conf state for the system group under:

```text
<data-dir>/meta/raft/system/
```

K3s restart validation also proved that partition raft groups need durable raft consensus storage. Partition groups now persist raft state under:

```text
<data-dir>/meta/raft/space-partition-<n>/
```

This prevents restarted or replacement pods from receiving committed-index heartbeats for partition groups whose local raft log was empty.

## Migration from current behavior

Existing deployments may already have local random cluster IDs.

### Standalone deployments

No migration required. Existing local cluster ID may remain local metadata.

### Multi-node experimental deployments

These should be treated as unsafe. Migration should require explicit operator action.

Recommended migration path:

1. Stop writes.
2. Pick or repair authoritative data.
3. Start a new raft cluster with fresh system metadata.
4. Import/restore authoritative data.
5. Do not try to automatically merge pods with different cluster IDs.

### Development compose deployments

`make compose-reset` should produce one shared authoritative cluster ID after the new bootstrap protocol.

## Implementation phases

### Phase 1 — Reproduction test and fail-fast local identity changes

- Add tests proving three fresh raft-mode data dirs must not generate three random cluster IDs.
- Change local identity creation so raft mode creates pending identity without random cluster ID.
- Keep standalone behavior unchanged.

Acceptance:

- local unit tests fail on old behavior and pass on new pending identity behavior.

### Phase 2 — Durable system raft storage

- Wire persistent raft storage into system group startup.
- Persist hard state, entries, and snapshots.
- Restore system metadata after restart.

Acceptance:

- committed system metadata survives full process restart.

### Phase 3 — Bootstrap metadata proposal

- Implement node-1 bootstrap coordinator for empty system metadata.
- Generate cluster ID once inside `system.cluster.bootstrap_metadata`.
- Include all configured static nodes and placement.
- Make non-coordinators wait for committed metadata.

Acceptance:

- three fresh nodes converge on one `cluster_id`.

### Phase 4 — Metadata validation and readiness gating

- Validate local identity/config against system metadata.
- Add readiness blockers.
- Update cluster status/health to report authoritative metadata.
- Fail closed on mismatch.

Acceptance:

- mismatched node is NotReady and cannot receive client traffic.

### Phase 5 — Partition group startup from metadata

- Start partition groups based on system metadata placement.
- Stop treating env peer count as final authority after metadata exists.

Acceptance:

- partition placement reported by admin status matches system metadata.

### Phase 6 — Kubernetes/compose validation

- Validate compose three-node bootstrap.
- Validate K3s StatefulSet bootstrap.
- Validate parallel and ordered startup.
- Validate PVC loss/rejoin behavior.

Acceptance:

- all pods report the same cluster ID after fresh bootstrap and after restart.

## Test plan

### Unit tests

- `LoadOrCreate` in standalone mode creates local cluster ID as before.
- `LoadOrCreate` in raft multi-node mode does not invent a random cluster ID.
- System metadata bootstrap command validates cluster ID, node count, partition count, and replica factor.
- Local identity validation rejects mismatched cluster ID.
- Local identity validation rejects mismatched raft node ID.

### Integration tests

- Three fresh data dirs converge on one system metadata cluster ID.
- Restart all three nodes; cluster ID remains unchanged.
- Delete one follower data dir; follower restores identity from system metadata.
- Start node with wrong raft local node ID; node fails readiness.
- Start node with incompatible partition count; node fails readiness.

### Compose tests

After `make compose-reset && make compose-up`:

```sh
mycel cluster status # against each node
```

must show one shared cluster ID and expected members.

### Kubernetes tests

- Fresh three-pod StatefulSet bootstrap.
- Rolling restart.
- Parallel startup.
- One PVC loss and rejoin.
- One pod with stale/mismatched metadata.

## Open questions

1. Should V1 node IDs be derived from raft node IDs (`node_1`) or StatefulSet names (`myceld-0`)?
2. Should the bootstrap coordinator always be raft node 1, or should it be explicitly configured?
3. Should static V1 require all configured nodes to be present before bootstrap, or allow quorum bootstrap with missing nodes?
4. Should cluster name changes be allowed after bootstrap?
5. How should dynamic membership be layered on after static V1?

## Recommended decisions for V1

- Use raft node ID 1 as bootstrap coordinator.
- Use deterministic node IDs based on raft node IDs for internal identity.
- Treat `MYCELD_CLUSTER_NAME` as a label; system metadata cluster ID is generated once and persisted.
- Require partition count and replica factor to match metadata exactly on restart.
- Require quorum to commit initial metadata.
- Do not client-ready any node until system metadata is applied and validated.
- Keep standalone local identity behavior unchanged.

## Definition of done

This design is implemented when:

- fresh three-node raft deployments produce one shared cluster ID;
- all nodes report the same authoritative metadata;
- a node cannot independently self-bootstrap a random cluster ID in raft multi-node mode;
- system metadata survives restart;
- mismatched local metadata fails closed;
- health/readiness reflect metadata validity;
- compose and K3s tests prove convergence after reset, restart, and follower PVC loss.
