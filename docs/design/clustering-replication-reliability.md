# Clustering and Replication Reliability

## Status

Urgent design investigation and implementation plan. MycelDB raft clustering is not production-safe until the required correctness, deployment, and validation work in this document is complete.

## Context

Knot PKM is running on K3s against `myceldb/mycel:v0.4.3` with a three-pod StatefulSet:

- `myceld-0`
- `myceld-1`
- `myceld-2`

The intended raft configuration is:

```text
MYCELD_CLUSTER_ENGINE=raft
MYCELD_CLUSTER_RAFT_NODE_COUNT=3
MYCELD_CLUSTER_RAFT_REPLICA_FACTOR=3
MYCELD_CLUSTER_RAFT_PARTITION_COUNT=16
MYCELD_CLUSTER_RAFT_NODE_ADDRS=myceld-0.myceld-headless:9091,myceld-1.myceld-headless:9091,myceld-2.myceld-headless:9091
```

Each pod derives `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID` from its StatefulSet ordinal.

The Kubernetes `myceld` ClusterIP service selects all three pods. The current orchestration uses `sessionAffinity: ClientIP`, but this is not a correctness boundary. It does not make MycelDB replicated, does not prove that all pods share the same cluster identity, and does not make daemon-local sessions portable across pods.

## Observed failure

Knot PKM journal editing fails on empty journal days with:

```text
rpc error: code = InvalidArgument desc = invalid graph input: from node: graph entity not found
```

The failing app operation is:

```text
POST /api/nodes
```

The app is creating a node and a `contains` edge:

```text
journal --contains--> new journal entry
```

Mycel reports that the edge `from` journal node does not exist.

The affected PKM space has divergent graph state across Mycel pods:

```text
myceld-0: nodes=166, edges=158
myceld-1: nodes=166, edges=158
myceld-2: nodes=177, edges=167
```

Three empty journal-day nodes exist only on `myceld-2`:

- `2026-07-30`
- `2026-08-11`
- `2026-08-29`

They do not exist on `myceld-0` or `myceld-1`.

The user-visible bug is therefore explained by cross-pod inconsistency: a read path observes a journal node on one pod, while a later write path hits another pod that lacks that node. The write fails when Mycel validates the edge endpoint.

Additional evidence: each pod reports a different `cluster_id`, despite being configured as one raft cluster. That strongly suggests the pods are not participating in one authoritative replicated cluster.

## Root-cause hypotheses

These hypotheses are not mutually exclusive. Current evidence suggests several are true at once.

### H1 — The configured pods do not share one authoritative cluster identity

Current cluster identity is created locally per data directory when clustering metadata is initialized. If each StatefulSet PVC bootstraps independently, each pod can acquire a different `cluster_id` even when `MYCELD_CLUSTER_NAME` and raft addresses match.

A raft cluster must have one durable cluster identity and one agreed bootstrap configuration. If every node invents local identity independently, the deployment can look like a cluster but behave like separate local daemons trying to exchange raft messages.

Required conclusion: a node with a mismatched `cluster_id` must not become ready as a member of the service.

### H2 — Raft metadata is not the source of cluster authority

The code has a system raft state machine for bootstrap metadata and node registration, but startup still derives peer membership from environment variables. The bootstrap metadata is not clearly proposed once, persisted, restored, and enforced as the authoritative cluster configuration.

That means the intended raft metadata model may exist in code but not be the actual production authority.

### H3 — Raft group storage is not durable enough for restart/recovery

The raft implementation currently uses in-memory raft storage for groups. There is file-backed persistent storage code, but the daemon raft runtime does not appear to wire it as the authoritative group log/hard-state/snapshot storage.

If raft logs are memory-only while graph segments are persisted locally, a restart can leave persisted graph data and raft state inconsistent. Even if this is not the only cause of the current divergence, it prevents reliable replicated-state-machine semantics.

Required conclusion: raft log/hard-state/snapshots must be durable and must be treated as the source of replicated truth.

### H4 — Graph writes may not be fully covered or consistently routed through raft

Graph commits have raft paths, but the deployment has observed persistent graph divergence. That can happen if:

- some writes bypass raft and commit to local graph segments only;
- raft commit/apply succeeds on one pod but is not durably replicated/applied on others;
- reads and writes occur during no-leader windows and fall back to local state;
- session/transaction handling causes writes to be staged/committed on local pod state without a stable cluster route;
- raft groups are not actually the same cluster because identity/bootstrap differs.

Required conclusion: in raft mode, every durable graph mutation must be accepted only through the owning raft partition leader and must become visible only after quorum commit and apply.

### H5 — Reads may be served from local state without a consistency guarantee

Current graph read paths include leader forwarding, but no-leader or misconfigured cases can return local data instead of failing closed. If a node serves local committed graph data that is not known to be current for the partition, clients can observe data that cannot be safely mutated elsewhere.

Required conclusion: in raft mode, a read must be either:

- leader-validated/read-index consistent, or
- explicitly documented and marked as stale/local.

For V1 reliability, reads should fail closed when no safe leader/commit-index guarantee exists.

### H6 — Spaces/domains/users/session records are replicated differently from graph segments

Different subsystems have different raft coverage:

- user/admin refresh sessions are intended to use system raft;
- spaces/domains have raft paths;
- graph commits have partition raft paths;
- blobs, semantic records, schema records, backup records, automation records, and session/transaction runtime state have uneven or incomplete coverage.

If graph state is partition-replicated but sessions are local, or schema is local while graph validation is cluster-wide, application behavior can diverge across pods.

Required conclusion: every subsystem must have an explicit cluster-mode consistency model: replicated, leader-routed, derived/rebuildable, or local-only and not exposed through the load-balanced service.

### H7 — Kubernetes load balancing is incompatible with current session/transaction semantics

The API exposes daemon-owned sessions and transactions. The session manager keeps open sessions and transaction overlays in process-local memory. `OpenSession`, `BeginTransaction`, graph operations, query operations, and `CommitTransaction` therefore require a consistent home daemon unless transaction state is made portable or routed.

A ClusterIP over all pods can send different TCP connections, clients, jobs, or retries to different pods. `sessionAffinity: ClientIP` helps only in some network paths and does not solve:

- multiple app/server pods with different source IPs;
- restarts or connection churn;
- L7/gRPC balancing;
- failover to another pod;
- token/session portability;
- local graph divergence.

Required conclusion: until session/transaction state is cluster-aware, clients must not be routed arbitrarily across all pods for one logical workflow.

### H8 — Readiness probes are too weak

A TCP readiness probe only proves that the gRPC port is open. It does not prove:

- cluster identity matches;
- raft groups have leaders;
- this node has applied through required indexes;
- graph partitions are not divergent;
- the node can safely serve reads/writes.

Required conclusion: raft-mode readiness must be correctness-aware.

## Correctness model proposal

MycelDB must explicitly support two modes before Kubernetes deployments can be safe.

### Mode 1 — Standalone

Standalone mode is one daemon owning one data directory.

Guarantees:

- all state is local to one daemon;
- sessions and transactions are daemon-local;
- graph writes commit locally;
- Kubernetes should run one replica;
- the service should target only that one pod.

This is the safe current production fallback.

### Mode 2 — Raft replicated cluster

Raft mode is a replicated-state-machine cluster with one system group and partition groups.

Required V1 guarantees:

- one durable cluster identity;
- one durable bootstrap configuration;
- all durable state belongs to either the system raft group or a deterministic partition raft group;
- writes are accepted by the owning group leader or forwarded to it;
- a successful write means quorum-committed and leader-applied;
- reads are leader/read-index consistent by default;
- followers must not serve authoritative reads unless a safe read-index/lease mechanism is implemented;
- no node becomes Kubernetes-ready unless it is a valid member of the cluster and can safely route or serve API requests.

### Subsystem consistency targets

| Subsystem | Required raft-mode model |
| --- | --- |
| Cluster identity/bootstrap metadata | System raft, durable, one authoritative value. |
| Operators/admins/users | System raft for durable identity records and refresh sessions. |
| Access grants/capabilities | System or partition raft depending on scope; must be authoritative cluster-wide. |
| Spaces | System or partition raft with deterministic ownership; list/read must be consistent. |
| Domains | Partition-owned durable state; schema and graph validation must agree cluster-wide. |
| Sessions | Either home-node routed with explicit affinity metadata, or replicated/durable. V1 can be home-node routed. |
| Transactions | Home-node routed for in-flight overlays in V1; commits must go through raft. Long-term can use leader-owned transaction contexts. |
| Graph nodes/edges | Partition raft. All commits quorum-replicated; reads leader/read-index consistent. |
| Blobs | Metadata through raft; payload replication must be durable and verified, or payload store must be shared/object-backed. |
| Schemas | Partition raft or system raft by domain ownership; must not be pod-local in cluster mode. |
| Semantic indexes | Derived/rebuildable state may be local, but authoritative index metadata/config/checkpoints must be rafted. Search results must document freshness. |
| Automations | Definitions/invocations/audit records must be rafted or explicitly owned by one scheduler leader. Derived worker state can be local. |
| Backups | Policy/config must be rafted; execution should be leader-elected/single-runner. |
| Change streams | Events generated from committed raft graph changes; checkpoints must be comparable across replicas. |

### API operation guarantees

#### `OpenSession`

In raft mode, `OpenSession` must return a session with an explicit home node or route token, or the service must guarantee all future calls for that session reach the same home node.

Minimum V1:

- response includes server-side metadata internally mapping `session_id -> home_node_id`;
- any node receiving a session RPC either serves it if local or forwards to the home node;
- if home node is unavailable, in-flight sessions fail with a retryable/session-lost error.

#### `BeginTransaction`

A transaction is created on the session home node in V1.

Guarantees:

- read-only transaction uses a safe committed revision;
- read-write transaction stages local overlay on the home node;
- transaction response records base revision;
- future operations are routed to transaction home.

#### `ListNodes` / `GetNode`

In raft mode, committed graph reads must be leader/read-index consistent for the relevant partition/domain.

Inside a read-write transaction, reads must include the transaction overlay. Therefore V1 should route the whole transaction to its home node and ensure that home node either:

- performs safe leader reads and overlays local changes, or
- forwards to leader with overlay context, or
- requires the home node to be the relevant leader for write transactions.

Do not silently fall back to local stale reads.

#### `ApplyGraphOperations` / graph mutations

Mutations are staged in the transaction overlay. On commit, the overlay is converted into a deterministic graph commit command for the owning partition.

Guarantees:

- edge endpoint validation is performed against a consistent snapshot plus overlay;
- the parent node cannot be visible on one pod but missing for validation on another authoritative write path;
- commit is accepted only by the partition leader or forwarded to it.

#### `CommitTransaction`

A successful commit means:

- the graph commit command was proposed to the owning raft group;
- it reached quorum;
- the leader applied it;
- the committed revision is returned;
- followers will eventually apply it and readiness/diagnostics expose lag.

If quorum or leader is unavailable, commit fails closed. It must not write local-only graph state in raft mode.

## Kubernetes deployment design

### Current ClusterIP-over-all-pods service is unsafe as the primary correctness mechanism

A ClusterIP selecting all pods can be used only if MycelDB itself guarantees routing/forwarding/read consistency. Kubernetes service affinity is not a database consistency model.

Until MycelDB has robust leader routing and session home-node forwarding, applications must not rely on a load-balanced `myceld` service over all pods for writes.

### Recommended service model by maturity

#### Immediate safe model

Run one authoritative pod:

- StatefulSet replicas: `1`, or
- service selector pins to one known-good pod, e.g. `statefulset.kubernetes.io/pod-name=myceld-2`.

This is standalone semantics.

#### Medium-term model

Expose explicit services:

- `myceld-leader` or `myceld-router`: stable endpoint for writes and strongly consistent reads.
- `myceld-members`: headless/member service for internode raft traffic.
- optionally `myceld-readonly`: only when follower stale/read-index reads are deliberately supported.

Clients should connect to a router/leader-aware endpoint, not arbitrary pods.

#### Long-term model

Any pod may receive client traffic only if it can safely route every request to the correct raft leader or session home node. Readiness must remove pods that cannot route safely.

### Session affinity

`sessionAffinity: ClientIP` is not sufficient. It may be retained as an optimization but must not be required for correctness.

Correctness should come from one of:

- leader/router endpoint;
- daemon-level forwarding by `session_id`/`transaction_id`/partition;
- replicated transaction/session state.

### StatefulSet bootstrap

StatefulSet startup must support deterministic cluster bootstrap:

- one bootstrap identity/config created exactly once;
- joining pods verify the bootstrap identity;
- PVC loss produces either a clean rejoin protocol or a hard failure requiring operator intervention;
- a pod with an unexpected local cluster ID never becomes ready;
- parallel pod startup must not create multiple independent clusters.

### Restart, rescheduling, and PVC loss

Expected behavior:

- Restart with same PVC: node reloads raft hard state/log/snapshot and rejoins.
- Restart without PVC: node is treated as a new or recovering member and must catch up from peers/snapshots before ready.
- Partial bootstrap: cluster remains unavailable until quorum-safe bootstrap completes; it must not accept local writes.
- Lost quorum: writes fail; reads fail unless explicitly stale reads are requested and safe.

## Immediate mitigation for Knot PKM

### Recommended short-term operational mode

Use single-authoritative-pod semantics until raft reliability work is complete.

Safest options:

1. Scale Mycel to one replica using the pod with the latest/superset graph data.
2. Or pin the `myceld` service to that pod, likely `myceld-2`, because it contains the missing journal nodes.

This prevents the application from observing nodes on one pod and writing to another pod that lacks them.

### Existing divergent data repair

Do not merge divergent graph data automatically without a tool that understands graph revisions and conflicts.

Safest repair path:

1. Stop writes from Knot PKM.
2. Snapshot/export all three pod data directories/PVCs.
3. Identify the authoritative pod by graph superset and app-level recency; current evidence suggests `myceld-2`.
4. Run Mycel as a single replica against the authoritative PVC.
5. Validate:
   - affected journal nodes exist;
   - node/edge counts match expected app state;
   - user login and journal editing work;
   - semantic/index jobs can rebuild if needed.
6. Archive non-authoritative PVCs for forensic analysis.
7. Recreate non-authoritative replicas only after raft cluster rejoin is fixed, or keep single-replica mode.

If data exists on multiple pods that is not a strict superset, build a dedicated export/diff/import repair tool before merging.

## Long-term fix plan

### Phase A — Fail closed and improve observability

Goals:

- stop silent split-brain behavior;
- make the current cluster state diagnosable.

Tasks:

- Add explicit cluster-mode readiness checks.
- Report cluster ID, local node ID, configured peers, and raft group status in health/admin APIs.
- Fail graph reads in raft mode when leader/route is unavailable; do not fall back to local stale state by default.
- Fail graph writes in raft mode unless the commit path is known to be raft-backed and quorum-safe.
- Require backend auth token for raft/internode traffic in cluster mode.
- Surface raft transport errors instead of dropping them silently.

Acceptance:

- a misconfigured three-pod deployment does not become green/ready;
- divergent cluster IDs are visible immediately;
- no-leader writes fail clearly rather than writing locally.

### Phase B — Durable raft runtime

Goals:

- make raft logs/hard state/snapshots durable;
- make raft the source of replicated truth.

Tasks:

- Wire persistent raft storage into daemon group startup.
- Persist hard state, entries, and snapshots per group.
- Add snapshot creation/restoration for system and partition state machines.
- Ensure graph/space/blob/schema/semantic state can be recovered from raft log/snapshot.
- Add restart tests proving no graph divergence after restart.

Acceptance:

- stop all pods, restart all pods, graph counts/checksums converge;
- PVC loss for one follower triggers catch-up/snapshot, not independent cluster bootstrap.

### Phase C — Authoritative cluster identity and bootstrap

Goals:

- one durable cluster ID;
- deterministic join/rejoin protocol.

Tasks:

- Bootstrap cluster metadata exactly once into system raft.
- Derive/validate local node identity from persisted system metadata.
- Reject startup when local metadata conflicts with system metadata.
- Add operator-visible cluster bootstrap status.
- Make `MYCELD_CLUSTER_NAME` a human label, not a substitute for cluster identity.

Acceptance:

- all pods report the same cluster ID;
- pod with mismatched cluster ID is NotReady and refuses client traffic;
- parallel StatefulSet startup cannot create three independent clusters.

### Phase D — Complete raft command coverage

Detailed plan: `../implementation/phase-d-raft-command-coverage-implementation-plan.md`.

Goals:

- all durable daemon state has an explicit raft-mode owner.

Tasks:

- Audit every WAL/durable record type.
- Add raft apply/propose paths or mark records derived/local-only.
- Ensure schema records are rafted.
- Ensure automation definitions/invocations/audit records are rafted or leader-owned.
- Ensure blob metadata is rafted and blob payload replication/storage is safe.
- Ensure semantic configuration/checkpoints are rafted; derived vector indexes can be rebuildable.
- Add compatibility tests for each subsystem.

Acceptance:

- no durable user-visible state is pod-local in raft mode unless explicitly documented as derived/rebuildable.

### Phase E — Leader routing and session/transaction routing

Goals:

- clients may connect to any ready pod without violating correctness;
- in-flight session/transaction workflows are routed safely.

Tasks:

- Add partition leader resolver APIs usable by all services.
- Implement forward-to-leader for writes.
- Implement safe leader/read-index reads.
- Add session home-node mapping.
- Route `session_id` and `transaction_id` operations to home node.
- Decide failover semantics for in-flight sessions: V1 can fail in-flight transactions on home-node loss; committed state remains safe.
- Consider embedding route/home metadata in session/transaction IDs or storing it in a rafted session directory.

Acceptance:

- `OpenSession` on pod A, graph read on pod B, graph write on pod C either succeeds through routing or fails with a documented retryable route/session error;
- no operation validates against stale local graph state.

### Phase F — Read consistency model

Goals:

- define and enforce read guarantees.

Tasks:

- Default to strong reads in raft mode.
- Add optional stale reads only with explicit API/config opt-in.
- Expose revision/read-index metadata in responses where useful.
- Ensure read-write transactions provide read-your-writes.

Acceptance:

- clients cannot observe a committed node that another pod cannot validate for mutation;
- tests prove read/write across different pods is safe.

### Phase G — Divergence detection and repair tooling

Goals:

- detect divergence early;
- provide safe forensic and repair tools.

Tasks:

- Add per-space/domain revision comparison across pods.
- Add graph checksums per domain/partition/revision.
- Add node/edge count diagnostics by domain and partition.
- Add raft applied index/commit index per group/pod.
- Add admin command/API for cluster consistency report.
- Add export/diff tooling for divergent graph recovery.

Acceptance:

- operators can see whether all replicas agree on a domain;
- repair tools can identify superset/conflict cases before data is discarded.

## Diagnostics to add

Minimum diagnostics:

```text
cluster_id
cluster_name
node_id
node_name
configured_peer_addrs
known_peer_ids
raft_group_id
raft_role leader/follower/candidate
raft_leader_id
raft_term
raft_commit_index
raft_applied_index
raft_last_index
raft_snapshot_index
transport_send_errors
proposal_failures/timeouts
per-domain committed_revision
per-domain node_count
per-domain edge_count
per-domain graph_checksum
per-domain schema_version/checksum
```

Expose through:

- Admin API;
- CLI (`mycel admin cluster ...`);
- readiness/health details;
- structured logs at startup and membership changes.

## Test plan

### Unit and component tests

- System raft bootstrap metadata persists and restores.
- Cluster ID mismatch rejects readiness/startup.
- Raft transport send failures are counted and surfaced.
- No-leader graph reads fail closed in raft mode.
- Every durable record type has a raft-mode handling decision.

### Multi-node integration tests

Run three in-process or containerized daemons with distinct data dirs.

Required tests:

1. **Graph write replication**
   - create node on node A;
   - wait for apply on B/C;
   - verify node and counts on all nodes.

2. **Cross-pod read/write workflow**
   - open session through A;
   - begin transaction through B;
   - create parent node through A;
   - create child + edge through C;
   - commit through B;
   - verify all replicas converge.

3. **Empty journal workflow**
   - create journal parent on pod A;
   - create entry and `contains` edge via pod B;
   - verify success;
   - verify all pods have parent, child, and edge.

4. **Reads and writes hit different pods**
   - read node through follower/non-home pod;
   - mutate related edge through another pod;
   - verify routing or documented safe failure.

5. **Restart durability**
   - create graph data;
   - stop all pods;
   - restart all pods;
   - verify same cluster ID, raft indexes, graph counts, checksums.

6. **Follower PVC loss**
   - delete one follower data dir;
   - restart follower;
   - verify it rejoins/catches up or fails safely until repaired.

7. **Split-brain prevention**
   - start one pod with wrong cluster metadata;
   - verify NotReady/refuses client traffic;
   - verify no local writes accepted.

8. **Token/session cross-node behavior**
   - login through A;
   - use token through B/C;
   - verify accepted when shared token model exists or rejected with documented pre-routing behavior.

9. **Blob/schema/semantic coverage**
   - create graph node with blob/schema-sensitive fields;
   - verify metadata and validation behavior are consistent across all pods.

### Kubernetes/K3s validation tests

Use the same StatefulSet/service shape as production-like deployments.

Required tests:

- ClusterIP over all pods with no client affinity.
- ClusterIP with `sessionAffinity: ClientIP`.
- dedicated leader/router service.
- pod restart during write traffic.
- rolling restart.
- one pod NotReady due to cluster ID mismatch.
- load-balanced Knot PKM journal create/edit flow.

Pass condition: clients must never observe a parent node from one pod and then fail to create an edge from another pod because that parent is missing.

## Recommended immediate decisions

1. Treat raft mode as experimental/unsafe for production-like Knot PKM until this plan is implemented and validated.
2. Run Knot PKM against one authoritative Mycel pod or one replica.
3. Preserve current divergent PVCs for forensic analysis.
4. Do not attempt automatic multi-pod data merge without graph-aware diff tooling.
5. Prioritize fail-closed readiness/diagnostics before adding more raft features.
6. Prioritize durable raft storage and cluster identity validation before advertising multi-node deployment as supported.

## Definition of done

MycelDB clustering is not considered reliable until all of the following are true:

- all pods in a raft deployment report the same cluster ID;
- raft state survives restart;
- every durable user-visible subsystem has an explicit raft-mode consistency model;
- graph writes are quorum-replicated and verified on all replicas;
- reads are leader/read-index consistent by default;
- sessions/transactions are either routed or replicated;
- Kubernetes readiness fails closed for unsafe nodes;
- diagnostics can prove per-domain graph convergence;
- K3s tests reproduce the Knot PKM journal workflow across load-balanced pods without divergence or missing-edge endpoint errors.
