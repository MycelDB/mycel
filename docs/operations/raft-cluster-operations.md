# Raft Cluster Operations

## Scope

This guide covers the current static multi-node Raft deployment model for MycelDB. It assumes the authoritative system Raft metadata work is enabled in the daemon image.

Standalone mode remains local and self-owned. Multi-node Raft mode is different: local files are caches only, and the system Raft metadata record is the authority for cluster identity, membership, partition count, replica factor, and placement.

## Readiness model

A Raft-mode node is client-ready only after all of these are true:

1. system metadata has been applied;
2. system metadata has been validated against bootstrap config and local cache;
3. local identity cache has the authoritative cluster ID;
4. partition groups have started from metadata placement;
5. the node state is `clustered`.

A TCP-open gRPC port is not enough to route client traffic to a pod.

Relevant local cache/storage paths under `MYCELD_DATA_DIR`:

```text
meta/clustering/node.json          # local identity cache
meta/clustering/local_state.json   # local node state cache
meta/clustering/membership.json    # diagnostic membership cache populated from metadata
meta/raft/system/                  # durable system Raft metadata log/state
meta/raft/space-partition-*/       # durable partition Raft log/state
```

## Healthy cluster checks

For each node, run:

```sh
mycel --daemon-addr <host:9091> -u <admin> -p <password> --output json cluster status
mycel --daemon-addr <host:9091> -u <admin> -p <password> --output json cluster health
```

Expected three-node static cluster properties:

- every node reports the same non-empty `cluster.cluster_id`;
- every node reports `node.state = clustered`;
- every node reports `node.admitted = true`;
- `readiness.client_ready = true`;
- `readiness.metadata_applied = true`;
- `readiness.metadata_validated = true`;
- `readiness.partition_groups_started = true`;
- `readiness.authoritative_cluster_id` matches `readiness.local_cluster_id`;
- `cluster.health.status = healthy`;
- `active_members = 3` for a three-node cluster.

Kubernetes example:

```sh
NS=knotbase-dev
for pod in myceld-0 myceld-1 myceld-2; do
  kubectl -n "$NS" exec "$pod" -- \
    mycel --daemon-addr 127.0.0.1:9091 \
      -u "$MYCELD_BOOTSTRAP_ADMIN_USERNAME" \
      -p "$MYCELD_BOOTSTRAP_ADMIN_PASSWORD" \
      --output json cluster status
  kubectl -n "$NS" exec "$pod" -- \
    mycel --daemon-addr 127.0.0.1:9091 \
      -u "$MYCELD_BOOTSTRAP_ADMIN_USERNAME" \
      -p "$MYCELD_BOOTSTRAP_ADMIN_PASSWORD" \
      --output json cluster health
done
```

## Pre-release validation gates

For the complete Raft-focused test matrix, including named tests and destructive Compose/K3s validation steps, see `raft-cluster-test-matrix.md`.

Run the fast Phase A, Phase D, Phase E, Phase F, and Phase G gates during normal development and before review:

```sh
cd mycel
make test-phase-a
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
go test ./...
```

`make test-phase-d` covers the Phase D raft command coverage guardrails, composite state-machine dispatch hardening, D5 fail-closed subsystem behavior, and multi-subsystem raft restart/convergence tests.

`make test-phase-e` covers session/transaction home-node routing, forwarded client request handling, cross-node transaction-overlay workflows, home-node loss/session-lost behavior, backend auth rejection, and leader-change read-write transaction safety.

`make test-phase-f` covers consensus read-index barriers, graph strong reads, read-only transaction current-read semantics, query/metadata read consistency, read metadata, default stale-read rejection, and admin/CLI read diagnostics.

`make test-phase-g` covers deterministic local graph checksums, local/admin/backend consistency diagnostics, cluster consistency classification, forensic export/diff, CLI output, script syntax, and manual repair planning guardrails.

Before publishing a clustering-capable image or release, also run the destructive local cluster gates:

```sh
cd mycel
make test-compose-cluster
make test-k3s-cluster
```

Or use the bundled release gate:

```sh
make test-cluster-release-gate
```

The bundled release gate runs `make test`, `make test-phase-d`, `make test-phase-e`, `make test-phase-f`, `make test-phase-g`, then the destructive compose and K3s validations. `make test-compose-cluster` resets the sibling compose environment under `../../knot_pkm/knot_pkm_server`. `make test-k3s-cluster` resets/reuses the local K3s/k3d environment. Treat both as manual/pre-release checks, not default per-PR CI.

## Snapshot and compaction policy

B2 snapshot recovery is implemented for current composite children at an initial contract level, but automatic production compaction remains disabled by default.

Configuration knobs are intentionally conservative:

```sh
MYCELD_CLUSTER_RAFT_COMPACTION_MODE=off          # default; no automatic compaction loop
MYCELD_CLUSTER_RAFT_COMPACTION_MODE=conservative # accepted for future gated rollout, currently no auto loop
MYCELD_CLUSTER_RAFT_SNAPSHOT_ENTRIES=0
MYCELD_CLUSTER_RAFT_SNAPSHOT_INTERVAL=0s
MYCELD_CLUSTER_RAFT_SNAPSHOT_MAX_LOG_BYTES=0
MYCELD_CLUSTER_RAFT_SNAPSHOT_MIN_RETAIN_ENTRIES=0
```

Current snapshot capability matrix:

- system metadata: snapshot-capable and covered by snapshot-only restart/catch-up tests;
- identity user/admin: initial system snapshot/restore for users/admins and refresh sessions;
- backup: initial system snapshot/restore for policy only; running jobs and local archives are not authoritative snapshot state;
- space/schema/graph: initial partition snapshot/restore; graph V1 snapshots are latest-state plus revision, not full tombstone history;
- blob: initial metadata snapshot/restore with fail-closed payload availability validation; payload bytes are not embedded in raft snapshots;
- semantic: initial system/partition metadata snapshot/restore; vector indexes are derived/rebuildable, and running maintenance work is restored as pending;
- automation/change-stream: not current composite raft children; local/worker state is not snapshotted as raft-authoritative state.

Use `mycel cluster raft-groups` to inspect each local group's `last_index` and `snapshot_index`. A nonzero `snapshot_index` indicates raft storage has a persisted snapshot for that group. Snapshot catch-up is only for admitted replicas in one authoritative cluster; it is not a divergent-PVC merge or repair workflow.

If snapshot restore fails, keep the node out of service, preserve its PVC, collect daemon logs and `meta/raft/*/snapshot.pb` evidence, and rejoin from a known-good/fresh replica only after Phase G consistency evidence is understood. Do not copy graph/blob/semantic directories between divergent PVCs.

## Existing pinned-pod split-brain migration warning

If an existing three-pod deployment has already diverged and the application service is pinned to one known-good pod/PVC, keep it pinned until a controlled migration. The fixed raft image does **not** automatically rebalance, merge, or repair divergent old PVC contents.

Do **not** roll the new image across all existing divergent PVCs expecting raft to reorganize historical local graph data. Raft can prevent unsafe behavior going forward only after the cluster is formed from a consistent source of truth; it is not a conflict resolver for previously split-brain local stores.

Safe migration path until Phase G repair tooling exists:

1. Keep the service pinned to the working pod.
2. Stop writes and schedule a maintenance window.
3. Snapshot all PVCs before changing images, selectors, or replicas.
4. Capture local evidence from every PVC:
   - `meta/clustering/node.json`;
   - `meta/clustering/local_state.json`;
   - `meta/clustering/membership.json`;
   - `meta/raft/system/`;
   - `meta/raft/space-partition-*/`;
   - graph store directories for affected spaces.
5. Export application data from the pinned authoritative pod through the client import/export API where possible.
6. Bring up a fresh cluster with empty PVCs and the fixed image.
7. Import data through normal raft-owned APIs.
8. Run cluster health, raft-group diagnostics, data-plane validation, and app-level journal/login validation before switching traffic.

If multiple pods contain unique data that is not a strict superset, preserve all PVCs and follow the manual evidence workflow in `raft-cluster-manual-repair-workflows.md`. Do not manually merge segment files or copy graph directories between PVCs.

## Local graph consistency diagnostics

Phase G local diagnostics can report the latest committed graph revision, node/edge counts, and deterministic checksums for one space/domain on the contacted daemon:

```sh
mycel --daemon-addr <host:9091> -u <admin> -p <password> \
  --output json cluster consistency \
  --space-id <space-id> \
  --domain-id <domain-id>
```

The response is local-only evidence. It may include the local raft group status for the relevant partition, but it does not collect from peers or prove cluster-wide consistency. Use it to inspect one pod/PVC at a time and to prepare forensic comparisons.

To collect from expected raft replicas and classify the currently observable latest-state evidence, use the cluster report command:

```sh
mycel --daemon-addr <host:9091> -u <admin> -p <password> \
  --output json cluster consistency-report \
  --space-id <space-id> \
  --domain-id <domain-id>
```

Report statuses are `consistent`, `lagging`, `divergent`, `degraded`, and `unknown`. V1 reports compare latest-state checksums only (`latest_state_graph_v1_sha256_no_historical_compare`) and never repair, merge, delete, overwrite, or rebalance data.

## Forensic export and entity diff

When investigating divergent PVCs, first snapshot/copy all PVCs. Then collect bounded local exports from the pinned-good pod and from isolated archived PVCs. Use an explicit source label so later reports identify the pod/PVC source:

```sh
mycel --daemon-addr <pinned-good:9091> -u <admin> -p <password> \
  --output json cluster forensic-export \
  --space-id <space-id> \
  --domain-id <domain-id> \
  --source-label pinned-good \
  --page-size 1000 > pinned-good.json

mycel --daemon-addr <isolated-archived-pvc:9091> -u <admin> -p <password> \
  --output json cluster forensic-export \
  --space-id <space-id> \
  --domain-id <domain-id> \
  --source-label archived-pvc-b \
  --page-size 1000 > archived-pvc-b.json

mycel cluster forensic-diff --left pinned-good.json --right archived-pvc-b.json
mycel --output json cluster forensic-diff --left pinned-good.json --right archived-pvc-b.json
```

If an export is truncated, repeat with `--page-token <next_page_token>` and keep each page. V1 diff compares the entities present in the supplied export files; it does not automatically fetch all pages. The diff reports IDs only present on one side and entities with differing canonical fields/checksums. It is read-only and never repairs, merges, deletes, overwrites, or rebalances data. Use `raft-cluster-manual-repair-workflows.md` and `scripts/planGraphRepairWorkflow.sh` to choose a manual recovery path after evidence is complete.

## Normal Kubernetes operations

### Rolling restart

```sh
kubectl -n knotbase-dev rollout restart statefulset/myceld
kubectl -n knotbase-dev rollout status statefulset/myceld --timeout=10m
```

After the rollout, verify all pods still report the same cluster ID.

### StatefulSet pod/PVC replacement

If a pod loses its PVC but keeps the same StatefulSet ordinal, the replacement should rejoin as the same raft node ID and recover authoritative identity from system metadata:

```sh
kubectl -n knotbase-dev scale statefulset/myceld --replicas=2
kubectl -n knotbase-dev wait --for=delete pod/myceld-2 --timeout=3m
kubectl -n knotbase-dev delete pvc myceld-data-myceld-2
kubectl -n knotbase-dev scale statefulset/myceld --replicas=3
kubectl -n knotbase-dev rollout status statefulset/myceld --timeout=10m
```

Then validate cluster status and health on all pods. The cluster ID must remain unchanged.

## Readiness fields

The admin cluster status and health APIs expose a `readiness` object:

| Field | Meaning |
| --- | --- |
| `client_ready` | The node is safe for client traffic. |
| `metadata_applied` | The node has applied authoritative system Raft metadata. |
| `metadata_validated` | The metadata matches local bootstrap config and identity cache. |
| `partition_groups_started` | Partition raft groups have started from authoritative placement. |
| `authoritative_cluster_id` | Cluster ID from system Raft metadata. |
| `local_cluster_id` | Cluster ID cached in local identity files. |
| `expected_member_count` | Static cluster member count from authoritative metadata/config. |
| `readiness_blockers` | Operator-facing reasons that `client_ready` is false. |

Use the readiness object before reading logs. Logs are still useful for root cause, but the API should identify common blockers such as missing metadata, validation failures, and partition startup delays.

## Raft group diagnostics

Use the raft group diagnostics command to inspect local group health:

```sh
mycel --daemon-addr <host:9091> -u <admin> -p <password> cluster raft-groups
mycel --daemon-addr <host:9091> -u <admin> -p <password> --output json cluster raft-groups
```

Important fields:

| Field | Meaning |
| --- | --- |
| `group_id` | Raft group name, e.g. `system` or `space-partition-0`. |
| `kind` | `system` or `partition`. |
| `leader_node_id` | Current local view of the leader; `0` means no known leader. |
| `health` / `health_reason` | Local group health summary and reason. |
| `term` | Current raft term seen locally. |
| `commit_index` | Highest committed raft log index known locally. |
| `applied_index` | Highest committed index applied to the local state machine. |
| `apply_lag` | `commit_index - applied_index`; sustained non-zero lag means apply is behind. |
| `last_index` | Highest local raft log index. |
| `snapshot_index` | Snapshot index, when a snapshot is available. |
| `read_diagnostics.read_index_attempts` | Strong-read barriers attempted on this local group. |
| `read_diagnostics.read_index_successes` | Strong-read barriers that completed read-index and local apply wait. |
| `read_diagnostics.read_index_failures` | Strong-read barriers that failed closed. |
| `read_diagnostics.read_index_timeouts` | Failures caused by context cancellation/deadline. |
| `read_diagnostics.read_index_no_leader` | Read barriers rejected because no leader was known. |
| `read_diagnostics.read_index_not_leader` | Local node was not leader; the request should route to the leader instead of serving stale local state. |
| `read_diagnostics.apply_wait_failures` | The read-index quorum check completed but local apply did not reach the read index before failure/deadline. |
| `read_diagnostics.last_failure_reason` | Sanitized reason such as `no_leader`, `not_leader`, `deadline_exceeded`, `canceled`, or `apply_wait_deadline_exceeded`. |
| `read_diagnostics.last_read_index` | Last successful raft read-index safe point. |
| `read_diagnostics.last_applied_wait_index` | Last read index whose apply wait failed. |
| `read_diagnostics.last_applied_wait_success` | Last read index successfully reached by local apply before serving. |
| `read_diagnostics.last_applied_wait_millis` | Duration of the most recent apply wait. Sustained high values indicate apply lag. |

A group with `leader_node_id=0` or `health=no_leader` cannot safely accept writes or strong committed reads for that group. During startup or rolling restart this may be transient; if it persists, check peer reachability, backend auth, and raft transport logs.

Read diagnostics help distinguish read failures:

- `read_index_no_leader > 0`: the group had no leader. Check election stability and transport.
- `read_index_not_leader > 0`: requests reached a follower. Check route metadata and leader routing; the daemon must not serve local stale state.
- `read_index_timeouts > 0` with no transport errors: quorum or local apply may be slow; compare `apply_lag` and `last_applied_wait_millis`.
- `apply_wait_failures > 0`: the leader obtained a read index but local state did not apply through it before deadline; inspect disk/state-machine apply latency.
- `last_failure_reason=apply_wait_deadline_exceeded`: strong reads are failing closed due to local apply lag rather than stale fallback.

Read-index failures and slow apply waits are logged with group ID, local node ID, leader node ID, read index when available, sanitized reason, and duration. Logs must not include request payloads, auth tokens, or user data.

## Backend auth policy

Multi-node raft deployments must set `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` to the same non-empty secret on every pod/node. Myceld fails configuration validation when `MYCELD_CLUSTER_RAFT_NODE_ADDRS` describes a multi-node raft cluster and the backend auth token is empty.

Use a generated secret, for example:

```sh
openssl rand -base64 32
```

Static V1 clusters do not support coordinated token rotation yet. To rotate, update all node configuration/secrets together and perform a controlled restart. During mismatched rotation, peers with the old/new token mix will reject internode RPCs and raft transport diagnostics will show `auth_failures`.

Single-node raft/dev deployments may omit the token, but production clustered deployments should not.

## Raft transport diagnostics

`GetClusterRuntimeStatus` exposes aggregate raft transport diagnostics under `raft_transport`:

| Field | Meaning |
| --- | --- |
| `send_attempts` | Raft messages the local node attempted to send. |
| `send_failures` | Failed raft message sends. |
| `auth_failures` | Send failures caused by backend authentication/authorization rejection. |
| `missing_sender_failures` | Sends where the target node had no configured sender/address. |
| `last_error_at` / `last_error` | Last transport failure time and sanitized error text. |
| `last_group_id`, `last_source_node_id`, `last_target_node_id`, `last_message_type` | Context for the most recent failure. |
| `targets[]` | Per group/target-node counters and last failure context. |

Transport failures are also logged with group ID, source node, target node, raft message type, reason, and sanitized error text. Logs and diagnostics must not include backend auth tokens.

Common causes:

- `missing_sender_failures`: `MYCELD_CLUSTER_RAFT_NODE_ADDRS` is missing an entry, has the wrong order, or names a peer that is not part of the static V1 cluster.
- connection/refused/DNS errors in `last_error`: pod/service DNS mismatch, peer pod not ready, network policy/firewall, or backend port not serving.
- `auth_failures`: `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` differs between pods, is missing on one side, or was rotated inconsistently.

## Phase D subsystem ownership and unsupported behavior

In raft mode after Phase D:

| Subsystem/state | Raft-mode behavior |
| --- | --- |
| System cluster metadata | System raft authoritative. |
| Admin/user identity and refresh sessions | System raft authoritative. |
| Spaces, domains, ACL grants, schemas, graph commits, blob metadata, semantic space/maintenance | Partition raft authoritative. |
| Semantic global configuration and accounting/audit | System raft authoritative. |
| Blob payload files | Payload must be locally available or fetched/checksummed from peers before raft metadata apply exposes it. |
| Backup policy/delete | System raft authoritative; scheduled/manual execution is system-leader-only. |
| Automation definitions, invocations, run/audit state, proposals, policies, schedule checkpoints | Unsupported/fail-closed for local durable writes in raft mode until raft ownership or leader-owned scheduling is implemented. Worker scratch state is local/derived. |
| Change-stream durable history/checkpoints/subscriptions | Raft-mode subscriptions fail closed; local publish/history writes are skipped until streams are derived from committed raft graph changes. |
| Legacy embedding provider-key WAL records | Unsupported/superseded in raft-mode daemon by semantic global credentials/config. |
| Vector index files/search materialization | Derived/local and rebuildable from graph plus semantic raft configuration; freshness is not yet a linearizable read guarantee. |

## Client routing and session/transaction behavior

Phase E V1 makes daemon-local client workflows safe across ready pods by routing session- and transaction-scoped unary requests to the encoded home node. In raft mode, newly created session and transaction IDs encode their home node:

```text
s.<raft-node-id>.<uuid>
tx.<raft-node-id>.<uuid>
```

Any ready pod receiving a unary session, transaction, graph, query, or metadata-catalog request with a remote-home ID forwards the protobuf request over the authenticated cluster backend to the home node. The home node executes the normal client API adapter with the forwarded principal and returns the protobuf response.

V1 boundaries:

- in-flight sessions and transaction overlays remain home-node local;
- home-node loss is not transparent failover: existing sessions/transactions return a retryable route error or a documented session-lost/not-found error;
- read-write transactions require their home node to remain the graph partition leader for the target space;
- leader changes during an active read-write transaction fail safely instead of committing local-only state;
- streaming `CreateBlobNode` and import/export streams fail closed for remote-home transaction IDs rather than buffering and forwarding streams;
- Phase F V1 now provides formal read-index/linearizable committed-read semantics by default; stale reads remain disabled unless future explicit config/request opt-in is implemented.

Common client-visible routing failures:

| gRPC code | Typical meaning | Operator/client action |
| --- | --- | --- |
| `Unavailable` | Home node, backend route, graph partition leader, or raft group is unavailable. | Retry after readiness/health recovers; check backend addresses, raft leaders, and transport diagnostics. |
| `NotFound` | Session/transaction ID is unknown on its encoded home node, often after home-node restart/loss of in-flight state. | Treat the in-flight session/transaction as lost; open a new session and retry idempotent work from committed state. |
| `FailedPrecondition` | Session/transaction is closed, home metadata conflicts, route-loop guard fired, or write leadership changed mid-transaction. | Do not fall back to local state. Retry by starting a new transaction/session if appropriate. |
| `Unauthenticated` | Internode backend auth token mismatch or missing token on a protected backend. | Verify `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` is identical on all nodes. |
| `InvalidArgument` | Malformed route-bearing ID or invalid request payload. | Fix client/request construction. |

Routing diagnostics are local/internal in this tranche. Session diagnostics distinguish local/remote route counts and active local/remote sessions/transactions. Client-router diagnostics count forwarding attempts, successes, failures, route-loop rejections, and sanitized last-failure context. Backend forwarded-request diagnostics count received/dispatched/failure requests, auth/cluster rejections, route-loop rejections, and sanitized last-failure context.

## Graph operation fail-closed behavior

In raft mode, graph operations do not fall back to local file state when the node cannot prove a safe raft route:

- graph reads and transaction base revision lookup require a known partition leader;
- backend local graph reads reject mismatched `space_id` values and reject requests that reach a non-leader node;
- read-write transaction reads and writes run on the transaction home node and require that node to remain the partition leader so overlays are not bypassed;
- graph mutations require the local node to be the target partition leader before local validation/staging;
- graph commits/proposals return retryable unavailable errors when the partition group is missing, leaderless, or no longer local to the transaction home;
- clustered local write paths reject subsystem mutation if the subsystem has not been wired to a raft executor.

Clients may still see retryable `Unavailable` errors during leader changes, home-node loss, backend auth/transport problems, no-leader windows, or read-index/apply-wait failures. Retry via a healthy/ready endpoint only after the route/leader/read-barrier condition recovers; do not retry by forcing local state access.

## Readiness blockers and recovery

### `system metadata not applied`

The node has not applied the authoritative metadata record yet.

Check:

```sh
kubectl -n <ns> logs pod/<pod> --tail=200
kubectl -n <ns> get pods -l app.kubernetes.io/name=myceld
```

Likely causes:

- system Raft quorum is unavailable;
- peer DNS or backend addresses are wrong;
- bootstrap coordinator node `1` did not start;
- network policy or service configuration blocks peer traffic.

Recovery:

- restore quorum;
- verify `MYCELD_CLUSTER_RAFT_NODE_ADDRS` and StatefulSet ordinal mapping;
- do not delete multiple PVCs at once unless intentionally rebuilding the cluster.

### `system metadata validation failed`

The committed metadata conflicts with local config or cache.

Common causes:

- changed partition count or replica factor after bootstrap;
- changed raft node ID / StatefulSet ordinal mapping;
- local cached cluster ID belongs to a different cluster;
- backend advertise address does not match metadata.

Recovery:

1. Stop client writes.
2. Compare local cache files with authoritative configuration.
3. If only one node cache is stale and the node has no unique data to preserve, replace that node/PVC so it rejoins from metadata.
4. If multiple nodes have divergent cluster IDs, do not merge PVCs. Treat the deployment as unsafe and restore/import from a chosen authoritative source.

### `partition groups are not started`

System metadata was accepted, but partition groups are not client-ready.

Check logs for raft group startup errors. Verify partition count, replica factor, and placement in system metadata match configured static cluster values.

### `active member count ... below expected ...`

The node can see fewer active members than the static cluster expects.

Recovery:

- wait briefly during rollout;
- verify all pods are Ready;
- check pod restarts and logs;
- restore missing pods/PVCs before routing client traffic broadly.

### `cluster_id` mismatch

A mismatch means the local cache and system metadata disagree. This is fail-closed by design.

Do not copy files between PVCs and do not edit `node.json` by hand except as a last-resort forensic action on an offline copy. Prefer replacing the bad node/PVC or restoring from backup.

## Unsafe scenarios

Avoid these operations unless intentionally rebuilding the cluster:

- starting three independent raft pods from old images that self-bootstrap local cluster IDs;
- manually combining data from PVCs that report different cluster IDs;
- changing `MYCELD_CLUSTER_RAFT_PARTITION_COUNT` after bootstrap;
- changing `MYCELD_CLUSTER_RAFT_REPLICA_FACTOR` after bootstrap;
- reusing a PVC from another cluster.

## Local validation commands

Fast tests:

```sh
go test ./internal/clustering ./internal/clustering/consensus ./internal/daemon/app ./internal/daemon/api/admin
```

Full tests:

```sh
go test ./...
```

Compose validation:

```sh
make test-compose-cluster
```

Fast cluster identity guard:

```sh
make test-cluster-identity
```

Compose validation:

```sh
make test-compose-cluster
```

K3s/k3d validation:

```sh
make test-k3s-cluster
```

`make test-k3s-cluster` builds a local image from the current checkout, imports it into the `knotbase-dev` k3d cluster when available, deploys the Mycel StatefulSet resources, and validates fresh bootstrap, rolling restart, and one-PVC replacement/rejoin. It is destructive to the target namespace. Useful overrides:

```sh
MYCEL_K3S_CLUSTER=knotbase-dev \
MYCEL_K3S_NAMESPACE=knotbase-dev \
MYCEL_K3S_IMAGE=myceldb/mycel:k3s-local \
MYCEL_K3S_RESET=true \
make test-k3s-cluster
```
