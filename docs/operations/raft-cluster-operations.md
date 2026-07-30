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

Local k3d validation uses a locally built image imported into the k3d cluster, then runs fresh bootstrap, rolling restart, and one-PVC replacement checks.
