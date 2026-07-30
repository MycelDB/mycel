# Problem 1 Reproduction: Pods Do Not Share One Cluster Identity

## Summary

Problem 1 from `docs/design/clustering-replication-reliability.md` is reproducible locally with the three-node compose deployment.

Even when all three daemons are configured with the same cluster name, raft node count, raft node addresses, and distinct raft local node IDs, each daemon creates its own local `cluster_id` and reports itself as a healthy one-member cluster.

This means the deployment is not bootstrapping one authoritative MycelDB cluster identity. It is starting three locally bootstrapped cluster identities next to each other, while the experimental raft runtime separately starts raft groups from environment-provided numeric node IDs and addresses.

## Reproduction environment

Repository/branch:

```text
mycel/improved_clustering
```

Compose stack:

```text
../knot_pkm/knot_pkm_server/compose.dev.yml
```

Relevant compose configuration:

- three Mycel daemon containers:
  - `myceld-a`
  - `myceld-b`
  - `myceld-c`
- shared `MYCELD_CLUSTER_NAME=knot-pkm-dev`
- `MYCELD_CLUSTER_RAFT_NODE_COUNT=3`
- `MYCELD_CLUSTER_RAFT_REPLICA_FACTOR=3`
- `MYCELD_CLUSTER_RAFT_PARTITION_COUNT=16`
- `MYCELD_CLUSTER_RAFT_NODE_ADDRS=myceld-a:9091,myceld-b:9091,myceld-c:9091`
- local node IDs:
  - `myceld-a`: `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID=1`
  - `myceld-b`: `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID=2`
  - `myceld-c`: `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID=3`

## Commands

Reset the stack and start fresh:

```sh
cd ../knot_pkm/knot_pkm_server
make compose-reset
make compose-up
```

Inspect each node identity file:

```sh
for s in myceld-a myceld-b myceld-c; do
  echo "=== $s local node.json"
  docker compose -f compose.dev.yml exec -T "$s" \
    sh -c 'cat /data/mycel/meta/clustering/node.json'
  echo
done
```

Inspect cluster status through each daemon:

```sh
for port in 19091 19092 19093; do
  echo "=== cluster status via $port"
  docker compose -f compose.dev.yml exec -T myceld-a \
    mycel --daemon-addr "host.docker.internal:$port" \
      -u admin -p admin-password --output json cluster status
done
```

Inspect membership through each daemon:

```sh
for port in 19091 19092 19093; do
  echo "=== members via $port"
  docker compose -f compose.dev.yml exec -T myceld-a \
    mycel --daemon-addr "host.docker.internal:$port" \
      -u admin -p admin-password --output json cluster members
done
```

Inspect health through each daemon:

```sh
for port in 19091 19092 19093; do
  echo "=== health via $port"
  docker compose -f compose.dev.yml exec -T myceld-a \
    mycel --daemon-addr "host.docker.internal:$port" \
      -u admin -p admin-password --output json cluster health
done
```

Inspect startup logs:

```sh
for s in myceld-a myceld-b myceld-c; do
  echo "=== $s daemon.log relevant"
  docker compose -f compose.dev.yml exec -T "$s" \
    sh -c 'grep -E "clustering ready|experimental raft groups started" /data/mycel/log/myceld.log | tail -5'
done
```

## Observed evidence

### Local node identities

Each daemon created a different `cluster_id`.

`myceld-a`:

```json
{
  "node_name": "node-a",
  "cluster_id": "cluster_ff1dabd9-8284-4ab8-b475-0dbe82e4644d",
  "cluster_name": "knot-pkm-dev",
  "backend_advertise_addr": "myceld-a:9091",
  "cluster_admitted": true,
  "cluster_bootstrap": true
}
```

`myceld-b`:

```json
{
  "node_name": "node-b",
  "cluster_id": "cluster_81f9d623-e357-42c6-93da-5bbec33b0b91",
  "cluster_name": "knot-pkm-dev",
  "backend_advertise_addr": "myceld-b:9091",
  "cluster_admitted": true,
  "cluster_bootstrap": true
}
```

`myceld-c`:

```json
{
  "node_name": "node-c",
  "cluster_id": "cluster_5e06f2c2-bf03-45e8-ba7e-710c65fc9de9",
  "cluster_name": "knot-pkm-dev",
  "backend_advertise_addr": "myceld-c:9091",
  "cluster_admitted": true,
  "cluster_bootstrap": true
}
```

### Cluster status

Each daemon reports its own distinct cluster ID and only itself as a peer.

`myceld-a` reports:

```text
cluster_id=cluster_ff1dabd9-8284-4ab8-b475-0dbe82e4644d
peers=[node-a self]
```

`myceld-b` reports:

```text
cluster_id=cluster_81f9d623-e357-42c6-93da-5bbec33b0b91
peers=[node-b self]
```

`myceld-c` reports:

```text
cluster_id=cluster_5e06f2c2-bf03-45e8-ba7e-710c65fc9de9
peers=[node-c self]
```

### Membership

Each daemon reports a one-member cluster, with itself as the only member.

### Health

Each daemon reports healthy despite being only a one-member local cluster:

```json
{
  "status": "healthy",
  "active_members": 1
}
```

This is misleading in a deployment configured for three raft nodes.

### Logs

Each daemon logs a different cluster ID during clustering initialization:

```text
myceld-a clustering ready cluster_id=cluster_ff1dabd9-8284-4ab8-b475-0dbe82e4644d
myceld-b clustering ready cluster_id=cluster_81f9d623-e357-42c6-93da-5bbec33b0b91
myceld-c clustering ready cluster_id=cluster_5e06f2c2-bf03-45e8-ba7e-710c65fc9de9
```

Yet each daemon also starts experimental raft groups using the numeric raft configuration:

```text
myceld-a experimental raft groups started local_node_id=1 node_count=3 partition_count=16 group_count=17
myceld-b experimental raft groups started local_node_id=2 node_count=3 partition_count=16 group_count=17
myceld-c experimental raft groups started local_node_id=3 node_count=3 partition_count=16 group_count=17
```

## Characterization

There are currently two different notions of clustering:

1. **Local clustering identity/membership**
   - persisted under `/data/mycel/meta/clustering/node.json`
   - each fresh data directory creates its own random `cluster_id`
   - each node marks itself `cluster_admitted=true` and `cluster_bootstrap=true`
   - membership/health APIs report only local file-based membership

2. **Experimental raft runtime**
   - started from environment variables
   - uses numeric node IDs `1..N`
   - uses `MYCELD_CLUSTER_RAFT_NODE_ADDRS`
   - does not reconcile with local `cluster_id`
   - does not appear to use system raft bootstrap metadata as the authoritative cluster identity

As a result, a deployment can look like a three-node raft setup while the public/admin cluster identity layer says there are three independent one-node clusters.

This is a split-brain bootstrap bug. The system does not have one authoritative cluster identity, and the readiness/health APIs do not detect that the configured three-node deployment has failed to form one cluster.

## Likely code path

`internal/clustering/store.go` creates a random cluster ID whenever the local node identity file does not exist:

```go
id = NodeIdentity{
    NodeID: "node_" + uuid.NewString(),
    ClusterID: "cluster_" + uuid.NewString(),
    ClusterName: opts.ClusterName,
    ClusterAdmitted: clustered,
    ClusterBootstrap: clustered,
}
```

This happens independently on every fresh PVC/data directory.

`internal/clustering/manager.go` then initializes membership from this local identity. If `ClusterAdmitted` and `ClusterBootstrap` are true, the node inserts itself into its own membership store.

`internal/clustering/registration/handler.go` can register with seed nodes, but no seeds are configured in the compose/K3s raft deployment. Therefore each node remains a self-only cluster in the file-membership model.

`internal/daemon/app/raft_experimental.go` separately starts raft groups from `RaftLocalNodeID`, `RaftNodeCount`, and `RaftNodeAddrs`, independent of the file-based `cluster_id`.

## Why this matters

A database cluster needs one authoritative cluster identity and one membership/placement source of truth.

The current behavior allows:

- three pods to be marked clustered and healthy;
- all three pods to have different `cluster_id`s;
- public cluster status to report only self-membership;
- raft groups to start anyway;
- Kubernetes readiness to treat all pods as usable;
- clients to load-balance across pods that are not proven to share one replicated state.

This directly supports the production-like failure mode where graph data exists on one pod but not another.

## Minimal expected behavior

For a three-node configured raft cluster, one of these should happen:

1. All nodes converge on one cluster ID and one membership view; or
2. Nodes that cannot verify the shared cluster identity fail startup or become NotReady.

The current behavior does neither.

## Immediate fix direction

The first reliability fix should fail closed before attempting deeper raft correctness changes.

Suggested near-term changes:

1. In raft mode, do not allow each node to self-bootstrap a random cluster ID independently.
2. Add a configured or bootstrapped authoritative cluster ID.
3. Require every configured raft node to validate against that cluster ID.
4. Make cluster health/readiness fail when:
   - raft node count is greater than one;
   - local membership has only self;
   - peer cluster IDs disagree;
   - system raft metadata is missing or conflicts with local identity.
5. Ensure Admin cluster status distinguishes file-membership state from raft group state.

## Open design decision

We need to decide how the authoritative cluster ID is created and distributed.

Possible approaches:

1. **Explicit configured cluster ID**
   - operator provides `MYCELD_CLUSTER_ID`
   - simple and Kubernetes-friendly
   - must be protected from accidental changes

2. **Bootstrap node creates cluster ID and joiners adopt it**
   - one bootstrap pod initializes cluster metadata
   - joiners must contact bootstrap/seed
   - needs deterministic bootstrap coordination

3. **System raft bootstrap metadata is source of truth**
   - preferred long-term model
   - first committed system raft entry defines cluster ID, node IDs, placement, partition count, replica factor
   - nodes must persist and validate this metadata on restart

A practical path may combine (1) and (3): require an explicit configured cluster ID initially, then persist and enforce it through system raft metadata.
