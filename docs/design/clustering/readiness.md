# Cluster readiness contract

Mycel exposes cluster lifecycle as a coarse node state plus explicit readiness
gates. The node state answers "what phase is this daemon in?"; the readiness
gates answer "what is this daemon safe to do right now?"

## Lifecycle state

The local node lifecycle state remains intentionally coarse:

- `initializing`: the daemon is starting or applying cluster authority.
- `standalone`: no clustered Raft runtime is active.
- `clustered`: cluster metadata was applied and partition groups were started.
- `failed`: startup or cluster authority failed.
- `stopped`: the daemon was stopped.

Operators should not infer write safety from the lifecycle state alone. A node can
be `clustered` while Raft groups are still electing leaders.

## Readiness gates

`ClusterReadiness` reports these dimensions:

- `process_ready`: the daemon process is serving authenticated admin API calls.
- `metadata_applied`: authoritative cluster metadata has been applied locally.
- `metadata_validated`: local identity/configuration validated against metadata.
- `metadata_ready`: metadata is applied and validated, and the local node is
  admitted with the expected cluster identity.
- `partition_groups_started`: local Raft partition group processes were started.
- `raft_ready`: started local Raft groups have elected/known leaders.
- `read_ready`: metadata and Raft gates are ready for client read paths.
- `write_ready`: schema and graph-partition writes can route to known Raft
  leaders.
- `client_ready`: top-level readiness-probe signal. In clustered Raft mode this
  is true only when `write_ready` is true.

`partition_groups_started=true` is not sufficient for clustered write readiness.
Readiness probes and write-heavy dependents should wait for `client_ready=true`
or `write_ready=true`.

## Clustered write-ready requirements

For clustered Raft mode, write readiness requires at least:

1. authoritative metadata is applied and validated;
2. the local node is admitted and cluster IDs match;
3. required local Raft groups are started;
4. every local write-relevant Raft group reports a non-zero leader;
5. no readiness blocker is active.

When groups are started but leaders are still missing, status output includes a
blocker such as:

```text
raft groups without leaders: space-partition-0, system
```

## Operator guidance

Use JSON status for automation:

```sh
mycel --output json cluster status
```

Use the probe-friendly command for Docker/Kubernetes/local-dev gates:

```sh
mycel cluster readiness check
```

The readiness check exits non-zero until `client_ready`, `read_ready`, and
`write_ready` are true and no blockers remain.
