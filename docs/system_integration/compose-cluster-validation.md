# Compose Cluster Validation

## Command

```sh
make test-compose-cluster
```

Run from the `mycel/` directory.

## What it does

This destructive Docker Compose test validates the local Mycel-owned Compose raft cluster fixture under `tests/compose/cluster/`. It:

1. resets Compose resources and starts the cluster;
2. validates fresh bootstrap and shared cluster identity;
3. validates cluster health/readiness;
4. creates graph data through one node and verifies graph reads/queries through
   the cluster;
5. restarts `myceld-a`, `myceld-b`, and `myceld-c`;
6. revalidates cluster identity and data-plane behavior after restart;
7. verifies persisted file-source identity diagnostics.

## Parameters

The make target builds the current checkout as `local/mycel:dev` by default and uses `tests/compose/cluster/compose.yml`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `MYCEL_COMPOSE_IMAGE` | `local/mycel:dev` | Local image tag built by the target. |
| `MYCEL_COMPOSE_FILE` | `tests/compose/cluster/compose.yml` | Compose file used by validators and lifecycle commands. |
| `MYCEL_COMPOSE_SERVICES` | `myceld-a,myceld-b,myceld-c` | Comma-separated service list used by validators. |
| `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` | `mycel-compose-cluster-token` | Backend auth token used by the Compose cluster. |

The target also creates a temporary `MYCEL_COMPOSE_DATA_PLANE_STATE` file to
preserve fixture IDs between pre-restart and post-restart validation.

## How to interpret results

The test passes when `make` exits `0`. Treat any non-zero exit as a system
integration failure.

Important failure classes:

- cluster identity mismatch: investigate raft metadata/bootstrap state;
- health/readiness failure: inspect Compose service logs and readiness output;
- graph write/read/query mismatch: investigate data-plane routing, raft group
  ownership, or graph storage convergence;
- failure after restart only: investigate persisted state, rejoin behavior, or
  startup ordering.

## Cleanup

The target resets Compose resources at start. If interrupted, clean up with:

```sh
docker compose -f tests/compose/cluster/compose.yml down -v --remove-orphans
```
