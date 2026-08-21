# Compose Cluster Validation

## Command

```sh
make test-compose-cluster
```

Run from the `mycel/` directory.

## What it does

This destructive Docker Compose test validates the local Compose raft cluster
used by the sibling Knot PKM development environment. It:

1. resets Compose resources and starts the cluster;
2. validates fresh bootstrap and shared cluster identity;
3. validates cluster health/readiness;
4. creates graph data through one node and verifies graph reads/queries through
   the cluster;
5. restarts `myceld-a`, `myceld-b`, and `myceld-c`;
6. revalidates cluster identity and data-plane behavior after restart;
7. verifies persisted file-source identity diagnostics.

## Parameters

The make target uses the sibling repository at `../../knot_pkm/knot_pkm_server`.
The main environment override is:

| Variable | Default | Meaning |
| --- | --- | --- |
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

The target resets Compose resources at start. If interrupted, clean up from the
sibling Compose project with its normal Compose reset/down commands.
