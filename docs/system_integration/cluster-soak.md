# Cluster Soak Test

## Command

```sh
make test-cluster-soak
```

Run from the `mycel/` directory.

## What it does

This optional long-running destructive Docker Compose system integration test
repeatedly validates cluster identity and data-plane behavior while applying
periodic daemon restarts. It is intended for extended local confidence after
raft-sensitive changes.

## Parameters

| Variable | Example | Meaning |
| --- | --- | --- |
| `MYCEL_CLUSTER_SOAK_WRITES` | `3` | Number of write/validation iterations used by the soak script. |
| `MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS` | `true` | Reserved future mode; currently fails closed. |
| `MYCEL_CLUSTER_SOAK_REPLACE_PVC` | `true` | Reserved future mode; currently fails closed. |

Example:

```sh
MYCEL_CLUSTER_SOAK_WRITES=3 make test-cluster-soak
```

## How to interpret results

The test passes when the make target exits `0` after all iterations complete.

Failure interpretation:

- identity mismatch: investigate raft system metadata convergence;
- data-plane mismatch: inspect graph write/read/query evidence from the failing
  iteration;
- restart-only failure: inspect daemon logs around the restart window;
- reserved flag failure: expected fail-closed behavior until safe snapshot/PVC
  replacement harnesses exist.

## Safety

This test is destructive and long-running. It resets local Compose resources and
should not be run as part of normal `make test`.
