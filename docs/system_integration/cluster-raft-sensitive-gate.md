# Raft-Sensitive Cluster Gate

## Command

```sh
make test-cluster-raft-sensitive-gate
```

Run from the `mycel/` directory.

## What it does

This optional destructive gate is intended for raft-sensitive changes. It runs:

1. `make test`
2. `make test-phase-d`
3. `make test-phase-e`
4. `make test-phase-f`
5. `make test-phase-g`
6. `make test-k3s-raft-disruption-smoke`
7. `make test-k3s-raft-disruption-edges`

It keeps disruption validation explicit while bundling the fast raft phase gates
with disposable K3s pod-restart pressure tests.

## Parameters

This target inherits configuration from the underlying raft disruption targets.
The most common override is:

| Variable | Meaning |
| --- | --- |
| `MYCEL_RAFT_DISRUPT_IMAGE` | Image tag built and loaded into the disposable k3d cluster. |

See [Raft disruption test harness](raft-disruption-test-harness.md) for direct
harness parameters.

## How to interpret results

The gate passes when the make target exits `0` and both disruption targets print
`Raft disruption test: PASS`.

Failures should be interpreted by the first failing target:

- phase gate failure: focused in-process raft regression;
- disruption smoke failure: basic restart/write/read/convergence issue;
- disruption edge failure: relationship persistence or convergence issue under
  restart pressure.

Preserve the printed artifact directory for failed disruption runs.
