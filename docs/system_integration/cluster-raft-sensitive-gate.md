# Raft-Sensitive Cluster Gate

## Command

```sh
make test-cluster-raft-sensitive-gate
```

Run from the `mycel/` directory.

## What it does

This optional destructive gate is intended for raft-sensitive changes. It runs:

1. `make test`
2. `make test-integration-raft-subsystems`
3. `make test-integration-routing`
4. `make test-integration-client-admin`
5. `make test-integration-graph-consistency`
6. `make test-k3s-raft-disruption-smoke`
7. `make test-k3s-raft-disruption-edges`

It keeps disruption validation explicit while bundling the fast focused
integration bundles with disposable K3s pod-restart pressure tests. The
destructive disruption targets now delegate to Mycel Lab scenarios/suites. The
historical `test-phase-*` names remain as compatibility aliases.

## Parameters

This target inherits configuration from the underlying raft disruption targets
and expects a sibling Mycel Lab checkout at `MYCEL_LAB_ROOT` (default
`../mycel-lab`).
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

- focused integration bundle failure: in-process raft regression;
- disruption smoke failure: basic restart/write/read/convergence issue;
- disruption edge failure: relationship persistence or convergence issue under
  restart pressure.

Preserve the printed artifact directory for failed disruption runs.
