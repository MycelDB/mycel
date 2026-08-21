# Raft Disruption Test

This file covers the raft disruption system integration test and its supported
variations. The make targets and direct harness invocations all exercise the same
underlying test: create a disposable k3d/K3s cluster, apply write pressure,
restart pods, and verify raft/data convergence.

## Commands

Common make targets:

| Variation | Command | Purpose |
| --- | --- | --- |
| Smoke | `make test-k3s-raft-disruption-smoke` | Fast default `nodes` workload with one pod restart. |
| Small/all-pod restart | `make test-k3s-raft-disruption` | `small` profile with all pods restarted one at a time. |
| Edge workload | `make test-k3s-raft-disruption-edges` | Relationship workload under pod restart pressure. |

The reusable harness can also be invoked directly:

```sh
go run ./cmd/mycel-raft-disrupttest \
  --driver k3s \
  --provisioner k3d \
  --profile smoke \
  --workload nodes \
  --restart-node 0 \
  --image myceldb/mycel:raft-disrupt-local \
  --confirm-destructive
```

Run from the `mycel/` directory.

Useful direct variations:

```sh
# Smoke nodes workload.
go run ./cmd/mycel-raft-disrupttest \
  --driver k3s \
  --provisioner k3d \
  --profile smoke \
  --workload nodes \
  --image myceldb/mycel:raft-disrupt-local \
  --confirm-destructive

# Small edge workload, restarting every pod.
go run ./cmd/mycel-raft-disrupttest \
  --driver k3s \
  --provisioner k3d \
  --profile small \
  --workload edges \
  --restart-node all \
  --image myceldb/mycel:raft-disrupt-local \
  --confirm-destructive

# Medium multi-space workload, restarting every pod.
go run ./cmd/mycel-raft-disrupttest \
  --driver k3s \
  --provisioner k3d \
  --profile medium \
  --workload multi-space \
  --restart-node all \
  --image myceldb/mycel:raft-disrupt-local \
  --confirm-destructive
```

## What it does

The harness creates a fresh disposable k3d/K3s cluster, deploys a three-pod
mycel raft StatefulSet, runs a graph workload, optionally restarts pods during
write pressure, verifies final convergence, writes artifacts, and deletes the
cluster unless failure retention is requested.

The generated StatefulSet uses:

- `podManagementPolicy: Parallel` so raft peers can start together;
- a headless peer Service with `publishNotReadyAddresses: true` so not-yet-ready
  peers can discover each other;
- an application-level readiness probe using `mycel cluster readiness check`.

## Parameters

| Flag | Values/default | Meaning |
| --- | --- | --- |
| `--driver` | `k3s` | Cluster driver. Only K3s is currently supported. |
| `--provisioner` | `k3d` | Cluster provisioner. Only k3d is currently supported. |
| `--profile` | `smoke`, `small`, `medium`, `soak` | Workload duration, writer count, and write rate. |
| `--workload` | `nodes`, `edges`, `multi-space` | Graph workload to run. |
| `--restart-node` | pod name, ordinal, `all` | Pod restart sequence. Empty defaults to one pod. |
| `--image` | image tag | myceld image to load into the disposable cluster. |
| `--partition-count` | positive integer | Number of raft graph partitions. |
| `--artifacts-dir` | `artifacts/raft-disruption` | Root directory for run artifacts. |
| `--setup-only` | boolean | Deploy and collect setup artifacts without running workload. |
| `--no-disruption` | boolean | Run workload without restarting pods. |
| `--keep-cluster-on-failure` | boolean | Keep failed disposable cluster for debugging. |
| `--confirm-destructive` | required | Acknowledges create/delete/restart behavior. |

Profiles:

| Profile | Duration | Writers | Rate |
| --- | ---: | ---: | ---: |
| `smoke` | 30s | 1 | 5/s |
| `small` | 2m | 2 | 20/s |
| `medium` | 10m | 4 | 50/s |
| `soak` | 1h | 8 | 100/s |

Workloads:

| Workload | What it writes | Expected count per acknowledged write | When to use |
| --- | --- | --- | --- |
| `nodes` | `ChaosWrite` nodes | 1 node | Fast baseline raft write/read/convergence validation. |
| `edges` | `ChaosParent`, `ChaosChild`, and `CHAOS_EDGE` | 2 nodes + 1 edge | Relationship persistence and edge convergence validation. |
| `multi-space` | `ChaosWrite` nodes distributed across three spaces | 1 node in one scope | Cross-space/session setup and per-scope convergence validation. |

## Variation guidance

Start with `smoke` before longer runs. Use `small` for routine raft-sensitive
local validation, `medium` for stronger confidence after the small variation
passes, and `soak` only when you intentionally want a long-running destructive
run.

Recommended sequence:

1. `make test-k3s-raft-disruption-smoke`
2. `make test-k3s-raft-disruption`
3. direct `--profile small --workload edges --restart-node all`
4. direct `--profile medium --workload edges --restart-node all`
5. direct `--profile medium --workload multi-space --restart-node all`

## How to interpret results

The command prints a concise result summary:

```text
Raft disruption test: PASS
Writes: attempted=... successful=... ambiguous=... transientFailures=... permanentFailures=...
Committed read checks: checks=... failures=...
Final counts:
  client: nodes=... edges=...
  myceld-0: nodes=... edges=...
  myceld-1: nodes=... edges=...
  myceld-2: nodes=... edges=...
Artifacts: artifacts/raft-disruption/<timestamp>-<cluster-name>
```

`PASS` means:

- final counts met the workload's expected minimum;
- client and per-pod local consistency counts converged;
- committed read checks did not fail;
- no write attempts had permanent failures;
- cluster identity diagnostics did not show a mismatch.

Transient write failures are expected during pod restarts. `ambiguous` writes are
durable writes observed in final counts above the acknowledged `successful`
count. They usually mean a write RPC timed out or disconnected after the server
may have committed it.

`FAIL` writes a compact summary plus `error.txt` containing the full error.

## Artifacts

```text
artifacts/raft-disruption/<timestamp>-<cluster-name>/
  result-summary.json
  error.txt
  setup/summary.json
  setup/pods.txt
  setup/services.txt
  setup/statefulset.txt
  scenario/write-events.jsonl
  scenario/read-events.jsonl
  scenario/scenario-summary.json
  failure/*
```

Start with `result-summary.json`, then inspect `scenario/read-events.jsonl` for
read failures and `scenario/write-events.jsonl` for write failure timing. Use
`failure/` for Kubernetes state captured on failure.

## Safety

The harness always creates a new disposable cluster and deletes it on success and
failure unless `--keep-cluster-on-failure` is set. Do not point it at production
clusters or namespaces.
