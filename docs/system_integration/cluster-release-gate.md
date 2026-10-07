# Cluster Release Gate

## Command

```sh
make test-cluster-release-gate
```

Run from the `mycel/` directory.

## What it does

This is the full pre-release cluster validation gate. It expands to:

1. `make test`
2. `make test-integration-raft-subsystems`
3. `make test-integration-routing`
4. `make test-integration-client-admin`
5. `make test-integration-graph-consistency`
6. `make test-compose-cluster`
7. `make test-k3s-cluster`
8. `make test-k3s-system-backup-restore`

The focused integration targets are normal package/in-process test bundles. The
historical `test-phase-*` names remain as compatibility aliases.

The destructive Compose, k3d cluster, and k3d system backup/restore targets now
delegate to Mycel Lab suites. The K3s system backup/restore suite is included
because the coordinated backup path records raft freeze/checkpoint evidence and
validates restore into fresh PVCs.

## Parameters

This gate inherits parameters from the underlying make targets and scripts. It
expects a sibling Mycel Lab checkout at `MYCEL_LAB_ROOT` (default
`../mycel-lab`) for delegated destructive suites. See:

- [Compose cluster validation](compose-cluster-validation.md)
- [K3s cluster validation](k3s-cluster-validation.md)
- [K3s system backup/restore](k3s-system-backup-restore.md)

## How to interpret results

The gate passes when the make target exits `0`. A failure belongs to the first
failing prerequisite target in the sequence. Rerun that target directly to reduce
noise and inspect its artifacts/logs.

Common interpretation:

- failure before destructive targets: normal Go/unit/in-process integration regression;
- Compose failure: local Compose raft/data-plane/backup issue;
- K3s failure: Kubernetes raft deployment/restart/PVC issue;
- system backup/restore failure: release-blocking backup safety or restore
  correctness issue.

## Safety

This gate is destructive because it runs Compose and K3s system tests. Do not run
it against production resources.
