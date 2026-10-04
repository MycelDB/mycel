# K3s System Backup/Restore Validation

## Command

```sh
make test-k3s-system-backup-restore
```

Run from the `mycel/` directory.

## What it does

This destructive k3d/K3s release-gate test delegates to the native Mycel Lab
`k3d-system-backup-restore` suite. Mycel Lab owns the disposable k3d lifecycle,
cluster backup operation events, PVC replacement/restore operations, run
tracking, and artifacts.

It validates coordinated full-cluster backup and offline restore using normal
graph workloads. It:

1. creates a fresh disposable k3d/K3s cluster;
2. writes workload data through normal daemon/client graph APIs;
3. verifies pre-backup per-pod count convergence;
4. runs one coordinated cluster system backup;
5. verifies raft freeze/checkpoint evidence in `backup-set.json`;
6. wipes the namespace, including PVCs;
7. restores each ordinal archive into fresh PVCs;
8. restarts the StatefulSet;
9. verifies restored cluster health, shared identity, PVC replacement evidence,
   and restored workload GQL reads through every pod.

The restore path is explicit operator tooling. It must not automatically choose
an authoritative node or repair split-brain state.

## Parameters

The target builds the local image and executes `mycel-lab run suite
k3d-system-backup-restore --confirm-destructive` from `MYCEL_LAB_ROOT` (default
`../mycel-lab`).

Required local tools:

```sh
kubectl version --client=true
k3d version
docker version
```

Useful direct invocation:

```sh
cd "$MYCEL_LAB_ROOT"
go run ./cmd/mycel-lab run suite k3d-system-backup-restore --confirm-destructive
```

The legacy `cmd/mycel-system-backuptest` binary remains available only as a
manual fallback while native Mycel Lab destructive evidence is accumulated.

## How to interpret results

The test passes when the make target exits `0`. PASS means workload data was
written through normal APIs, backup metadata validated, old PVC UIDs changed,
fresh PVCs were restored from backup archives, the restored cluster became
healthy with shared identity, and restored workload queries succeeded through the
pods.

Important failures:

- missing raft freeze/checkpoint evidence: block release for backup safety;
- restore fails before StatefulSet restart: inspect PVC/archive placement;
- restored cluster unhealthy: inspect raft readiness and pod logs;
- restored workload count mismatch: investigate backup completeness or restore
  ordering;
- restored GQL read failed on every pod: investigate restored metadata/session
  availability;
- PVC UID did not change: the test did not prove restore from backup and must be
  treated as failed.

Artifacts are written under the Mycel Lab artifact root for the run and include
resolved scenario data, runtime events, environment captures, backup metadata,
PVC evidence, and failure diagnostics.

## Cleanup

Mycel Lab manages disposable K3s resources. If interrupted, delete retained k3d
clusters and prune unused Docker volumes when needed.
