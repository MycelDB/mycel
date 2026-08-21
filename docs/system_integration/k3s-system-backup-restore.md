# K3s System Backup/Restore Validation

## Command

```sh
make test-k3s-system-backup-restore
```

Run from the `mycel/` directory.

## What it does

This destructive k3d/K3s release-gate test validates coordinated full-cluster
backup and offline restore using normal graph workloads. It:

1. creates a fresh disposable k3d/K3s cluster;
2. writes workload data through normal daemon/client graph APIs;
3. verifies pre-backup per-pod count convergence;
4. runs one coordinated cluster system backup;
5. verifies raft freeze/checkpoint evidence in `backup-set.json`;
6. wipes the namespace, including PVCs;
7. restores each ordinal archive into fresh PVCs;
8. restarts the StatefulSet;
9. verifies restored cluster health, per-pod local consistency counts, and a restored workload GQL read through a session-capable pod.

The restore path is explicit operator tooling. It must not automatically choose
an authoritative node or repair split-brain state.

## Parameters

The target builds the local image and executes `cmd/mycel-system-backuptest`.
Required local tools:

```sh
kubectl version --client=true
k3d version
docker version
```

Useful direct invocation:

```sh
go run ./cmd/mycel-system-backuptest \
  --driver k3s \
  --provisioner k3d \
  --profile backup-smoke \
  --workload edges \
  --image myceldb/mycel:system-backup-restore-local \
  --confirm-destructive
```

Key parameters:

| Flag | Meaning |
| --- | --- |
| `--profile backup-smoke|backup-small|backup-multi-space` | Workload size. |
| `--workload nodes|edges|multi-space` | Data shape written before backup. |
| `--backup-dir` | Backup directory inside each pod. |
| `--keep-cluster-on-failure` | Retain failed disposable cluster for debugging. |
| `--confirm-destructive` | Required destructive-action acknowledgement. |

## How to interpret results

The test passes when the make target exits `0` and prints `System
backup/restore test: PASS`. PASS means workload data was written through normal
APIs, pre-backup counts converged, backup metadata validated, old PVCs were
deleted, fresh PVCs were restored from backup archives, restored local
consistency counts matched the pre-backup durable counts on every pod, and at
least one restored session-capable pod could read the workload through normal GQL.

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

Artifacts are written under:

```text
artifacts/system-backup-restore/<timestamp>-<cluster-name>/
  result-summary.json
  error.txt
  setup/*
  workload/write-events.jsonl
  workload/read-events.jsonl
  backup/backup-set.json
  restore/*
  failure/*
```

## Cleanup

The script manages disposable K3s resources. If interrupted, delete retained k3d
clusters and prune unused Docker volumes when needed.
