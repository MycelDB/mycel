# System Integration Tests

This directory contains operator-facing documentation for destructive and
cross-process system integration tests. These tests create or reset Docker
Compose, k3d, K3s, Kubernetes, or local cluster resources. They are not part of
normal `make test` unless explicitly included in a gate below.

Run commands from the `mycel/` directory unless a test document says otherwise.

## Tests

| Test | Command | Purpose |
| --- | --- | --- |
| [Compose cluster validation](compose-cluster-validation.md) | `make test-compose-cluster` | Validate fresh Docker Compose raft bootstrap, identity, health, graph data-plane behavior, restart stability, and persisted file-source identity. |
| [Compose user backup/restore](compose-user-backup-restore.md) | `make test-compose-user-backup-restore` | Validate principal-scoped exports, wiped-cluster restore, graph/blob payload restoration, and per-node verification. |
| [K3s cluster validation](k3s-cluster-validation.md) | `make test-k3s-cluster` | Validate fresh k3d/K3s raft bootstrap, shared cluster identity, data-plane behavior, rolling restart, and one-PVC replacement/rejoin. |
| [K3s system backup/restore](k3s-system-backup-restore.md) | `make test-k3s-system-backup-restore` | Validate coordinated full-cluster backup, raft freeze/checkpoint evidence, PVC wipe, ordinal restore, and workload-driven graph verification. |
| [Raft disruption test](raft-disruption-test-harness.md) | `make test-k3s-raft-disruption-smoke`, `make test-k3s-raft-disruption`, `make test-k3s-raft-disruption-edges`, or `go run ./cmd/mycel-raft-disrupttest ...` | Reusable disposable k3d/K3s pod-restart pressure test with smoke, small, edge, multi-space, medium, and soak variations. |
| [Cluster release gate](cluster-release-gate.md) | `make test-cluster-release-gate` | Full pre-release cluster validation gate combining normal tests, raft phase gates, Compose, K3s, and system backup/restore. |
| [Raft-sensitive gate](cluster-raft-sensitive-gate.md) | `make test-cluster-raft-sensitive-gate` | Optional destructive gate for raft-sensitive changes, including disposable disruption smoke and edge workload checks. |
| [Cluster soak](cluster-soak.md) | `make test-cluster-soak` | Optional long-running Docker Compose cluster soak with repeated identity/data-plane validation and restarts. |

## Result interpretation

A system integration test is acceptable when its command exits `0` and its
summary or script output shows that all documented assertions passed. For raft
and cluster tests, also verify that:

- final per-node counts converge;
- committed/read-index checks do not fail;
- no cluster ID mismatch is reported;
- permanent write failures are zero;
- any transient failures are explained by intentional disruption and do not hide
  data loss.

Most tests write artifacts under either a test-specific temporary directory or
`artifacts/raft-disruption/<timestamp>-<cluster-name>/`. Keep those artifacts
for failed runs and attach them to bug reports.

## Safety

- These tests are destructive. They may delete local Docker Compose resources,
  k3d clusters, Kubernetes namespaces, PVCs, and disposable volumes.
- Do not point these tests at production clusters or namespaces.
- Prefer the documented make targets so cleanup and artifact collection remain
  consistent.
- Divergence tooling is forensic/read-only by default. Do not use these tests to
  perform automatic repair.
