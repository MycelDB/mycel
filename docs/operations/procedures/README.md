# Operational Procedures

This directory contains operator runbooks and validation procedures.

| Procedure | Purpose |
| --- | --- |
| [Start standalone mode](standalone-start.md) | Start a single-node standalone `myceld`, capture bootstrap credentials, smoke test, and stop it. |
| [Backup and restore](backup-restore.md) | Backup/restore options, including daemon/system and principal-scoped export/import. |
| [Cluster system backup and restore](cluster-system-backup-restore.md) | End-to-end target procedure for coordinated full-cluster backup sets and offline restore. |
| [Split-brain recovery](split-brain-recovery.md) | Trusted-source recovery from an operator-selected authoritative pod. |
| [Raft cluster operations](raft-cluster-operations.md) | Cluster health, readiness, routing, restart, and recovery checks. |
| [Raft manual repair workflows](raft-cluster-manual-repair-workflows.md) | Read-only forensic workflows and manual recovery planning. |
| [Raft cluster test matrix](raft-cluster-test-matrix.md) | Release and cluster validation gates. |
| [Compose cluster validation](compose-cluster-validation.md) | Docker Compose cluster validation procedure. |
| [K3s cluster validation](k3s-cluster-validation.md) | K3s/k3d cluster validation procedure. |
| [Build and test commands](build-test-commands.md) | Common `make` commands for local development and validation. |
