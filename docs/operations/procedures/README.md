# Operational Procedures

This directory contains operator runbooks and validation procedures.

| Procedure | Purpose |
| --- | --- |
| [Start standalone mode](standalone-start.md) | Start a single-node standalone `myceld`, capture bootstrap credentials, smoke test, and stop it. |
| [REPL GQL tutorial](repl-gql-tutorial.md) | Connect to a space/domain in the REPL, insert sample graph data with GQL, and query it. |
| [Backup and restore](backup-restore.md) | Backup/restore options, including daemon/system and principal-scoped export/import. |
| [S3 blob payload storage](s3-blob-storage.md) | Configure S3-backed storage for immutable blob payload bytes. |
| [Cluster system backup and restore](cluster-system-backup-restore.md) | End-to-end target procedure for coordinated full-cluster backup sets and offline restore. |
| [Split-brain recovery](split-brain-recovery.md) | Trusted-source recovery from an operator-selected authoritative pod. |
| [Raft cluster operations](raft-cluster-operations.md) | Cluster health, readiness, routing, restart, and recovery checks. |
| [Raft manual repair workflows](raft-cluster-manual-repair-workflows.md) | Read-only forensic workflows and manual recovery planning. |
| [System integration tests](../../system_integration/README.md) | Destructive Compose, K3s, backup/restore, release-gate, soak, and raft disruption validation. |
| [Build and test commands](build-test-commands.md) | Common `make` commands for local development and validation. |
