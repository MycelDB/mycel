# Implementation Plan Archive

Implementation documents are planning and historical artifacts. They explain how
features were or may be implemented, but they are not the authoritative operator
runbooks for current deployments.

Use:

- [Design docs](../design/README.md) for current architecture.
- [Operations docs](../operations/README.md) for runbooks and CLI usage.
- This archive for release history, phased plans, validation reports, and unfinished design work.

Release buckets use major/minor versions only. Patch releases are associated
with their minor release bucket instead of creating patch-version directories.

See the [reclassification audit](reclassification-audit.md) for the #135 audit
notes and Git evidence used to move historical plans out of `unreleased/`.

## Release buckets

| Bucket | Contents |
| --- | --- |
| [v0.2](v0.2/README.md) | Daemon foundation, semantic embedding generation, package-boundary, quiesce, and node-local backup plans. |
| [v0.3](v0.3/README.md) | Distributed runtime, raft clustering, service lifecycle, subsystem package, and WAL plans. |
| [v0.4](v0.4/README.md) | Schema, GQL, graph automation, GWL schema management, and node content metadata plans. |
| [v0.5](v0.5/README.md) | Raft reliability, read consistency, divergence diagnostics, subsystem snapshots, and clustering hardening plans. |
| [v0.6](v0.6/README.md) | User-scoped backup/restore tooling and documentation reorganization work. |
| [v0.7](v0.7/README.md) | Backup, graph notification, adjacency indexing, semantic maintenance, and cleanup plans. |
| [v0.8](v0.8/README.md) | Indexed query, REPL, and unified-principal identity plans. |
| [v0.9](v0.9/README.md) | Automation, inference, intelligence access, semantic generation rules, raft reliability harness, query, Console follow-up, activity, and system-test plans/reports. |
| [v0.11](v0.11/README.md) | Lexical search plans and validation reports. |
| [v0.12](v0.12/README.md) | Hybrid lexical/semantic search plans and validation reports. |
| [v0.14](v0.14/README.md) | Encryption-at-rest plans and inventory notes. |
| [v0.17](v0.17/README.md) | Advanced storage, graph checkpoints, persistent indexes, and graph write latency plans. |
| [unreleased](unreleased/README.md) | Future implementation plans not yet assigned to a shipped minor release. |
