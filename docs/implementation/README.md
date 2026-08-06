# Implementation Plan Archive

Implementation documents are planning and historical artifacts. They explain how
features were or may be implemented, but they are not the authoritative operator
runbooks for current deployments.

Use:

- [Design docs](../design/README.md) for current architecture.
- [Operations docs](../operations/README.md) for runbooks and CLI usage.
- This archive for release history, phased plans, and unfinished design work.

## Release buckets

| Bucket | Contents |
| --- | --- |
| [v0.2](v0.2/README.md) | Daemon foundation, semantic embedding generation, package-boundary, quiesce, and node-local backup plans. |
| [v0.3](v0.3/README.md) | Distributed runtime, raft clustering, service lifecycle, subsystem package, and WAL plans. |
| [v0.4](v0.4/README.md) | Schema, GQL, graph automation, GWL schema management, and node content metadata plans. |
| [v0.5](v0.5/README.md) | Raft reliability, read consistency, divergence diagnostics, subsystem snapshots, and clustering hardening plans. |
| [v0.6](v0.6/README.md) | User-scoped backup/restore tooling and documentation reorganization work. |
| [unreleased](unreleased/README.md) | Plans not yet assigned to a tagged release or retained for future cleanup. |
