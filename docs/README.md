# MycelDB Documentation

MycelDB is a daemon-first graph data system with user identity, spaces/domains,
transactional graph operations, blob-backed nodes, schema validation, semantic
indexing, automation hooks, and raft-based clustering.

This directory is organized by audience and intent:

- [Design](design/README.md) explains the current system architecture and major subsystems.
- [Operations](operations/README.md) explains how to run, operate, validate, and recover MycelDB.
- [Implementation](implementation/README.md) archives implementation plans by release.
- [Roadmap](roadmap/gql-roadmap.md) tracks forward-looking product and GQL work.

## Quick links

| Task | Start here |
| --- | --- |
| Understand the system at a high level | [Design overview](design/system-overview.md) |
| Find a client/API contract | [Client API design](design/api/README.md) |
| Find an admin/operator API contract | [Admin API design](design/admin/README.md) |
| Run or script the CLI | [CLI operations](operations/cli/README.md) |
| Operate a raft cluster | [Raft cluster operations](operations/procedures/raft-cluster-operations.md) |
| Validate release/cluster behavior | [Raft cluster test matrix](operations/procedures/raft-cluster-test-matrix.md) |
| Plan split-brain recovery | [Split-brain recovery](operations/procedures/split-brain-recovery.md) |
| Review shipped implementation plans | [Implementation archive](implementation/README.md) |

## Feature areas

- Identity and access control: users, operators, sessions, delegated sessions, and space access.
- Spaces and domains: user-owned graph containers and domain-scoped graph sessions.
- Graph and query: transactional node/edge operations, GQL/query execution, metadata catalogs, and change streams.
- Blobs: content-addressed blob storage exposed through raw blob APIs and blob-backed graph nodes.
- Schema: domain-scoped schema management and schema-aware graph/query validation.
- Semantic and inference: semantic indexes, embedding/inference configuration, maintenance, and migration APIs.
- Automation: graph-triggered automation design and implementation plans.
- Clustering: system raft metadata, partitioned raft ownership, strong reads, diagnostics, and manual recovery workflows.
- Backup and restore: quiesce/backup design plus user-scoped export/import procedures.

## Documentation status

Design documents describe the intended/current architecture. Implementation plans
are historical or in-progress planning artifacts and may describe phased work,
tradeoffs, and previous names. Operator-facing recovery and validation guidance
belongs under [operations](operations/README.md).
