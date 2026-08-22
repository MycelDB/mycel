# Design Documentation

Design docs describe the current mycel architecture and the major subsystems
that make up the daemon. Start with the [system overview](system-overview.md),
then use the component indexes below for detail.

## Core overview

- [System overview](system-overview.md) — high-level architecture, data flows, and subsystem relationships.

## Component areas

| Component | Contents |
| --- | --- |
| [Client API](api/README.md) | Client-facing gRPC/API contracts for auth, spaces, domains, sessions, graph, query, blobs, semantic, import/export, metadata, and change streams. |
| [Admin API](admin/README.md) | Operator/admin API contracts for users, operators, domains, backup, semantic/inference, and maintenance. |
| [Identity](identity/README.md) | Users, operators, sessions, auth refresh, and access-control model. |
| [Spaces and domains](spaces-domains/README.md) | Space/domain ownership, visibility, and domain-scoped work. |
| [Graph](graph/README.md) | Node/edge model, query behavior, node metadata, and GQL/schema interactions. |
| [Blobs](blobs/README.md) | Blob-backed graph nodes and raw blob APIs. |
| [Schema](schema/README.md) | Domain schemas, schema management, and schema-aware validation. |
| [Semantic](semantic/README.md) | Semantic generation rules, embeddings, inference packages, physical search indexes, and maintenance surfaces. |
| [Automation](automation/README.md) | Graph automation design and automation roadmaps. |
| [Clustering](clustering/README.md) | Raft clustering, system metadata authority, consistency, and reliability. |
| [Backup and restore](backup-restore/README.md) | Quiescing, backup, restore, and user-scoped portability design. |
| [Runtime](runtime/README.md) | Daemon/runtime boundaries, subsystem lifecycle, initialization, and service contracts. |
| [Persistence](persistence/README.md) | WAL and persistence-oriented design. |

## Historical planning

Implementation plans and phased migration notes are archived under
[implementation](../implementation/README.md). Prefer design docs for current
architecture and operations docs for runbooks.
