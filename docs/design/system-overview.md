# mycel System Overview

mycel is a daemon-centered graph database and knowledge substrate. The daemon
owns identity, authorization, graph sessions, transactions, storage, semantic
maintenance, backup coordination, and cluster membership. Clients interact with
mycel through gRPC APIs and the `mycel` CLI rather than embedding storage
packages directly.

## Major components

### Daemon and runtime

The daemon is the composition root. It loads configuration, initializes runtime
services, wires subsystem managers to API adapters, enforces readiness, and owns
shutdown/quiesce coordination. Subsystems own their domain behavior; daemon API
packages translate between public protobuf contracts and internal subsystem
interfaces.

See [runtime](runtime/README.md).

### Identity and access

Identity distinguishes standard users, operators, and system principals. Users
own or access spaces. Operators manage users, cluster state, backup/restore, and
other admin surfaces through Admin APIs. Auth sessions and delegated sessions are
used to perform user-scoped operations safely without exporting passwords or
active tokens.

See [identity](identity/README.md).

### Spaces and domains

A space is the top-level user-visible container. A domain is a flat graph
partition within a space. Graph sessions and transactions are domain-scoped,
while access control is primarily space-scoped in the current model.

See [spaces and domains](spaces-domains/README.md).

### Graph, query, and change streams

Graph data is represented as nodes and edges with labels, properties, payload,
and metadata. Clients open sessions and transactions, then read/write graph data
or execute queries. Change stream surfaces provide graph event observation.

See [graph](graph/README.md) and [client API](api/README.md).

### Blob storage

Blob payload bytes are content-addressed and can be referenced by blob-backed
graph nodes. User-scoped export/import embeds blob metadata and chunks in the
per-domain import/export stream when requested.

See [blobs](blobs/README.md).

### Schema

Schemas are domain-scoped. They constrain node labels, edge labels, properties,
and query behavior when strict schema validation is enabled. Schema validation is
enforced through graph/query paths and supports application-specific graph
shapes.

See [schema](schema/README.md).

### Semantic and inference

Semantic subsystems manage embedding/index configuration, model endpoint
capabilities, maintenance work, dirty/backfill processing, and search surfaces.
Vector indexes are treated as derived/rebuildable state where possible.

See [semantic](semantic/README.md).

### Automation

Automation design describes graph-triggered workflows that can react to graph
changes, evaluate GQL conditions, and invoke configured actions or inference
providers.

See [automation](automation/README.md).

### Backup and restore

System backup design covers quiesce-aware daemon backup. User-scoped backup and
restore exports explicit user-visible spaces/domains through operator tooling,
then restores into a fresh or selected target user without exporting plaintext
passwords or active sessions.

See [backup and restore](backup-restore/README.md) and [operations procedures](../operations/procedures/README.md).

### Clustering and raft ownership

Clustered mycel uses system raft metadata as the authoritative source for
cluster identity, membership, and partition placement. Durable user-visible
writes in raft mode must be raft-owned, derived/rebuildable, or fail closed.
Committed reads default to strong/read-index semantics. Diagnostics and manual
repair workflows remain forensic/read-only first.

See [clustering](clustering/README.md).

## API layers

- Client APIs serve authenticated user/application workflows.
- Admin APIs serve operator workflows and cross-user/system administration.
- Backend/internal APIs support cluster peer communication and are not public
  client surfaces.

## Operational model

Normal operation and recovery guidance lives under [operations](../operations/README.md).
Implementation plans live under [implementation](../implementation/README.md) and
should be treated as historical/phase artifacts unless linked by current design
or operations docs.
