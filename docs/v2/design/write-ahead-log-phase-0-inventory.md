# Write-Ahead Log Phase 0 Inventory

## Status

Completed initial inventory for WAL implementation planning.

This document records the Phase 0 decisions for [Write-Ahead Log Implementation Plan](write-ahead-log-implementation-plan.md).

## Decisions

### First conversion target

Use the space metadata module as the first WAL-backed bounded context, specifically:

```text
internal/daemon/modules/space
internal/space/storage/spaces
internal/space/storage/domains
internal/space/storage/acl
```

Start with `CreateSpace` because it is small enough to reason about but exercises a useful multi-store mutation:

1. create space in `meta/spaces.json`
2. create default domain in `meta/domains.json`
3. grant owner admin ACL in `meta/space_acl.json`

This is a better proving ground than graph commits because graph storage already has transaction segments and optimistic commit behavior. It is also better than auth/session stores because token/session mutations include security-sensitive lifecycle behavior that should be converted after the WAL foundation is proven.

### Applied-LSN strategy

Use a daemon-level WAL progress file for the first implementation:

```text
meta/wal/progress.json
```

Initial shape:

```json
{
  "applied_lsn": 0,
  "updated_at": "2026-07-13T00:00:00Z"
}
```

Rationale:

- simple for Phase 1-4
- sufficient while one daemon process applies records synchronously
- avoids touching every file-backed store before the WAL framework exists

Known limitation: a crash after applying one file in a multi-file applier but before updating global `applied_lsn` can replay the full record. Therefore Phase 4 records must be idempotent. For `CreateSpace`, the record will include resolved IDs/timestamps and apply operations will upsert-or-confirm matching state rather than generate new values.

Longer term, this can evolve to per-store progress or a transactionally updated metadata store.

### WAL directory layout

Use daemon data directory paths:

```text
<data_dir>/wal/
  0000000000000001.wal
  0000000000100000.wal

<data_dir>/meta/wal/
  progress.json
  checkpoint.json       # Phase 7
```

### Payload encoding

Use a binary WAL frame envelope with deterministic JSON payloads for Phase 1-4.

Rationale:

- avoids public proto churn while WAL record shapes are still internal
- keeps early records inspectable for debugging
- works well with the existing file-backed JSON stores
- the frame envelope still gives length, version, LSN, type, payload encoding, and checksum

The envelope should reserve `payload_encoding` so protobuf can be introduced later without changing segment framing.

### Initial record type namespace

Use stable string or numeric constants under `internal/wal`. Recommended naming:

```text
space.create.v1
space.delete.v1
space.domain.create.v1
space.domain.update.v1
space.domain.delete.v1
space.acl.grant.v1
space.acl.revoke.v1
```

For Phase 4, prefer one aggregate record for `CreateSpace`:

```text
space.create_with_default_domain.v1
```

This preserves the atomic intent of the current command even though it touches three metadata files.

## Durable mutation inventory

### Space metadata

Primary files:

```text
internal/daemon/modules/space/module.go
internal/space/storage/spaces/file_store.go
internal/space/storage/domains/file_store.go
internal/space/storage/acl/file_store.go
internal/graph/template/storage/file_store.go
```

Durable files touched:

```text
<data_dir>/meta/spaces.json
<data_dir>/meta/domains.json
<data_dir>/meta/space_acl.json
<data_dir>/templates/<space_id>.json or equivalent template store files
<data_dir>/graphs/<space_id>/... removed during deletion
```

Mutation entrypoints observed:

- `Module.CreateSpace`
- `Module.DeleteSpace`
- `Module.GrantSpaceUser`
- `Module.CreateDomain`
- `Module.UpdateDomain`
- `Module.DeleteDomain`
- `Module.CreateTemplate`
- `Module.UpdateTemplate`
- `Module.DeleteTemplate`
- store-level `spaces.Create`, `spaces.DeleteByID`
- store-level `domains.Create`, `domains.Update`, `domains.DeleteByID`, `domains.DeleteForSpace`
- store-level `acl.GrantSystemRole`, `acl.RevokeSystemRole`, `acl.Grant`, `acl.Revoke`, `acl.DeleteForUser`, `acl.DeleteForSpace`

Current persistence behavior:

- in-memory slice/index updated
- entire JSON document persisted with `filestore.WriteFileAtomic`
- limited rollback in some store methods if persistence fails
- no cross-file atomic transaction across spaces/domains/ACL/templates

WAL notes:

- Good first target.
- Record payloads must include resolved UUIDs and timestamps.
- Aggregate commands touching multiple files should be idempotent on replay.
- Delete operations that remove graph/template directories need careful treatment; file deletion should be idempotent.

### Identity users and sessions

Primary files:

```text
internal/daemon/modules/user/module.go
internal/identity/storage/user/file_store.go
internal/identity/storage/session/file_store.go
internal/daemon/modules/user/store.go
```

Mutation entrypoints observed:

- `CreateUser`
- `DeleteUser`
- `SetUserPassword`
- `CreateAuthSession`
- `RevokeUserSession`
- `RevokeUserSessions`
- session store `Create`, `Update`, `RevokeByID`, `RevokeFamily`, `DeleteExpiredRedacted`

Current persistence behavior:

- file-backed JSON stores using atomic full-file writes
- timestamps and IDs are generated in command/store paths
- password hashes and refresh token hashes are produced before persistence

WAL notes:

- Convert after space metadata.
- WAL records must never store plaintext passwords or refresh tokens.
- Records may store password hashes/token hashes if that is already the durable state.
- Session expiry cleanup should be represented explicitly or treated as deterministic maintenance mutation.

### Admin/operator metadata

Primary files:

```text
internal/daemon/modules/admin/module.go
internal/daemon/modules/admin/store.go
```

Mutation entrypoints observed:

- bootstrap default admin creation
- `SetOperatorPassword`
- `CreateOperator`
- `UpdateOperator`
- `DeleteOperator`
- `GrantRole` / `RevokeRole`
- `GrantCapability` / `RevokeCapability`
- operator session create/revoke through identity session store

Current persistence behavior:

- file-backed JSON store with temp-file write and rename
- module performs important invariants such as last-system-admin protection

WAL notes:

- Convert after user/session stores or alongside them.
- Bootstrap behavior needs special migration rules so existing data dirs do not generate duplicate bootstrap records.

### Graph storage

Primary files:

```text
internal/daemon/modules/graph/module.go
internal/graph/storage/store.go
internal/graph/storage/txn.go
internal/graph/filesession/*
```

Durable files touched:

```text
<data_dir>/graphs/<space_id>/manifest.mycel
<data_dir>/graphs/<space_id>/segments/*.kseg
```

Mutation entrypoints observed:

- graph module `CreateNode`, `UpdateNode`, `DeleteNode`
- graph module `CreateEdge`, `UpdateEdge`, `DeleteEdge`
- `CommitTransactionGraph`
- storage txn `PutNode`, `DeleteNode`, `PutEdge`, `DeleteEdge`, `Commit`, `CommitWithInfo`

Current persistence behavior:

- graph storage already appends records to node/edge/txn segments
- commit records establish committed transaction state
- indexes are rebuilt from committed segment records on open
- optimistic concurrency is based on graph revision/modification revision

WAL notes:

- Do not convert first.
- Eventually the daemon WAL should wrap graph transaction commit at the logical command/commit level.
- Need to decide whether existing graph segments remain the graph-local storage log while daemon WAL is the cross-context authoritative mutation stream.

### Blob storage

Primary files:

```text
internal/blob/storage/store.go
internal/daemon/modules/blob/module.go
```

Mutation entrypoints observed:

- blob store `Put`
- blob store `Delete`
- blob module `DeleteBlob`
- blob upload helper writes temporary/content files

WAL notes:

- Large blob bytes should not be embedded directly in WAL records.
- WAL should record blob metadata and content-address/reference after bytes are safely staged.
- Need a staging protocol so replay can complete or discard partial blob writes.

### Semantic/inference metadata and maintenance

Primary files:

```text
internal/semantic/storage/file_store.go
internal/semantic/accounting/file_store.go
internal/semantic/vectorstore/mycel_file.go
internal/semantic/maintenance/*
internal/daemon/api/admin/inference_service.go
internal/daemon/api/admin/semantic_service.go
```

Mutation entrypoints observed:

- inference catalog create/update/delete operations in `semantic/storage`
- semantic index delete
- credential grants and inference policies
- maintenance `AppendGraphDirtyEvent`
- maintenance `SaveCheckpoint`
- accounting `Append`
- vector backend `Delete`

Current persistence behavior:

- mostly JSON file stores with atomic writes
- maintenance appends/checkpoints drive background work
- vector backend can remove index directories

WAL notes:

- Convert after the base WAL and simpler metadata stores.
- External provider calls must not happen during WAL replay.
- Use outbox-style records for work that triggers background or external effects.

### Embedding provider store

Primary files:

```text
internal/embedding/store/file_store.go
```

Mutation entrypoints observed:

- `UpdateKey`
- `DeleteKey`

Current persistence behavior:

- JSON file-backed provider key store using atomic writes

WAL notes:

- Small/simple candidate, but lower value than space metadata for proving multi-file aggregate records.
- Ensure secrets are handled according to existing durable-state rules.

### Backup and daemon metadata

Primary files:

```text
internal/daemon/modules/backup/module.go
internal/backup/*
internal/daemon/modules/changestream/module.go
```

Mutation entrypoints observed:

- backup policy update
- backup run history/status writes
- backup deletion
- changestream persisted state writes

WAL notes:

- Backup/checkpoint integration is Phase 7.
- Backup policy changes can be WAL-backed later as daemon metadata records.
- WAL files themselves must be included/excluded from backups according to the checkpoint design.

## First WAL record payload: CreateSpaceWithDefaultDomain

Recommended Phase 4 payload:

```json
{
  "space": {
    "space_id": "uuid",
    "owner_id": "uuid",
    "name": "string",
    "status": "active",
    "settings": {},
    "created_at": "RFC3339Nano",
    "updated_at": "RFC3339Nano"
  },
  "default_domain": {
    "domain_id": "uuid",
    "space_id": "uuid",
    "key": "main",
    "name": "Main",
    "description": "",
    "discovery_mode": "normal",
    "search_mode": "normal",
    "semantic_mode": "normal",
    "read_only": false,
    "default": true,
    "created_at": "RFC3339Nano",
    "updated_at": "RFC3339Nano"
  },
  "owner_grant": {
    "grant_id": "uuid-or-derived-id-if-current-store-has-one",
    "space_id": "uuid",
    "user_id": "uuid",
    "permissions": ["admin"],
    "created_at": "RFC3339Nano"
  }
}
```

If the current ACL store does not use grant IDs/timestamps, omit those fields or add them only when the store schema is upgraded.

Replay behavior:

- If matching space/domain/grant already exists with identical durable fields, treat as success.
- If an ID/key exists with conflicting fields, fail recovery as corruption or partial-apply conflict.
- Do not generate IDs or timestamps in the applier.

## Phase 1-4 implementation implications

Before converting `CreateSpace`, storage packages will need applier-only methods that accept fully resolved durable models, for example:

```text
spaces.ApplyCreate(recordSpace)
domains.ApplyCreate(recordDomain)
acl.ApplyGrant(recordGrant)
```

These methods should be private or explicitly documented as WAL applier entrypoints, so request handlers cannot bypass the WAL path.

## Commands run

Inventory used source inspection with `find` and `rg` across:

```text
mycel/internal/space
mycel/internal/identity
mycel/internal/graph/storage
mycel/internal/blob/storage
mycel/internal/semantic/storage
mycel/internal/embedding/store
mycel/internal/daemon
```
