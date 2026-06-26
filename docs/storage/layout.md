# Storage Layout

MycelDB stores all file-backed data under a single data root.

This document is the root map for the storage directory. Detailed file formats and responsibilities are documented in the sibling storage documents:

- [meta.md](meta.md): global metadata, users, spaces, ACL, domains, templates, inference definitions, credentials, secrets, and accounting
- [graphs.md](graphs.md): per-space graph storage, graph manifests, graph segment files, recovery, and graph indexes
- [blobs.md](blobs.md): content-addressed blob storage and blob lifecycle
- [semantic.md](semantic.md): semantic indexes, graph/config event logs, dirty queues, policy decisions, and append-only vector records
- [segment-store.md](segment-store.md): current graph segment store details

## Directory Tree

```text
<data-root>/
  meta/
    users.json
    spaces.json
    access.json
    domains.json
    system.json

    templates/
      <space_id>.json

    embedding/                         # current embedding metadata
      embeddings.json

    inference/                         # proposed advanced inference definitions
      packages.json
      model_endpoints.json
      models.json
      model_endpoint_capabilities.json
      vector_stores.json

    secrets/                           # proposed encrypted secret records or external secret refs
      secrets.json

    credentials/                       # proposed credential metadata; grants are space-owned
      credentials.json

    semantic_events/                   # proposed global semantic config event log
      semantic-config-000001.ksem

    accounting/                        # proposed append-only inference usage ledger and derived indexes
      manifest.json
      inference-usage-000001.kusag
      indexes/
        by_principal/<principal_id>/YYYY-MM.kidx
        by_space/<space_id>/YYYY-MM.kidx
        by_domain/<domain_id>/YYYY-MM.kidx
        by_node/<node_id>/YYYY-MM.kidx
      rollups/
        principal-monthly.json

  graphs/
    <space_id>/
      .space
      manifest.mycel
      segments/
        txns-000001.kseg
        nodes-000001.kseg
        edges-000001.kseg

      embeddings/                      # current append-only embedding vector store
        manifest.kemb
        segments/
          embeddings-000001.kvec

      semantic/                        # proposed advanced semantic index storage
        indexes.json
        credential_grants.json
        inference_policies.json
        index_state.json
        dirty_queue.json
        policy_decisions.json

        events/
          graph-dirty-000001.ksem

        indexes/
          <semantic_index_id>/
            manifest.ksem
            records/
              embeddings-000001.kvec
            external_refs.json

  blobs/
    <space_id>/
      objects/
        <aa>/
          <sha256-hex>
      tmp/
```

## Top-Level Responsibilities

### `meta/`

Stores global and cross-space metadata. Most files are JSON-backed today.

Examples:

- users and password material
- space metadata
- ACL rules
- domain metadata
- graph template definitions
- current embedding provider keys/profiles
- proposed inference/model-endpoint/model/capability/vector-store definitions
- proposed credentials and secrets
- proposed append-only inference usage accounting ledger, indexes, and rollups

See [meta.md](meta.md).

### `graphs/`

Stores per-space graph data.

Current graph records are append-only binary records in `.kseg` segment files. On open, Mycel scans committed transaction records and rebuilds in-memory indexes.

Current embeddings are also stored under each graph space in append-only `.kvec` segment files.

The proposed advanced semantic system stores per-space semantic index definitions and per-index vector records under `graphs/<space_id>/semantic/`.

See [graphs.md](graphs.md) and [semantic.md](semantic.md).

### `blobs/`

Stores immutable content-addressed blob payloads by space.

Blob files are separated from graph structure so large binary data can be backed up, tiered, or restored independently from graph metadata.

See [blobs.md](blobs.md).

## Storage Guarantees

- Metadata files are JSON-backed and rewritten atomically by the file-store helpers.
- Graph mutations are append-only and transaction-scoped.
- Uncommitted graph records are ignored during recovery.
- Blob objects are immutable and content-addressed.
- Current and proposed vector records are append-only; stale vector records are ignored logically until compaction.
- Persisted indexes are intentionally deferred; in-memory indexes are rebuilt from authoritative files.

## Delete Behavior

Deleting a space removes the owning graph and blob directories:

```text
graphs/<space_id>/
blobs/<space_id>/
```

Metadata records for the space, templates, domains, ACL rules, semantic indexes, space-owned credential grants, and space-owned policy references must also be removed or tombstoned by their owning managers.

## ID Format

Public IDs remain UUIDs:

- `UserID`
- `SpaceID`
- `DomainID`
- `TemplateID`
- `NodeID`
- `EdgeID`
- `BlobID`
- semantic/inference resource IDs

Graph node and edge IDs generated by MycelDB use UUIDv7.
