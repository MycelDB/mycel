# Metadata Storage

MycelDB stores global and cross-space metadata under:

```text
<data-root>/meta/
```

Most metadata stores are JSON-backed. The current implementation rewrites these files atomically through the file-store helpers rather than appending records.

## Directory Tree

```text
meta/
  users.json
  spaces.json
  access.json
  domains.json
  system.json

  templates/
    <space_id>.json

  embedding/
    embeddings.json

  inference/          # proposed advanced inference definitions
    packages.json
    model_endpoints.json
    models.json
    model_endpoint_capabilities.json
    vector_stores.json

  secrets/            # proposed advanced secret store
    secrets.json

  credentials/        # proposed advanced credential metadata; grants are space-owned
    credentials.json

  semantic_events/    # proposed global semantic config event log
    semantic-config-000001.ksem

  accounting/         # proposed append-only inference usage ledger
    manifest.json
    inference-usage-000001.kusag
    indexes/
      by_principal/<principal_id>/YYYY-MM.kidx
      by_space/<space_id>/YYYY-MM.kidx
      by_domain/<domain_id>/YYYY-MM.kidx
      by_node/<node_id>/YYYY-MM.kidx
    rollups/
      principal-monthly.json
```

## Required Metadata Files

For an initialized store, these files are required:

```text
meta/users.json
meta/spaces.json
meta/access.json
```

If one required metadata file exists while another required file is missing, startup should fail instead of silently recreating an empty store and risking data loss.

## `users.json`

Stores users and password material.

Current logical structure:

```json
[
  {
    "user": {
      "id": "uuid",
      "ref": "martin",
      "display_name": "Martin",
      "status": "active",
      "settings": {}
    },
    "password": "password-hash-or-encoded-password-material"
  }
]
```

When a user-store encryption key is configured, the entire user store is encrypted rather than stored as a plaintext array.

Encrypted structure:

```json
{
  "format": "mycel-user-store-v1",
  "nonce_b64": "...",
  "cipher_b64": "..."
}
```

Notes:

- User references are indexed case-insensitively in memory.
- User IDs are UUIDs.
- The current plaintext mode logs a warning and is intended for local/dev use only.

## `spaces.json`

Stores space metadata.

Current structure:

```json
[
  {
    "space_id": "uuid",
    "owner_id": "user-uuid",
    "name": "Personal PKM",
    "status": "active",
    "settings": {
      "max_space_bytes": 0,
      "target_chunk_bytes": 0,
      "max_chunk_bytes": 0,
      "max_asset_bytes": 0,
      "max_pdf_bytes": 0,
      "compaction_enabled": false
    }
  }
]
```

Notes:

- Space ownership and access boundaries are represented here and in `access.json`.
- Per-space graph content lives under `graphs/<space_id>/`.
- Per-space blob content lives under `blobs/<space_id>/`.

## `access.json`

Stores system and space ACL rules.

Current structure:

```json
{
  "system_rules": [
    {
      "id": "uuid",
      "user_id": "user-uuid",
      "roles": ["superuser"]
    }
  ],
  "space_rules": [
    {
      "id": "uuid",
      "space_id": "space-uuid",
      "user_id": "user-uuid",
      "roles": ["reader", "writer", "admin"]
    }
  ]
}
```

Notes:

- System rules grant global permissions such as superuser/admin capabilities.
- Space rules grant per-space read/write/admin permissions.
- ACL indexes are rebuilt in memory from this file.

## `domains.json`

Stores graph domain metadata and, in the current transitional implementation, early domain embedding policy records.

Current structure:

```json
{
  "domains": [
    {
      "id": "uuid",
      "space_id": "space-uuid",
      "key": "personal-pkm",
      "name": "Personal PKM",
      "description": "",
      "default": true,
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ],
  "embedding_policies": [
    {
      "domain_id": "domain-uuid",
      "provider_id": "openai",
      "model_id": "text-embedding-3-small"
    }
  ]
}
```

Notes:

- Domains are logical graph/query partitions inside spaces.
- Every graph node belongs to exactly one domain.
- The current `embedding_policies` concept is expected to evolve into first-class semantic indexes and inference policies.

## `templates/<space_id>.json`

Stores immutable template versions for a space.

Current structure:

```json
[
  {
    "id": "uuid",
    "space_id": "space-uuid",
    "key": "logseq.journal",
    "version": "1.0.0",
    "display_name": "Logseq Journal",
    "description": "Journal root",
    "system": false,
    "properties": {
      "allow_extra": true,
      "required": [],
      "allowed": []
    },
    "children": {
      "allowed": true,
      "allowed_templates": []
    }
  }
]
```

Notes:

- Template identity is `space_id + key + version`.
- Existing template versions are immutable; changes should create a new version.
- Template indexes are rebuilt in memory from all `templates/*.json` files.

## Current Embedding Metadata: `embedding/embeddings.json`

Current embedding metadata combines provider keys and embedding profiles.

```json
{
  "keys": [
    {
      "id": "uuid",
      "owner_id": "user-uuid",
      "provider_id": "openai",
      "name": "OpenAI default",
      "is_default": true,
      "disabled": false,
      "api_key_ciphertext": "encrypted-payload",
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ],
  "profiles": [
    {
      "id": "uuid",
      "owner_id": "user-uuid",
      "name": "PKM notes",
      "provider_id": "openai",
      "model_id": "text-embedding-3-small",
      "source_mode": "self|subtree",
      "include_props": ["title"],
      "max_depth": 0,
      "minimum_text_length": 0,
      "target_template_keys": ["logseq.journal"],
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Notes:

- Provider keys are user-owned.
- API keys are encrypted when an encryption key is configured.
- Profiles are a current low-level abstraction. The advanced semantic design replaces them with model endpoints, models, semantic indexes, credentials, grants, and policies.

## Proposed Advanced Inference Metadata

Advanced inference metadata is described in detail in [semantic.md](semantic.md), but its global metadata lives under `meta/`:

```text
meta/inference/packages.json
meta/inference/model_endpoints.json
meta/inference/models.json
meta/inference/model_endpoint_capabilities.json
meta/inference/vector_stores.json
meta/secrets/secrets.json
meta/credentials/credentials.json
```

These stores are global/deployment-level or cross-space metadata:

- inference packages define installable bundles of model-endpoint/model/capability/vector-store definitions
- model endpoints define callable AI service endpoints
- models define vector-space/model metadata
- model endpoint capabilities define which endpoint can serve which model for which operation
- vector stores define embedded or external vector backends
- secrets store encrypted payloads or external secret references
- credentials bind principals to model endpoint auth material

Credential grants and inference/content policies are space-owned because they govern processing of content in a specific space. They are stored under `graphs/<space_id>/semantic/`; see [semantic.md](semantic.md).

Global semantic configuration events are operational append-only events under `meta/semantic_events/`. They notify semantic analyzers about changes such as model endpoint capabilities, model definitions, vector stores, credential revocations, and scoped space-owned semantic configuration changes.

Inference usage accounting events are append-only operational records under `meta/accounting/`. They are not graph data. The authoritative ledger is `inference-usage-*.kusag`; user/space/domain/node indexes and rollups are derived and rebuildable. Phase 3 implements the local ledger foundation, explicit rebuild commands, and CLI reports. See [../semantic/accounting.md](../semantic/accounting.md).

## Atomic Writes and Recovery

Metadata files should be written through the file-store helper so updates are atomic from the perspective of process crashes.

On startup, metadata managers load JSON files and rebuild in-memory indexes. Persisted secondary indexes are intentionally deferred.
