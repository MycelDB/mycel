# Semantic and Embedding Storage

Status: design draft  
Related design: [`../semantic/README.md`](../semantic/README.md)

This document describes current embedding storage and the proposed advanced semantic/inference storage layout.

The advanced embedding model introduces provisioned inference definitions, credentials, credential grants, inference policies, semantic indexes, dirty maintenance work, policy decisions, and append-only vector records.

## Storage Principles

- Keep graph mutations fast; never call an inference runtime synchronously in the graph write path.
- Store global/deployment definitions and principal-owned credentials under `meta/`.
- Store graph content in existing append-only graph segments under `graphs/<space_id>/segments/`.
- Store semantic index definitions, credential grants, inference policies, and operational state under the owning space.
- Store embedded vector records in append-only `.kvec` files.
- Preserve enough provenance to audit which runtime, model, credential grant, and policy decision produced or skipped an embedding.
- Keep JSON metadata simple initially; move to append-only metadata logs or per-record files later only if needed.

## Current Relevant Layout

Current Mycel storage is approximately:

```text
<data-root>/
  meta/
    users.json
    spaces.json
    access.json
    domains.json
    templates/
      <space_id>.json
    embedding/
      embeddings.json

  graphs/
    <space_id>/
      .space
      manifest.mycel
      segments/
        txns-000001.kseg
        nodes-000001.kseg
        edges-000001.kseg
      embeddings/
        manifest.kemb
        segments/
          embeddings-000001.kvec

  blobs/
    <space_id>/
      objects/
        <aa>/<sha256-hex>
      tmp/
```

Today:

- `meta/embedding/embeddings.json` stores provider API keys and embedding profiles.
- `graphs/<space_id>/embeddings/manifest.kemb` stores the active embedding segment name.
- `graphs/<space_id>/embeddings/segments/*.kvec` stores append-only embedding records.
- graph nodes carry `DomainID`, and embedding records also carry `DomainID`.
- `meta/domains.json` stores domain metadata and the current early domain embedding policy concept.

## Current JSON Structures

### `meta/embedding/embeddings.json`

Current structure:

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
      "api_key_ciphertext": "base64-or-json-encrypted-payload",
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
      "include_props": ["prop_name"],
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

- `keys` are user-owned provider API keys.
- `api_key_ciphertext` is encrypted by the embedding metadata manager when an encryption key is configured.
- `profiles` combine provider/model/source-selection concerns. The advanced model replaces this with semantic indexes, runtimes, models, credentials, and policies.

### `graphs/<space_id>/embeddings/manifest.kemb`

Current structure:

```json
{
  "format": "mycel-embeddings-v1",
  "active_segment": "embeddings-000001.kvec",
  "created_at": "RFC3339 timestamp",
  "updated_at": "RFC3339 timestamp"
}
```

Fields:

- `format`: storage format identifier.
- `active_segment`: segment file appended by new embedding records.
- `created_at`: manifest creation time.
- `updated_at`: last append/touch time.

## Current `.kvec` Append-Only Block Structure

Current `.kvec` files are append-only binary segment files.

Path:

```text
graphs/<space_id>/embeddings/segments/embeddings-000001.kvec
```

### Segment Header

The file starts with a fixed segment header:

| Field | Encoding | Description |
| --- | --- | --- |
| magic | ASCII bytes `KEMBSEG1` | Identifies embedding segment files. |
| version | uint16 little-endian | Current value: `1`. |
| reserved | uint16 little-endian | Current value: `0`; reserved for future flags. |

### Record Block

Each appended record block has a fixed binary header followed by metadata JSON and vector bytes:

```text
record := record_header metadata_json vector_payload
```

Fixed header:

| Field | Encoding | Description |
| --- | --- | --- |
| magic | uint32 little-endian, `0x4b524543` (`KREC`) | Identifies a record block. |
| version | uint16 little-endian | Current value: `1`. |
| flags | uint16 little-endian | Current value: `0`; reserved. |
| record_id | 16 UUID bytes | Embedding record ID. |
| space_id | 16 UUID bytes | Owning space. |
| node_id | 16 UUID bytes | Embedded graph node. |
| profile_id | 16 UUID bytes | Current embedding profile ID, or nil UUID. |
| created_at_unix_nano | int64 little-endian | Creation timestamp. |
| dimensions | uint32 little-endian | Number of vector dimensions. |
| metadata_len | uint32 little-endian | Byte length of metadata JSON. |
| vector_len | uint32 little-endian | Byte length of vector payload. Must equal `dimensions * 4`. |
| crc32 | uint32 little-endian | CRC-32 over `metadata_json || vector_payload`. |

Metadata JSON payload:

```json
{
  "domain_id": "uuid",
  "provider_id": "openai",
  "model_id": "text-embedding-3-small",
  "source_mode": "self|subtree",
  "source_hash": "sha256-or-other-source-hash"
}
```

Vector payload:

```text
dimensions * float32 little-endian
```

Although the domain model exposes vectors as `[]float64`, the current file format stores each component as a 32-bit float.

### Append and Recovery Behavior

- New records are appended; existing records are never overwritten.
- Regeneration appends a new record with a new `source_hash` and later `created_at`.
- Search reconstructs all records by scanning the segment and keeps only the latest logical record for each node/profile/source-mode identity.
- Compaction is deferred.

## Proposed Advanced Layout

The advanced model should separate global inference definitions/credentials from space-owned grants, policies, semantic indexes, dirty work, and vector records.

Proposed layout:

```text
<data-root>/
  meta/
    inference/
      packages.json
      runtimes.json
      models.json
      vector_stores.json
    secrets/
      secrets.json
    credentials/
      credentials.json

  graphs/
    <space_id>/
      manifest.mycel
      segments/
        txns-000001.kseg
        nodes-000001.kseg
        edges-000001.kseg

      semantic/
        indexes.json
        credential_grants.json
        inference_policies.json
        index_state.json
        dirty_queue.json
        policy_decisions.json

        indexes/
          <semantic_index_id>/
            manifest.ksem
            records/
              embeddings-000001.kvec
            external_refs.json
```

## Proposed Global JSON Structures

### `meta/inference/packages.json`

Tracks installed inference definition packages.

```json
{
  "packages": [
    {
      "id": "uuid",
      "name": "standard-openai",
      "version": "2026.06",
      "source": "file:///path/standard-openai.yaml",
      "checksum": "sha256:...",
      "installed_at": "RFC3339 timestamp",
      "installed_by": "principal-ref",
      "definition_counts": {
        "runtimes": 1,
        "models": 2,
        "vector_stores": 0,
        "semantic_index_templates": 0
      }
    }
  ]
}
```

Fields:

- `source`: path or URL from which the package was installed.
- `checksum`: content checksum used for audit/reinstall checks.
- `definition_counts`: summary only; actual definitions live in their own files.

### `meta/inference/runtimes.json`

Stores provisioned inference runtimes.

```json
{
  "runtimes": [
    {
      "id": "uuid",
      "key": "openai-public",
      "name": "OpenAI Public API",
      "connector_type": "openai-compatible",
      "endpoint": "https://api.openai.com/v1",
      "network_class": "external_https",
      "privacy_class": "third_party",
      "auth_modes": ["api_key"],
      "operations": ["embeddings", "chat"],
      "enabled": true,
      "metadata": {},
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Fields:

- `connector_type`: static code-backed connector enum. Initial values: `openai-compatible`, `anthropic`, `ollama`, `azure-openai`, `bedrock`, `custom-http`, `local-process`.
- `endpoint`: service URL or local endpoint.
- `network_class`: e.g. `local`, `private_network`, `external_https`.
- `privacy_class`: e.g. `local_only`, `enterprise_private`, `third_party`.
- `operations`: supported operation types.

### `meta/inference/models.json`

Stores model metadata.

```json
{
  "models": [
    {
      "id": "uuid",
      "key": "openai/text-embedding-3-small",
      "operation": "embeddings",
      "model_name": "text-embedding-3-small",
      "connector_types": ["openai-compatible"],
      "dimensions": 1536,
      "modality": "text",
      "vector_space_key": "openai/text-embedding-3-small",
      "metadata": {},
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Fields:

- `connector_types`: connector enum values that can execute this model shape.
- `vector_space_key`: compatibility key; vectors from different vector spaces must not be compared directly.
- `dimensions`: required for embedded vector store validation.

### `meta/inference/vector_stores.json`

Stores configured vector-store backends.

```json
{
  "vector_stores": [
    {
      "id": "uuid",
      "key": "mycel-file",
      "name": "Mycel embedded file vector store",
      "type": "mycel-file",
      "config": {},
      "privacy_class": "local_only",
      "enabled": true,
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Fields:

- `type`: backend implementation family, such as `mycel-file`, `qdrant`, `pgvector`, `pinecone`, or `custom-http`.
- `config`: backend-specific non-secret configuration. Secret material belongs in the secret store.

### `meta/secrets/secrets.json`

Stores encrypted secret payloads or references to external secret managers.

```json
{
  "secrets": [
    {
      "id": "uuid",
      "owner_type": "user|space|organization|system",
      "owner_id": "principal-id",
      "kind": "inline_encrypted|external_ref",
      "ciphertext": {
        "algorithm": "AES-256-GCM",
        "nonce_b64": "...",
        "cipher_b64": "..."
      },
      "external_ref": "vault://path/to/secret",
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Rules:

- Exactly one of `ciphertext` or `external_ref` should be populated.
- Raw API keys must never be stored in plaintext.

### `meta/credentials/credentials.json`

Stores credential metadata.

```json
{
  "credentials": [
    {
      "id": "uuid",
      "key": "martin-openai",
      "name": "Martin OpenAI",
      "runtime_id": "uuid-or-runtime-key",
      "owner_type": "user",
      "owner_id": "user-uuid",
      "auth_type": "api_key",
      "secret_ref": "secret-uuid",
      "status": "active|revoked|expired|disabled",
      "is_default": true,
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp",
      "last_used_at": "RFC3339 timestamp"
    }
  ]
}
```

Fields:

- `runtime_id`: runtime this credential can authenticate to.
- `secret_ref`: reference to `meta/secrets/secrets.json` or an external secret manager.
- `is_default`: default only within the credential owner/runtime resolution scope.

## Proposed Per-Space JSON Structures

Space-owned semantic governance lives under `graphs/<space_id>/semantic/`. This includes credential grants and inference policies because they govern processing of content in the owning space.

### `graphs/<space_id>/semantic/credential_grants.json`

Stores credential grants for one owning space.

A grant authorizes one credential for one processing scope.

```json
{
  "grants": [
    {
      "id": "uuid",
      "credential_id": "uuid",
      "scope": {
        "space_id": "uuid",
        "domain_id": "uuid",
        "semantic_index_id": "uuid",
        "node_id": "uuid",
        "include_descendants": true
      },
      "operations": ["embeddings"],
      "runtime_id": "uuid-or-runtime-key",
      "model_id": "uuid-or-model-key",
      "priority": 0,
      "is_default": true,
      "granted_by": "principal-ref",
      "created_at": "RFC3339 timestamp",
      "expires_at": "RFC3339 timestamp"
    }
  ]
}
```

Rules:

- `credential_id` is required.
- The owning space is implied by the file location; `scope.space_id` may be retained for validation but should match the owning directory.
- `scope` may be broad, such as a domain, or narrow, such as a subtree.
- `runtime_id` and `model_id` are optional constraints but recommended.
- Resolution chooses the most specific compatible grant and errors on ambiguous same-specificity grants unless priority/default breaks the tie.

### `graphs/<space_id>/semantic/inference_policies.json`

Stores content-processing policies for one owning space.

```json
{
  "policies": [
    {
      "id": "uuid",
      "scope": {
        "space_id": "uuid",
        "domain_id": "uuid",
        "semantic_index_id": "uuid",
        "node_id": "uuid",
        "include_descendants": true
      },
      "effect": "allow|deny|restrict",
      "operations": ["embeddings", "chat"],
      "no_inference": false,
      "allowed_privacy_classes": ["local_only", "enterprise_private"],
      "disallow_third_party": true,
      "require_local_runtime": true,
      "reason": "Private journal subtree",
      "created_by": "principal-ref",
      "created_at": "RFC3339 timestamp",
      "expires_at": "RFC3339 timestamp"
    }
  ]
}
```

Rules:

- The owning space is implied by the file location; `scope.space_id` may be retained for validation but should match the owning directory.
- Policies are scope references; they are not embedded directly into graph node records.
- Deny/restrict policies must be evaluated before credential grants.
- Node/subtree policies override broader domain/space allowances.


### `graphs/<space_id>/semantic/indexes.json`

Stores semantic index definitions for a space.

```json
{
  "indexes": [
    {
      "id": "uuid",
      "space_id": "uuid",
      "domain_id": "uuid",
      "key": "notes-search",
      "name": "Notes Search",
      "purpose": "semantic_search|chat_rag|task_search|autocomplete",
      "enabled": true,
      "source_policy": {
        "template_keys": ["logseq.journal", "logseq.page"],
        "tags": ["public"],
        "property_selectors": {
          "kind": "note"
        },
        "source_mode": "self|subtree|custom",
        "max_depth": 0,
        "include_props": ["title", "tags"],
        "minimum_text_length": 1
      },
      "binding": {
        "runtime_id": "uuid-or-runtime-key",
        "model_id": "uuid-or-model-key",
        "vector_store_id": "uuid-or-vector-store-key"
      },
      "refresh_policy": {
        "mode": "manual|dirty_debounce|scheduled|disabled",
        "debounce_duration": "1m",
        "schedule": "cron-expression"
      },
      "processing_policy_ref": "policy-uuid",
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Notes:

- The binding intentionally does not include a credential/API key.
- Credentials are resolved from global credential metadata and space-owned grants in `graphs/<space_id>/semantic/credential_grants.json` when a runtime call is needed.

### `graphs/<space_id>/semantic/index_state.json`

Stores operational state for each semantic index.

```json
{
  "states": [
    {
      "semantic_index_id": "uuid",
      "state": "created|inactive_missing_runtime|inactive_missing_credentials|active|backfilling|degraded|disabled|failed|retired",
      "last_backfill_at": "RFC3339 timestamp",
      "last_refresh_at": "RFC3339 timestamp",
      "last_error": "error text",
      "dirty_count": 12,
      "record_count": 1250,
      "skipped_policy_count": 3,
      "credential_resolution_failure_count": 1,
      "source_policy_hash": "sha256:...",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Fields:

- `dirty_count`: pending work count for the index.
- `record_count`: last known logical or physical record count, depending on implementation.
- `source_policy_hash`: detects index definition changes requiring re-evaluation/backfill.

### `graphs/<space_id>/semantic/dirty_queue.json`

Stores semantic-index maintenance work.

```json
{
  "items": [
    {
      "id": "uuid",
      "semantic_index_id": "uuid",
      "space_id": "uuid",
      "domain_id": "uuid",
      "target_node_id": "uuid",
      "source_node_id": "uuid",
      "reason": "node_created|node_updated|node_deleted|node_moved|policy_changed|index_changed|manual_backfill",
      "status": "pending|running|complete|failed|cancelled",
      "earliest_run_at": "RFC3339 timestamp",
      "attempts": 0,
      "last_error": "error text",
      "created_at": "RFC3339 timestamp",
      "updated_at": "RFC3339 timestamp"
    }
  ]
}
```

Rules:

- Work should coalesce by `semantic_index_id + target_node_id`.
- For `self` indexes, `target_node_id` is usually the changed node.
- For `subtree` indexes, `target_node_id` is usually the semantic root selected by the index.

### `graphs/<space_id>/semantic/policy_decisions.json`

Stores persisted policy decisions for audit/debug flows.

```json
{
  "decisions": [
    {
      "id": "uuid",
      "scope": {
        "space_id": "uuid",
        "domain_id": "uuid",
        "semantic_index_id": "uuid",
        "node_id": "uuid",
        "include_descendants": false
      },
      "operation": "embeddings",
      "runtime_id": "uuid-or-runtime-key",
      "model_id": "uuid-or-model-key",
      "allowed": false,
      "matched_policy_ids": ["policy-uuid"],
      "reason": "subtree requires local runtime",
      "created_at": "RFC3339 timestamp"
    }
  ]
}
```

Notes:

- This store can be bounded or compacted.
- Not every transient query decision must be persisted forever.
- Decisions that produce embedding records or skip durable backfill work should be inspectable.

### `graphs/<space_id>/semantic/indexes/<semantic_index_id>/manifest.ksem`

Stores per-index embedded-vector manifest metadata.

```json
{
  "format": "mycel-semantic-index-v1",
  "semantic_index_id": "uuid",
  "vector_store_id": "uuid-or-vector-store-key",
  "active_record_segment": "embeddings-000001.kvec",
  "record_segments": ["embeddings-000001.kvec"],
  "created_at": "RFC3339 timestamp",
  "updated_at": "RFC3339 timestamp"
}
```

Fields:

- `active_record_segment`: append target for new embedded vector records.
- `record_segments`: ordered list scanned to rebuild in-memory vector indexes.

### `graphs/<space_id>/semantic/indexes/<semantic_index_id>/external_refs.json`

Stores references when vectors live in an external vector store.

```json
{
  "records": [
    {
      "id": "uuid",
      "space_id": "uuid",
      "domain_id": "uuid",
      "semantic_index_id": "uuid",
      "node_id": "uuid",
      "source_hash": "sha256:...",
      "vector_store_id": "uuid-or-vector-store-key",
      "external_vector_id": "qdrant-point-id",
      "runtime_id": "uuid-or-runtime-key",
      "model_id": "uuid-or-model-key",
      "vector_space_key": "openai/text-embedding-3-small",
      "created_at": "RFC3339 timestamp"
    }
  ]
}
```

Rules:

- Used only when Mycel does not own the local vector payload.
- External deletion/compaction requires coordination with the external backend.

## Proposed Advanced `.kvec` Block Structure

For embedded vector storage, each semantic index should own append-only vector segments:

```text
graphs/<space_id>/semantic/indexes/<semantic_index_id>/records/embeddings-000001.kvec
```

The segment remains append-only. Regeneration appends a new record; stale records are ignored logically until compaction.

### Segment Header

Use a versioned segment header. It may reuse the current `KEMBSEG1` magic for compatibility or introduce a new semantic-specific magic.

Recommended header:

| Field | Encoding | Description |
| --- | --- | --- |
| magic | ASCII bytes, e.g. `KEMBSEG2` | Identifies advanced embedding vector segment. |
| version | uint16 little-endian | Format version. |
| flags | uint16 little-endian | Reserved. |

### Record Block

Recommended advanced record layout:

```text
record := record_header metadata_json vector_payload_or_empty
```

Fixed header:

| Field | Encoding | Description |
| --- | --- | --- |
| magic | uint32 little-endian, e.g. `KREC` | Record marker. |
| version | uint16 little-endian | Record format version. |
| flags | uint16 little-endian | Flags such as tombstone/external-ref/no-inline-vector. |
| record_id | 16 UUID bytes | Embedding record ID. |
| space_id | 16 UUID bytes | Owning space. |
| domain_id | 16 UUID bytes | Owning graph domain. |
| semantic_index_id | 16 UUID bytes | Semantic index that produced the record. |
| node_id | 16 UUID bytes | Embedded graph node. |
| runtime_id | 16 UUID bytes or nil | Runtime used to generate the vector. |
| model_id | 16 UUID bytes or nil | Model metadata ID. |
| vector_store_id | 16 UUID bytes or nil | Vector store backend ID. |
| credential_id | 16 UUID bytes or nil | Credential used, if any. |
| credential_grant_id | 16 UUID bytes or nil | Grant authorizing use, if any. |
| policy_decision_id | 16 UUID bytes or nil | Policy decision applied, if persisted. |
| created_at_unix_nano | int64 little-endian | Creation timestamp. |
| dimensions | uint32 little-endian | Number of dimensions. |
| metadata_len | uint32 little-endian | Byte length of metadata JSON. |
| vector_len | uint32 little-endian | Byte length of vector payload. May be `0` for external refs. |
| crc32 | uint32 little-endian | CRC-32 over `metadata_json || vector_payload`. |

Recommended metadata JSON payload:

```json
{
  "source_mode": "self|subtree|custom",
  "source_hash": "sha256:...",
  "vector_space_key": "openai/text-embedding-3-small",
  "external_vector_id": "optional-external-id",
  "runtime_key": "openai-public",
  "model_key": "openai/text-embedding-3-small",
  "vector_store_key": "mycel-file"
}
```

Vector payload:

```text
dimensions * float32 little-endian
```

For external vector stores, `vector_len` may be `0`, and `external_vector_id`/`external_refs.json` identifies the external record.

### Logical Freshness Key

Search should choose the newest record for a logical identity such as:

```text
semantic_index_id + node_id + source_mode
```

If future indexes allow multiple source policies per node within one semantic index, the freshness key should include a source-policy or source-selector hash.

## Node Creation and Dirty Work Persistence

If a block is created under a parent whose effective policy recommends embeddings:

1. The graph mutation is committed to `graphs/<space_id>/segments/*.kseg`.
2. Mycel resolves applicable semantic indexes from `graphs/<space_id>/semantic/indexes.json`.
3. Mycel evaluates inherited policies from `graphs/<space_id>/semantic/inference_policies.json`.
4. Mycel writes or coalesces dirty work in `graphs/<space_id>/semantic/dirty_queue.json`.
5. A background maintainer later processes dirty work.
6. The maintainer resolves credential metadata from `meta/credentials/credentials.json` and authorization grants from `graphs/<space_id>/semantic/credential_grants.json`.
7. Generated records are appended to the semantic index vector store.
8. State is updated in `graphs/<space_id>/semantic/index_state.json`.

This keeps graph writes fast and durable while making semantic maintenance resumable after process restarts.

## Recovery and In-Memory Indexes

On startup, Mycel can rebuild semantic runtime state from:

- global runtime/model/vector-store definitions
- credentials and grants
- inference policies
- semantic index definitions
- dirty queue records
- policy decision records
- vector record manifests and segments

Recommended in-memory indexes:

- runtime by ID/key
- model by ID/key/vector-space key
- credential by owner/runtime/status
- grant by space/domain/index/node scope
- policy by space/domain/index/node scope
- semantic index by space/domain/key/purpose
- dirty queue by semantic index/target node
- latest vector record by semantic index/node/source identity

## Compaction

Initial implementation can defer compaction. Later compaction should address:

- stale vector records superseded by newer source hashes
- completed dirty queue entries
- expired policy decisions
- deleted/revoked credentials and grants
- deleted nodes/subtrees
- semantic indexes that have been retired
- external vector store tombstones

Compaction should preserve audit requirements where configured.
