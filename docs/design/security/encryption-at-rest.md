# Encryption at rest

Mycel needs first-class encryption at rest for user data and user-derived
persistent artifacts. This design defines the target architecture for encrypted
local/block storage, client-side encrypted object-store payloads, encrypted
consensus/recovery data, encrypted derived indexes, key management, and
verification for new encrypted deployments.

This supersedes the removed `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` setting.
That legacy setting encrypted only inline daemon-managed secrets, such as
inference provider API-key material. It did not encrypt graph data, blob
payloads, WAL records, Raft logs, snapshots, backups, search indexes, semantic
vectors, or most metadata. New deployments use the `MYCELD_ENCRYPTION_*`
envelope-encryption configuration for both user-data artifacts and inline
secrets; `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is rejected at startup.

## Goals

- Encrypt user data and user-derived artifacts before they are persisted to
  local disk, block storage, or object storage.
- Use authenticated encryption so tampering is detected before plaintext is
  accepted.
- Avoid a single static data key for every persisted byte.
- Support standalone development defaults and explicit cluster-grade key
  configuration.
- Support key rotation, disaster recovery, and backup/restore without rewriting
  all data when only the wrapping key changes.
- Preserve Mycel API authorization as the source of truth. Storage encryption is
  defense-in-depth, not an authorization mechanism.
- Make encryption observable and testable: operators should be able to tell
  whether encryption is enabled, which provider is in use, and whether persisted
  user-data artifacts are encrypted.

## Non-goals

- Hiding data from a live daemon that has successfully unlocked the data keys.
- Replacing graph/space/domain authorization with object-store policies.
- Implementing end-to-end client-held keys in the first version.
- Providing searchable encryption. Search and semantic features require the
  daemon to decrypt or derive searchable plaintext in trusted process memory.
- Relying only on filesystem, disk, cloud volume, S3 SSE, or object-store KMS
  encryption. Those remain useful defense-in-depth controls, but Mycel needs an
  application-level encryption boundary for portable backups, local files, and
  object payloads.

## Threat model

Encryption at rest should protect against:

- offline reads of copied data directories, PVC snapshots, disks, or filesystem
  backups;
- reads of object-store payloads without access to Mycel's key material;
- accidental inclusion of plaintext user content in backup/restore bundles;
- operators or automation inspecting storage files directly without key access;
- integrity attacks that modify encrypted artifacts outside Mycel.

It does not protect against:

- a compromised running daemon with key material in memory;
- an attacker who has both ciphertext and the active key-encryption key or KMS
  decrypt authority;
- plaintext logged before the logging path is fixed;
- plaintext emitted through legitimate API calls by authorized principals.

## Encryption algorithm

Use envelope encryption with authenticated encryption:

- **AEAD**: AES-256-GCM.
- **Data-encryption keys (DEKs)**: random 256-bit keys generated per encryption
  scope.
- **Key-encryption key (KEK)**: provided by a configured key provider and used
  only to wrap/unwrap DEKs.
- **AAD**: authenticated additional data that binds ciphertext to its storage
  context.

AES-256-GCM is widely reviewed, hardware-accelerated on common platforms, and
provides confidentiality plus integrity.

Nonce/key reuse must be impossible by construction. Each encrypted record or
chunk gets a unique nonce under its DEK. For chunked files, the format stores a
random per-file nonce prefix and derives per-chunk nonces from a monotonic chunk
index, or stores random nonces per chunk. The selected implementation must have
unit tests for nonce uniqueness.

## Key hierarchy

```text
Key provider / KEK
  wraps
DEK records
  encrypt
storage records, files, chunks, snapshots, and object payloads
```

### KEK providers and deployment modes

Encryption-at-rest must support both frictionless development and production
key-management. The daemon should distinguish the encryption mode from the KEK
provider:

1. `disabled`
   - no application-level encryption at rest;
   - intended for local development, tests, disposable lab clusters, and
     debugging workflows where operators need to inspect persisted fixtures;
   - must be explicit and observable in startup logs, health/admin status, and
     diagnostics;
   - should remain available even when production deployments default to
     encrypted mode in a later release.
2. `static-env`
   - envelope encryption is enabled;
   - the KEK is supplied as an explicit base64-encoded 32-byte value from
     environment/config;
   - useful for tests and small self-managed deployments;
   - acceptable when deployment simplicity matters, but not preferred for
     production Kubernetes because environment variables can leak through pod
     specs, process environments, shell history, and operational tooling;
   - rotation requires replacing the value and rewrapping DEKs.
3. `static-file`
   - envelope encryption is enabled;
   - the KEK is read from a root-readable or daemon-readable file, typically a
     mounted Kubernetes Secret or sealed-secret material;
   - preferred over `static-env` for simple K3s/Kubernetes deployments;
   - when used in K3s, operators should enable K3s `--secrets-encryption` so the
     Kubernetes Secret that carries the KEK is encrypted in Kubernetes storage.
4. `vault-transit`
   - envelope encryption is enabled;
   - Vault Transit wraps/unwraps DEKs and Mycel stores only wrapped DEKs plus
     Vault key identifiers in metadata;
   - preferred for production self-managed K3s/on-prem deployments;
   - Vault Transit is a Vault secrets engine, not a standalone service, so this
     mode requires operating Vault;
   - Vault may run inside the K3s cluster for MVP/small production, but an
     external Vault cluster provides stronger isolation and disaster recovery.
5. `cloud-kms`
   - envelope encryption is enabled;
   - AWS KMS, GCP Cloud KMS, Azure Key Vault/Managed HSM, or a compatible cloud
     KMS wraps/unwraps DEKs;
   - preferred for cloud-hosted deployments that already rely on a cloud control
     plane.

A generated `local-file` KEK stored beside the data directory is not a preferred
full encryption-at-rest mode because compromise of the data directory is likely
to expose both ciphertext and KEK. Full user-data encryption should default to
`disabled` for development rather than pretending a colocated generated key is a
strong at-rest boundary.

Provider interface:

```go
type KeyProvider interface {
    ProviderID() string
    ActiveKeyID(ctx context.Context) (string, error)
    WrapKey(ctx context.Context, keyID string, plaintextDEK []byte, aad []byte) (WrappedKey, error)
    UnwrapKey(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error)
}
```

Vault/KMS providers must not be used on the per-record read/write hot path. They
are used at startup, partition/store open, key creation, and rotation boundaries
to wrap or unwrap DEKs. The daemon performs bulk data encryption/decryption
locally with AES-256-GCM after caching unwrapped DEKs in process memory.

The implementation should keep an in-memory DEK cache keyed by DEK ID and
wrapped-key version. Cache entries must be cleared on daemon shutdown and when
key-provider configuration changes. Operators should be able to bound cache
lifetimes, but cache expiry must not cause KMS/Vault calls for every graph/blob
record.

### DEK scopes

Use separate DEKs by data class and ownership boundary so key rotation and
future selective re-encryption are tractable.

Recommended initial scopes:

| Scope | Example DEK owner | Notes |
| --- | --- | --- |
| `system` | cluster/global metadata | For metadata that is not space/domain-owned but may contain sensitive values. |
| `space-domain` | `(space_id, domain_id, data_class)` | Default for graph, blob metadata, search, semantic, automation data. |
| `wal` | `(partition_id, segment_id)` or `(partition_id, epoch)` | Keeps WAL segment encryption independent from logical data stores. |
| `raft` | `(partition_id, raft_group, snapshot/log_segment)` | Keeps consensus artifacts independently rotatable. |
| `blob-object` | `(space_id, domain_id, blob_id)` or domain DEK | Per-object DEK maximizes isolation; domain DEK reduces KMS overhead. Start with per-object for object-store payloads if feasible. |
| `backup` | `(backup_id)` | Backups should be independently restorable with their own manifest-wrapped DEKs. |

The first implementation may use fewer scopes if clearly documented, but it
must not use one global DEK for all user data.

## Encrypted artifact format

Every encrypted artifact should carry enough metadata to decrypt and verify it
without external side channels other than access to the key provider.

Common envelope header:

```json
{
  "magic": "MYCELENC",
  "version": 1,
  "algorithm": "AES-256-GCM",
  "key_provider": "static-env|static-file|vault-transit|cloud-kms",
  "kek_key_id": "...",
  "dek_id": "uuid",
  "wrapped_dek": "base64",
  "nonce": "base64",
  "aad": {
    "data_class": "graph-node-segment|wal-record|raft-snapshot|blob-object|...",
    "space_id": "...",
    "domain_id": "...",
    "partition_id": "...",
    "object_id": "...",
    "format_version": 1
  },
  "ciphertext": "..."
}
```

Binary stores should use a compact binary form of the same fields rather than
JSON. Large files and object-store payloads should use a streaming/chunked form:

```text
file header: magic, version, algorithm, DEK metadata, chunk size, AAD base
chunk N: nonce or nonce suffix, ciphertext, GCM tag
```

AAD must include stable identifiers that prevent ciphertext from being copied to
another space/domain/data class and accepted as valid. AAD itself is not secret;
only include values that are acceptable as plaintext metadata.

## What must be encrypted

Principle:

> If user data can be reconstructed from it, encrypt it at rest.

### Graph and user content

Encrypt:

- node records and node payload/properties/meta;
- edge records and edge payload/properties/meta;
- graph segment files;
- graph recovery/replay artifacts;
- any domain-scoped graph state that includes user-defined labels, values, or
  schema-derived content.

### Blob payloads and metadata

Encrypt:

- local blob payload files;
- object-store blob payloads before upload;
- temporary/staging files used for blob upload/download/copy;
- blob metadata if it includes user filenames, MIME declarations, or
  domain-scoped ownership metadata considered sensitive.

Object-store SSE-KMS remains optional defense-in-depth. Mycel-side client-side
object encryption is the primary application-level guarantee.

### WAL, Raft, replication, and recovery

Encrypt:

- WAL records;
- Raft log entries;
- Raft snapshots;
- partition recovery/replay state;
- replication/fetch queues when they persist user-derived payloads;
- durable command-dedupe payloads if they include user data.

This is the highest-priority storage class because WAL/Raft can otherwise leak
plaintext even when primary data stores are encrypted.

### Search and semantic artifacts

Encrypt:

- lexical index segments, terms, postings, and compaction outputs;
- semantic/vector index files;
- embedding records and vector stores;
- semantic dirty queues, checkpoints, and work records when they contain
  user-derived IDs, hashes, text snippets, prompts, or node references;
- hybrid-search or future ranking caches that include user-derived signals.

### Identity, access, and sensitive metadata

Encrypt or separately classify:

- usernames, emails, and display names if deployments consider them sensitive;
- ACL subject/member lists if they reveal tenant or user relationships;
- authentication refresh token state and any persisted credential material;
- inference provider API keys and external service secrets.

The existing inline secret encryption should be replaced by the envelope system
using the configured `MYCELD_ENCRYPTION_*` key provider. Support for
`MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is not required for this change. This
encryption-at-rest work targets brand-new encrypted systems, not migration of
previous installations.

### Backups, exports, and diagnostics

Encrypt:

- system backup archives;
- Raft/system snapshots included in backups;
- space/domain exports when they include user content;
- generated diagnostics that include persisted user data.

Diagnostics should also continue to redact secrets and avoid logging user
content whenever possible.

## What may remain plaintext

Keep plaintext limited to operational metadata needed to find, route, and unlock
ciphertext:

- encryption format version and algorithm;
- key provider ID, KEK key ID, DEK ID, wrapped DEK;
- nonce or chunk nonce metadata;
- ciphertext length and ciphertext checksum;
- cluster/node IDs, partition IDs, Raft membership, and non-sensitive topology;
- non-sensitive daemon configuration.

Plaintext filenames and object keys should not include user-provided names. IDs
should be opaque UUIDs or content hashes, not user-derived strings.

## Configuration

Introduce explicit encryption-at-rest configuration:

```text
MYCELD_ENCRYPTION_AT_REST=disabled|enabled
MYCELD_ENCRYPTION_KEK_PROVIDER=static-env|static-file|vault-transit|cloud-kms
MYCELD_ENCRYPTION_STATIC_KEY_B64=<base64-32-byte-key>
MYCELD_ENCRYPTION_STATIC_KEY_FILE=<path-to-base64-32-byte-key>
MYCELD_ENCRYPTION_VAULT_ADDR=<https://vault.example:8200>
MYCELD_ENCRYPTION_VAULT_TRANSIT_KEY=<transit-key-name>
MYCELD_ENCRYPTION_VAULT_NAMESPACE=<optional-vault-namespace>
MYCELD_ENCRYPTION_VAULT_AUTH_METHOD=token|kubernetes|approle
MYCELD_ENCRYPTION_CLOUD_KMS_PROVIDER=aws|gcp|azure
MYCELD_ENCRYPTION_CLOUD_KMS_KEY_ID=<provider-key-id>
MYCELD_ENCRYPTION_CLOUD_KMS_ENDPOINT=<optional-endpoint>
MYCELD_ENCRYPTION_DEK_CACHE_TTL=5m
```

Replacement rules:

- `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is superseded and should be removed.
  Support for this setting is not required for the encryption-at-rest
  implementation.
- New deployments must use `MYCELD_ENCRYPTION_*` settings instead of
  `MYCELD_USER_STORE_ENCRYPTION_KEY_B64`.
- If `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is present after removal, startup
  should reject it with a clear error rather than silently treating it as a full
  encryption-at-rest key.
- Cluster deployments must use a shared key provider or shared KMS/Vault access
  for every node that can own/read encrypted partitions.
- Cluster/mesh deployments must not silently auto-generate incompatible per-node
  KEKs.

Recommended deployment configuration:

| Deployment | Recommended mode |
| --- | --- |
| Local development/tests/lab | `MYCELD_ENCRYPTION_AT_REST=disabled` |
| Tiny self-managed install | `enabled` + `static-env` |
| Simple K3s/Kubernetes install | `enabled` + `static-file` from a mounted Secret; enable K3s `--secrets-encryption` |
| Production self-managed K3s/on-prem | `enabled` + `vault-transit` |
| Cloud-hosted production | `enabled` + `cloud-kms` |

If encryption is enabled but the daemon cannot unlock required DEKs, startup or
partition activation must fail closed.

## Startup and readiness

At startup, the daemon should:

1. load encryption configuration;
2. if encryption is disabled, log and expose a clear non-secret warning that
   user data is stored unencrypted;
3. if encryption is enabled, initialize the configured key provider;
4. run a provider health/unlock check;
5. load encryption metadata for stores that are opened during startup;
6. refuse writes if encryption is required but unavailable;
7. surface encryption state in readiness/health/admin diagnostics without
   exposing key material.

Cluster readiness should fail when a node cannot unwrap DEKs for partitions it
owns.

## Backup and restore

Backups must include:

- encryption manifest version;
- key provider identity and KEK key IDs;
- wrapped backup DEKs and any wrapped store DEKs needed for restore;
- encrypted data artifacts;
- explicit statement of whether restore requires external KMS access or a
  separately supplied key bundle.

Backups must not include plaintext KEKs or plaintext DEKs.

Restore must validate that required keys are available before publishing restored
metadata or joining a restored node to a cluster.

## Rotation

Support two kinds of rotation:

1. **KEK rewrap**
   - unwrap existing DEKs with the old KEK;
   - wrap the same DEKs with the new KEK;
   - update metadata atomically;
   - does not rewrite encrypted user data.
2. **DEK rotation / data re-encryption**
   - generate new DEKs;
   - rewrite encrypted artifacts gradually by data class/scope;
   - expose progress and resumability;
   - support offline maintenance first, then online background rotation where
     safe.

KEK rewrap is required for the first production-ready release. DEK rotation may
be phased by storage class.

## New deployment boundary

Encryption-at-rest is a storage-format change for brand-new encrypted systems.
The first implementation does not need to convert previous plaintext data
directories, WAL/Raft artifacts, blob stores, derived indexes, or inline
daemon-managed secrets.

Supported initial deployment modes:

- `disabled`: plaintext development/test/lab behavior.
- `enabled`: encrypted-at-rest behavior for freshly initialized systems.

If encryption is enabled against a data directory that already contains
plaintext user-data artifacts, startup should fail closed with a clear error.
Operators should create a new encrypted deployment and import/recreate data
through explicit product-level workflows rather than relying on in-place storage
conversion.

## Observability and verification

Expose non-secret encryption status:

- enabled/disabled;
- provider type;
- active KEK key ID;
- number of wrapped DEKs by data class;
- whether any plaintext artifacts are detected in an encrypted deployment;
- last key-provider error;
- per-store encryption readiness.

Tests must include known-plaintext scans that write distinctive user content and
verify it is absent from persisted artifacts in:

- graph segment files;
- WAL records;
- Raft logs and snapshots;
- blob local/object-store payloads;
- lexical indexes;
- semantic/vector stores;
- backups and exports;
- temporary/staging directories.

Known-plaintext tests must avoid false positives from legitimate API responses
or test logs.

## Open questions

- Should new production deployments default to encryption enabled or remain
  opt-in for the first release?
- Should object-store blobs use per-object DEKs from day one, or domain-scoped
  DEKs with object-level AAD?
- Should `vault-transit` or a cloud KMS provider be implemented first after
  `static-env`/`static-file`?
- Should identity/ACL metadata be encrypted in the first tranche or classified
  as phase-two privacy-sensitive metadata?
- What admin API surface should expose encryption status and rotation controls?
- What is the supported rollback story after writes occur in encrypted format?

## Related implementation plan

See [Encryption at rest implementation plan](../../implementation/unreleased/encryption-at-rest-implementation-plan.md).
