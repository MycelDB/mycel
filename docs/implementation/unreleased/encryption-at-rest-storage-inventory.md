# Encryption at rest storage inventory

Status: initial EAR0 inventory for [mycel#56](https://github.com/MycelDB/mycel/issues/56).

This inventory classifies persistent and semi-persistent Mycel artifacts for the
new-system encryption-at-rest implementation. The first implementation targets
fresh encrypted deployments and does not require in-place conversion of previous
plaintext data directories.

## Classification legend

- **Must encrypt**: contains user data or user-derived artifacts that can reveal
  user data.
- **Operational plaintext**: may remain plaintext because it is needed for
  routing, discovery, unlocking, or safe startup and should not contain user
  content.
- **No user data**: operational artifact with no expected user content.
- **Phase later**: should be encrypted before broad production claims, but is not
  needed for EAR1-EAR3.

## Inventory

| Area | Example paths/components | Classification | Notes |
| --- | --- | --- | --- |
| Encryption marker/status | `DATA_DIR/meta/encryption/*` | Operational plaintext | May store mode, provider ID, format versions, and non-secret status. Must not contain plaintext KEKs/DEKs. |
| WAL segments | `DATA_DIR/wal/*.wal` | Must encrypt | EAR3 encrypts WAL record payloads with AEAD; frame LSN/type/schema metadata remains plaintext for recovery. |
| WAL progress/checkpoint | `DATA_DIR/meta/wal/progress.json`, `checkpoint.json` | Operational plaintext initially | Tracks applied LSNs/checkpoints. It should not contain user payloads. Revisit if checkpoint values grow user-derived metadata. |
| Raft persistent storage | `DATA_DIR/meta/raft/<group>/{entries,snapshot,hard_state,conf_state}.pb` | Must encrypt | EAR3 encrypts persistent files. This protects raft log entries and snapshots that may embed user data. |
| Cluster local state | `DATA_DIR/meta/clustering/*` | Operational plaintext | Cluster/node IDs and readiness state may stay plaintext unless future privacy requirements change. |
| Graph segments | graph storage files under data dir | Must encrypt | EAR4. Graph nodes, edges, labels, properties, payloads, and metadata are user data. |
| Blob local payloads | blob payload/staging directories | Must encrypt | EAR4. Temp/staging paths must not create plaintext escape hatches. |
| Object-store blob payloads | S3-compatible object payload bytes | Must encrypt | EAR4. Object keys remain domain-scoped and opaque; payload bytes are client-side encrypted before upload. |
| Blob metadata | blob metadata stores/WAL/raft records | Must encrypt when user-derived | EAR4/EAR3. Metadata in WAL/Raft is covered by EAR3; primary stores follow EAR4. |
| Backups/snapshots/exports | backup archives, system snapshots, exports | Must encrypt | EAR5. Backup manifests may keep non-secret key IDs/wrapped DEK metadata plaintext. |
| Lexical indexes | lexical term/posting/segment stores | Must encrypt | EAR6. Terms and postings can reconstruct user text. |
| Semantic/vector stores | embeddings, vector index files, dirty queues | Must encrypt | EAR6. Embeddings and source references are user-derived. |
| Automation artifacts | persisted execution inputs/outputs/state | Must encrypt when user-derived | EAR6. Prompt/input/output material may contain user data. |
| Inference credentials | daemon-managed API-key credentials | Must encrypt | EAR7 replaces the old inline-secret key path with the envelope provider. |
| Activity/audit events | activity store, diagnostics if persisted | Phase later / classify per field | Encrypt if events include user content, prompts, payload snippets, or sensitive principal data. |
| Logs | `DATA_DIR/log/myceld.log` | No user data by policy | Logs should avoid user payloads and secrets. If known-plaintext scans find user data, fix logging paths separately. |
| TLS material | configured cert/key files | Outside Mycel data scope | Managed by deployment. Do not copy into diagnostics. |

## EAR1-EAR3 implementation boundary

EAR1-EAR3 cover:

- `internal/encryption` shared envelope helpers and static providers;
- daemon config/startup wiring for `MYCELD_ENCRYPTION_*`;
- WAL record payload encryption;
- Raft persistent storage file encryption.

EAR1-EAR3 do not yet encrypt graph primary stores, blob payload stores,
backups, exports, lexical indexes, semantic/vector stores, or automation
artifacts. Those remain in later milestones before encryption-at-rest is complete
for user data.
