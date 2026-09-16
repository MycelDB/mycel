# Encryption at rest implementation plan

Status: EAR0-EAR8 implementation complete for fresh encrypted deployments using the local static key providers; Vault Transit, cloud KMS, and full KEK rewrap tooling remain future provider/operations work. Tracks [mycel#56](https://github.com/MycelDB/mycel/issues/56).

This plan implements the [encryption at rest design](../../design/security/encryption-at-rest.md). The work is intentionally phased because encryption affects persistence, WAL, Raft, blobs, backups, indexes, semantic/vector stores, configuration, operations, and tests.

## Principles

- Fail closed when encryption is required but unavailable.
- Encrypt write-ahead/consensus/recovery paths before relying on encrypted primary stores.
- Treat object-store SSE-KMS and disk encryption as defense-in-depth, not as the application-level guarantee.
- Use envelope encryption and AEAD; do not use one global data key for all persisted bytes.
- Keep key material out of logs, diagnostics, JSON artifacts, and test output.
- Make every phase independently testable with known-plaintext scans.

## Milestones

| Milestone | Outcome |
| --- | --- |
| EAR0 | Complete: final design direction, storage inventory, and breaking-change decisions. |
| EAR1 | Complete: shared encryption/key-provider package with disabled/static-env/static-file modes and format tests. |
| EAR2 | Complete: daemon config/startup wiring, fresh-deployment preflight, and encryption status logging. |
| EAR3 | Complete: WAL payload encryption and persistent Raft storage file encryption. |
| EAR4 | Complete: graph store and blob local/object-store payload encryption. |
| EAR5 | Complete: backup archive encryption for fresh encrypted deployments. |
| EAR6 | Complete: lexical, semantic/vector, and inference derived-store encryption. |
| EAR7 | Complete: inline secrets use the envelope model; legacy user-store secret key path removed. |
| EAR8 | Complete for this implementation slice: fail-closed startup/config checks, ciphertext/no-plaintext regression tests, docs, and full test validation. |

## EAR0: Design and inventory

Goal: agree on scope and produce a storage map before implementation starts.

Tasks:

- Land the design doc and this implementation plan.
- Inventory all persistence paths and classify each as:
  - must encrypt;
  - privacy-sensitive, phase-two encryption;
  - safe plaintext operational metadata;
  - no user data.
- Inventory temporary/staging paths.
- Inventory test artifact paths that may capture plaintext.
- Decide first supported modes/providers:
  - required first modes: `disabled`, `static-env`, `static-file`;
  - production provider sequence: `vault-transit` for self-managed K3s/on-prem,
    then one or more `cloud-kms` providers for cloud-hosted deployments;
  - no generated colocated data-directory KEK for full user-data encryption by
    default; local development should use `disabled` unless explicitly testing
    encryption.
- Document K3s key-management guidance:
  - `static-file` from a mounted Kubernetes Secret is preferred over env vars
    for simple K3s deployments;
  - enable K3s `--secrets-encryption` when Kubernetes Secrets carry KEKs;
  - Vault Transit is a secrets engine inside Vault and is not installed as a
    standalone component;
  - in-cluster Vault is acceptable for MVP/small deployments, while external
    Vault provides stronger production isolation.
- Confirm the new-system boundary:
  - required initial modes: `disabled` for dev/test/lab and `enabled` for fresh
    encrypted systems;
  - no in-place conversion of previous plaintext data directories, WAL/Raft
    artifacts, blob stores, derived indexes, or inline secrets is required;
  - enabling encryption on a non-empty plaintext data directory should fail
    closed with clear operator guidance.
- Decide per-object vs per-domain DEKs for object-store blob payloads.
- Decide whether raw identity/ACL metadata is in first tranche.

Deliverables:

- `docs/design/security/encryption-at-rest.md`.
- `docs/implementation/unreleased/encryption-at-rest-implementation-plan.md`.
- [Encryption at rest storage inventory](encryption-at-rest-storage-inventory.md).

Validation:

- `python3 scripts/checkDocs.py docs README.md`.

## EAR1: Encryption package and key providers

Goal: create reusable cryptographic primitives independent of storage code.

Implementation sketch:

- Add `internal/encryption`.
- Define types:
  - `Config`;
  - `KeyProvider`;
  - `WrappedKey`;
  - `DEKScope`;
  - `EnvelopeHeader`;
  - `Encryptor` / `Decryptor`;
  - chunked stream reader/writer helpers.
- Implement modes/providers:
  - `disabled` mode that bypasses encryption intentionally and reports that
    user data is stored unencrypted;
  - `static-env` provider for an explicit base64-encoded 32-byte KEK;
  - `static-file` provider for a mounted file containing a base64-encoded
    32-byte KEK;
  - test fake provider with deterministic failure injection.
- Implement AES-256-GCM helpers:
  - record encryption;
  - chunked stream encryption;
  - AAD encoding/canonicalization;
  - header encode/decode;
  - DEK cache with TTL and shutdown clear.
- Add package-level errors:
  - unavailable provider;
  - invalid key material;
  - unsupported algorithm/version;
  - authentication failure;
  - missing DEK/wrapped key.

Tests:

- AES-GCM round trip.
- Wrong AAD fails.
- Wrong key fails.
- Header tampering fails.
- Unsupported version fails.
- Disabled mode bypasses encryption only when explicitly configured and reports
  non-secret warning/status.
- Static-env provider rejects non-base64 and wrong key sizes.
- Static-file provider rejects missing, world-readable when policy disallows,
  non-base64, and wrong-size key files.
- Chunked encryption handles empty, small, exact-boundary, and multi-chunk input.
- Nonce uniqueness test for many chunks/records.
- DEK cache eviction and close clears plaintext keys.

Acceptance:

- Storage-independent encryption library exists.
- No tests log plaintext keys or DEKs.
- `go test ./internal/encryption` passes.

## EAR2: Daemon configuration, startup, and status

Goal: make encryption a first-class daemon capability before stores use it.

Tasks:

- Add daemon config fields and env parsing:
  - `MYCELD_ENCRYPTION_AT_REST=disabled|enabled`;
  - `MYCELD_ENCRYPTION_KEK_PROVIDER=static-env|static-file|vault-transit|cloud-kms`;
  - `MYCELD_ENCRYPTION_STATIC_KEY_B64`;
  - `MYCELD_ENCRYPTION_STATIC_KEY_FILE`;
  - `MYCELD_ENCRYPTION_VAULT_*` placeholders for Vault Transit;
  - `MYCELD_ENCRYPTION_CLOUD_KMS_*` placeholders for cloud KMS;
  - `MYCELD_ENCRYPTION_DEK_CACHE_TTL`.
- Remove `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` from the encryption-at-rest
  configuration model; this change does not support that setting.
- Initialize `internal/encryption` in daemon app startup.
- Add runtime host accessors for encryption services.
- Add health/readiness status for encryption provider availability.
- Add admin diagnostics surface for non-secret encryption state.
- Ensure cluster/mesh mode does not auto-generate incompatible per-node KEKs.
- Ensure enabled encryption in cluster/mesh mode requires a shared provider or
  shared Vault/KMS access on every node that may own/read encrypted partitions.
- Update operations docs for config and startup behavior, including:
  - developer/test `disabled` mode;
  - small deployment `static-env` mode;
  - K3s `static-file` mode using mounted Secrets and K3s
    `--secrets-encryption`;
  - Vault Transit as the recommended self-managed production provider.

Tests:

- Config env parsing.
- Explicit disabled mode startup and status warning.
- Static-env and static-file key loading and reload behavior.
- Cluster mode fail-closed when encryption is enabled but provider unavailable
  or inconsistent across nodes.
- Startup fails closed when encryption is enabled on an existing plaintext data
  directory.
- Status output redacts key material.
- `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is not accepted as a replacement for
  the new `MYCELD_ENCRYPTION_*` settings.

Acceptance:

- Daemon can start with encryption disabled and clearly reports unencrypted
  at-rest state.
- Daemon can start with encryption enabled and a valid `static-env` or
  `static-file` provider.
- Daemon fails closed with encryption enabled and invalid/missing provider config.
- Readiness/admin status indicates encryption state without exposing secrets.

## EAR3: WAL and Raft encryption

Goal: protect write-ahead, consensus, and recovery artifacts before primary stores depend on encryption.

Tasks:

- Add optional encryption to WAL record payloads.
- Add WAL record envelope metadata or encrypted payload wrapper.
- Ensure WAL replay can decrypt records before dispatching appliers.
- Add encryption to Raft command payloads before they enter persistent Raft logs.
- Add encryption to Raft snapshots and snapshot restore paths.
- Bind AAD to record type, schema version, LSN/segment, partition ID, Raft group, and snapshot ID where applicable.
- Keep routing/partition metadata plaintext only when required for log operation.
- Ensure remote/follower apply paths can unwrap DEKs using shared provider config.
- Update system backup/restore tests to include encrypted WAL/Raft artifacts.

Tests:

- WAL known-plaintext scan.
- WAL replay after restart with encryption enabled.
- WAL wrong AAD/key fails.
- Raft three-node replication with encrypted commands.
- Raft leader failover with encrypted logs.
- Raft snapshot/restore with encrypted snapshot payloads.
- Backup/restore gate with encrypted WAL/Raft.

Acceptance:

- User content in WAL/Raft payloads is not visible as plaintext when encryption is enabled.
- Encrypted deployments reject plaintext WAL/Raft artifacts rather than trying
  to read or convert them in place.

## EAR4: Graph and blob storage encryption

Goal: encrypt primary user stores and object payloads.

### Graph storage

Tasks:

- Encrypt graph segment record payloads.
- Bind AAD to space ID, domain ID, segment ID, record kind, transaction ID, and entity ID.
- Decide whether graph segment headers remain plaintext for scanning/recovery.
- Add encrypted graph store format versioning.
- Add graph known-plaintext tests covering node content, properties, payload, meta, edge properties, and labels when labels are considered sensitive.

Tests:

- Create/query graph with encryption enabled.
- Restart and replay encrypted graph segments.
- Corrupt ciphertext/tag and verify read fails.
- Known-plaintext scan of graph data directory.

### Blob local store

Tasks:

- Encrypt local blob payload files before writing to disk.
- Use blob ID, space ID, domain ID, backend, and content digest as AAD.
- Ensure temp/staging files are encrypted or use private in-memory/temp handling.
- Preserve content-addressed integrity: blob ID remains SHA-256 of plaintext payload unless explicitly changed by design review.

Tests:

- Upload/open/delete local blob with encryption enabled.
- Known-plaintext scan of blob directories and staging directories.
- Corrupt ciphertext/tag fails on open.

### Object-store payloads

Tasks:

- Encrypt payload before S3-compatible `PutObject`.
- Store envelope metadata in object metadata, sidecar object, or payload header.
- Continue using domain-scoped object keys from issue #55.
- Treat S3 SSE-KMS as optional additional protection.
- Ensure object-store reads stream-decrypt and verify tags.

Tests:

- Fake S3 client receives ciphertext, not plaintext.
- Open object-store blob returns original plaintext through API.
- Object metadata/header is sufficient for decrypt.
- Wrong AAD/key fails.

Acceptance:

- Graph and blob user content is encrypted at rest.
- API behavior remains unchanged for authorized users.
- Object-store raw payload bytes are ciphertext.

## EAR5: Backup, restore, and export encryption

Goal: prevent backup/restore artifacts from becoming plaintext escape hatches.

Tasks:

- Add encryption manifest to system backups.
- Include wrapped backup DEKs and provider/key IDs.
- Encrypt backup archive entries that contain user data or user-derived data.
- Ensure restore validates key availability before publishing metadata.
- Decide domain export behavior:
  - encrypted exports by default when encryption at rest is enabled;
  - explicit plaintext export option only for authorized/interactive flows;
  - clear warning in CLI/API docs.
- Update backup/restore runbooks.

Tests:

- Backup archive known-plaintext scan.
- Restore with correct provider succeeds.
- Restore without key provider fails before publishing restored state.
- Restore with wrong key fails authentication.
- K3s/compose backup gates with encryption enabled.

Acceptance:

- Backups generated with encryption enabled do not contain plaintext user data.
- Restore path is fail-closed when keys are unavailable.

## EAR6: Derived store encryption

Goal: encrypt indexes and derived artifacts that can leak user content.

Storage classes:

- lexical index storage;
- lexical compaction output;
- semantic/vector store files;
- embedding records;
- semantic dirty queues/work state/checkpoints;
- automation execution state and rendered inputs/outputs where user-derived;
- activity/audit events if they include sensitive user data.

Tasks:

- Add encryption adapters to each store or to common file-store primitives.
- Bind AAD to space/domain/index IDs and data class.
- Ensure compaction writes encrypted outputs atomically.
- Ensure derived stores can be rebuilt if encrypted derived-store validation
  fails.

Tests:

- Known-plaintext scan for lexical terms/postings.
- Known-plaintext scan for semantic source text/embedding records.
- Search/semantic APIs still return expected results.
- Compaction/rebuild with encryption enabled.

Acceptance:

- User-derived search/semantic artifacts are not plaintext on disk.

## EAR7: Inline secret replacement

Goal: replace the existing daemon-managed inline secret encryption path with the
envelope subsystem.

Tasks:

- Store new secrets using `internal/encryption` and the configured key provider.
- Update inference credential creation/rotation to use envelope encryption.
- Remove `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` from documented configuration
  and startup paths.
- Reject `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` if it is configured after the
  removal release, with an error that explains `MYCELD_ENCRYPTION_*` must be
  used instead.
- Document the breaking change and new-system boundary: previous installations
  are not converted in place; fresh encrypted systems should create inference/API
  credentials under the new envelope provider.

Tests:

- New inline secret decrypts with envelope provider.
- Wrong key/provider fails closed.
- Startup rejects `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` once the old path is
  removed.
- Docs and examples no longer recommend the old setting.

Acceptance:

- New secrets use the same key-provider architecture as encryption at rest.
- The old inline-secret key path is removed rather than maintained as an
  alternate decryptor.

## EAR8: Validation, rotation, and hardening

Goal: make encryption operationally safe for fresh encrypted deployments.

Implementation status: startup/config fail-closed behavior and ciphertext/no-plaintext regression tests are implemented for the fresh-deployment static-provider slice. Full online/offline KEK rewrap tooling is deferred with the unimplemented Vault Transit and cloud KMS providers.

Tasks:

- Add plaintext inventory/scan tool:
  - per data class;
  - reports candidate plaintext artifacts in encrypted deployments;
  - never prints matched plaintext snippets by default.
- Add startup validation that rejects plaintext user-data artifacts when
  encryption is enabled.
- Future provider work: add a KEK rewrap command:
  - validates old/new provider access;
  - supports changing from `static-env`/`static-file` to `vault-transit` or
    `cloud-kms` by rewrapping DEKs without rewriting user data;
  - atomically updates wrapped DEKs;
  - emits non-secret progress.
- Add optional DEK rotation for at least blob payloads and backups.
- Add release-gate scenario with encryption enabled on a fresh data directory.
- Update docs:
  - operations startup config;
  - fresh encrypted deployment requirements;
  - backup/restore key requirements;
  - key rotation;
  - disaster recovery;
  - validation scans.

Tests:

- Plaintext scan finds seeded plaintext fixture in an encrypted deployment.
- Startup validation rejects plaintext fixture when encryption is enabled.
- KEK rewrap changes wrapped-key metadata without rewriting ciphertext.
- Release gate with encryption enabled passes on a fresh data directory.

Acceptance:

- Operators can enable, validate, rewrap, rotate, and recover fresh encrypted deployments.

## Cross-repo impact

Likely daemon-only initially, with follow-up changes in other repos when public
surfaces are added:

- `mycel-api`: admin encryption status/rotation APIs if exposed over gRPC.
- `mycel-go-sdk`: generated bindings and helpers for admin encryption APIs.
- `mycel-rust-sdk`: generated bindings and helpers for admin encryption APIs.
- `mycel-console`: encryption status/rotation UI if exposed.
- `mycel-lab`: reliability scenarios with encryption enabled.

## Validation checklist before implementation PRs merge

Every implementation PR should state which of these were run:

- `go test ./...`
- `make test`
- `python3 scripts/checkDocs.py docs README.md`
- targeted known-plaintext scan tests for affected stores
- raft/cluster tests when WAL/Raft/storage paths are affected
- backup/restore tests when backup artifacts or restore behavior change

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Data loss from bad key configuration | Fail closed before writes; backup key-provider validation; restore preflight. |
| Nonce reuse | Centralize AEAD helpers; unit-test nonce uniqueness; avoid caller-supplied nonces except through safe APIs. |
| Plaintext copies in WAL/Raft/backup remain | Encrypt WAL/Raft first; known-plaintext scans across all artifact roots. |
| Performance regression | DEK cache, chunked streaming, benchmarks for graph/blob/index paths. |
| Irrecoverable backups | Backup manifests include wrapped DEKs and key IDs; restore preflight documents required external KMS/key access. |
| Logs leak user data | Redaction and known-plaintext tests for artifacts; separate logging cleanup issues if needed. |
| Multi-node clusters use inconsistent static keys | Cluster mode requires explicit shared provider or shared Vault/KMS access; readiness fails if unwrap fails. |

## Review questions

- Should encryption be opt-in for the first release or default for new deployments?
- Should `vault-transit` or a cloud KMS provider be implemented first after `static-env`/`static-file`?
- Should object-store blobs use per-object DEKs immediately?
- Which admin API/status surfaces are required for the first implementation?
- Should identity/ACL metadata be included in EAR4/EAR6 or deferred to privacy-sensitive phase two?
- What rollback support is required after encrypted writes begin?
