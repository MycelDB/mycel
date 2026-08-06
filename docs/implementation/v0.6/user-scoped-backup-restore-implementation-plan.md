# User-Scoped Backup and Restore Implementation Plan

## Status

Proposed.

This plan adds an explicit operator-controlled export/import path for recovering one user’s data from an authoritative `myceld` node and restoring it into a clean cluster or a selected target user. It is intended for cases such as split-brain recovery where an operator has pinned traffic to a known-good pod and wants to migrate user data without trusting divergent peer PVCs.

This is **not** automatic raft/PVC repair. It does not choose an authoritative node, merge divergent histories, overwrite peer PVCs, or repair a live split-brain cluster in place.

## Goals

- Export all durable, user-owned Mycel data for one user from a selected daemon endpoint.
- Restore that export into a fresh or existing cluster with explicit conflict policy.
- Support dry-run validation before writes.
- Preserve IDs by default for clean-cluster recovery.
- Support optional user remapping when restoring into a different user identity.
- Include integrity metadata, checksums, source endpoint metadata, and version information.
- Keep every phase functional and safe to deploy.

## Non-goals

- No automatic divergent PVC repair.
- No automatic merge/rebalance/overwrite of raft groups.
- No background self-healing based on these backups.
- No plaintext password export.
- No export of active auth sessions/refresh tokens.
- No generated public SDK/API code committed unless explicitly approved.

## Terminology

- **Authoritative source**: the operator-selected daemon endpoint/pod whose data is trusted for export.
- **User archive**: portable user-scoped backup artifact produced by export.
- **Preserve IDs**: restore user, spaces, domains, graph nodes/edges, blobs, schemas, and ACL references with their original IDs where possible.
- **Remap user**: restore data owned by source user under a different target user ID while preserving or remapping internal references.
- **Subsystem**: Mycel durable owner area such as identity, space, schema, graph, blob, semantic, backup config, etc.

## Operator workflows

### Split-brain recovery to a fresh cluster

```sh
# Against the pinned known-good pod.
mycel admin user-export \
  --daemon-addr <good-pod>:9091 \
  -u admin -p '<password>' \
  --user alice \
  --out alice.mycel-user-backup.tar.zst

# Against a fresh v0.5+ cluster.
mycel admin user-import \
  --daemon-addr <fresh-cluster>:9091 \
  -u admin -p '<password>' \
  --in alice.mycel-user-backup.tar.zst \
  --mode preserve-ids \
  --dry-run

mycel admin user-import \
  --daemon-addr <fresh-cluster>:9091 \
  -u admin -p '<password>' \
  --in alice.mycel-user-backup.tar.zst \
  --mode preserve-ids
```

### Restore into another user

```sh
mycel admin user-import \
  --daemon-addr <target>:9091 \
  -u admin -p '<password>' \
  --in alice.mycel-user-backup.tar.zst \
  --mode remap-user \
  --target-user bob \
  --dry-run
```

## Archive format

Use a deterministic tar archive with optional zstd compression:

```text
mycel-user-backup-v1.tar.zst
├── manifest.json
├── identity/user.json
├── identity/acl.jsonl
├── spaces/spaces.jsonl
├── spaces/domains.jsonl
├── schemas/schemas.jsonl
├── graph/<space_id>/<domain_id>/nodes.jsonl
├── graph/<space_id>/<domain_id>/edges.jsonl
├── blobs/metadata.jsonl
├── blobs/payloads/<blob_id>
├── semantic/config.jsonl
├── semantic/metadata.jsonl
├── checksums/SHA256SUMS
└── warnings.jsonl
```

### Manifest V1 fields

```json
{
  "format": "mycel-user-backup-v1",
  "created_at": "RFC3339",
  "source": {
    "cluster_id": "...",
    "daemon_addr": "...",
    "node_id": "...",
    "raft_node_id": 1,
    "mycel_version": "v0.5.0"
  },
  "user": {
    "user_id": "...",
    "username": "alice"
  },
  "options": {
    "include_blob_payloads": true,
    "include_semantic_metadata": true,
    "quiesced": true
  },
  "counts": {
    "spaces": 1,
    "domains": 2,
    "nodes": 100,
    "edges": 120,
    "blobs": 5
  },
  "checksums": {
    "algorithm": "sha256",
    "file": "checksums/SHA256SUMS"
  }
}
```

## Data scope

### Included in V1

- User identity metadata required to recreate or map the user.
- Spaces owned by the user.
- Domains in those spaces.
- ACL grants involving the user and exported spaces/domains.
- Domain schemas for exported domains.
- Graph nodes and edges in exported domains.
- Blob metadata and, by default, blob payloads referenced by exported graph/blob records.
- Semantic metadata/configuration that is authoritative and user/space scoped.
- Warnings for omitted derived or non-authoritative state.

### Excluded in V1

- Plaintext passwords.
- Password hashes unless an explicit future preserve-identity mode is approved.
- Active sessions and refresh tokens.
- Running semantic maintenance jobs.
- Derived vector indexes; these should rebuild from restored authoritative metadata/content.
- Backup run history and local archive inventory.
- Cluster raft metadata, raft logs, snapshots, and peer/PVC state.

## API shape

Prefer Admin API because user export/import crosses identity, space, ACL, schema, graph, blob, and semantic subsystem boundaries.

Add to `mycel-api` in a new admin proto, for example:

```text
api/proto/mycel/admin/v1/user_backup.proto
```

Service:

```protobuf
service AdminUserBackupService {
  rpc ExportUser(ExportUserRequest) returns (stream ExportUserResponse);
  rpc ImportUser(stream ImportUserRequest) returns (ImportUserResponse);
  rpc ValidateUserBackup(stream ValidateUserBackupRequest) returns (ValidateUserBackupResponse);
}
```

High-level messages:

```protobuf
message ExportUserRequest {
  string user_id = 1;
  string username = 2;
  UserBackupFormat format = 3;
  UserExportOptions options = 4;
}

message ImportUserMetadata {
  UserBackupFormat format = 1;
  UserImportMode mode = 2;
  string target_user_id = 3;
  string target_username = 4;
  UserImportConflictPolicy conflict_policy = 5;
  bool dry_run = 6;
}
```

V1 transport can stream archive bytes as chunks to keep daemon internals independent from archive storage location. A future enhancement can expose structured records if needed.

## CLI shape

Add admin commands under the existing CLI admin surface:

```sh
mycel admin user-export --user <user-or-id> --out <path> [--include-blobs=true] [--no-quiesce]
mycel admin user-import --in <path> --mode preserve-ids|remap-user --target-user <user-or-id> --dry-run
mycel admin user-backup-validate --in <path>
```

Safety defaults:

- `user-import` defaults to `--dry-run` unless `--yes` is provided for mutating import.
- Conflict policy defaults to `fail`.
- Destructive replacement requires an explicit `--replace-owned-data --yes` pair and should be deferred beyond V1 unless clearly required.

## Internal architecture

Add a user backup subsystem package, for example:

```text
internal/userbackup/service
internal/userbackup/archive
```

Daemon API adapters remain in:

```text
internal/daemon/api/admin
```

The service should orchestrate existing subsystem managers instead of reaching into storage implementation details when possible.

Suggested interfaces:

```go
type UserBackupExporter interface {
    ExportUser(ctx context.Context, input ExportUserInput, w ArchiveWriter) (ExportSummary, error)
}

type UserBackupImporter interface {
    ValidateUserBackup(ctx context.Context, r ArchiveReader, options ImportOptions) (ValidationReport, error)
    ImportUser(ctx context.Context, r ArchiveReader, options ImportOptions) (ImportSummary, error)
}
```

Subsystem contributors can be introduced incrementally:

```go
type UserBackupContributor interface {
    UserBackupContributorName() string
    ExportUserRecords(ctx context.Context, scope UserScope, sink RecordSink) error
    ValidateUserRecords(ctx context.Context, source RecordSource, opts ImportOptions) error
    ImportUserRecords(ctx context.Context, source RecordSource, opts ImportOptions) error
}
```

## Consistency model

### Export

Default export should quiesce writes for a consistent logical archive:

1. Authenticate operator.
2. Resolve source user.
3. Acquire daemon quiesce lease, unless `--no-quiesce` is explicitly passed.
4. Collect user identity/space/schema/graph/blob/semantic data from the selected endpoint.
5. Record raft/read metadata and warnings in the manifest.
6. Release quiesce lease.

If quiesce is unavailable, export should fail by default. `--no-quiesce` may be allowed for emergency forensic export, but must mark the archive as potentially non-repeatable.

### Import

Import should be transactional where subsystem support exists, but cross-subsystem import cannot be assumed globally atomic in V1. Therefore:

- Always provide dry-run validation.
- Write an import plan before mutation.
- Apply in dependency order.
- Record completed phases in an import report.
- Fail closed on first error.
- Avoid partial destructive changes in V1.

Dependency order:

1. Target user resolution/creation.
2. Spaces.
3. Domains.
4. ACLs that require spaces/domains.
5. Schemas.
6. Blob payload staging.
7. Graph import through normal transaction/raft paths.
8. Blob metadata publish if not already covered by graph references.
9. Semantic authoritative metadata/configuration.
10. Post-import validation.

## Conflict policies

V1 policies:

- `fail`: fail if any target ID/name exists with incompatible data.
- `skip-existing`: import only missing records; report skipped records.
- `upsert-compatible`: update only records proven identical or compatible.

Defer destructive policies:

- `replace-owned-data`
- `overwrite-conflicts`
- `delete-missing`

These can be added later with explicit flags and stronger tests.

## Security and privacy

- Require admin/operator capability, e.g. `CAPABILITY_SYSTEM_BACKUP_SPACE` or a new `CAPABILITY_USER_BACKUP`.
- Never export plaintext passwords.
- Treat archives as sensitive; blob payloads and graph content may contain private user data.
- Include optional archive encryption in a later phase, or document external encryption for V1.
- Avoid logging record payloads.
- Redact tokens/secrets from manifest and warnings.

## Phase 0 — Design/API contract

### Tasks

1. Add `user_backup.proto` in `mycel-api` with additive Admin service definitions.
2. Define archive format enum and import/export option messages.
3. Define validation and summary report messages.
4. Document archive V1 in `mycel-api` overview docs.
5. Decide capability name.

### Tests

```sh
cd mycel-api && make test
```

### Acceptance

- Proto lint passes.
- No daemon behavior changed.

## Phase 1 — Archive library and dry-run validation skeleton

### Tasks

1. Add `internal/userbackup/archive` with deterministic tar/zstd reader/writer.
2. Implement `manifest.json` and `checksums/SHA256SUMS` read/write.
3. Add corruption detection tests.
4. Add `ValidateUserBackup` daemon service skeleton that validates format/checksums only.
5. Add CLI `admin user-backup-validate`.

### Tests

```sh
go test ./internal/userbackup/archive ./internal/daemon/api/admin ./internal/cli/cmd
```

### Acceptance

- Archive integrity validation works without importing data.
- Malformed archives fail closed with actionable errors.

## Phase 2 — Export V1 from a selected endpoint

### Tasks

1. Implement operator-authenticated `ExportUser` API adapter in `internal/daemon/api/admin`.
2. Implement user backup service export orchestration.
3. Export identity metadata without credentials.
4. Export owned spaces/domains/ACLs.
5. Export schemas.
6. Export graph nodes/edges using strong read paths.
7. Export blob metadata and payloads referenced by exported records.
8. Export semantic authoritative metadata/configuration where available.
9. Add CLI `admin user-export`.
10. Add manifest warnings for omitted derived state.

### Tests

- Unit tests for each contributor.
- Archive golden-shape tests.
- Single-node export integration test with user, space, domain, schema, graph, and blob.
- Raft-mode export from a non-leader endpoint should route reads correctly or fail closed.

### Acceptance

```sh
go test ./internal/userbackup/... ./internal/daemon/api/admin ./internal/cli/cmd
```

Exporting a known test user creates a valid archive with expected counts/checksums.

## Phase 3 — Import dry-run planner

### Tasks

1. Implement import archive parser into a dependency graph/import plan.
2. Resolve target mode: preserve IDs or remap user.
3. Detect ID/name conflicts before mutation.
4. Validate blob payload checksums.
5. Validate graph edge endpoint closure within archive/target.
6. Validate schema references.
7. Add CLI `admin user-import --dry-run`.

### Tests

- Dry-run succeeds for empty target.
- Dry-run detects existing conflicting user/space/domain IDs.
- Dry-run detects missing blob payloads/checksum mismatch.
- Dry-run detects edge endpoints missing from archive/target.

### Acceptance

Dry-run produces a deterministic report and performs no writes.

## Phase 4 — Import V1 into fresh cluster

### Tasks

1. Implement non-destructive preserve-ID import for empty/fresh target clusters.
2. Create/resolve target user without password export. Options:
   - require pre-created target user; or
   - create disabled user requiring password reset flow.
3. Restore spaces/domains/ACLs.
4. Restore schemas.
5. Stage blob payloads.
6. Import graph via normal session/transaction APIs or graph subsystem service methods that still use raft-owned writes.
7. Restore semantic authoritative metadata/configuration and schedule derived rebuilds.
8. Produce import summary and post-import validation.

### Tests

- Export user from source cluster, import into fresh single-node cluster, verify graph/blob/schema round trip.
- Repeat in raft-mode 3-node cluster.
- Verify restored graph reads succeed through any healthy node.
- Verify semantic derived rebuild markers are set when semantic data is restored.

### Acceptance

A fresh cluster can restore a complete user archive without manual storage copying.

## Phase 5 — Remap-user restore

### Tasks

1. Implement source-user to target-user ID mapping.
2. Remap ownership and ACL references.
3. Preserve space/domain/node/edge/blob IDs by default unless conflicts require fail.
4. Add optional username matching rules.
5. Extend dry-run and import reports with mapping tables.

### Tests

- Restore Alice archive into Bob.
- Verify Bob can access restored spaces/domains.
- Verify Alice-specific ACL references are remapped or reported.

### Acceptance

Remapped restore works without changing graph topology or blob references.

## Phase 6 — Operator hardening and docs

### Tasks

1. Add split-brain recovery runbook using pinned authoritative pod.
2. Document PVC quarantine warnings.
3. Add examples for two-user export/import.
4. Add archive privacy warning.
5. Add admin UI read-only display/download/import dry-run hooks only if desired.

### Tests

- CLI help snapshots or command tests.
- Manual runbook validation against local 3-node cluster.

### Acceptance

Operators can follow docs to export users from a pinned pod and import into a fresh cluster.

## Release gates

Before enabling this as recommended recovery tooling:

```sh
cd mycel-api && make test
cd mycel && make test
cd mycel && make test-compose-cluster
cd mycel && make test-k3s-cluster
```

Manual destructive recovery rehearsal:

1. Create 3-node cluster.
2. Create two users with distinct spaces, graph data, and blobs.
3. Export both users from one selected pod.
4. Create fresh 3-node cluster.
5. Import both users.
6. Verify user access, graph consistency, blob payload reads, and semantic rebuild behavior.

## Open questions

- Should V1 require the target user to pre-exist, or create a disabled user with reset-required metadata?
- Should archive encryption be built in for V1 or documented as external encryption?
- Which exact capability should gate user export/import?
- How much semantic metadata is authoritative enough for V1, and what should be rebuild-only?
- Should backup import invoke existing Client ImportExportService for graph data, or use graph subsystem methods directly under an admin service orchestration layer?
- Should preserve-ID import into a non-empty cluster be allowed only when dry-run proves there are no conflicts?
