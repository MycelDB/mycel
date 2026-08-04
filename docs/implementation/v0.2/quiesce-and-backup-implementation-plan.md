# Quiesce and Backup Implementation Plan

## Status

Proposed.

This plan implements the behavior described in [Quiesce and Backup Design](../../design/backup-restore/quiesce-and-backup.md). It depends on the cross-cutting daemon lifecycle model described in [Daemon Service Interfaces Design](../../design/runtime/daemon-service-interfaces.md) and should be coordinated with [Daemon Service Interfaces Implementation Plan](../v0.3/daemon-service-interfaces-implementation-plan.md). Work is split into phases so the quiesce foundation can be validated before adding scheduled backups.

## Acceptance criteria

- `myceld` can quiesce daemon services for backup without logging users out.
- New non-exempt RPCs, including reads unless explicitly exempted/proven safe, are rejected with `codes.Unavailable` while quiesced.
- Active admitted work drains before backup snapshot copy starts.
- Semantic background maintenance does not write during backup snapshot copy.
- Operators can configure backup policy through Admin APIs.
- Operators can trigger, list, inspect, and delete backups through Admin APIs and CLI.
- Scheduled backups run when enabled and apply retention.
- Unit tests cover quiesce gates, coordinator rollback, service integration, backup archive creation, scheduler decisions, and retention.
- Documentation is updated after implementation.

## Phase 0: API and storage decisions

### Goals

Define the external contract and internal persistence layout before implementation.

### Tasks

1. Add `api/proto/mycel/admin/v1/backup.proto` to `mycel-api`.
2. Define messages for:
   - `BackupPolicy`
   - `BackupStatus`
   - `BackupSummary`
   - `QuiesceStatus`
   - `QuiesceParticipantStatus`
   - `TriggerBackupRequest/Response`
   - `ListBackupsRequest/Response`
   - `UpdateBackupPolicyRequest/Response`
3. Generate Go stubs in `mycel-api`.
4. Vendor/sync generated API into `myceldb/mycel` according to the existing API workflow.
5. Define default policy values and daemon startup environment variables:

```text
MYCELD_BACKUP_ENABLED=false
MYCELD_BACKUP_DIR=
MYCELD_BACKUP_INTERVAL=24h
MYCELD_BACKUP_RETENTION_COUNT=7
MYCELD_BACKUP_INCLUDE_LOGS=false
MYCELD_BACKUP_COMPRESSION=zip  # legacy archive-format seed; maps to archive_format
MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT=2m
MYCELD_BACKUP_TIMEOUT=30m
MYCELD_BACKUP_RETRY_AFTER=5s
MYCELD_BACKUP_STATUS_HISTORY_LIMIT=20
MYCELD_BACKUP_ALLOW_READS_DURING_BACKUP=false
```

6. Decide daemon-local persistence path for backup policy and run history, for example:

```text
meta/backup/policy.json
meta/backup/runs.jsonl
```

### Tests

- `go test ./...` in `mycel-api`.
- Compile generated code in `myceldb/mycel`.

### Acceptance

- API compiles.
- No implementation behavior yet.

## Phase 1: Quiesce core package

Status: implemented. `internal/daemon/quiesce` now provides `Gate`, `Coordinator`, participant/lease/status DTOs, ordered quiesce orchestration, reverse release, rollback on participant failure, and `ErrQuiesced` to `codes.Unavailable` mapping.

### Goals

Implement reusable gate/coordinator primitives independent of backup.

### Files

```text
internal/daemon/quiesce/gate.go
internal/daemon/quiesce/coordinator.go
internal/daemon/quiesce/status.go
internal/daemon/quiesce/gate_test.go
internal/daemon/quiesce/coordinator_test.go
```

### Tasks

1. Implement `Gate.Enter(ctx)`.
2. Implement `Gate.Quiesce(ctx, req)` and lease release.
3. Implement `Coordinator.Register`.
4. Implement `Coordinator.QuiesceAll` with ordered acquisition and reverse release.
5. Implement rollback if one participant fails.
6. Add status snapshots.
7. Map `ErrQuiesced` to `codes.Unavailable` through helper functions.

### Unit tests

- `Enter` increments active count and release decrements it.
- `Quiesce` waits for active entrants to drain.
- `Enter` fails while quiesced.
- Lease release reopens the gate.
- Context cancellation while waiting for drain returns an error and reopens appropriately.
- Coordinator quiesces participants in registration order.
- Coordinator releases in reverse order.
- Coordinator rolls back already-acquired leases when a later participant fails.
- Status reports active count, reason, since timestamp, and quiesced state.

### Acceptance

```sh
go test ./internal/daemon/quiesce
```

## Phase 2: Runtime and gRPC ingress integration

Status: implemented. Daemon server startup now creates/registers an `api-ingress` gate with the runtime quiesce coordinator and wraps unary/stream RPCs with quiesce interceptors. The ingress gate is registered first so daemon-wide quiesce stops new RPCs before lower-level service gates close. Non-exempt calls enter/release the gate and receive `codes.Unavailable` while quiesced; exempt methods bypass the gate.

### Goals

Add daemon-wide quiesce coordination and a global ingress safety net.

### Files

```text
internal/daemon/runtime/runtime.go
internal/daemon/app/app.go
internal/daemon/server/server.go
internal/daemon/server/server_test.go
```

### Tasks

1. Add `Quiesce *quiesce.Coordinator` to `Runtime`.
2. Instantiate coordinator during daemon app initialization before modules initialize.
3. Add unary and stream gRPC interceptors that call an ingress gate.
4. Exempt only backup/quiesce control/status methods.
5. Return `codes.Unavailable` when requests are rejected due to quiesce.
6. Register the ingress gate with the coordinator as `api-ingress` or keep it as a separate pre-coordinator gate that backup closes first.

### Unit tests

- Non-exempt unary RPC enters/releases the gate.
- Non-exempt RPC returns `codes.Unavailable` while quiesced.
- Exempt backup/status methods bypass the ingress gate.
- Stream interceptor behavior mirrors unary behavior.

### Acceptance

```sh
go test ./internal/daemon/server ./internal/daemon/app ./internal/daemon/runtime
```

## Phase 3: Service-level quiesce gates

Status: implemented. Graph, blob, semantic, space, user, admin, session, and change-stream modules now own service-level gates, register deterministic participants during init, and wrap their mutating/durable write entrypoints. Template catalog writes are gated through the space module, which owns the template manager. Semantic maintenance loops, manual maintenance/backfill operations, and Admin semantic/inference metadata mutations enter the semantic gate before starting work.

### Goals

Register quiesce participants for modules that own durable writes.

### Files and services

```text
internal/daemon/modules/graph/module.go
internal/daemon/modules/blob/module.go
internal/daemon/modules/semantic/module.go
internal/daemon/modules/space/module.go
internal/daemon/modules/user/module.go
internal/daemon/modules/admin/module.go
internal/daemon/modules/session/module.go
internal/daemon/modules/changestream/module.go
```

Template writes may be gated from the graph or space module depending on the current owner of the template manager.

### Tasks

1. Add a gate to graph module and wrap durable commit paths.
2. Add a gate to blob module and wrap upload/delete/metadata write paths.
3. Add a custom semantic participant:
   - pause analyzer/worker scheduling
   - gate manual maintenance/backfill operations
   - wait for active semantic work
   - resume on release
4. Add gates to space/domain/ACL write paths.
5. Add gates to identity/user/admin/session persistence write paths.
6. Add gate coverage for template catalog writes.
7. Ensure module init registers participants with deterministic names.
8. Ensure errors are returned as transient `codes.Unavailable` at API boundaries.

### Unit tests

- Graph commit returns unavailable when graph gate is quiesced.
- Active graph commit blocks quiesce until it finishes.
- Blob upload/delete returns unavailable when blob gate is quiesced.
- Space create/delete and domain mutations return unavailable when space gate is quiesced.
- User/admin/session persistence writes return unavailable when identity/admin gates are quiesced.
- Template writes return unavailable when template gate is quiesced.
- Semantic manual analyze/process/backfill does not start while semantic is quiesced.
- Semantic scheduler does not start new analyzer/worker runs after quiesce begins.
- Existing semantic work drains before semantic participant returns its lease.

### Acceptance

Implemented validation:

```sh
go test ./internal/daemon/modules/... ./internal/daemon/quiesce ./internal/daemon/server
go test ./...
git diff --check
```

## Phase 4: Backup manager and snapshot archive

Status: implemented. `internal/backup.Manager` now supports manual trigger with single-flight protection, backup directory validation with symlink resolution, daemon-wide quiesce acquisition/release, data-dir staging outside the data dir, log include/exclude policy, symlink skipping, archive creation through `.tmp` for `zip`, `tar`, `tar.gz`, and `tar.zst`, sidecar manifest creation with archive size/checksum, and success/failure run status recording.

### Goals

Implement manual, quiesced backup archive creation.

### Files

```text
internal/backup/manager.go
internal/backup/policy.go
internal/backup/snapshot.go
internal/backup/manifest.go
internal/backup/manager_test.go
internal/backup/snapshot_test.go
```

### Tasks

1. Implement backup manager with a single-flight mutex so only one backup runs at a time.
2. Validate backup directory:
   - non-empty
   - not equal to data dir
   - not under data dir
   - writable by daemon
3. Acquire `Runtime.Quiesce.QuiesceAll` before copying.
4. Copy data dir to a staging directory outside data dir.
5. Do not follow symlinks.
6. Optionally exclude logs when `include_logs=false`.
7. Create configured archive format as `.tmp`.
8. Write manifest with backup id, timestamp, size, checksum, daemon version, and policy summary.
9. Atomic rename completed archive.
10. Always release quiesce leases in `defer`.
11. Record success/failure status.

### Unit tests

- Backup rejects backup directory inside data directory.
- Backup creates archive and manifest for a fixture data dir.
- Backup excludes logs when configured.
- Backup includes logs when configured.
- Backup does not follow symlinks.
- Backup releases quiesce lease when copy fails.
- Concurrent trigger returns already-running/conflict error (`codes.Aborted` at the Admin API boundary).
- Archive is not visible as complete until atomic rename.
- Manifest checksum matches archive bytes.

### Acceptance

Implemented validation:

```sh
go test ./internal/backup ./internal/daemon/modules/backup
go test ./...
git diff --check
```

## Phase 5: Admin backup API implementation

Status: implemented. `mycel-api` now defines `AdminBackupService` with policy, trigger, status, list, and delete RPCs. The daemon registers `AdminBackupService` when a backup manager is present, authorizes calls with `CAPABILITY_SYSTEM_BACKUP_SPACE`, exempts safe backup control/status RPCs from ingress quiesce deadlock while preserving token authentication, persists policy updates under `meta/backup/policy.json`, starts/stops the backup scheduler when policy enablement changes, and maps backup/quiesce conflicts to appropriate gRPC statuses.

### Goals

Expose backup policy, trigger, status, list, and delete through Admin APIs.

### Files

```text
internal/daemon/api/admin/backup_service.go
internal/daemon/api/admin/backup_service_test.go
internal/daemon/modules/admin or internal/daemon/modules/backup
internal/daemon/server/server.go
```

The exact module home can be `internal/daemon/modules/backup` if backup state grows beyond the API adapter.

### Tasks

1. Register Admin Backup service in daemon server.
2. Implement `GetBackupPolicy`.
3. Implement `UpdateBackupPolicy` with validation.
4. Implement `TriggerBackup`.
5. Implement `GetBackupStatus`.
6. Implement `ListBackups` by reading manifests in backup directory.
7. Implement `DeleteBackup`.
8. Redact sensitive paths or values if needed in status responses.
9. Ensure backup control methods are authenticated as Admin/operator methods.
10. Ensure backup trigger methods are exempt from ingress quiesce deadlock but still protected from concurrent backup conflicts.

### Unit tests

- Policy update validates interval, retention, backup directory, schedule fields, and archive format.
- Disabled policy does not schedule backups.
- Trigger invokes backup manager.
- Status includes participant quiesce states.
- List returns only complete backups with manifests.
- Delete removes archive and manifest.
- Unauthorized client calls are rejected.
- Backup trigger works while general ingress is quiesced, if it is the method that owns quiescing.

### Acceptance

Implemented validation:

```sh
# myceldb/mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 generate
go test ./...
git diff --check

# myceldb/mycel
go test ./internal/daemon/api/admin ./internal/daemon/server ./internal/daemon/app ./internal/backup ./internal/daemon/modules/backup
go test ./...
git diff --check
```

## Phase 6: Scheduler and retention

Status: implemented. The backup module now runs an enabled-policy scheduler with tracked `next_run_at`, recomputes next run from `last_success_at` or scheduler start time, reconfigures on policy updates, stops cleanly on daemon/runtime shutdown, and avoids overlapping manual/scheduled backups through the manager single-flight guard. The backup manager applies retention by count after successful backups and ignores incomplete `.tmp` files when listing/retaining backups.

### Goals

Run periodic backups when enabled and keep only configured backups.

### Files

```text
internal/backup/scheduler.go
internal/backup/retention.go
internal/backup/scheduler_test.go
internal/backup/retention_test.go
internal/daemon/app/app.go
internal/daemon/runtime/runtime.go
```

### Tasks

1. Start scheduler during daemon startup when backup policy is enabled.
2. Reconfigure scheduler when policy changes.
3. Compute `next_run_at` from `last_success_at` or daemon start time.
4. Trigger backup when the configured interval/daily/weekly schedule is due.
5. Avoid overlapping scheduled and manual backups.
6. Apply retention by count after successful backup.
7. Optionally support max-age retention later.
8. Stop scheduler cleanly on daemon shutdown.

### Unit tests

- Scheduler does not run when disabled.
- Scheduler triggers when interval elapses or a daily/weekly wall-clock slot is due.
- Policy update changes next run.
- Manual backup and scheduled backup do not overlap.
- Retention keeps newest N complete backups.
- Retention ignores incomplete `.tmp` files.
- Scheduler stops on context cancellation/daemon close.

### Acceptance

Implemented validation:

```sh
# myceldb/mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 generate
go test ./...
git diff --check

# myceldb/mycel
go test ./internal/backup ./internal/daemon/modules/backup ./internal/daemon/api/admin
go test ./...
git diff --check
```

## Phase 7: CLI and SDK helpers

Status: implemented for daemon CLI. The `mycel admin backup` command tree now supports policy get/set, trigger, status, list, and delete over the Admin Backup gRPC API. JSON output is supported for automation, including pagination metadata for list responses; text output is concise for operators; duration flags accept Go duration syntax; and transient quiesce/backup `Unavailable` errors are surfaced as temporary conditions. Minimal Admin auth RPCs needed by fresh backup CLI invocations are quiesce-exempt so status/list/trigger remain operable during backup while token authentication is still enforced. SDK helper expansion remains optional/future work.

### Goals

Make the feature operable through the daemon-only CLI and SDKs.

### Files

```text
internal/cli/cmd/backup.go
internal/cli/cmd/backup_test.go
```

SDK repositories can add helpers after API stabilizes.

### CLI commands

```text
mycel admin backup policy get
mycel admin backup policy set --enabled --schedule interval --interval-hours 24 --keep 7 --dir /path/to/backups --archive-format zip
mycel admin backup policy set --enabled --schedule daily --time-of-day 22:00 --timezone UTC --archive-format tar.zst
mycel admin backup policy set --enabled --schedule weekly --time-of-day 02:00 --weekday sun --weekday wed --run-missed
mycel admin backup trigger
mycel admin backup status
mycel admin backup list
mycel admin backup delete <backup-id>
```

### Unit tests

- CLI formats policy and status output.
- CLI sends correct update policy request.
- CLI handles trigger/list/delete responses.
- CLI surfaces `Unavailable` as temporary backup/quiesce state.

### Acceptance

Implemented validation:

```sh
go test ./internal/cli/cmd -run 'TestAdminBackup|TestBackupCLI' -count=1
go test ./internal/cli/cmd
go test ./...
git diff --check
```

## Phase 8: Integration and failure-mode tests

Status: implemented. Phase 8 now has integration/failure-mode coverage for graph, blob, and semantic service gates during backup quiesce, including active-work drain behavior, transient `codes.Unavailable` write rejection during quiesce, write success after release, and offline archive restore into a fresh daemon data directory that can initialize and list restored admin resources.

### Goals

Verify full daemon behavior under concurrent writes and backup triggers.

### Tests

1. Start daemon test fixture with temporary data dir and backup dir.
2. Begin graph transaction, start commit, trigger backup, verify backup waits or returns expected transient state.
3. Trigger backup while semantic maintenance worker is active; verify no semantic writes occur during copy window.
4. Trigger backup while blob upload is active; verify backup waits for upload to finish or rejects new upload.
5. Verify new writes during quiesce receive `codes.Unavailable`.
6. Verify writes succeed after quiesce release.
7. Restore archive offline into a temporary directory and verify daemon can open/list basic resources.

### Acceptance

Implemented validation:

```sh
go test ./internal/daemon/modules/graph ./internal/daemon/modules/blob ./internal/daemon/modules/semantic ./internal/daemon/app -run Phase8 -count=1
go test ./internal/daemon/... ./internal/backup/...
go test ./...
git diff --check
```

## Phase 9: Documentation and operator guidance

Status: implemented. Operator-facing backup guidance now documents daemon ownership, transient `Unavailable` behavior, policy fields/defaults, backup directory safety requirements, CLI commands, offline restore, archive security, and expected retry behavior. The public API boundary is documented in `mycel-api` and the daemon-only boundary explicitly forbids application-side live data directory copies.

### Goals

Document final behavior after implementation is complete.

### Files to update

```text
README.md
doc.go
docs/design/quiesce-and-backup.md
docs/implementation/quiesce-and-backup-implementation-plan.md
docs/design/daemon-only-boundary.md
docs/design/admin/backup.md
current CLI docs
current command docs
```

### Documentation content

- Explain that backups are daemon-owned.
- Explain that users are not logged out by default.
- Document transient `Unavailable` behavior during backup.
- Document policy fields and defaults.
- Document backup directory safety requirements.
- Document manual trigger/list/status/delete commands.
- Document offline restore procedure.
- Document security considerations for backup archives.
- Document expected application behavior: retry transient errors.

### Acceptance

Implemented validation:

```sh
go test ./...
git diff --check
```

- Docs match implemented API names and CLI flags.
- `git diff --check` passes.

## Final validation

Run from `myceldb/mycel`:

```sh
go test ./...
make test
make build
scripts/check-public-surface.sh --workspace /Users/martinbeauvais/Projects/knotbase/Knotbase --strict
git diff --check
```

Run from `myceldb/mycel-api`:

```sh
go test ./...
git diff --check
```

If SDK helper changes are included, run each SDK's test/build suite as well.
