# Quiesce and Backup Design

## Status

Implemented daemon-backed MVP.

This document defines daemon-owned quiescing and periodic backup behavior for Mycel. The implemented daemon produces consistent backups of `MYCELD_DATA_DIR` without logging users out or requiring application processes to open Mycel storage directly. Public control is exposed through `mycel.admin.v1.AdminBackupService` and the `mycel admin backup` CLI; see [Admin Backup API](admin/backup.md).

Service lifecycle and capability interfaces used by this design are defined in [Daemon Service Interfaces Design](daemon-service-interfaces.md).

## Goals

- Let operators enable/disable scheduled backups through Admin APIs.
- Let operators configure backup interval, backup directory, retention count, and related policy.
- Support manual backup triggers through Admin APIs/CLI.
- Produce a consistent archive of the daemon data directory.
- Avoid logging users out for routine backups.
- Make quiesce state visible to operators and clients.
- Keep backup and quiesce implementation inside `myceld`; applications continue using daemon APIs only.

## Non-goals

- Online restore through Admin APIs in the first version.
- Cross-node distributed snapshots.
- Application-specific backups such as Knot PKM registration exports.
- Direct application access to the Mycel data directory.
- Long-term queuing of writes while a backup is in progress.

## Why quiesce is required

A backup cannot safely zip the live data directory while the daemon is writing. Mycel writes durable state from multiple bounded contexts:

- graph transaction segments and manifests
- blob objects and metadata
- space/domain/ACL stores
- identity/auth/user/session stores
- template catalogs
- semantic maintenance logs, work state, vector records, and backfill state
- change-stream records and daemon metadata

Existing storage-level locks are local to a store or package. They do not coordinate the whole data directory. A consistent backup needs a daemon-wide quiesce operation that stops new mutating work, drains active work, pauses background writers, then snapshots files.

## User-visible behavior

When `myceld` is quiesced for backup:

- Active writes already admitted are allowed to finish.
- New writes are rejected quickly with a transient error.
- Clients should retry with bounded backoff.
- Users are not logged out by default.
- Reads may be rejected unless explicitly proven side-effect-free. The current policy field `allow_reads_during_backup` is reserved for safe-read behavior and defaults to false.

Implemented gRPC error for rejected non-exempt RPCs, including reads unless explicitly exempted/proven safe:

```text
code = Unavailable
desc = myceld is temporarily quiesced for backup
```

If surfaced through HTTP by an application gateway, use:

```http
503 Service Unavailable
Retry-After: 5
```

## Architecture

Backup uses service-level quiescing coordinated by a daemon-wide coordinator.

```text
Admin Backup API / scheduler
        |
        v
QuiesceCoordinator
        |
        +-- api ingress participant
        +-- semantic participant
        +-- graph participant
        +-- blob participant
        +-- space participant
        +-- identity/auth participant
        +-- template participant
        +-- change stream participant, if needed
        |
        v
BackupManager copies MYCELD_DATA_DIR
```

The coordinator controls ordering and rollback. Each service owns the details of stopping new work, draining active work, and resuming safely. These participants are daemon service capabilities: passive modules can register a generic gate, while background modules such as semantic and backup can register custom participants and also implement lifecycle `Start`/`Stop` behavior.

## Quiesce package

Add an internal package:

```text
internal/daemon/quiesce/
  coordinator.go
  gate.go
  status.go
```

Core types:

```go
type Request struct {
    Reason string
    Mode   Mode
    Source string
}

type Mode string

const ModeBackup Mode = "backup"

type Participant interface {
    Name() string
    Quiesce(ctx context.Context, req Request) (Lease, error)
    Status() ParticipantStatus
}

type Lease interface {
    Release(ctx context.Context) error
}
```

The coordinator preserves registration order for quiescing and releases leases in reverse order:

```go
type Coordinator struct {
    participants []Participant
}

func (c *Coordinator) Register(p Participant)
func (c *Coordinator) QuiesceAll(ctx context.Context, req Request) (*CompositeLease, error)
func (c *Coordinator) Status() Status
```

If a participant fails to quiesce, the coordinator releases all previously acquired leases and returns the error.

## Gate participant

Most services can use a generic gate.

```go
type Gate struct {
    name string
    mu sync.Mutex
    cond *sync.Cond
    closed bool
    active int
    reason string
    since time.Time
}

func (g *Gate) Enter(ctx context.Context) (release func(), err error)
func (g *Gate) Quiesce(ctx context.Context, req Request) (Lease, error)
func (g *Gate) Status() ParticipantStatus
```

Write paths call `Enter` before mutating state:

```go
release, err := m.gate.Enter(ctx)
if err != nil {
    return status.Error(codes.Unavailable, "graph service is quiesced")
}
defer release()
```

Quiesce closes the gate, prevents new entrants, waits for `active == 0`, then returns a lease. Releasing the lease reopens the gate.

## Module registration

The daemon runtime owns the coordinator:

```go
type Runtime struct {
    Config   config.Config
    Logger   *slog.Logger
    Modules  map[string]Module
    Quiesce  *quiesce.Coordinator
    LogPath  string
    close    func() error
}
```

`internal/daemon/app/app.go` creates the coordinator before module initialization. Modules register participants during `Init`:

```go
func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
    m.gate = quiesce.NewGate("graph")
    rt.Quiesce.Register(m.gate)
    // existing initialization
}
```

Complex modules can register custom participants instead of the raw gate.

## Service participants

### API ingress

A gRPC interceptor blocks new non-exempt RPCs during backup. This is an outer safety net and gives clients consistent transient errors.

Exempt only backup/quiesce status and control methods needed to observe or release a backup operation.

### Graph

The graph participant gates durable graph writes, especially transaction commit. Staged in-memory transaction edits can exist, but commit must not run while backup is copying.

Important write paths:

- `TransactionService.CommitTransaction`
- graph mutation RPCs that persist directly, if any are added later
- graph module commit helpers

The commit RPC should be covered as a whole so backup waits for graph segment writes, session commit, semantic dirty append, and change-stream publication to finish.

### Blob

The blob participant gates:

- blob upload
- blob delete
- blob metadata writes
- blob promotion/staging writes

This prevents copying while an object or metadata file is partially written.

### Semantic

Semantic needs a custom participant because it has background workers.

Quiesce should:

1. Pause analyzer/worker scheduling.
2. Prevent new manual maintenance/backfill work.
3. Wait for active semantic maintenance/vector writes to drain.
4. Resume scheduling when the lease is released.

Manual Admin APIs such as analyze, process, retry, cancel, and backfill should also enter the semantic gate.

A later version may cancel long-running provider calls to reduce backup latency, but the first version can wait for already-active work to finish.

### Space/access/domain

The space participant gates:

- create/delete space
- create/delete domain
- ACL/grant changes
- directory removal or cascading cleanup

Space deletion is especially important because it can remove whole graph or semantic directories.

### Identity/auth

The identity/auth participant gates:

- user create/update/delete
- password changes
- session creation/revocation
- admin account changes
- login, if login persists refresh sessions

Users should not be logged out by default. Existing sessions remain valid after backup completes.

### Template

The template participant gates template add/update/delete/import operations.

### Change stream

Change-stream writes are usually downstream of graph commits. If all graph commits are drained before backup, separate change-stream quiescing may not be required. Add a participant if the change-stream module gains independent writers.

## Backup manager

Implemented packages:

```text
internal/backup/
  manager.go
  policy.go
  snapshot.go
  retention.go
  manifest.go
internal/daemon/modules/backup/
  module.go
  types.go
```

Backup flow:

```text
manual trigger or scheduler tick
  -> acquire backup mutex
  -> coordinator.QuiesceAll(reason=backup)
  -> copy MYCELD_DATA_DIR to staging directory outside data dir
  -> compress staging directory to backup-dir/mycel-backup-<timestamp>.zip.tmp
  -> write manifest/checksum
  -> atomic rename .tmp to .zip
  -> apply retention
  -> release quiesce leases in reverse order
```

The backup directory must not be inside the data directory. The daemon also rejects a backup directory equal to the data directory and performs symlink-aware containment validation before writing.

## Backup policy

Implemented policy fields:

```text
enabled: bool
backup_dir: string
interval_seconds: int64
retention_count: int32
include_logs: bool
compression: zip
quiesce_drain_timeout_seconds: int64
backup_timeout_seconds: int64
retry_after_seconds: int64
status_history_limit: int32
allow_reads_during_backup: bool
```

Runtime status exposes `last_success_at` and `next_run_at`; failures are reported in backup status history/status. Policy is persisted under daemon metadata at `meta/backup/policy.json`, not inside any application space.

## Defaults

Backups are configured by default but disabled by default. Operators must explicitly enable scheduled backups.

Recommended effective backup defaults:

```text
enabled: false
backup_dir: sibling of data dir, for example /data/mycel-backups when MYCELD_DATA_DIR=/data/mycel
interval: 24h
retention_count: 7
include_logs: false
compression: zip
quiesce_drain_timeout: 2m
backup_timeout: 30m
retry_after: 5s
```

Recommended quiesce behavior defaults:

```text
reject_new_work: true
queue_new_work: false
allow_reads_during_backup: false initially
release_order: reverse registration order
on_participant_failure: rollback acquired leases
status_history_limit: 20
```

`GetBackupPolicy` should return effective values after defaults are applied so operators can see exactly what will happen.

## Environment variables

Daemon startup configuration may provide defaults for the persisted policy. Admin API updates override the persisted policy; unset policy fields fall back to daemon defaults.

Recommended environment variables:

```text
MYCELD_BACKUP_ENABLED=false
MYCELD_BACKUP_DIR=
MYCELD_BACKUP_INTERVAL=24h
MYCELD_BACKUP_RETENTION_COUNT=7
MYCELD_BACKUP_INCLUDE_LOGS=false
MYCELD_BACKUP_COMPRESSION=zip
MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT=2m
MYCELD_BACKUP_TIMEOUT=30m
MYCELD_BACKUP_RETRY_AFTER=5s
MYCELD_BACKUP_STATUS_HISTORY_LIMIT=20
MYCELD_BACKUP_ALLOW_READS_DURING_BACKUP=false
```

If `MYCELD_BACKUP_DIR` is unset, the daemon may derive a sibling directory from `MYCELD_DATA_DIR`, but it must still validate that the resolved backup directory is not inside the data directory before enabling backups.

## Admin API surface

Implemented Admin APIs in `mycel-api` and `myceld`:

```text
GetBackupPolicy
UpdateBackupPolicy
TriggerBackup
GetBackupStatus
ListBackups
DeleteBackup
```

`GetBackupStatus` includes quiesce participant status plus current/last backup status, including `last_success_at` and `next_run_at`. `ListBackups` returns completed backups and pagination metadata.

## Restore

First implementation supports offline restore only:

```text
stop myceld
verify the archive manifest/checksum
move current data dir aside or choose an empty restore dir
unpack backup archive into the target data dir
start myceld with MYCELD_DATA_DIR pointing at the restored dir
verify basic resources through daemon APIs/CLI
```

Online restore through Admin APIs is deferred because it requires a stronger state replacement protocol.

## Security and safety

- Backup directory must not be under `MYCELD_DATA_DIR`.
- Do not follow symlinks while copying.
- Use temporary files and atomic rename for completed archives.
- Include a manifest and checksum.
- Do not include credential secret values in Admin status responses.
- Consider backup encryption in a later phase.
- Treat backup archives as sensitive because they contain graph data, users, sessions, metadata, semantic state, and possibly encrypted secrets.

## Knot PKM coordination

Knot PKM can optionally add its own application maintenance gate for better UX. A coordinated backup would look like:

```text
1. PKM server enters maintenance mode.
2. PKM waits for active requests to drain.
3. PKM or operator triggers myceld backup.
4. myceld quiesces services and snapshots data.
5. myceld releases quiesce.
6. PKM exits maintenance mode.
```

This is optional. Mycel backup consistency must not depend on Knot PKM behavior.
