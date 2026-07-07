# Admin Backup API

## Status

Implemented daemon-backed MVP.

Protobuf source:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/backup.proto
```

## Service

```text
mycel.admin.v1.AdminBackupService
```

Implemented RPCs:

- `GetBackupPolicy`
- `UpdateBackupPolicy`
- `TriggerBackup`
- `GetBackupStatus`
- `ListBackups`
- `DeleteBackup`

The service manages daemon-owned backups of `MYCELD_DATA_DIR`. Applications and tools must not copy or open the data directory directly; use this Admin API, the CLI, or SDK helpers that call the Admin API.

## Authorization

Requires an operator bearer token with backup administration capability:

```text
CAPABILITY_SYSTEM_BACKUP_SPACE
```

The daemon still authenticates backup RPCs during quiesce. The minimal Admin auth RPCs needed for a fresh CLI invocation are quiesce-exempt so operators can inspect or trigger backup status while the daemon is already quiescing.

## Policy fields and defaults

| Field | Default | Description |
|---|---:|---|
| `enabled` | `false` | Enables scheduled backups when true. Manual triggers are still available when false. |
| `backup_dir` | sibling of data dir, e.g. `~/mycel_data-backups` | Directory for completed archives and sidecar manifests. |
| `interval_seconds` | `86400` / `24h` | Scheduled backup interval. After a manual success, the next scheduled run is pushed out by this interval. |
| `retention_count` | `7` | Number of newest complete backups to keep. Older complete backups are deleted after successful backup runs. |
| `include_logs` | `false` | Include daemon logs in the archive. |
| `compression` | `zip` | Archive format. |
| `quiesce_drain_timeout_seconds` | `120` / `2m` | Maximum time to stop new work and drain active participants. |
| `backup_timeout_seconds` | `1800` / `30m` | Maximum time for the backup trigger operation. |
| `retry_after_seconds` | `5` | Delay used by the scheduler after transient failures or backup conflicts. |
| `status_history_limit` | `20` | Number of recent status entries retained in daemon memory/status. |
| `allow_reads_during_backup` | `false` | Reserved for proven-safe reads; default behavior is conservative. |

The effective policy is persisted by the daemon under `meta/backup/policy.json` when updated through the Admin API.

Environment variables can seed daemon startup policy:

```text
MYCELD_BACKUP_ENABLED
MYCELD_BACKUP_DIR
MYCELD_BACKUP_INTERVAL
MYCELD_BACKUP_RETENTION_COUNT
MYCELD_BACKUP_INCLUDE_LOGS
MYCELD_BACKUP_COMPRESSION
MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT
MYCELD_BACKUP_TIMEOUT
MYCELD_BACKUP_RETRY_AFTER
MYCELD_BACKUP_STATUS_HISTORY_LIMIT
MYCELD_BACKUP_ALLOW_READS_DURING_BACKUP
```

## CLI

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy get

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set --enabled --dir /var/backups/mycel --interval 24h --keep 7

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup trigger --reason 'before upgrade'

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup status

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup list --page-size 50

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup delete '<backup-id>'
```

Add `--output json` for structured output. `admin backup list --output json` returns the full `ListBackupsResponse`, including `next_page_token`.

## Backup directory safety

The daemon validates `backup_dir` before writing:

- it must be non-empty after effective defaults are applied;
- it must not equal the data directory;
- it must not be inside the data directory;
- symlinks are resolved before containment checks;
- the directory must be writable.

Completed backups are written as a zip archive plus a sidecar manifest. Temporary/incomplete `.tmp` files are ignored by list and retention logic.

## Quiesce behavior and client errors

Backups are quiesced. The daemon stops new non-exempt work, waits for admitted writes/background writers to finish, snapshots the data directory outside the data directory, writes the archive atomically, then releases quiesce.

While quiesced, new non-exempt RPCs return transient gRPC errors. This includes mutating RPCs and may include reads unless they are explicitly exempted/proven safe:

```text
code = Unavailable
```

Applications should retry transient `Unavailable` errors with bounded backoff. Users are not logged out by default. Selected backup status/list/trigger RPCs and the minimal Admin auth RPCs needed by a fresh CLI invocation remain available so operators can inspect backup progress.

Only one backup may run at a time. A concurrent manual trigger returns `codes.Aborted` with an already-running/backup-running message; operators may retry after the active backup completes.

## Offline restore runbook

Online restore is intentionally not part of the first API. To restore:

1. Stop `myceld` for the target data directory.
2. Choose the backup archive and verify its sidecar manifest/checksum.
3. Create a new empty restore directory outside the current data directory.
4. Extract the zip archive into that restore directory. Preserve file modes where possible.
5. Point `MYCELD_DATA_DIR` at the restored directory, or move the restored directory into place while the daemon is stopped.
6. Start `myceld` and verify basic resources with daemon APIs or CLI commands.
7. Keep the original data directory until the restore is verified.

Do not extract untrusted archives as a privileged user. Treat backup archives as sensitive: they may contain graph content, blob content, identity metadata, session metadata, semantic records, and optionally logs.

## Notes and limitations

- Count-based retention is implemented; max-age retention is deferred.
- Restore is offline only; no online restore Admin API exists yet.
- Cross-node/distributed snapshots are out of scope for the MVP.
