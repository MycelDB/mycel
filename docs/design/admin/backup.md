# Admin Backup API

## Status

Repository-backed deployment backup API implemented. The protobuf source lives in:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/backup.proto
```

## Service

```text
mycel.admin.v1.AdminBackupService
```

The public daemon backup contract is repository-backed only. Daemon-local backup
files and local backup status/list/delete/trigger RPCs have been removed from
the public API. Standalone daemons are represented as one-node backup sets;
clustered deployments include all authoritative members and convergence
evidence.

Core RPCs include:

- `GetBackupPolicy` / `UpdateBackupPolicy`
- `ListBackupRepositories` / `GetBackupRepository` / `CreateBackupRepository` /
  `UpdateBackupRepository` / `ValidateBackupRepository` /
  `DeleteBackupRepository`
- `StartBackup` / `GetBackupOperation` / `CancelBackupOperation` /
  `ListBackupOperations`
- `ListBackupSets` / `GetBackupSet` / `ValidateBackupSet` /
  `DeleteBackupSet`
- `PlanRepositoryRetention` / `ApplyRepositoryRetention`
- `GetBackupObservability`
- `PlanRestore` / `StartRestore` / `GetRestoreOperation` /
  `ListRestoreOperations` / `CancelRestoreOperation`

## Authorization

Requires an operator bearer token with backup administration capability:

```text
CAPABILITY_SYSTEM_BACKUP_SPACE
```

The daemon still authenticates backup RPCs during quiesce. Backup operation
status/list/cancel/validate and repository observability paths are
quiesce-exempt so operators can inspect in-progress work while a backup quiesce
lease is active.

## Policy fields

`BackupPolicy` now contains repository-backed defaults only:

| Field | Description |
|---|---|
| `default_repository_id` | Repository ID used when start/list/retention requests omit `repository_id`. |
| `capture_provider` | Default capture primitive (`auto`, filesystem immutable copy, CSI snapshot, AWS EBS snapshot). |
| `quiesce_read_profile` | Read profile enforced while backup quiesce is held. |
| `allow_read_only_during_quiesce` | Whether explicitly registered read-only RPCs may proceed during quiesce. |
| `derived_subsystems` | Policy for included, discarded, or rebuild-required derived artifacts. |
| `retention_policy` | Repository backup-set retention settings. |
| `validation_options` | Default validation depth for backup-set checks. |

Local filesystem paths may still be used internally for scratch/staging and
restore targets, but they are not durable public backup artifacts.

## CLI

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy get

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set \
  --default-repository-id repo-prod \
  --capture-provider filesystem_immutable_copy \
  --quiesce-read-profile control_only \
  --keep-last-successful 7 \
  --require-complete-marker

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup start \
  --repository-id repo-prod \
  --reason 'before upgrade' \
  --archive-format tar.zst \
  --wait

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup list --repository-id repo-prod

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup validate \
  --repository-id repo-prod \
  --backup-set-id backup-set-...

mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup restore-plan \
  --repository-id repo-prod \
  --backup-set-id backup-set-... \
  --target-mode new-deployment-bootstrap \
  --identity-mode preserve-backup-id
```

Add `--output json` for structured output.

## Quiesce behavior and client errors

Backups are quiesced. The daemon stops new non-exempt work, waits for admitted
writes/background writers to finish, snapshots authoritative state into
repository-backed artifacts, publishes `backup-set.json`, then publishes the
`COMPLETE` marker.

While quiesced, new non-exempt RPCs return transient gRPC errors. This includes
mutating RPCs and may include reads unless they are explicitly exempted/proven
safe:

```text
grpc code = Unavailable
message   = service is quiesced
ErrorInfo reason = MYCEL_QUIESCED
```

Clients should treat this as retryable with bounded exponential backoff. Users
are not logged out by backup quiesce.
