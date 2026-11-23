# Minimum observability and alerting

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio production needs enough visibility to detect data-loss risk,
availability risk, quota-accounting risk, and degraded search behavior before
users are affected or backups become untrustworthy.

Perfect observability is not required for first production, but a minimum health
and alerting baseline is mandatory.

## Mandatory first-cut signals

### Cluster health

- pod/daemon health;
- Raft leader presence;
- quorum status;
- under-replicated or unavailable partitions;
- leader distribution when available.

### Storage and capacity pressure

- persistent volume capacity;
- used and free disk/PVC bytes;
- object-store access failures when object storage is part of the deployment;
- storage pressure alerts.

### Backup health

- last successful backup time;
- last backup failure reason;
- last successful restore verification or restore drill time;
- stale-backup alert.

The daemon exposes a backup/restore observability snapshot through
`AdminBackupService.GetBackupObservability` and the CLI command
`mycel admin backup cluster observability`. The snapshot contains non-secret
metric rows plus evaluated alert conditions. Operators can poll the RPC/CLI and
translate the rows into Prometheus, CloudWatch, or another monitoring backend
without reading daemon logs.

Core backup/restore metric names are:

- `mycel_backup_cluster_operation_count{state,phase,repository_id}`;
- `mycel_backup_cluster_latest_operation_state{state,phase,backup_set_id}`;
- `mycel_backup_cluster_latest_success_age_seconds{repository_id}`;
- `mycel_backup_cluster_latest_success_timestamp_seconds{repository_id}`;
- `mycel_backup_cluster_active_operation_age_seconds{backup_set_id,phase}`;
- `mycel_backup_cluster_operation_duration_seconds{backup_set_id,state}`;
- `mycel_backup_cluster_operation_bytes{backup_set_id,state}`;
- `mycel_backup_cluster_operation_nodes{backup_set_id,kind}`;
- `mycel_backup_cluster_blocker_count{backup_set_id,reason}`;
- `mycel_restore_cluster_operation_count{state,phase,repository_id}`;
- `mycel_restore_cluster_latest_operation_state{state,restore_operation_id}`;
- `mycel_restore_cluster_active_operation_age_seconds{restore_operation_id}`;
- `mycel_restore_cluster_operation_duration_seconds{restore_operation_id,state}`;
- `mycel_restore_cluster_operation_bytes{restore_operation_id,state}`;
- `mycel_restore_cluster_operation_nodes{restore_operation_id,kind}`;
- `mycel_restore_cluster_blocker_count{restore_operation_id,reason}`;
- `mycel_restore_cluster_cleanup_task_count{restore_operation_id,cleanup_state,resource_kind}`;
- `mycel_backup_repository_retention_action_count{repository_id,action,reason}`;
- `mycel_backup_repository_retention_warning_count{repository_id}`;
- `mycel_backup_repository_incomplete_backup_count{repository_id}`;
- `mycel_backup_repository_last_retention_apply_timestamp_seconds{repository_id}`;
- `mycel_backup_repository_last_retention_apply_failed{repository_id}`;
- `mycel_backup_repository_last_retention_apply_warning_count{repository_id}`;
- `mycel_backup_repository_last_retention_apply_error_count{repository_id}`;
- `mycel_backup_repository_last_retention_apply_blocked_count{repository_id}`;
- `mycel_backup_repository_last_retention_apply_deleted_objects{repository_id}`.

The repository retention/incomplete-prefix metrics are included only when the
request sets `include_retention_probe=true`, because they perform object-store
list/read operations. The probe is dry-run only; it never deletes repository
objects.

### Search and projection health

- lexical index availability;
- semantic projection readiness;
- projection lag/freshness;
- semantic consistency policy outcomes where applicable;
- search channel degraded/unavailable status.

### Usage accounting health

- usage measurement freshness;
- counter reconciliation failures;
- counter authority/freshness errors;
- inability to produce quota-safe usage.

### API health

- client/admin API request latency;
- error rate by major gRPC status class;
- authentication/session error trends, including stale delegated-session reasons.

## Mandatory first-cut capabilities

Before CommonFolio production, operators should have:

- a cluster health dashboard;
- alert rules for quorum risk, storage pressure, backup failure/staleness,
  projection/index outage, usage accounting staleness, and elevated API errors;
- structured logs that preserve correlation IDs while redacting credentials and
  private data;
- runbooks linked from alerts or alert descriptions.

## Backup/restore alert rules

The observability snapshot evaluates these alert conditions directly and returns
Prometheus-style expressions so deployments can mirror them in their monitoring
stack:

| Alert | Severity | Expression | Runbook |
| --- | --- | --- | --- |
| `MycelClusterBackupFailed` | critical | `mycel_backup_cluster_latest_operation_state{state=~"failed|aborted"} > 0` | inspect `admin backup cluster status`, validate repository access, and rerun after root cause is fixed. |
| `MycelBackupFreshnessStale` | warning | `mycel_backup_cluster_latest_success_age_seconds < 0 or mycel_backup_cluster_latest_success_age_seconds > <threshold>` | start a repository-backed backup and verify `backup-set.json` plus `COMPLETE`. |
| `MycelClusterBackupOperationStale` | warning | `mycel_backup_cluster_active_operation_age_seconds > <threshold>` | check blockers/quiesce status; cancel only when the phase is safe. |
| `MycelClusterBackupBlockers` | warning | `mycel_backup_cluster_blocker_count > 0` | fix the reported convergence, quorum, capacity, or repository blocker before retrying. |
| `MycelClusterRestoreFailed` | critical | `mycel_restore_cluster_latest_operation_state{state=~"failed|failed_requires_operator"} > 0` | stop target rollout and follow restore failure triage before starting pods. |
| `MycelClusterRestoreBlockers` | critical | `mycel_restore_cluster_blocker_count > 0` | resolve target, dependency, source-fencing, capacity, or object-store blockers before mutation. |
| `MycelClusterRestoreOperationStale` | warning | `mycel_restore_cluster_active_operation_age_seconds > <threshold>` | inspect the restore operation phase and helper pod evidence; avoid manual PVC changes until classified. |
| `MycelClusterRestoreCleanupIncomplete` | warning | `mycel_restore_cluster_cleanup_task_count{cleanup_state!~"completed|succeeded"} > 0` | delete transient helpers/secrets only after evidence is captured. |
| `MycelRepositoryIncompleteBackups` | warning | `mycel_backup_repository_incomplete_backup_count > 0` | run retention preview/apply after verifying no active operation references the prefix. |
| `MycelRepositoryRetentionWarnings` | warning | `mycel_backup_repository_retention_warning_count > 0 or mycel_backup_repository_retention_action_count{action=~"blocked|warn"} > 0` | review invalid completed sets, blocked active references, and partial deletes. |
| `MycelRepositoryRetentionApplyFailed` | warning | `mycel_backup_repository_last_retention_apply_failed > 0 or mycel_backup_repository_last_retention_apply_error_count > 0` | inspect the latest retention apply response, rerun a dry-run plan, and retry after repository delete failures are fixed. |

Recommended thresholds for first production are:

- backup freshness: twice the scheduled backup interval, capped by the recovery
  point objective if it is stricter;
- stale active operation: the configured backup timeout or one hour, whichever is
  explicit for the environment;
- incomplete repository backup prefix: the retention policy's
  `delete_incomplete_after` duration.

All labels and alert evidence must remain credential-free. Do not add raw S3,
MinIO, KMS, TLS, access-token, or password values to metric labels, alert
annotations, Lab artifacts, or Dashboard text.

## Launch gate

CommonFolio production should require:

1. dashboards for cluster health, storage pressure, backup health, search health,
   usage freshness, and API error/latency basics;
2. alert rules for the highest-risk conditions;
3. operator runbooks for those alerts;
4. evidence that backup and usage-accounting alerts fire in staging or Mycel Lab
   when the corresponding failure is injected.

## Follow-up design work

- Define the metrics contract and naming scheme.
- Decide which metrics are daemon-native versus deployment/operator-provided.
- Define log redaction rules and tests.
- Add traces for high-value request paths such as writes, search, backup, and
  restore.
- Build richer Console/operator UI surfaces after the metrics contract is stable.
