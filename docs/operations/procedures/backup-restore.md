# Backup and Restore Procedures

MycelDB has two operator backup/restore surfaces:

1. Repository-backed daemon/deployment backup sets through `mycel admin backup`.
2. Principal-scoped backup/restore through `mycel admin user-backup` for explicit
   operator-selected export/import of a principal's visible spaces/domains.

## Daemon/deployment backup

Repository-backed backup sets are the only public durable daemon backup model.
Standalone deployments are represented as one-node backup sets. Clustered
deployments include all authoritative members and raft convergence evidence.
Local filesystem paths are used only as daemon-internal scratch/staging or as an
offline restore target; they are not operator-facing durable backup artifacts.

Configure the default repository-backed policy:

```sh
mycel --daemon-addr <pod-or-node>:9091 \
  --username <operator> --password <password> \
  admin backup policy set \
  --default-repository-id repo-prod \
  --capture-provider filesystem_immutable_copy \
  --quiesce-read-profile control_only \
  --keep-last-successful 7 \
  --require-complete-marker
```

Start a backup set explicitly:

```sh
mycel --daemon-addr <healthy-node>:9091 \
  --username <operator> --password <password> \
  admin backup start \
  --reason "before maintenance" \
  --repository-id repo-prod \
  --archive-format tar.zst \
  --wait
```

Validate and plan restore from repository truth:

```sh
mycel --daemon-addr <healthy-node>:9091 \
  --username <operator> --password <password> \
  admin backup validate \
  --repository-id repo-prod \
  --backup-set-id backup-set-...

mycel --daemon-addr <healthy-node>:9091 \
  --username <operator> --password <password> \
  admin backup restore-plan \
  --repository-id repo-prod \
  --backup-set-id backup-set-... \
  --target-mode new-deployment-bootstrap \
  --identity-mode preserve-backup-id
```

Operators should also poll `mycel admin backup observability --output json` or
the `GetBackupObservability` RPC for backup freshness, failed operations,
restore blockers, retention warnings, and incomplete repository-prefix alerts.
For the subsystems that write under `MYCELD_DATA_DIR`, their quiesce coverage,
and the restore expectations for authoritative versus derived state, see
[Backup-critical durable writer inventory](backup-durable-writer-inventory.md).

A full daemon/deployment backup includes the authoritative automation directory,
including procedure-inference attempts, immutable retained changes, high-water
and earliest-retained sequences, committed authorization revisions, and
procedure-control fences/barriers. After restore, do not reuse a collector cursor
until the restored high-water and earliest-retained state/history sequences have
been compared with the collector checkpoint. A cursor ahead of restored
high-water or behind an earliest-retained watermark requires an explicit
reconciliation decision; a fresh snapshot alone cannot prove missed history.

The destructive K3s validation gate is moving to repository-backed backup sets;
legacy local backup-file flows are retired from public release qualification.

## Principal-scoped export/import

Use principal-scoped backup for trusted-source recovery into a fresh cluster or a
selected target principal. The command group is still named `user-backup` for
archive-format compatibility. The operation does not automatically repair
divergent PVCs or choose an authoritative source. Principal-scoped exports do
not export the daemon-owned procedure-inference audit ledger, attempt-change
watermarks, authorization revisions, or procedure-control barriers. Use a full
system backup when those authoritative records must be preserved.

Export from the operator-selected source endpoint:

```sh
mycel --daemon-addr <source-pod-or-node>:9091 \
  --username <operator> --password <password> \
  admin user-backup export \
  --principal-id <source-principal-id> \
  --file user-backup.tar.zst \
  --include-blobs \
  --source-label <source-label>
```

`--user-id` remains a deprecated alias for compatibility with the
`mycel-user-backup-v1` archive format.

Validate before import:

```sh
mycel admin user-backup validate --file user-backup.tar.zst
```

Plan restore; this is dry-run by default:

```sh
mycel --daemon-addr <target-node>:9091 \
  --username <operator> --password <password> \
  admin user-backup import \
  --file user-backup.tar.zst \
  --target-username <target-principal-username> \
  --create-user
```

Execute restore explicitly:

```sh
mycel --daemon-addr <target-node>:9091 \
  --username <operator> --password <password> \
  admin user-backup import \
  --file user-backup.tar.zst \
  --target-username <target-principal-username> \
  --create-user \
  --new-password <new-password> \
  --execute
```

Keep `--new-password` out of shell history when possible. A future credential
reset flow should replace direct CLI password entry.
