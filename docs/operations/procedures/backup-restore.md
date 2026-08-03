# Backup and Restore Procedures

mycel currently has two backup/restore-oriented surfaces:

1. Admin backup policy/status operations for daemon-owned backups.
2. User-scoped backup/restore through `mycel admin user-backup` for explicit
   operator-selected export/import of a standard user's visible spaces/domains.

## Daemon/system backup

Operators can configure, trigger, list, and delete daemon-owned system backups:

```sh
mycel --daemon-addr <pod-or-node>:9091 \
  --username <operator> --password <password> \
  admin backup policy set --dir /path/outside/data --archive-format tar.zst --keep 5 --interval-hours 24

mycel --daemon-addr <pod-or-node>:9091 \
  --username <operator> --password <password> \
  admin backup trigger --reason "before maintenance"
```

Full-system restore is offline-only: stop `myceld`, restore the archive contents
into the corresponding empty data directory/PVC, then start `myceld` against the
restored data. In a multi-pod StatefulSet, capture one system backup per pod and
restore each archive to the matching ordinal PVC.

The destructive K3s validation gate is:

```sh
make test-k3s-system-backup-restore
```

It creates graph/blob data, captures a system backup from each pod, wipes the
namespace/PVCs, restores each ordinal archive into fresh PVCs, restarts the
StatefulSet, and verifies graph/blob payloads through every pod.

## User-scoped export/import

Use user-scoped backup for trusted-source recovery into a fresh cluster or a
selected target user. The operation does not automatically repair divergent PVCs
or choose an authoritative source.

Export from the operator-selected source endpoint:

```sh
mycel --daemon-addr <source-pod-or-node>:9091 \
  --username <operator> --password <password> \
  admin user-backup export \
  --user-id <source-user-id> \
  --file user-backup.tar.zst \
  --include-blobs \
  --source-label <source-label>
```

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
  --target-username <target-user> \
  --create-user
```

Execute restore explicitly:

```sh
mycel --daemon-addr <target-node>:9091 \
  --username <operator> --password <password> \
  admin user-backup import \
  --file user-backup.tar.zst \
  --target-username <target-user> \
  --create-user \
  --new-password <new-password> \
  --execute
```

## Safety notes

- Backups do not export plaintext passwords or active sessions/tokens.
- Restore defaults to dry-run planning.
- Destructive domain replacement is not exposed by user-scoped restore.
- Space/domain IDs are remapped; graph node/edge/blob IDs are preserved where
  supported by domain import.

See also [split-brain recovery](split-brain-recovery.md).
