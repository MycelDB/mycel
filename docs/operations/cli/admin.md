# `mycel admin`

Admin management surfaces.

Authentication mode: **principal auth with admin capabilities**.

## Common tasks

- Legacy aliases for principal identity management.
- Create/delete spaces as an admin-capable principal.
- Run node-local daemon backup, raft-storage-safe coordinated cluster backup, and principal-scoped backup/restore commands.
- Inspect and append curated Activity Events with `mycel admin activity`.

## Examples

```sh
mycel --output json admin list
```

```sh
mycel admin backup cluster start \
  --reason "before maintenance" \
  --output-dir /mnt/mycel-backups \
  --archive-format tar.zst \
  --wait
```

```sh
mycel admin backup cluster validate --backup-set /mnt/mycel-backups
```

```sh
mycel admin backup cluster restore-plan --backup-set /mnt/mycel-backups/backup-set-...
```

```sh
mycel admin backup cluster restore-local \
  --backup-set /mnt/mycel-backups/backup-set-... \
  --ordinal 0 \
  --data-dir /data/mycel
```

Coordinated cluster backup creates one backup set with one archive per pod/PVC,
records raft barriers, requires raft freeze/checkpoint evidence in the final
`backup-set.json`, and keeps full-system restore offline/operator-driven.
`restore-plan` and `restore-local` are local/offline primitives and do not
contact a running daemon; run them only while the target cluster is stopped and
the target data directory is empty.

```sh
mycel admin user-backup validate --file user.tar.zst
```

```sh
mycel admin activity list --category lifecycle
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
