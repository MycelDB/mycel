# `mycel admin`

Operator/admin management surfaces.

Authentication mode: **operator**.

## Common tasks

- List/create/update operators and users.
- Create/delete spaces as an operator.
- Run node-local daemon backup, raft-storage-safe coordinated cluster backup, and user-scoped backup/restore commands.

## Examples

```sh
mycel --output json admin list
```

```sh
mycel admin backup cluster trigger \
  --reason "before maintenance" \
  --output-dir /mnt/mycel-backups \
  --archive-format tar.zst
```

```sh
mycel admin backup cluster validate --backup-set /mnt/mycel-backups
```

Coordinated cluster backup creates one backup set with one archive per pod/PVC,
records raft barriers, requires raft freeze/checkpoint evidence in the final
`backup-set.json`, and keeps full-system restore offline/operator-driven.

```sh
mycel admin user-backup validate --file user.tar.zst
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
