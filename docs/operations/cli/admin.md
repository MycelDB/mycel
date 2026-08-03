# `mycel admin`

Operator/admin management surfaces.

Authentication mode: **operator**.

## Common tasks

- List/create/update operators and users.
- Create/delete spaces as an operator.
- Run admin backup and user-scoped backup/restore commands.

## Examples

```sh
mycel --output json admin list
```

```sh
mycel admin user-backup validate --file user.tar.zst
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
