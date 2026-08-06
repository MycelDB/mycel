# `mycel user`

Manage standard users through the operator API.

Authentication mode: **operator**.

## Common tasks

- Create/find/list/get users.
- Disable/enable/delete users.
- Set passwords and manage sessions.

## Examples

```sh
mycel user add --user-username alice --new-password secret
```

```sh
mycel user find --user-username alice
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
