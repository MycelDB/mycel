# `mycel user`

Compatibility aliases for managing human principals.

Authentication mode: **principal auth with admin identity capabilities**.

## Common tasks

- Create/find/list/get human principals using legacy user-oriented flags.
- Disable/enable/delete principals.
- Set passwords and manage sessions.

## Examples

```sh
mycel user add --user-username alice --new-password secret
# Prefer for new automation:
mycel principal create --principal-username alice --new-password secret --login-enabled
```

```sh
mycel user find --user-username alice
# Prefer for new automation:
mycel principal find --principal-username alice
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
