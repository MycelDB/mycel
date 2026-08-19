# `mycel auth`

Principal login, refresh, logout, whoami, self-access discovery, and auth-session management.

Authentication mode: **principal**.

## Common tasks

- Login and refresh principal auth sessions.
- Inspect current-principal effective roles and capabilities.
- List or revoke auth sessions.

## Examples

```sh
mycel auth whoami
```

```sh
mycel auth access -u alice -p alice-password
```

```sh
mycel auth access -u alice -p alice-password --scope domain --space-id sp_main --domain-id dom_default --output json
```

```sh
mycel auth session list
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
