# `mycel transaction`

Transaction helper commands.

Authentication mode: **user**.

## Common tasks

- Begin, inspect, commit, rollback, or close graph transactions.

## Examples

```sh
mycel transaction begin <session-id> --mode read-write
```

```sh
mycel transaction commit <tx-id>
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
