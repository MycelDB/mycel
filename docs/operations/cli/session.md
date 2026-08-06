# `mycel session`

Manage graph sessions and transactions.

Authentication mode: **user**.

## Common tasks

- Open/get/heartbeat/close sessions.
- Begin/get/commit/rollback/close transactions.

## Examples

```sh
mycel session open --space-id <space-id> --domain-id <domain-id>
```

```sh
mycel session transaction begin <session-id> --mode read-write
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
