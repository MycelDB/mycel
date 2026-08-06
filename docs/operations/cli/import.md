# `mycel import`

Import Mycel data through daemon gRPC.

Authentication mode: **user**.

## Common tasks

- Import domain JSON into a read-write transaction.
- Support append/upsert/replace-domain for domain import.

## Examples

```sh
mycel import domain --transaction-id <tx-id> --file domain.json --dry-run
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
