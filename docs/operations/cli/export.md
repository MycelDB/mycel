# `mycel export`

Export Mycel data through daemon gRPC.

Authentication mode: **user**.

## Common tasks

- Export domain content from a readable transaction.
- Include blobs when requested.

## Examples

```sh
mycel export domain --transaction-id <tx-id> --file domain.json --include-blobs
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
