# `mycel blob`

Manage raw blob content through daemon gRPC.

Authentication mode: **user**.

## Common tasks

- Upload raw blobs.
- Read metadata, download content, or delete unreferenced blobs.

## Examples

```sh
mycel blob upload file.txt --space-id <space-id>
```

```sh
mycel blob download <blob-id> --space-id <space-id>
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
