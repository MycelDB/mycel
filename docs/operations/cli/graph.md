# `mycel graph`

Manage graph nodes and edges through daemon transactions.

Authentication mode: **user**.

## Common tasks

- Create/get/list/update/delete nodes.
- Create/get/list/delete edges.
- Create blob-backed nodes.

## Examples

```sh
mycel graph node create --transaction-id <tx-id> --content "hello"
```

```sh
mycel graph blob-node create file.txt --transaction-id <tx-id>
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
