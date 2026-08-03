# `mycel query`

Run daemon graph queries.

Authentication mode: **user**.

## Common tasks

- Query nodes with filters.
- Execute GQL against a space/domain.

## Examples

```sh
mycel query nodes --transaction-id <tx-id> --label Note
```

```sh
mycel query gql --space-id <space-id> --domain <key> "MATCH ..."
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
