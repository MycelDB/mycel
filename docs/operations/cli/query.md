# `mycel query`

Run daemon graph queries.

Authentication mode: **principal**.

## Common tasks

- Query nodes with filters.
- Execute GQL against a space/domain.

## Examples

In the REPL, connect once and run GQL without repeating IDs:

```text
mycel> login alice secret
mycel> connect space Notes
mycel[Notes/default]> gql MATCH (n) RETURN n FETCH FIRST 10 ROWS ONLY
```

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
