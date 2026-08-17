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

Parameterized and aliased GQL:

```sh
mycel query gql --space-id <space-id> --domain <key> \
  --param name=Levi \
  'MATCH (p:Person {name: $name}) RETURN p.name AS name'
```

Read-write GQL prints returned rows and mutation counters:

```sh
mycel query gql --space-id <space-id> --domain <key> \
  "MERGE (p:Person {name: 'Levi'}) RETURN p.name AS name"
```

Path binding and graph projection:

```sh
mycel query gql --space-id <space-id> --domain <key> \
  "MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person) RETURN GRAPH path"
```

Explain a GQL query without executing graph reads or mutations:

```sh
mycel query gql --space-id <space-id> --domain <key> --explain \
  "MATCH (j:JournalEntry) RETURN j ORDER BY j.date"
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
