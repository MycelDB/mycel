# `mycel semantic`

Manage semantic generation rules, semantic search, and rule maintenance.

Authentication mode: **mixed**.

## Common tasks

- Search enabled semantic rule bindings as a user.
- Manage semantic generation rules and maintenance as an operator.

## Rule lifecycle

```sh
mycel semantic rule list --space-id <space-id> --domain default
mycel semantic rule get notes-search --space-id <space-id> --domain default
mycel semantic rule validate --file semantic-rule.yaml
mycel semantic rule create --file semantic-rule.yaml
mycel semantic rule update notes-search --space-id <space-id> --file semantic-rule.yaml
mycel semantic rule disable notes-search --space-id <space-id>
mycel semantic rule enable notes-search --space-id <space-id>
mycel semantic rule delete notes-search --space-id <space-id> --purge-vectors
```

`create` and `validate` also support simple flags for common node-type rules,
but JSON/YAML files are preferred for repeatable operator workflows. Use
`--file -` to read a rule definition from stdin.

Minimal rule file:

```yaml
space_id: <space-id>
domain_id: <domain-id>
key: notes-search
display_name: Notes Search
enabled: true
trigger:
  events: [changed]
  labels: [Note]
selector:
  mode: node_type
  labels: [Note]
source:
  mode: self
embeddings:
  - key: search
    purpose: search
    intelligence_profile: embedding-default
    vector_store: mycel-file
    enabled: true
storage:
  searchable: true
  physical_index: exact
```

## Search

```sh
mycel semantic search \
  --space-id <space-id> \
  --domain default \
  --rule notes-search \
  --binding search \
  --text "query text"
```

If `--rule` is omitted, the daemon searches enabled searchable rule bindings
that the caller is allowed to read. If `--binding` is supplied, `--rule` should
also identify the rule binding to search.

## Maintenance and backfill

```sh
mycel semantic maintenance status --space-id <space-id>
mycel semantic maintenance list --space-id <space-id> --rule notes-search --binding search
mycel semantic maintenance analyze --space-id <space-id> --rule notes-search --binding search
mycel semantic maintenance process --space-id <space-id> --limit 10

mycel semantic rule backfill notes-search \
  --space-id <space-id> \
  --binding search \
  --limit 100
```

Backfill is explicit: choose the rule and embedding binding, and add `--force`
only when regenerating current embeddings is intended.

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
