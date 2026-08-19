# `mycel automation`

Manage graph automation definitions, invocations, and run records.

Authentication mode: **user**.

Automations reference inference profiles and model/capability refs. They do not
embed provider API keys, raw endpoint URLs, or credential secret refs.

## Domain scope

Prefer space plus domain refs:

```sh
--space-id <space-id> --domain default
```

`--domain` accepts a domain key or UUID when `--space-id` is provided. A bare
UUID still works for compatibility. `--domain-id` is also accepted for scripts
that already store the resolved domain UUID.

## Definition commands

```sh
mycel automation validate examples/automations/summarize_page.json

mycel automation create examples/automations/summarize_page.json \
  --space-id <space-id> \
  --domain default

mycel automation update summarize_page examples/automations/summarize_page.json \
  --space-id <space-id> \
  --domain default

mycel automation list --space-id <space-id> --domain default
mycel automation get summarize_page --space-id <space-id> --domain default
mycel automation enable summarize_page --space-id <space-id> --domain default
mycel automation disable summarize_page --space-id <space-id> --domain default
mycel automation delete summarize_page --space-id <space-id> --domain default
```

`mycel automation put` remains available as a create-or-update compatibility
command; prefer `create` and `update` in new scripts.

Use `--output json` with list and run-inspection commands when machine-readable
responses are needed.

If `condition.gql` is omitted, the automation treats the trigger as matched and
uses the triggering node as `changed`. Keep `condition.gql` when the automation
needs an additional GQL guard beyond the `on` event/label filter.

## Graph-context automations

Conditions can return node aliases in addition to `changed`. Those aliases can be
used as input targets and `update_node.target` values. For example, a
`JournalEntry` trigger can select its parent `journal`:

```json
"condition": {
  "gql": "MATCH (journal:Journal)-[r:HAS_ENTRY]->(changed:JournalEntry) RETURN changed, journal"
},
"input": {
  "target": "journal",
  "mode": "gql_template",
  "context": {
    "entries": {
      "gql": "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry ORDER BY r.properties.position FETCH FIRST 200 ROWS ONLY",
      "limit": 200
    }
  },
  "template": "Date: {{journal.properties.date}}\n{{#each entries}}- {{entry.payload.text}}\n{{/each}}"
}
```

Context queries are read-only and must be bounded in GQL with `FETCH FIRST`;
the optional `limit` field further caps accepted rows. `gql_template`
supports scalar interpolation plus `{{#each name}}...{{/each}}` loops over named
context result sets. Target-scoped
idempotency and debounce/coalescing are available under `safety.idempotency` and
`safety.debounce`; see `examples/automations/summarize_daily_journal.json`.

Run records expose graph-context diagnostics including `target_alias`,
`target_node_id`, per-context row counts, and coalesced invocation IDs when a
pending invocation is skipped in favor of a newer one.

## Runs and invocations

```sh
mycel automation runs \
  --space-id <space-id> \
  --domain default \
  --automation summarize_page \
  --status failed \
  --limit 20

mycel automation run get <run-id> --space-id <space-id> --domain default

mycel automation invocation retry <invocation-id> --space-id <space-id> --domain default
mycel automation invocation cancel <invocation-id> --space-id <space-id> --domain default
```

Run records include neutral inference provenance such as profile, capability,
credential grant, policy decision, provider request ID, token usage, actor,
on-behalf-of, and automation owner references.

## Related docs

- [CLI index](README.md)
- [Inference CLI](inference.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
