# `mycel automation`

Manage graph automation definitions, invocations, and run records.

Authentication mode: **user**.

Automations reference inference profiles and model/capability refs. They do not
embed provider API keys, raw endpoint URLs, or credential secret refs.

The daemon supports splitting reusable graph procedures from runtime automation
bindings. The `mycel automation` CLI remains a compatibility surface for combined
automation definitions; use `mycel procedure` and `mycel automation-binding` for
first-class split-model management.

## Domain scope

Prefer space plus domain refs:

```sh
--space-id <space-id> --domain default
```

`--domain` accepts a domain key or UUID when `--space-id` is provided. A bare
UUID still works for compatibility. `--domain-id` is also accepted for scripts
that already store the resolved domain UUID.

## Procedure and binding commands

```sh
mycel procedure validate examples/procedures/page-summary.json
mycel procedure create examples/procedures/page-summary.json --space-id <space-id> --domain default
mycel procedure update page-summary examples/procedures/page-summary.json --space-id <space-id> --domain default
mycel procedure list --space-id <space-id> --domain default
mycel procedure get page-summary --space-id <space-id> --domain default
mycel procedure delete page-summary --space-id <space-id> --domain default

mycel automation-binding validate examples/automation-bindings/page-summary-user.json
mycel automation-binding create examples/automation-bindings/page-summary-user.json --space-id <space-id> --domain default
mycel automation-binding update page-summary-user examples/automation-bindings/page-summary-user.json --space-id <space-id> --domain default
mycel automation-binding list --space-id <space-id> --domain default
mycel automation-binding get page-summary-user --space-id <space-id> --domain default
mycel automation-binding enable page-summary-user --space-id <space-id> --domain default
mycel automation-binding disable page-summary-user --space-id <space-id> --domain default
mycel automation-binding delete page-summary-user --space-id <space-id> --domain default
```

Binding summaries show procedure refs, trigger type, and runtime
actor/on-behalf/owner fields. A binding can be created by an operator/user while
its runtime context executes later as the `automation` actor on behalf of a
configured principal, subject to inference grants and policies.

## Legacy definition commands

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

To migrate old combined definitions into explicit procedure+binding records while
keeping the legacy file readable:

```sh
mycel automation migrate-combined --space-id <space-id> --domain default --dry-run
mycel automation migrate-combined --space-id <space-id> --domain default
```

Migration prints the source automation ID, generated procedure ID, binding ID,
runtime owner/on-behalf principal, and a warning when the legacy owner looks like
an operator/admin principal. The generated binding uses the same ID as the legacy
automation, so runtime selection prefers the explicit binding and avoids duplicate
execution while the legacy definition remains available for compatibility.

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
on-behalf-of, and automation owner references. Newer run records may also include
`binding_id`, `procedure_id`, `owner_principal_id`, and
`event_origin_principal_id`.

Legacy combined automations still infer ownership from the creating principal for
compatibility. Procedure/binding-backed automations store runtime context on the
binding so an operator-created binding can execute as the built-in `automation`
actor on behalf of a user principal under explicit grants/policies.

## Related docs

- [CLI index](README.md)
- [Inference CLI](inference.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
