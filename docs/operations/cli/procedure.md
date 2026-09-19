# `mycel automation procedure`

Manage reusable graph automation procedures.

Authentication mode: **user**.

A procedure defines reusable graph work: input extraction/rendering, inference
operation/profile refs, prompt, output format, output actions, and safety limits.
It intentionally does **not** define triggers, scopes, or durable runtime
principal context. Those belong to an automation binding.

Canonical command path:

```sh
mycel automation procedure ...
```

Compatibility aliases remain available for existing scripts:

```sh
mycel procedure ...
mycel procedures ...
mycel graph-procedure ...
mycel graph-procedures ...
```

Use the canonical path in new docs and scripts.

## Domain scope

Most commands require a domain. Prefer:

```sh
--space-id <space-id> --domain default
```

`--domain` accepts a domain key or UUID when `--space-id` is provided.
`--domain-id` remains available for compatibility but is deprecated.

## Validate

Local validation checks JSON shape and procedure model rules without contacting
the daemon:

```sh
mycel automation procedure validate examples/procedures/page-summary.json
```

Server validation resolves the domain and applies daemon validation. With
`--output json`, it prints normalized procedure JSON:

```sh
mycel --output json automation procedure validate examples/procedures/page-summary.json \
  --server \
  --space-id <space-id> \
  --domain default
```

Use server validation before `put` in provisioning scripts.

## Create, update, and put

Prefer `put` for idempotent provisioning:

```sh
mycel automation procedure put examples/procedures/page-summary.json \
  --space-id <space-id> \
  --domain default
```

`put` creates the procedure when it does not exist and updates it when it does.
Use `--id` to override or supply the JSON `id` field:

```sh
mycel automation procedure put procedure.json \
  --id knot-pkm.page-summary \
  --space-id <space-id> \
  --domain default
```

Explicit create/update commands remain available:

```sh
mycel automation procedure create procedure.json --space-id <space-id> --domain default
mycel automation procedure update knot-pkm.page-summary procedure.json \
  --space-id <space-id> \
  --domain default
```

## Inspect and delete

```sh
mycel automation procedure list --space-id <space-id> --domain default
mycel automation procedure get knot-pkm.page-summary --space-id <space-id> --domain default
mycel automation procedure delete knot-pkm.page-summary --space-id <space-id> --domain default
```

Use `--output json` for machine-readable responses.

## Minimal structure

A typical procedure includes:

```json
{
  "id": "knot-pkm.page-summary",
  "name": "Summarize PKM page",
  "version": 1,
  "status": "enabled",
  "input": {
    "target": "page",
    "mode": "gql_template",
    "context": {
      "entries": {
        "gql": "MATCH (page)-[r:contains]->(entry:pkm.page_entry) RETURN entry, r FETCH FIRST 200 ROWS ONLY",
        "limit": 200
      }
    },
    "template": "{{page.properties.title}}\n{{#each entries}}{{entry.payload.text}}\n{{/each}}"
  },
  "inference": {
    "operation": "summarize",
    "profile": "page-summary"
  },
  "prompt": "Create a concise summary.",
  "output": {
    "mode": "text",
    "actions": [
      {
        "update_node": {
          "target": "page",
          "set": {
            "properties.summary": "$result.text"
          }
        }
      }
    ]
  }
}
```

The inference ref should point to a configured profile/model/capability path.
Credential grants and runtime principals are evaluated when a binding triggers
execution.

## Related docs

- [`mycel automation`](automation.md)
- [`mycel automation binding`](automation-binding.md)
- [Inference CLI](inference.md)
