# `mycel automation binding`

Manage graph automation bindings.

Authentication mode: **user**.

A binding connects a reusable procedure to trigger/schedule/manual runtime
configuration. It defines scope, trigger conditions, runtime actor and
on-behalf-of principal context, debounce, and idempotency. The referenced
procedure defines the reusable work.

Canonical command path:

```sh
mycel automation binding ...
```

Compatibility aliases remain available for existing scripts:

```sh
mycel automation-binding ...
mycel automation-bindings ...
```

Broad top-level aliases such as `mycel binding` and `mycel bindings` are
compatibility aliases only. Do not use them in new docs or scripts.

## Domain scope

Most commands require a domain. Prefer:

```sh
--space-id <space-id> --domain default
```

`--domain` accepts a domain key or UUID when `--space-id` is provided.
`--domain-id` remains available for compatibility but is deprecated.

## Validate

Local validation checks JSON shape and binding model rules without contacting the
daemon:

```sh
mycel automation binding validate examples/automation-bindings/page-summary-user.json
```

Server validation resolves the domain and checks daemon state, including the
referenced procedure. With `--output json`, it prints normalized binding JSON:

```sh
mycel --output json automation binding validate examples/automation-bindings/page-summary-user.json \
  --server \
  --space-id <space-id> \
  --domain default
```

Use server validation before `put` in provisioning scripts.

## Create, update, and put

Prefer `put` for idempotent provisioning:

```sh
mycel automation binding put examples/automation-bindings/page-summary-user.json \
  --space-id <space-id> \
  --domain default
```

`put` creates the binding when it does not exist and updates it when it does. Use
`--id` to override or supply the JSON `id` field:

```sh
mycel automation binding put binding.json \
  --id knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
```

Explicit create/update commands remain available:

```sh
mycel automation binding create binding.json --space-id <space-id> --domain default
mycel automation binding update knot-pkm.user.example.page-summary.entry-trigger binding.json \
  --space-id <space-id> \
  --domain default
```

## Inspect, enable, disable, and delete

```sh
mycel automation binding list --space-id <space-id> --domain default
mycel automation binding get knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
mycel automation binding enable knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
mycel automation binding disable knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
mycel automation binding delete knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
```

Use `--output json` for machine-readable responses.

## Minimal graph-event binding structure

```json
{
  "id": "knot-pkm.user.example.page-summary.entry-trigger",
  "name": "Summarize pages when entries change",
  "procedure_id": "knot-pkm.page-summary",
  "procedure_version": 1,
  "status": "enabled",
  "scope": {
    "space_id": "<space-id>",
    "domain_id": "<domain-id>"
  },
  "trigger": {
    "type": "graph_event",
    "events": ["node.created", "node.updated"],
    "labels": ["pkm.page_entry"],
    "condition": {
      "gql": "MATCH (page:pkm.page)-[:contains*1..2]->(changed:pkm.page_entry) RETURN changed, page"
    }
  },
  "runtime": {
    "actor_principal_id": "automation",
    "owner_principal_id": "<owner-principal-id>",
    "on_behalf_of_principal_id": "<owner-principal-id>",
    "inference_profile": "page-summary"
  },
  "debounce": {
    "duration": "45s",
    "coalesceBy": "page"
  },
  "idempotency": {
    "scope": "target",
    "target": "page",
    "skipIfOutputUnchanged": true
  }
}
```

## Runtime principal and grants

The automation worker normally runs as the built-in `automation` actor. When the
binding uses `runtime.on_behalf_of_principal_id`, inference credential grants
must explicitly allow that on-behalf-of principal for usage mode `automation` and
the procedure operation. See [Inference CLI](inference.md).

Server validation catches structural and daemon-state problems such as a missing
referenced procedure. Missing credential grants or access policies are enforced
when the automation attempts to execute.

## Related docs

- [`mycel automation`](automation.md)
- [`mycel automation procedure`](procedure.md)
- [Inference CLI](inference.md)
