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
