# `mycel inference`

Configure mycel inference subsystem catalog resources, credentials, grants,
profiles, policies, decisions, and usage telemetry.

Authentication mode: **operator**.

## Resource commands

- `mycel inference package ...` (`packages` alias)
- `mycel inference endpoint ...` (`model-endpoint` compatibility alias)
- `mycel inference model ...`
- `mycel inference capability ...`
- `mycel inference vector-store ...`
- `mycel inference credential ...`
- `mycel inference grant ...`
- `mycel inference profile ...`
- `mycel inference policy ...`
- `mycel inference decision ...`
- `mycel inference usage ...`

List commands support `--page-size`, `--page-token`, and `--output json` where
paging is exposed by the daemon API.

## Common catalog tasks

```sh
mycel inference package apply examples/inference/standard-openai-embeddings.json
mycel inference package list

mycel inference endpoint list
mycel inference endpoint disable openai
mycel inference endpoint enable openai

mycel inference model list --operation embeddings
mycel inference capability list --operation embeddings
mycel inference vector-store list
```

## Credentials and grants

Credential output is redacted; API key values are not printed after submission. Prefer stdin so the key is not captured in shell history.

```sh
printf '%s' "$OPENAI_API_KEY" | mycel inference credential create openai-key \
  --model-endpoint openai \
  --owner-type system \
  --owner-id system \
  --secret-stdin

printf '%s' "$OPENAI_API_KEY_V2" | mycel inference credential rotate openai-key \
  --secret-stdin

mycel inference credential list --owner-type system
mycel inference credential disable openai-key
mycel inference credential revoke openai-key
```

Grant a credential to a scoped workload. For graph automations, add explicit
on-behalf-of principals when the automation worker acts for another principal.

```sh
mycel inference grant openai-key \
  --space-id <space-id> \
  --domain default \
  --operation chat \
  --model-endpoint openai \
  --model gpt-4o-mini \
  --grantee-principal-id automation \
  --allow-on-behalf-of-principal-id <owner-principal-id>

mycel inference grant list --space-id <space-id>
mycel inference grant expire <grant-id> --space-id <space-id>
mycel inference grant delete <grant-id> --space-id <space-id>
```

`mycel inference credential grant ...` remains as a compatibility alias for
older scripts; prefer top-level `mycel inference grant ...`.

## Profiles and policies

```sh
mycel inference profile create summarize-page \
  --space-id <space-id> \
  --operation chat \
  --purpose automation \
  --domain default \
  --model gpt-4o-mini \
  --privacy-class third_party

mycel inference profile list --space-id <space-id> --purpose automation
mycel inference profile get summarize-page --space-id <space-id>
mycel inference profile disable summarize-page --space-id <space-id>
mycel inference profile enable summarize-page --space-id <space-id>

mycel inference policy allow \
  --space-id <space-id> \
  --domain default \
  --operation chat \
  --privacy-class third_party \
  --reason "automation summaries allowed"

mycel inference policy deny --space-id <space-id> --domain default --operation chat
mycel inference policy list --space-id <space-id>
mycel inference policy expire <policy-id> --space-id <space-id>
```

## Decisions and usage

```sh
mycel inference decision get <policy-decision-id> --space-id <space-id>

mycel inference usage list \
  --space-id <space-id> \
  --domain default \
  --usage-mode automation \
  --status succeeded \
  --page-size 50

mycel inference usage summarize \
  --space-id <space-id> \
  --domain default \
  --group-by operation \
  --group-by usage_mode
```

Usage telemetry is provider-neutral and contains token counts, latency, statuses,
and resource references only. It does not contain pricing, credits, margins, or
billing data.

## Related docs

- [CLI index](README.md)
- [Automation CLI](automation.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
