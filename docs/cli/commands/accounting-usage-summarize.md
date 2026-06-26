# `mycel accounting usage summarize`

Summarizes model endpoint token usage for a time period.

## Examples

```sh
mycel accounting usage summarize --from 2026-06-01 --to 2026-06-30 --user martin
mycel accounting usage summarize --from 2026-06-01 --to 2026-06-30 --space personal-pkm --domain personal-pkm --group-by user,operation,model-endpoint
mycel accounting usage summarize --from 2026-06-01 --to 2026-06-30 --node <node_id>
```

## Common filters

```text
--user <user-id-or-ref>
--space <space-id-or-key>
--domain <domain-id-or-key>
--node <node-id>
--semantic-index <index-id-or-key>
--operation content_embedding|query_embedding|chat|summarize|rerank|classify
--model-endpoint <endpoint-id-or-key>
--model <model-id-or-key>
--credential-grant <grant-id>
--status success|failed|cancelled
```

## Common groupings

```text
--group-by user
--group-by space,domain
--group-by operation,model-endpoint,model
--group-by node
```

## Output metrics

```text
call_count
success_count
failed_count
input_tokens
output_tokens
total_tokens
provider_reported_tokens
estimated_tokens
unavailable_token_count
```
