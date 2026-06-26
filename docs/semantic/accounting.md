# Inference Accounting

Every model endpoint call made by Mycel must produce an auditable accounting record.

Accounting is separate from graph data. Graph data is mutable user content; accounting records are append-only operational records used to answer:

```text
who used how many tokens, when, for what operation, in which space/domain/node scope, against which model endpoint/model, using which credential grant?
```

## Authoritative Ledger

The authoritative accounting store is an append-only usage ledger, not graph nodes.

Recommended storage path:

```text
meta/accounting/
  manifest.json
  inference-usage-000001.kusag
  inference-usage-000002.kusag
```

The ledger is authoritative. Secondary indexes and rollups are derived and rebuildable.

## Usage Event

Every successful or failed model endpoint call should append one `InferenceUsageEvent`.

Examples of accounted calls:

- content embedding generation
- query embedding generation
- chat completion
- summarization
- reranking
- classification
- background backfill
- dirty refresh
- any future model endpoint operation

Conceptual fields:

```text
id
call_id
request_id
created_at
status                  # success, failed, cancelled
operation               # content_embedding, query_embedding, chat, summarize, rerank, classify
reason                  # user_request, backfill, dirty_refresh, semantic_cleanup, import, manual

actor_principal_id      # authenticated session principal or worker principal
effective_principal_id  # principal that executed the call
on_behalf_of_principal_id

space_id
domain_id
semantic_index_id
target_node_id
source_node_ids

model_endpoint_id
model_endpoint_key
model_id
model_key
model_endpoint_capability_id

credential_id
credential_grant_id
policy_decision_id

input_tokens
output_tokens
total_tokens
token_count_source      # provider_reported, estimated, unavailable
provider_request_id
error_code
error_message
metadata
```

Do not store raw prompts or graph content in the accounting ledger by default. Store identifiers, operation context, provenance, token counts, status, and optional hashes/metadata. Prompt/content traces should be a separate opt-in debugging facility if needed later.

## Principal Attribution

Interactive calls use the authenticated session principal:

```text
actor_principal_id = session user
effective_principal_id = session user
```

Background semantic maintenance uses a worker principal and a space-owned credential grant:

```text
actor_principal_id = semantic-maintenance-worker
effective_principal_id = semantic-maintenance-worker
on_behalf_of_principal_id = user or space owner represented by the grant
credential_grant_id = grant authorizing background use
```

This allows reports to attribute offline work to the user/space/grant that authorized it without requiring a live user session.

## Token Counts

Providers differ in token accounting support. Each event must identify the source of token counts:

```text
provider_reported   # preferred for accounting
estimated           # calculated by Mycel/provider adapter
unavailable         # provider did not report and no estimator was available
```

For embeddings, output tokens are usually `0`; input tokens and total tokens should still be recorded when available.

## Secondary Indexes

Usage reports must support filtering by user/principal, space, domain, node, operation, model endpoint, model, credential grant, and time period.

Recommended derived indexes:

```text
meta/accounting/indexes/
  by_principal/<principal_id>/YYYY-MM.kidx
  by_space/<space_id>/YYYY-MM.kidx
  by_domain/<domain_id>/YYYY-MM.kidx
  by_node/<node_id>/YYYY-MM.kidx
  by_operation/<operation>/YYYY-MM.kidx
  by_model_endpoint/<model_endpoint_id>/YYYY-MM.kidx
  by_model/<model_id>/YYYY-MM.kidx
  by_credential_grant/<credential_grant_id>/YYYY-MM.kidx
```

Index entries point to ledger segment offsets. If indexes are missing or corrupt, Mycel can rebuild them by scanning `inference-usage-*.kusag`.

## Rollups

Rollups are optional derived summaries for fast dashboards and reports:

```text
meta/accounting/rollups/
  principal-monthly.json
  space-monthly.json
  domain-monthly.json
  endpoint-monthly.json
```

Rollups are not authoritative. They can be rebuilt from the ledger.

Typical summary fields:

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

## CLI Reporting

Accounting reports are exposed through CLI commands under `mycel accounting usage`.

See:

- [accounting usage summarize](../cli/commands/accounting-usage-summarize.md)
- [accounting usage events](../cli/commands/accounting-usage-events.md)
- [accounting usage export](../cli/commands/accounting-usage-export.md)
- [accounting usage rebuild-indexes](../cli/commands/accounting-usage-rebuild-indexes.md)
- [accounting usage rebuild-rollups](../cli/commands/accounting-usage-rebuild-rollups.md)
