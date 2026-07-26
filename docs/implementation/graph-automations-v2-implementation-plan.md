# Graph automations V2 implementation plan

## Goal

Graph Automations V2 turns the V1 execution foundation into a useful AI automation subsystem. V2 keeps the same safety posture—async, durable, schema-aware, and constrained—but replaces V1 placeholder execution with provider-backed generation, real condition evaluation, richer rendering, structured output validation, and a minimal graph-expanding action set.

V2 should remain narrower than a general workflow engine. It should support common enrichment and extraction use cases without allowing arbitrary server-side code or unbounded graph scans/mutations.

## Current baseline

V1 provides:

- canonical JSON automation definitions
- client/admin proto APIs and generated SDK clients
- CLI CRUD/list/run inspection commands
- file-backed definitions, invocations, and runs
- change-stream ingestion for `node.created` and `node.updated`
- label/event prefiltering
- async polling worker
- changed-node field rendering
- deterministic placeholder output
- changed-node update action
- self-write metadata and loop prevention
- initial mycel-admin Tauri command/service wrappers

V2 should build incrementally on this without changing the V1 storage layout unnecessarily.

## V2 scope

Supported in V2:

- real GQL condition evaluation with `changed` and `old` bindings
- schema-aware field path validation for inputs and outputs
- provider-backed text generation through the existing inference/provider configuration surface
- token usage accounting for every provider-backed attempt
- estimated LLM cost accounting for every provider-backed attempt
- deterministic fake provider for tests
- configurable rendering modes for changed node and matched graph context
- structured JSON output mode with JSON Schema validation
- action set:
  - update changed node field
  - create node
  - create edge
  - upsert edge between existing nodes
- bounded graph context from GQL result rows
- idempotency based on automation ID, version, changed element, action fingerprint, and input hash
- retry/backoff and max-attempt handling
- worker operational configuration
- admin UI for list/detail/status/enable-disable/run inspection
- docs and examples for summarization, classification, and entity extraction

Not supported in V2:

- synchronous transaction hooks
- arbitrary imperative code/plugins inside automation definitions
- unbounded graph rewrites
- scheduled/time-based automations
- multi-step workflow DAGs
- edge-triggered automations beyond action targets
- external webhooks/actions
- human approval gates
- cross-domain mutations

## Definition model changes

Keep canonical JSON as the API/storage format. Extend the existing model rather than replacing it.

### Condition

V2 condition syntax remains GQL:

```json
"condition": {
  "gql": "MATCH (changed:Page)-[:MENTIONS]->(p:Person) RETURN changed, collect(p) AS people"
}
```

Requirements:

- condition must reference `changed`
- planner/executor receives reserved bindings:
  - `changed`: new node state
  - `old`: previous node state when available
- condition execution must have a max row/time/cost limit
- only successful non-empty result sets proceed to rendering
- false/empty condition records skipped invocation with `condition_false`

### Input rendering

Support two rendering modes:

```json
"input": {
  "target": "changed",
  "fields": ["properties.title", "payload.text"],
  "mode": "markdown"
}
```

and context-aware rendering:

```json
"input": {
  "mode": "template",
  "template": "# {{changed.properties.title}}\n\n{{changed.payload.text}}\n\nPeople: {{#each people}}{{properties.name}}{{/each}}"
}
```

V2 should keep templating deliberately small:

- dotted paths
- scalar formatting
- arrays/collections from GQL result aliases
- no arbitrary functions beyond safe built-ins such as `json`, `join`, and `default`

### Model/provider

Extend model config:

```json
"model": {
  "provider": "openai",
  "model": "gpt-4o-mini",
  "temperature": 0.2,
  "maxOutputTokens": 800
}
```

Provider resolution should use existing inference/provider configuration where possible. If no provider is configured, execution fails with a clear retryable/non-retryable error depending on configuration state.

### Output

Support text and JSON output modes:

```json
"output": {
  "mode": "json",
  "schema": {
    "type": "object",
    "properties": {
      "summary": {"type": "string"},
      "topics": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["summary"]
  },
  "actions": []
}
```

The provider response must be parsed and validated before actions run.

### Actions

V2 action set:

#### update_node

Already exists for `changed`; V2 validates field paths against domain schema.

```json
{
  "update_node": {
    "target": "changed",
    "set": {
      "payload.summaryMarkdown": "$result.summary"
    }
  }
}
```

#### create_node

```json
{
  "create_node": {
    "as": "topic",
    "labels": ["Topic"],
    "properties": {
      "name": "$item"
    },
    "for_each": "$result.topics"
  }
}
```

#### create_edge

```json
{
  "create_edge": {
    "from": "changed",
    "to": "$refs.topic",
    "label": "HAS_TOPIC"
  }
}
```

#### upsert_edge

Same shape as `create_edge`, but dedupes by `(from, to, label)`.

V2 should reject actions that cannot be statically bounded. `for_each` arrays must have configurable max item limits.

## Architecture workstreams

### 1. Model and validation

Files/packages:

```text
internal/automation/model
internal/automation/service
```

Tasks:

- extend `AutomationDefinition` for rendering mode, structured output, provider params, and new actions
- add validation for:
  - allowed event types
  - `changed`-anchored condition
  - bounded action count
  - valid action target refs
  - max `for_each` limit
  - schema-aware field paths when domain schema is available
- add compatibility tests for existing V1 definitions

Acceptance:

- valid V1 definitions still validate
- invalid unbounded/unsafe V2 definitions are rejected
- tests cover each action type and output mode

### 2. GQL condition execution

Files/packages:

```text
internal/automation/service
internal/query/gql/execution
internal/session/service
```

Tasks:

- add an automation condition executor wrapper
- bind `changed` and `old` values into query evaluation
- enforce max rows, max duration, and read-only execution
- map false/empty result to skipped invocation
- persist condition diagnostics in run metadata

Acceptance:

- matching condition proceeds
- false condition skips without provider call
- broad/unanchored condition fails validation or execution limits
- condition result aliases are available to renderers

### 3. Renderer V2

New package:

```text
internal/automation/render
```

Tasks:

- move V1 field rendering out of `service/execute.go`
- implement markdown field renderer
- implement minimal safe template renderer
- support changed/old/GQL alias path lookup
- compute canonical rendered input and input hash
- add redaction hook for future secrets/private fields

Acceptance:

- stable rendering output for deterministic tests
- arrays and scalar paths render predictably
- missing optional paths render empty/default, while required paths fail clearly

### 4. Provider integration and token accounting

New package:

```text
internal/automation/provider
```

Tasks:

- define provider interface:
  - `GenerateText(ctx, request) (response, error)`
  - optional `GenerateJSON(ctx, request) (response, error)` or JSON mode via text
- implement fake provider for tests
- adapt existing inference/provider config to automation provider resolution
- record provider/model/token/error metadata in run records
- require provider responses to expose usage when the provider reports it:
  - input tokens
  - output tokens
  - total tokens
  - cached input tokens, when available
  - reasoning tokens, when available
  - sanitized provider-specific usage metadata
- support `usage_status` values such as `reported`, `estimated`, and `unavailable`
- classify errors as retryable/non-retryable

Acceptance:

- tests can inject fake provider
- fake provider can return deterministic token counts
- missing provider yields useful failure status
- provider response is persisted/audited without storing secrets
- every successful provider-backed attempt records input/output/total token fields or an explicit unavailable status

### 5. Cost accounting

New package:

```text
internal/automation/accounting
```

Tasks:

- define pricing model keyed by provider/model/pricing version
- compute estimated input/output/total cost from recorded token usage
- persist cost fields on run/attempt records:
  - input cost
  - output cost
  - total cost
  - currency
  - pricing source/version
  - estimation status
- keep cost calculation separate from provider invocation
- expose unavailable/unknown pricing explicitly instead of silently reporting zero
- add aggregation helpers for future per-domain/user budget reporting

Acceptance:

- run records include token and estimated cost fields
- unknown pricing records `cost_status: unavailable`
- pricing changes can be versioned without rewriting old run records
- tests cover normal pricing, cached input pricing, and unknown pricing

### 6. Structured output validation

New package:

```text
internal/automation/output
```

Tasks:

- parse provider result for text/json modes
- validate JSON output against definition schema
- support result path resolution (`$result.summary`, `$result.topics[]`)
- fail before mutation when output is invalid

Acceptance:

- invalid JSON output records failed run and no graph mutation
- valid JSON output feeds action engine
- output hashes are stable and recorded

### 7. Action engine

New package:

```text
internal/automation/actions
```

Tasks:

- move changed-node update mutation into action engine
- implement create node
- implement create edge
- implement upsert edge
- enforce per-run mutation limits
- tag all generated nodes/edges with automation metadata
- record mutation summary and transaction/revision references
- add dry-run planner for admin preview/testing

Acceptance:

- action engine applies all V2 action types in one graph transaction
- failures roll back the transaction
- self-write metadata suppresses recursive loops
- schema validation rejects invalid node/edge shapes

### 8. Idempotency and dedupe

Tasks:

- formalize idempotency keys:
  - invocation key: `(automation_id, version, event_id)`
  - execution key: `(automation_id, version, changed_element_id, input_hash)`
  - action fingerprint for create/upsert actions
- persist last successful input/output hashes per changed element where needed
- skip duplicate executions before provider calls
- make upsert edge deterministic

Acceptance:

- same input hash does not call provider twice when skip policy is enabled
- repeated create-node action with same result does not create unbounded duplicates when an upsert key is configured
- duplicate event delivery is safe

### 9. Worker operations

Tasks:

- add daemon config:
  - automation worker enabled
  - interval
  - batch size
  - max concurrency
  - per-run timeout
  - condition timeout
  - max rendered input bytes
  - max output bytes
  - optional per-domain/provider/model token and cost ceilings
- add graceful shutdown and in-flight cancellation
- add metrics/logging counters for queued/running/succeeded/failed/skipped
- add retry/backoff scheduling fields to invocation/run records

Acceptance:

- worker can be disabled in config
- failed retryable invocations retry with backoff up to max attempts
- shutdown does not corrupt records

### 10. API/SDK/CLI updates

#### mycel-api

Add or extend messages for:

- validation result / dry run
- invocation detail
- run detail with condition/render/provider/token/cost/action summaries
- retry/cancel invocation if feasible

Potential new RPCs:

```text
ValidateAutomation
PreviewAutomation
RetryAutomationInvocation
CancelAutomationInvocation
```

#### mycel-go-sdk

- regenerate protos
- expose helper methods for validate/preview/retry/cancel

#### mycel-rust-sdk

- update generated protos/submodule source
- expose new service clients or helpers

#### CLI

Add:

```sh
mycel automation validate --domain <domain-id> automation.json
mycel automation preview --domain <domain-id> automation.json --changed <node-id>
mycel automation invocation get --domain <domain-id> <invocation-id>
mycel automation invocation retry --domain <domain-id> <invocation-id>
```

Acceptance:

- API additions pass buf lint
- Go/Rust SDK tests pass
- CLI validates and previews definitions without mutating graph

### 11. Admin UI V2

Files/packages:

```text
mycel-admin/src/features/spaces/pages/SpaceDetailPage.tsx
mycel-admin/src/services/adminService.ts
mycel-admin/src/types/automations.ts
mycel-admin/src-tauri/src/commands/automations.rs
```

Tasks:

- add Automations tab to space/domain detail
- list domain automations
- view definition JSON
- enable/disable automation
- list recent invocations
- show run details:
  - status
  - changed element
  - condition result
  - rendered input hash
  - provider/model
  - input/output/total tokens
  - estimated cost/currency/pricing version
  - output hash
  - mutation summary
  - error details
- optional JSON editor for create/update if low risk
- add tests for UI states and service invocations

Acceptance:

- admin can inspect and toggle automations without CLI
- failed/skipped runs are understandable from UI
- frontend tests and Tauri cargo check pass

### 12. Documentation and examples

Tasks:

- update `docs/graph-automations.md`
- update `docs/design/graph-automations.md` if V2 decisions alter design
- add examples:
  - summarize page
  - classify note into topics
  - extract entities and create links
  - image/blob description if multimodal is pulled forward
- add operational guide for provider config, costs, retry, and safety

Acceptance:

- examples validate via CLI
- docs clearly distinguish V1 and V2 capabilities

## Suggested implementation phases

### Phase 1: V2 model, validation, and tests

- Extend definition structs and validation.
- Preserve V1 compatibility.
- Add validation tests for structured output and action types.

### Phase 2: condition and rendering runtime

- Implement real GQL condition execution.
- Implement renderer package and context alias rendering.
- Update worker to skip on false condition before provider call.

### Phase 3: provider boundary, token accounting, and cost accounting

- Add provider interface and fake provider.
- Wire deterministic fake provider into tests.
- Add run metadata for provider attempts.
- Record input/output/total tokens for every provider-backed attempt.
- Compute estimated costs through versioned pricing metadata.

### Phase 4: structured output and action engine

- Parse/validate JSON output.
- Implement action engine for update/create/upsert.
- Enforce mutation limits and transaction rollback.

### Phase 5: idempotency, retries, and operations

- Add idempotency records/indexes.
- Add retry/backoff.
- Add daemon config and metrics/logging.

### Phase 6: API/SDK/CLI polish

- Add validate/preview/retry/cancel APIs if still needed.
- Regenerate SDKs.
- Expand CLI commands.

### Phase 7: admin UI and docs

- Add admin UI tab and run detail view.
- Add docs and examples.
- Run full cross-repo validation.

## Test plan

### Unit tests

- definition validation
- GQL condition binding and limit handling
- renderer path/template behavior
- provider fake success/failure
- token usage mapping and unavailable usage status
- cost estimation with known and unknown pricing
- JSON output validation
- action planning and fingerprints
- idempotency key generation

### Integration tests

- node create triggers automation
- label mismatch does not trigger
- false condition records skipped invocation
- provider success records tokens/cost and updates changed node
- structured output creates node and edge
- duplicate event does not duplicate mutation
- automation self-write does not recurse indefinitely
- retryable provider failure retries and then succeeds
- non-retryable validation failure does not retry

### Cross-repo validation

```sh
cd myceldb/mycel-api && make test
cd myceldb/mycel && go test ./...
cd myceldb/mycel-go-sdk && go test ./...
cd myceldb/mycel-rust-sdk && MYCEL_API_ROOT=/path/to/mycel-api cargo test
cd myceldb/mycel-admin && npm test -- --runInBand
cd myceldb/mycel-admin/src-tauri && MYCEL_API_ROOT=/path/to/mycel-api cargo check
```

## Rollout and migration

- V1 definitions remain valid.
- New V2 fields are optional unless required by a selected mode/action.
- Storage layout remains compatible; add fields to JSON records rather than migrating directories.
- Provider-backed execution should be opt-in by daemon config until stable.
- Admin UI should mark placeholder/deterministic executions distinctly if mixed V1/V2 state exists.

## Open decisions

- Which existing inference/provider abstraction should automation use directly versus wrapping?
- Should provider output be stored verbatim, redacted, or referenced through blob/file storage?
- Should `create_node` require an upsert key in V2 to prevent duplicates?
- How much of JSON Schema should be supported for output validation initially?
- Should V2 include edge triggers, or reserve them for V3?
- Should preview execute against a temporary transaction/session or a read-only synthetic context?
