# Graph automations V1 implementation plan

## Goal

Implement V1 of graph automations as defined in `docs/design/graph-automations.md`: constrained asynchronous node automations that react to committed graph changes, evaluate a `changed`-anchored GQL condition, render text from the changed node, call an LLM provider, and update a configured field on that same node with auditable run records.

V1 should be safe, schema-aware, and intentionally narrow. It should establish durable storage and worker architecture that can later support V2 graph context, structured output, multimodal input, and graph-expanding actions.

## V1 scope

Supported:

- `node.created` and `node.updated` triggers
- schema type / label prefilter
- GQL condition anchored on `changed`
- deterministic text input rendering from configured changed-node fields
- prompt + rendered input LLM call
- text output
- one action class: update a configured property or payload field on the changed node
- async worker execution after graph transaction commit
- durable automation definitions, invocations, runs, and attempts
- retry/failure state
- self-loop prevention for automation-generated writes
- idempotency by automation ID, element ID, and input hash
- admin/client APIs for definition CRUD and run inspection

Not supported in V1:

- edge triggers
- arbitrary graph writes
- create node / create edge actions
- updating matched context nodes
- structured JSON output actions
- multimodal/blob input
- synchronous execution
- graph-wide scan conditions
- user-supplied imperative code
- scheduling or multi-step workflows

## Architecture overview

Add packages under:

```text
internal/automation/model
internal/automation/storage
internal/automation/service
internal/automation/worker
internal/automation/render
internal/automation/actions
```

High-level flow:

```text
graph commit
  -> graph change event recorded
  -> automation service selects candidate definitions
  -> invocation record created or deduped
  -> worker evaluates GQL condition with changed bound
  -> worker renders input
  -> worker calls LLM provider
  -> worker applies allowed changed-node field update
  -> run/attempt/audit records finalized
```

V1 should reuse semantic maintenance design patterns where useful, but remain a separate subsystem because automations are user/application-defined and may mutate graph data.

## Data model

### AutomationDefinition

Fields:

- `id`
- `name`
- `version`
- `scope`:
  - domain ID or domain key
- `status`: `enabled | disabled`
- `trigger`:
  - events: `node.created | node.updated`
  - labels/types prefilter
- `condition`:
  - GQL text
- `input`:
  - target: only `changed` in V1
  - fields: list of field paths, e.g. `properties.title`, `payload.text`
  - optional renderer mode: plain text / markdown field list
- `model`:
  - provider/model reference, initially resolved through existing inference/provider config
- `prompt`
- `output`:
  - mode: `text`
  - update target: only `changed`
  - set path: one configured field path
- `safety`:
  - `ignoreSelfWrites` default true
  - input hash fields
  - max attempts
- timestamps and created/updated actor

### AutomationInvocation

One durable consideration for a specific event and automation definition.

Fields:

- `id`
- `automation_id`
- `automation_version`
- `event_id`
- `domain_id`
- `changed_element_id`
- `changed_element_kind`: `node`
- `event_type`
- `input_hash`, when known
- `status`: `pending | skipped | running | succeeded | failed | cancelled`
- `skip_reason`
- timestamps

### AutomationRun / Attempt

Fields:

- `id`
- `invocation_id`
- `attempt_number`
- `status`
- `rendered_input_hash`
- optional stored rendered input reference
- provider/model used
- output hash
- mutation transaction/reference if available
- error code/message
- started/completed timestamps

## Storage

Implement file-backed storage first, matching existing daemon file storage style.

Suggested layout under daemon data dir:

```text
automations/
  definitions/<domain-id>/<automation-id>.json
  invocations/<domain-id>/<yyyy-mm-dd>/<invocation-id>.json
  runs/<domain-id>/<yyyy-mm-dd>/<run-id>.json
```

Requirements:

- atomic file writes
- list definitions by domain/status/event/type/label filters
- create/dedupe invocation by `(automation_id, automation_version, event_id)`
- record run attempts and final status
- no cross-domain leakage

## API changes

### mycel-api

Add protobuf service definitions for automation management. Keep APIs language-neutral.

Initial client/admin API surface can be identical unless admin-only controls are required later:

- `CreateAutomation`
- `UpdateAutomation`
- `DeleteAutomation`
- `GetAutomation`
- `ListAutomations`
- `EnableAutomation`
- `DisableAutomation`
- `ListAutomationInvocations`
- `GetAutomationRun`

V1 API messages should use canonical JSON for automation definition bodies, similar to schema APIs, to avoid locking the public API to a YAML DSL.

### SDKs

Update generated clients and add small helpers:

- `mycel-go-sdk`: automation service client/helpers
- `mycel-rust-sdk`: generated proto updates and ergonomic methods matching existing style

### CLI

Add `mycel automation` commands:

```sh
mycel automation put --domain <domain-id> automation.json
mycel automation list --domain <domain-id>
mycel automation get --domain <domain-id> <automation-id>
mycel automation enable --domain <domain-id> <automation-id>
mycel automation disable --domain <domain-id> <automation-id>
mycel automation runs --domain <domain-id> [--automation <id>]
```

YAML can be accepted later; V1 should start with canonical JSON.

## Graph change ingestion

V1 needs a durable signal after graph transactions commit.

Implementation options:

1. Reuse existing change stream if it captures enough node create/update detail.
2. Extend graph service transaction commit to append automation-relevant graph change records.

Required event fields:

- event ID
- domain ID
- event type
- changed node ID
- old/new node state or enough reference data to load latest node
- labels/schema type if cheaply available
- automation metadata for generated writes
- commit timestamp / LSN if available

V1 can evaluate against latest committed graph state, but run records must indicate that policy. Snapshot-at-commit can be deferred.

## Condition evaluation

Add an automation condition evaluator that wraps existing GQL execution.

Requirements:

- bind reserved variable `changed` to the changed node
- optionally bind `old` for node updates if available
- reject conditions that do not reference `changed`
- reject/limit conditions that imply graph-wide scans
- execute with read permissions for the automation actor/principal
- return matched row(s), but V1 only needs boolean pass/fail and the changed node

Initial accepted pattern can be narrow:

```gql
MATCH (changed:Label)
RETURN changed
```

Then allow simple `WHERE` predicates over `changed` fields once GQL support is confirmed.

## Rendering

Implement deterministic text rendering in `internal/automation/render`.

V1 renderer:

- input target must be `changed`
- read configured field paths from changed node
- render as stable markdown/plain text sections:

```text
# properties.title
...

# payload.text
...
```

Record the hash of rendered input. Optionally store rendered input based on audit/privacy config.

## LLM invocation

V1 should not invent a separate provider subsystem if an existing inference/provider path can be reused.

Tasks:

- define model reference format in automation definition
- resolve provider/model at run time
- call provider asynchronously from worker
- record provider/model in run attempt
- add timeout and retryable/non-retryable error classification

If provider configuration is not yet suitable for general text completions, create an internal automation LLM interface with one initial implementation and keep the API boundary narrow.

## Actions

Implement only changed-node field update.

Validation:

- action target must be `changed`
- action path must be configured in definition
- action path must be allowed by schema if domain schema exists
- action path must not be one of automation safety metadata fields
- output mode must be text

Mutation behavior:

- load latest node
- skip if target field already equals generated text
- update node in a new graph transaction
- include automation metadata on the write:

```json
{
  "automation": {
    "run_id": "...",
    "automation_id": "...",
    "generated": true,
    "depth": 1
  }
}
```

Use existing graph update APIs where possible. If graph metadata for writes does not exist yet, add a small internal write context object rather than exposing arbitrary metadata prematurely.

## Loop prevention and idempotency

V1 safety rules:

- ignore events produced by the same automation when `ignoreSelfWrites` is true
- compute input hash from configured input fields before calling the model
- if a successful invocation exists for `(automation_id, node_id, input_hash)`, skip
- skip target update if output unchanged
- enforce max attempts with exponential backoff
- enforce per-element rate limit if inexpensive; otherwise store design hook and defer enforcement to V1.1

## Worker lifecycle

Add an automation module to daemon runtime startup.

Components:

- analyzer/candidate selector:
  - reads graph change events
  - finds matching enabled definitions
  - creates invocations
- worker:
  - leases pending invocations
  - evaluates condition
  - renders input
  - calls LLM
  - applies action
  - finalizes status

Operational requirements:

- configurable worker count
- configurable poll interval
- graceful shutdown
- retry backoff
- failed/degraded logging
- no worker activity when automation subsystem disabled

## Schema integration

V1 schema-aware behavior:

- trigger prefilters may use schema node type/labels
- condition label checks should use schema-aware GQL behavior where domain schema exists
- output field path must validate against schema if schema declares fields
- generated fields should not bypass schema validation

No schema DSL extensions are required for V1. Automation definitions remain separate documents.

## mycel-console V1 UI

Add a read-oriented and basic management UI:

- Automations tab on domain/space detail
- list automation definitions
- show status/version/trigger summary
- enable/disable action
- JSON viewer/editor for definition body if low effort
- run/invocation history table
- error detail view

Full authoring UX can wait; V1 should make automations inspectable and operable.

## Testing plan

### Unit tests

- definition validation
- storage atomic read/write/list
- event candidate matching
- `changed`-anchored condition validation
- input rendering and hashing
- idempotency key behavior
- action path validation
- self-write skip logic

### Integration tests

- create automation definition
- create matching node
- worker runs and updates configured field
- non-matching label/type does not invoke
- false GQL condition records skipped invocation
- provider failure retries then fails
- repeated update with same input hash skips LLM call
- automation-generated write does not self-trigger infinitely
- schema validation rejects invalid output path

### API/SDK tests

- protobuf generation compiles
- Go SDK helper smoke tests
- Rust SDK generation/build
- admin UI command/API smoke tests

## Rollout phases

### Phase 1 — API and model skeleton

- Add automation protobuf services/messages in `mycel-api`.
- Regenerate Go/Rust SDKs.
- Add internal model types and validation in `mycel`.
- Add no-op daemon service registration and CLI stubs.

### Phase 2 — Storage and definition CRUD

- Implement file-backed automation definition storage.
- Add daemon API CRUD handlers.
- Add CLI `automation put/list/get/enable/disable`.
- Add SDK helpers.

### Phase 3 — Change ingestion and invocation creation

- Define graph change event adapter.
- Append/read node create/update events.
- Candidate-match enabled definitions by domain/event/label/type.
- Create durable invocation records with dedupe.

### Phase 4 — Worker execution without LLM

- Implement worker lifecycle, leases, retries, status transitions.
- Implement condition evaluation against changed node.
- Implement input rendering/hash.
- Add a fake model provider for tests.

### Phase 5 — LLM invocation and changed-node update action

- Connect worker to real provider abstraction.
- Implement changed-node field update action.
- Add schema validation and self-write metadata.
- Add idempotency and unchanged-output skip.

### Phase 6 — Admin visibility

- Add mycel-console automation list/detail/run history.
- Add enable/disable controls.
- Add formatted JSON definition view.

### Phase 7 — hardening and docs

- Document API and CLI usage.
- Add example `summarize_page` automation JSON.
- Add operational config docs.
- Run full repo validations.

## Validation commands

Expected final validation:

```sh
cd myceldb/mycel-api && make generate || true
cd myceldb/mycel-go-sdk && make generate && go test ./...
cd myceldb/mycel-rust-sdk && cargo test
cd myceldb/mycel && go test ./...
cd myceldb/mycel-console && npm test -- --runInBand
cd myceldb/mycel-console/src-tauri && cargo check
```

Adjust exact generation commands to each repo's current Makefile targets.

## Open decisions before implementation

- Should V1 automation definitions be client-domain resources, admin-domain resources, or both?
- Which actor/principal should own a domain automation run?
- Should rendered prompt/input be retained by default or only hashed?
- What is the initial provider configuration source for text generation?
- Should V1 expose create/update/delete APIs to normal clients, or require admin/operator access?
- Is latest-state condition evaluation acceptable for V1, or is commit snapshot required immediately?
