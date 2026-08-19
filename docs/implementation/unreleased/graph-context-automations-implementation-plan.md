# Graph Context Automations Implementation Plan

## Status

Proposed. This plan implements the design in
[Graph context automations](../../design/automation/graph-context-automations.md).
It is intentionally split into independently reviewable tranches. Each tranche
must leave the repository buildable, tested, documented, and safe by default.

The motivating workflow is a daily journal summary automation:

```text
JournalEntry created/updated
  -> select parent Journal
  -> collect all JournalEntry children for that Journal
  -> render the day as model input
  -> call inference through a profile
  -> write the summary to Journal.properties.summary
```

## Goals

- Allow graph automations to update a matched context node, not only `changed`.
- Allow conditions to bind typed aliases such as `journal` and `entry`.
- Add bounded context-input GQL queries that run after condition alias binding.
- Add deterministic multi-row rendering for named context result sets.
- Add aggregate idempotency keyed by automation/version/target/context hash.
- Add target-based debounce/coalescing for bursts of related graph changes.
- Expose diagnostics in run detail so operators can understand target selection,
  context collection, idempotency, coalescing, inference resolution, and writes.
- Keep inference profile/grant/policy enforcement unchanged and fail-closed.
- Update design, operations, examples, and tests for every implemented tranche.

## Non-goals

- Do not add arbitrary user-supplied server-side code.
- Do not execute automations synchronously inside the original graph write
  transaction.
- Do not add unbounded graph scans.
- Do not give model output unrestricted graph write authority.
- Do not add destructive repair, merge, rebalance, or implicit cascade behavior.
- Do not make `mycel-console` authoritative for safety; UI controls remain a
  client surface over daemon validation.
- Do not hand-edit generated protobuf files. If public API changes are needed,
  update source protobufs in `github.com/myceldb/mycel-api` and regenerate.

## Current baseline and assumptions

- Node-created and node-updated automations exist.
- `on` selects candidate automations by event and labels.
- `condition.gql` is now optional. If omitted, the engine treats the condition as
  matched and binds `changed` to the triggering node.
- When a condition is present, it must reference `changed`.
- Condition execution is read-only and can return aliases, but downstream action
  support is still centered on `changed` and newly created `$refs`.
- Input rendering supports fields and simple templates over `changed`, `old`, and
  row aliases.
- Structured output and create-node/create-edge/update-node actions exist, but
  `update_node.target` only supports `changed`.
- Inference requests run as actor `automation` on behalf of the automation owner
  and require matching profiles, capabilities, credential grants, and policies.

## Implementation phases

## GCA0: Baseline audit and acceptance fixtures

### Feature scope

Establish test fixtures and examples that describe the intended journal summary
workflow before changing runtime behavior.

### Tasks

1. Add a focused design test fixture under `examples/automations/` or
   `internal/automation/testdata/` for a future `summarize_daily_journal`
   automation.
2. Document the current limitation in operations docs: context aliases can be
   returned by conditions but cannot yet be action targets or multi-row input
   collections.
3. Inventory existing automation tests by layer:
   - model validation;
   - condition alias binding;
   - rendering;
   - output parsing;
   - action application;
   - manager execution;
   - daemon API/CLI;
   - console, if changed.
4. Add a failing or skipped test outline, if useful, that states the desired
   end-to-end behavior without requiring implementation in GCA0.

### Tests

- Existing automation model/service tests continue to pass.
- Docs examples validate locally where validation is expected to work today.

### Acceptance

```sh
go test ./internal/automation/model ./internal/automation/service ./internal/automation/actions ./internal/automation/render
make docs-check
git diff --check
```

## GCA1: Typed alias environment

### Feature scope

Introduce an internal typed alias environment shared by condition evaluation,
input rendering, and actions.

### Tasks

1. Add an internal alias type, conceptually:

   ```go
   type AliasValue struct {
       Node *graph.Node
       Edge *graph.Edge
       Scalar any
   }
   ```

   The exact package/name should align with existing automation subsystem
   boundaries.
2. Convert condition `rowAliases` to produce typed alias values rather than a
   loose `map[string]any`, or add a compatibility adapter for rendering.
3. Ensure `changed` is always present as a node alias for node automations,
   including omitted-condition runs.
4. Preserve `old` as a read-only condition binding for update events where an old
   node snapshot exists.
5. Validate alias names:
   - no empty aliases;
   - no reserved collisions except the engine-owned `changed` and `old`;
   - no unsupported alias value kinds in action-target positions.
6. Persist selected alias diagnostics on the run record or a run-detail envelope:
   alias name, kind, node/edge ID, and source phase. Do not persist full provider
   secrets or raw credential material.

### Tests

- Unit: omitted condition returns `changed` as a node alias.
- Unit: condition returning `journal, changed` records both node aliases.
- Unit: scalar aliases remain renderable but cannot be node action targets.
- Unit: unknown/missing aliases produce actionable validation errors.
- Regression: existing templates using `{{ changed.properties.title }}` still
  render identically.

### Acceptance

```sh
go test ./internal/automation/service ./internal/automation/render ./internal/automation/model -count=1
git diff --check
```

## GCA2: Action targets from condition aliases

### Feature scope

Allow `update_node.target` to reference a node alias returned by the condition,
for example `target: "journal"`.

### Tasks

1. Extend the action engine context to include the alias environment.
2. Update `update_node` target resolution:
   - `changed` remains supported;
   - any condition-returned node alias is supported;
   - scalar aliases, edge aliases, unknown aliases, and multi-valued aliases fail
     before mutation.
3. Preserve existing create-node refs:
   - `$refs.<name>` remains for nodes created earlier in the same action list;
   - condition aliases and `$refs` must have deterministic precedence and clear
     collision rules.
4. Ensure schema/path validation still applies when updating an alias target.
5. Ensure action-generated metadata is written to the target node.
6. Keep no-op detection: if the target already has the requested value, skip the
   graph write and record a no-op/unchanged action result.
7. Update run detail to include action target alias and resolved node ID.

### Tests

- Unit: `update_node target: journal` updates the parent Journal selected by a
  condition.
- Unit: `update_node target: changed` remains unchanged.
- Unit: unknown target alias returns `unknown node ref` or a more specific
  actionable error.
- Unit: edge/scalar alias cannot be used as `update_node.target`.
- Service integration: condition selects `journal`; output writes
  `Journal.properties.summary`; changed `JournalEntry` remains unchanged.
- Regression: create-node then create-edge with `$refs.marker` still works.

### Acceptance

```sh
go test ./internal/automation/actions ./internal/automation/service -count=1
git diff --check
```

## GCA3: Bounded context-input GQL queries

### Feature scope

Add named context queries under `input` so automations can collect related graph
context after condition aliases are bound.

Conceptual definition shape:

```json
"input": {
  "target": "journal",
  "mode": "gql_template",
  "context": {
    "entries": {
      "gql": "MATCH (journal:Journal)-[r:contains]->(entry:JournalEntry) RETURN entry, r ORDER BY r.order FETCH FIRST 200 ROWS ONLY"
    }
  },
  "template": "..."
}
```

### API/model tasks

1. Extend automation model `Input` with a context query map. Exact naming should
   be reviewed before implementation, but the persisted shape should be explicit
   and versionable.
2. Add validation:
   - context names must be non-empty stable identifiers;
   - GQL must be non-empty;
   - GQL must be read-only;
   - GQL must be bounded by `FETCH FIRST`, `LIMIT`, or an explicit context query
     limit;
   - query must reference at least one existing alias or `changed`, unless a
     policy explicitly permits broad scans;
   - row limits must not exceed daemon maximums.
3. Support alias binding in context query execution. A query pattern such as
   `MATCH (journal:Journal)-...` should resolve `journal` to the condition alias,
   not broad-scan all Journal nodes.
4. Define result shape: each named context query produces an ordered list of
   rows, where each row contains typed values by returned column name.
5. Record context diagnostics: query name, row count, bounded limit, truncation
   status, and errors.

### Tests

- Model validation accepts bounded context query definitions.
- Model validation rejects unbounded context queries.
- Model validation rejects context queries that do not reference known aliases.
- Service unit: context query bound to `journal` returns only entries for that
  journal.
- Service unit: row count over maximum fails closed.
- Regression: automations without `input.context` behave exactly as before.

### Acceptance

```sh
go test ./internal/automation/model ./internal/automation/service ./internal/query/gql/... -count=1
git diff --check
```

## GCA4: Deterministic multi-row rendering

### Feature scope

Add a constrained template renderer mode capable of iterating named context
collections.

Conceptual template:

```text
Summarize this day.

Date: {{ journal.properties.date }}

Entries:
{{#each entries}}
- {{ entry.payload.text }}
{{/each}}
```

### Tasks

1. Add a new input mode, such as `gql_template` or `template_v2`, without
   changing existing `template` semantics.
2. Implement a minimal deterministic template language:
   - scalar interpolation for aliases and context row fields;
   - `each` over named context collections;
   - stable rendering order matching query output order;
   - explicit behavior for missing values, probably empty string plus diagnostic;
   - no arbitrary function calls or code execution.
3. Add rendering context containing:
   - base aliases (`changed`, `old`, condition aliases);
   - named context result sets;
   - optional metadata such as row counts.
4. Include context row data in rendered-input hash computation.
5. Add truncation behavior:
   - fail closed by default when context exceeds configured max rows/tokens;
   - optionally allow explicit truncation policy in a later tranche.
6. Update docs with renderer syntax and examples.

### Tests

- Unit: simple alias interpolation works.
- Unit: `each entries` renders multiple rows in order.
- Unit: missing field renders predictably and records a diagnostic if the
  diagnostics channel exists.
- Unit: malformed template fails validation or rendering with an actionable
  error.
- Unit: rendered hash changes when any entry text/order changes.
- Regression: existing template/fields rendering remains unchanged.

### Acceptance

```sh
go test ./internal/automation/render ./internal/automation/service -count=1
make docs-check
git diff --check
```

## GCA5: Aggregate idempotency

### Feature scope

Allow idempotency to be scoped to a selected target alias and aggregate context
hash rather than only the changed node.

Conceptual shape:

```json
"safety": {
  "idempotency": {
    "scope": "target",
    "target": "journal",
    "inputHashFields": [
      "journal.properties.date",
      "entries[*].entry.payload.text",
      "entries[*].r.properties.order"
    ],
    "skipIfOutputUnchanged": true
  }
}
```

### Tasks

1. Extend the idempotency model with explicit scope:
   - `changed` or omitted: existing behavior;
   - `target`: key by selected target alias;
   - future scopes must be rejected until implemented.
2. Extend storage index records to support target alias ID without breaking
   existing successful-input index files.
3. Compute aggregate hashes from either:
   - the final rendered input text; or
   - explicit paths plus context metadata.

   The implementation should choose one primary path and document it. Rendered
   text hash is simpler; explicit path hashing is more transparent for partial
   re-render changes.
4. Persist successful aggregate records keyed by:
   - domain ID;
   - automation ID;
   - automation version;
   - target node/edge ID;
   - input hash.
5. Ensure idempotency checks use the target `journal`, not the changed
   `JournalEntry`, for aggregate automations.
6. Expose idempotency diagnostics in run detail.

### Tests

- Unit: target-scoped idempotency skips repeated runs for unchanged Journal
  context even when different child entries trigger the automation.
- Unit: changing one child entry changes the aggregate hash and allows a new run.
- Unit: existing changed-scoped idempotency remains unchanged.
- Storage: old successful-input index records remain readable.
- Service integration: repeated retry of the same unchanged journal context is
  skipped after a successful run.

### Acceptance

```sh
go test ./internal/automation/storage ./internal/automation/service -count=1
git diff --check
```

## GCA6: Debounce and target-based coalescing

### Feature scope

Add delayed execution and target coalescing so bursts of edits to entries under
the same journal result in one summary run.

Conceptual shape:

```json
"safety": {
  "debounce": {
    "duration": "30s",
    "coalesceBy": "journal"
  }
}
```

The final location and names should be reviewed; `execution` or `schedule` may
be a better top-level section than `safety`.

### Tasks

1. Extend definition validation:
   - debounce duration must parse as a positive bounded duration;
   - `coalesceBy` must name a node/edge alias available from condition or
     `changed`;
   - scheduled automations and graph-event debounce interactions must be clearly
     defined.
2. Extend invocation storage with coalescing metadata:
   - selected target alias;
   - selected target ID;
   - pending/coalesced status;
   - due time;
   - replacement/merged invocation IDs if needed.
3. Update worker scheduling:
   - when an invocation selects a coalesce target, delay processing until quiet;
   - new invocations for the same automation/version/target before due time
     extend or replace the pending run;
   - disable/cancel prevents pending coalesced work from running.
4. Ensure execution uses latest committed graph state at run time, not the state
   from the first event in the debounce window.
5. Record coalescing diagnostics in invocation/run detail.

### Tests

- Unit: multiple invocations for the same journal coalesce into one due run.
- Unit: invocations for different journals do not coalesce together.
- Unit: disabling an automation cancels or prevents pending coalesced runs.
- Unit: retry behavior is explicit and does not create duplicate coalesced runs.
- Integration: creating three entries quickly produces one journal summary run.
- Time-control tests must use injectable clocks rather than sleeping.

### Acceptance

```sh
go test ./internal/automation/service ./internal/automation/storage -count=1
git diff --check
```

## GCA7: Daemon API, CLI, and Console surfaces

### Feature scope

Expose the new behavior through existing admin/client automation surfaces and
operator tools.

### Tasks

1. If persisted/public API shapes change, update source protobufs in
   `mycel-api`, regenerate stubs, and keep generated artifacts policy-compliant.
2. Update CLI validation/get/list/run output where needed:
   - show target alias and target ID;
   - show context query diagnostics;
   - show rendered input/context hash;
   - show coalescing/idempotency decisions.
3. Add or update `mycel automation validate` to understand new fields.
4. Update `mycel-console` automation editor, if in scope:
   - allow editing context queries;
   - show run detail diagnostics;
   - do not make UI validation looser than daemon validation.
5. Ensure older daemon versions fail clearly when a definition uses unsupported
   graph-context features.

### Tests

- CLI tests for validating and retrieving a context automation definition.
- CLI tests for run detail including target/context diagnostics.
- Daemon API tests for create/update validation of context fields.
- Console component/service tests if UI is changed.
- Backward compatibility tests for existing automation definitions.

### Acceptance

```sh
go test ./internal/cli/cmd ./internal/daemon/api/admin ./internal/automation/... -count=1
make docs-check
git diff --check
```

If console changes are included, also run the console test/build commands from
the console repository.

## GCA8: End-to-end journal summary system test

### Feature scope

Add a deterministic end-to-end test that verifies the motivating behavior without
requiring a real external LLM provider.

### Tasks

1. Use the fake inference connector/profile package or test inference runtime.
2. Seed a domain graph:
   - one `Journal` node;
   - several `JournalEntry` child nodes;
   - ordered `contains` edges.
3. Register a `summarize_daily_journal` automation using:
   - `on.labels: [JournalEntry]`;
   - condition selecting parent `journal`;
   - context query collecting ordered entries;
   - renderer producing deterministic prompt text;
   - fake model output returning JSON summary;
   - `update_node target: journal` action.
4. Trigger entry creation/update events.
5. Process pending automation work.
6. Assert:
   - exactly one run succeeds;
   - the Journal node has `properties.summary` set;
   - child entries were not overwritten;
   - run detail records target alias, context row count, inference provenance,
     output hash, and mutation ID;
   - idempotency prevents redundant reruns for unchanged context;
   - debounce coalesces bursts when enabled.

### Tests

- In-process service integration test for manager/actions/rendering.
- Daemon-level API/CLI smoke test if a stable fake connector can be configured in
  test daemon startup.
- Optional console smoke/component test for run detail display.

### Acceptance

```sh
go test ./internal/automation/... ./internal/daemon/api/admin ./internal/cli/cmd -count=1
make test
git diff --check
```

## Documentation updates

Documentation must be updated as features land, not deferred to the final phase.

Required docs:

- `docs/design/automation/graph-context-automations.md`
  - keep design and implementation decisions aligned;
  - update open questions as they are answered.
- `docs/design/automation/graph-automations.md`
  - add graph-context automation section and clarify how V2/V3 capabilities map
    to implemented features.
- `docs/operations/cli/automation.md`
  - document optional conditions;
  - document context query input syntax;
  - document alias action targets;
  - document run detail diagnostics;
  - provide safe journal summary examples.
- `docs/operations/cli/inference.md`
  - cross-reference inference profile/grant/policy requirements for context
    automations where needed.
- `examples/automations/`
  - add `summarize_daily_journal.json` once validation supports the shape.

Docs acceptance for each documentation-affecting tranche:

```sh
make docs-check
git diff --check
```

## Safety and failure behavior

- Conditions and context queries must be read-only.
- Context queries must be bounded and anchored to existing aliases unless an
  explicit future policy permits broader scans.
- Ambiguous target alias binding fails closed by default.
- Missing target aliases fail before inference where possible.
- Missing or disabled inference profiles/capabilities/grants/policies continue
  to fail closed.
- Model output is parsed and validated before any graph mutation.
- Generated graph writes include automation metadata for loop prevention and
  audit.
- Debounced runs use latest committed graph state and record enough diagnostics
  to explain what was summarized.
- In raft mode, durable writes must remain raft-owned, derived/rebuildable, or
  fail closed according to repository safety rules.

## Open questions to resolve before implementation

1. Should the input context field be called `context`, `queries`, or
   `collections`?
2. Should multi-row condition results imply fan-out, or should fan-out require an
   explicit mode in the first implementation?
3. Should debounce live under `safety`, `execution`, or `schedule`?
4. Should aggregate idempotency hash final rendered input, explicit field paths,
   graph revision metadata, or a combination?
5. Should context query results be persisted in run detail, or only hashes,
   counts, diagnostics, and selected IDs?
6. Should GCA1/GCA2 support edge action targets immediately, or only node alias
   targets for the first tranche?
7. How should token-limit truncation be expressed: fail-only initially, or an
   explicit truncation policy?

## Suggested release gate

Before considering graph-context automations complete for the daily journal use
case, the following should pass:

```sh
go test ./internal/automation/... ./internal/daemon/api/admin ./internal/cli/cmd -count=1
make docs-check
git diff --check
```

For normal branch validation, also run:

```sh
make test
```

For raft/clustering-sensitive changes to automation persistence or graph writes,
consider the relevant phase targets listed in `AGENTS.md`.
