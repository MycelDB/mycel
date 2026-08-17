# Graph automations V3 implementation plan

## Goal

Graph Automations V3 evolves V2 automations from single-step reactive enrichment into constrained, auditable workflows. V3 adds multi-step orchestration, scheduling, bounded batch scans, tool/agent execution, human review, and stronger policy controls while preserving Mycel's graph-native, schema-aware, asynchronous safety model.

V3 is not a general-purpose server-side code runtime. Workflow definitions remain declarative and policy-limited.

## Baseline from V2

V2 provides:

- domain-scoped automation definitions
- node create/update triggers
- `changed` / `old` reserved GQL bindings
- schema-aware GQL condition execution
- deterministic rendering
- provider-backed text generation
- token/cost accounting
- structured JSON output
- constrained graph actions
- idempotency and retry/backoff
- worker controls
- CLI/API/admin inspection controls

V3 builds on these primitives instead of replacing them.

## V3 scope

Supported:

- multi-step workflow definitions
- durable workflow instances and step runs
- step dependencies and conditional branches
- scheduled triggers and bounded batch graph scans
- tool/agent execution boundary with explicit allowlists
- generated change proposals
- human approval/rejection before selected mutations
- per-domain policy and budget controls
- run timeline and approval queue visibility

Not supported initially:

- arbitrary user-supplied server code
- unbounded graph scans
- unbounded workflow recursion
- cross-domain writes by default
- external webhooks without explicit future policy work
- synchronous transaction hooks

## Phase 1: workflow definition model

### Scope

Add a V3 workflow definition model alongside existing single-step automation definitions.

### Model additions

Conceptual JSON:

```json
{
  "id": "enrich_research_note",
  "name": "Enrich research note",
  "version": 1,
  "status": "disabled",
  "on": {
    "events": ["node.created"],
    "labels": ["ResearchNote"]
  },
  "workflow": {
    "steps": [
      {
        "id": "summarize",
        "kind": "llm",
        "condition": {"gql": "MATCH (changed:ResearchNote) RETURN changed"},
        "input": {"fields": ["payload.text"]},
        "output": {"mode": "json", "schema": {"type": "object"}}
      },
      {
        "id": "extract_claims",
        "kind": "llm",
        "dependsOn": ["summarize"]
      },
      {
        "id": "propose_links",
        "kind": "proposal",
        "dependsOn": ["extract_claims"],
        "approval": "required"
      }
    ]
  }
}
```

### Tasks

- Extend automation model with optional `workflow` block.
- Define step kinds:
  - `condition`
  - `render`
  - `llm`
  - `action`
  - `proposal`
  - `tool`
- Validate:
  - unique step IDs
  - acyclic dependency graph
  - bounded step count
  - allowed step kinds
  - approval requirement for graph-expanding mutations by policy
  - no ambiguous V2 `output.actions` vs V3 workflow semantics unless explicitly allowed
- Preserve V2 definitions only as valid single-step definitions; no compatibility shims beyond direct model support.

### Acceptance

- V2 definitions still validate as single-step automations.
- V3 workflows validate dependency graph and safety constraints.
- Invalid cycles/unbounded definitions are rejected.

## Phase 2: workflow runtime/orchestrator

### Scope

Add durable workflow instance and step-run execution state.

### Data model

Persist:

```text
automations/
  workflow-instances/<domain>/<date>/<instance-id>.json
  workflow-steps/<domain>/<date>/<step-run-id>.json
```

Instance fields:

- workflow definition ID/version
- triggering invocation/event
- changed element
- status: `pending | running | waiting_approval | succeeded | failed | cancelled`
- current runnable steps
- timestamps

Step run fields:

- instance ID
- step ID
- attempt number
- status
- input/output hashes
- provider/tool metadata
- token/cost metadata
- mutation/proposal refs
- error/retry metadata

### Tasks

- Implement workflow scheduler that selects runnable steps when dependencies are satisfied.
- Add step state machine.
- Reuse V2 renderer/provider/output/action packages inside step execution.
- Support resume after daemon restart.
- Support retry/cancel at instance and step level.
- Enforce max workflow duration and max step attempts.

### Acceptance

- Workflow with three dependent steps executes in order.
- Failed retryable step resumes with backoff.
- Daemon restart does not lose runnable state.
- Cancelled workflow stops future steps.

## Phase 3: tool and agent execution boundary

### Scope

Introduce constrained tool/agent execution without arbitrary server-side code.

### Tool model

Tool step example:

```json
{
  "id": "search_related",
  "kind": "tool",
  "tool": "graph.search",
  "input": {
    "query": "$steps.extract_claims.output.claims"
  }
}
```

### Tasks

- Define internal tool interface:
  - name
  - input schema
  - output schema
  - execution policy
- Add initial built-in tools:
  - graph query/search
  - semantic search
  - blob metadata read
- Add explicit allowlists per domain/workflow.
- Add tool-call audit records.
- Add resource limits:
  - max calls per workflow
  - max input/output bytes
  - timeout
- Add optional agent adapter boundary but keep initial implementation deterministic/tool-only unless provider support is ready.

### Acceptance

- Tool steps require allowlist approval.
- Tool input/output are schema validated.
- Tool calls are audited and bounded.
- Disallowed tools fail validation or execution clearly.

## Phase 4: scheduling and bounded batch scans

### Scope

Support scheduled automations and explicit graph scan jobs.

### Trigger model

```json
"on": {
  "schedule": {
    "interval": "1h"
  },
  "scan": {
    "gql": "MATCH (n:Page) WHERE n.payload.needsSummary = true RETURN n LIMIT 100"
  }
}
```

### Tasks

- Add scheduler module with durable checkpoints.
- Add cron/interval parser or simple interval-only V3.0.
- Add bounded scan executor:
  - requires `LIMIT`
  - requires max rows
  - requires domain-scoped read policy
- Create one workflow instance per selected changed/scanned element.
- Add backpressure and queue limits.
- Prevent duplicate scheduled instances through scan fingerprint/idempotency keys.

### Acceptance

- Interval trigger creates workflow instances.
- Scan trigger refuses unbounded query.
- Duplicate scan result does not create duplicate active workflow instance.
- Scheduler survives restart with checkpoint state.

## Phase 5: human-in-the-loop review

### Scope

Add change proposals and approval gates for graph-expanding or high-risk actions.

### Data model

Proposal fields:

- proposal ID
- workflow instance/step ID
- proposed actions
- rendered diff/summary
- status: `pending | approved | rejected | expired | applied`
- reviewer actor
- timestamps

### Tasks

- Add proposal action mode to action engine.
- Store proposed mutations without applying them.
- Add approval API:
  - list proposals
  - get proposal detail
  - approve
  - reject
  - apply approved proposal
- Add policy rules requiring approval for selected action classes.
- Ensure approved mutations still pass schema validation at apply time.

### Acceptance

- Proposal-generating workflow pauses at `waiting_approval`.
- Approval applies mutations in a new transaction.
- Rejection marks workflow/step appropriately.
- Proposal audit includes before/after summary.

## Phase 6: policy, budget, and permissions

### Scope

Formalize tenant/domain safety for V3 workflows.

### Policy controls

- max workflow instances per domain
- max steps per workflow
- max depth/chain length
- max tool calls
- max provider calls
- token/cost budgets per domain/time window
- approval requirements by action class
- service principal permissions

### Tasks

- Add policy model and storage.
- Attach workflow runs to actor/service principal.
- Enforce permissions during:
  - condition evaluation
  - tool execution
  - provider calls
  - graph mutations
- Aggregate token/cost accounting for budget checks.
- Add policy violations as first-class run failure/skipped reasons.

### Acceptance

- Budget exhaustion prevents provider calls before cost is incurred where estimable.
- Permission failures are clear and audited.
- Approval policy gates graph-expanding actions.
- Cross-domain mutation is rejected unless explicitly permitted.

## Phase 7: admin UX, SDKs, CLI, and docs

### Admin UI

Add:

- workflow definition list/detail
- workflow instance timeline
- step run detail
- token/cost breakdown
- tool-call audit view
- proposal approval queue
- scheduler status
- policy/budget status

### CLI

Add:

```sh
mycel automation workflow validate workflow.json
mycel automation workflow put --domain <domain> workflow.json
mycel automation workflow instances --domain <domain>
mycel automation workflow instance get --domain <domain> <instance-id>
mycel automation proposal list --domain <domain>
mycel automation proposal approve --domain <domain> <proposal-id>
mycel automation proposal reject --domain <domain> <proposal-id>
```

### SDK/API

Add proto RPCs for:

- workflow instance list/get/cancel/retry
- step run detail
- proposal list/get/approve/reject
- policy get/update
- scheduler status

Regenerate:

- `mycel-go-sdk`
- `mycel-rust-sdk`
- `mycel-console` Tauri bindings as needed

### Docs/examples

Add examples:

- research note enrichment workflow
- scheduled stale summary refresh
- entity extraction with human-approved links
- semantic related-note proposal workflow

### Acceptance

- Admin can inspect workflow timelines and approve proposals.
- CLI can validate, inspect, approve, and cancel workflows.
- Docs explain V2 vs V3 capability boundaries.

## Cross-cutting test plan

### Unit tests

- workflow validation
- dependency sorting/cycle rejection
- scheduler intervals/checkpoints
- tool allowlist enforcement
- proposal serialization and diff summaries
- budget calculations
- policy enforcement

### Integration tests

- event-triggered workflow with dependent LLM/action steps
- scheduled scan creates bounded instances
- approval gate pauses and resumes workflow
- budget exhaustion prevents provider call
- restart resumes pending workflow steps
- cancel prevents future steps

### Cross-repo validation

```sh
cd myceldb/mycel-api && make test
cd myceldb/mycel && go test ./...
cd myceldb/mycel-go-sdk && go test ./...
cd myceldb/mycel-rust-sdk && MYCEL_API_ROOT=/path/to/mycel-api cargo test
cd myceldb/mycel-console && npm test -- --runInBand
cd myceldb/mycel-console/src-tauri && MYCEL_API_ROOT=/path/to/mycel-api cargo check
```

## Open decisions

- Should V3.0 allow only interval schedules, deferring cron syntax?
- Which graph-expanding actions require approval by default?
- Should workflow definitions live in the same API as V2 automations or a separate WorkflowService?
- What is the canonical proposal diff format?
- Should budget enforcement use estimated cost only, or later reconcile against provider billing exports?
- Should cross-domain workflows be V3 or deferred to V4?
