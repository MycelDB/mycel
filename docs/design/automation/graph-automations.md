# Graph automations design

## Goal

Graph automations let users and applications define reactive AI-powered work that runs when graph/database changes satisfy a graph-native condition.

An automation is similar to a stored procedure in that it is defined close to the data and reacts to database state, but it differs in important ways:

- it is event-driven
- it is asynchronous
- it may call AI/LLM providers through standalone inference profiles
- it produces auditable graph mutations
- it records neutral inference telemetry for LLM-backed runs
- it should be constrained and schema-aware rather than arbitrary imperative code

The primary use cases are derived content, enrichment, classification, summarization, extraction, and multimodal understanding.

Examples:

- when a PNG blob node is created, describe the image in markdown
- when a Knot PKM page is created or updated, summarize its content
- when a note mentions people/concepts/tasks, extract structured entities and create graph links
- when related notes are semantically similar, suggest or create relationship edges

## Non-goals

Graph automations are not intended to replace:

- semantic indexing maintenance
- full-text indexing maintenance
- general background jobs
- arbitrary user-supplied server-side code
- synchronous transaction hooks

The existing semantic maintenance pipeline remains a system-owned maintenance mechanism. Graph automations are user/application-defined reactive tasks.

## Terminology

### Automation

A persisted rule that defines:

- trigger events
- GQL condition
- input rendering
- AI prompt/inference profile configuration
- output/action policy
- safety/idempotency policy

### Trigger

The event selector that decides which graph changes may start evaluation.

Examples:

```text
node.created
node.updated
edge.created
edge.updated
```

### Condition

A GQL expression evaluated against the changed element and surrounding graph context.

### Invocation

A durable record that an automation was considered or executed for a specific graph change.

### Run

A concrete execution attempt that may call an AI model and apply graph mutations.

### Token usage

Provider-reported or locally estimated token counts for an AI call. At minimum this includes input tokens, output tokens, and total tokens. When available it can also include cached input tokens, reasoning tokens, tool-call tokens, provider request IDs, and provider-specific usage metadata.

### Inference usage event

A neutral telemetry record emitted by the inference subsystem for an automation LLM call. It captures profile, model, capability, credential grant, policy decision, provider request ID, status, latency, and token counts. It intentionally does not include product pricing, credits, billing, or cost fields.

## High-level flow

```text
graph transaction commits
        ↓
graph change event appended
        ↓
automation candidate selected by event type/schema/label
        ↓
GQL condition evaluated with changed element bound
        ↓
input rendered from changed element and graph context
        ↓
AI/LLM call resolved through an inference profile and made asynchronously
        ↓
policy decision and neutral usage telemetry recorded
        ↓
result validated/transformed
        ↓
graph mutation applied in a new transaction
        ↓
audit/run status recorded
```

Automations should not execute inside the original graph write transaction.

## Relationship to schema

Graph automations should be schema-aware.

Schema can define:

- which node/edge types can have automations
- default LLM renderers for node/edge types
- allowed automation output fields
- reserved fields for generated content
- multimodal blob rendering rules
- validation rules for structured output

Example schema fragment:

```yaml
nodeTypes:
  Page:
    labels: [Page]
    properties:
      - name: title
        type: string
    payload:
      - name: text
        type: string
      - name: summaryMarkdown
        type: string
    llm:
      renderers:
        default:
          text: |
            # {{properties.title}}

            {{payload.text}}
```

GQL conditions should resolve labels/properties/payload fields through schema when a domain schema exists.

## Automation definition model

A conceptual automation definition:

```yaml
id: summarize_page
name: Summarize page
version: 1
scope:
  domain: content
status: enabled
on:
  events: [node.created, node.updated]
  labels: [Page]
condition:
  gql: |
    MATCH (changed:Page)
    WHERE TEXT_CONTAINS(changed.payload.text, '')
    RETURN changed
input:
  target: changed
  fields: [payload.text]
inference:
  operation: chat
  profile: summarize-page
  parameters:
    responseFormat: text
    maxOutputTokens: 512
prompt: |
  Summarize this page in concise markdown.
output:
  mode: text
  actions:
    - update_node:
        target: changed
        set:
          payload.summaryMarkdown: $result.text
safety:
  ignoreSelfWrites: true
  idempotency:
    inputHashFields: [payload.text]
    skipIfOutputUnchanged: true
  rateLimit:
    maxRunsPerElementPerHour: 3
```

## GQL condition model

Conditions are expressed in GQL and evaluated with reserved variables.

For node events:

```text
changed = new node state
old     = previous node state, if available
```

For edge events:

```text
changed = new edge state
old     = previous edge state, if available
```

Initial conditions should be anchored on `changed` to avoid broad graph scans.

Example:

```gql
MATCH (changed:Page)
RETURN changed
```

Context expansion can still use graph patterns:

```gql
MATCH (changed:Page)-[:PART_OF]->(doc:Document)
RETURN changed, doc
```

The automation engine should reject or warn on conditions that do not reference `changed`, depending on policy.

## Input rendering

Input rendering converts graph elements and context rows into AI model input.

Supported input kinds over time:

- text from properties/payload
- markdown rendering through templates
- JSON rendering for structured context
- blob references
- images/audio/video for multimodal models
- neighborhood/path context

Rendering should be deterministic and auditable. The rendered input or its hash should be recorded with the run. For LLM-backed runs, the rendered input is also the basis for prompt/input token telemetry.

### Graph-context rendering

Implemented graph-context automations can set `input.target` to a condition-returned node alias, run bounded read-only named GQL queries under `input.context`, and render those rows with `mode: "gql_template"` plus `{{#each name}}...{{/each}}` blocks. See [Graph context automations](graph-context-automations.md) and `examples/automations/summarize_daily_journal.json`.

## Output and actions

Automation output should be constrained. The LLM should not get unrestricted write access to the graph.

Supported action classes:

- update fields on changed node/edge
- create a related node
- create an edge
- update another matched graph element
- propose changes for human approval
- emit a task/review item

Actions should be schema-validated before mutation.

## Execution semantics

Automations are asynchronous.

A graph commit only records the change. Automation workers evaluate and execute later.

Required execution properties:

- durable candidate/invocation records
- retries with backoff
- failure status
- idempotency keys
- rate limiting
- cancellation/disable support
- audit history
- inference policy decisions and neutral token telemetry for every LLM-backed attempt
- no unbounded recursive trigger loops

## Loop prevention

Automation-generated writes can trigger more automations. This is useful but dangerous.

Each automation write should include metadata such as:

```text
meta.automation.runID
meta.automation.automationID
meta.automation.generated = true
```

Safety controls:

- ignore self-generated writes by default
- max run depth
- max runs per element per time window
- input hash idempotency
- skip writes when output is unchanged
- optional manual approval for graph-expanding actions

## V1: constrained node automations

V1 should focus on the highest-value safe subset.

### Capabilities

- node-created and node-updated triggers
- label/type prefilter
- GQL condition anchored on `changed`
- text input rendering from changed node
- prompt-based LLM call
- text output
- update configured field on changed node
- async execution
- audit records
- inference profile and token usage records
- retry/failure state
- self-loop prevention

### Example: summarize Knot PKM page

```yaml
id: summarize_page
on:
  events: [node.created, node.updated]
  labels: [Page]
condition:
  gql: |
    MATCH (changed:Page)
    RETURN changed
input:
  target: changed
  fields:
    - payload.text
prompt: |
  Summarize this page in concise markdown.
output:
  mode: text
  actions:
    - update_node:
        target: changed
        set:
          payload.summaryMarkdown: $result.text
```

### V1 restrictions

- no edge triggers
- no arbitrary graph writes
- no structured output actions beyond configured field update
- no multimodal blob input
- no synchronous execution
- no graph-wide scans

## V2: graph context, structured output, accounting, and multimodal input

V2 expands automations from simple node enrichment to graph-aware AI workflows. LLM-backed generation uses standalone inference profiles so credential, grant, policy, and neutral usage telemetry behavior is shared with other mycel inference consumers.

### Capabilities

- edge-created and edge-updated triggers
- context GQL around changed node/edge
- structured JSON output with schema validation
- token accounting for every LLM invocation attempt
- neutral inference usage telemetry by profile/model/capability and policy decision
- create related nodes/edges
- update matched context nodes/edges
- multimodal blob rendering for images and documents
- human approval mode for graph-expanding writes
- richer trigger filters using schema type, label, field changes, and edge endpoint types

### Example: describe PNG blob

```yaml
id: describe_png_blob
on:
  events: [node.created]
  labels: [Blob]
condition:
  gql: |
    MATCH (changed:Blob)
    WHERE changed.properties.mimeType = 'image/png'
    RETURN changed
input:
  target: changed
  multimodal:
    mimeType: properties.mimeType
    blobRef: payload.blobRef
prompt: |
  Describe the content of this image.
  Return markdown.
output:
  mode: text
  actions:
    - update_node:
        target: changed
        set:
          payload.descriptionMarkdown: $result.text
```

### Example: extract concepts from page

```yaml
id: extract_page_concepts
on:
  events: [node.created, node.updated]
  labels: [Page]
condition:
  gql: |
    MATCH (changed:Page)-[:PART_OF]->(doc:Document)
    RETURN changed, doc
input:
  target: changed
  fields:
    - payload.text
output:
  mode: json
  schema:
    type: object
    properties:
      concepts:
        type: array
        items:
          type: string
  actions:
    - create_related_nodes:
        label: Concept
        namesFrom: $result.concepts
        edgeLabel: MENTIONS
        from: changed
```

### V2 restrictions

- structured actions must be declared in automation definition
- graph-expanding writes may require approval depending on domain policy
- multimodal input only through schema-approved blob fields

## V3: agentic workflows and automation orchestration

V3 introduces multi-step, tool-using, and scheduled automation workflows.

### Capabilities

- multi-step workflows
- tool/agent execution
- scheduled automations
- batch graph scans with explicit resource controls
- automation dependencies
- trigger chaining with policies
- conditional branches
- long-running workflows
- human-in-the-loop review queues
- generated change proposals before application
- cross-domain or cross-space workflows if permissions allow

### Example: research assistant workflow

```yaml
id: enrich_research_note
on:
  events: [node.created]
  labels: [ResearchNote]
workflow:
  steps:
    - summarize
    - extract_claims
    - find_related_notes
    - propose_links
    - await_approval
    - apply_approved_links
```

### V3 restrictions

V3 should require stronger policy controls:

- explicit tool allowlists
- per-domain token and provider-call ceilings
- approval gates
- workflow timeouts
- tenant isolation
- token and provider-call ceilings
- detailed audit logs

## Security and permissions

Automation runs must be associated with an actor:

- creating user
- owning application
- service principal
- domain automation principal

The run should only read/write what that actor or service principal is allowed to access.

Required controls:

- permission checks during condition evaluation
- permission checks during graph mutations
- secret isolation for model providers/tools
- audit log of prompt, inference profile/model refs, inputs, outputs, token usage, policy decisions, and writes
- redaction policy for sensitive payload fields

## Storage model

Potential persisted records:

```text
automations/
  definitions
  versions
  invocations
  runs
  attempts
  outputs
  audit
```

Invocation/run fields:

- automation ID/version
- triggering event ID
- changed element ID/type
- input hash
- rendered input hash or stored input reference
- inference profile/model/capability refs
- policy decision ID
- provider request ID when available
- token usage:
  - input tokens
  - output tokens
  - total tokens
  - cached input tokens when available
  - reasoning tokens when available
  - provider-specific sanitized usage metadata
- usage status and sanitized metadata
- output hash
- actions attempted
- graph mutation transaction ID
- status
- error
- timestamps

## Relationship to semantic maintenance

Graph automations should reuse lessons from semantic maintenance:

- dirty/change event ingestion
- durable queues
- analyzer/worker split
- retry/backoff
- degraded status

But automations should be separate from semantic maintenance because they are user/application-defined and may perform arbitrary AI enrichment or graph mutations.

Recommended packages:

```text
internal/automation/model
internal/automation/storage
internal/automation/service
internal/automation/worker
internal/automation/render
internal/automation/actions
```

## Open questions

- Should automation definitions be stored in Mycel, application-owned, or both?
- Should GQL condition evaluation use snapshot at original commit LSN or latest committed graph state?
- What is the first diagnostics API for warnings/failures?
- How much prompt/input/output should be retained for audit versus privacy?
- Should V1 be available in core Mycel, or first through Knot PKM only?
- How should model provider configuration be inherited from semantic/inference settings?
