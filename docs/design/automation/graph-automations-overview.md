# Graph automations

Graph automations are domain-scoped, asynchronous rules that react to committed graph changes. V1 supports constrained node automations: `node.created` / `node.updated` triggers, label prefilters, a `changed`-anchored GQL condition, text rendering from the changed node, and a single changed-node field update action.

See the design document at `docs/design/graph-automations.md` and the implementation plan at `docs/implementation/v0.4/graph-automations-v1-implementation-plan.md`.

## V1 definition format

Automation definitions are stored as canonical JSON documents. YAML/GWL-style authoring is intentionally deferred.

Minimal example:

```json
{
  "id": "summarize_page",
  "name": "Summarize page",
  "version": 1,
  "status": "disabled",
  "on": {
    "events": ["node.created", "node.updated"],
    "labels": ["Page"]
  },
  "condition": {
    "gql": "MATCH (changed:Page) RETURN changed"
  },
  "input": {
    "target": "changed",
    "fields": ["properties.title", "payload.text"]
  },
  "prompt": "Summarize this page in concise markdown.",
  "output": {
    "mode": "text",
    "actions": [
      {
        "update_node": {
          "target": "changed",
          "set": {
            "payload.summaryMarkdown": "$result.text"
          }
        }
      }
    ]
  }
}
```

A complete example lives at `examples/automations/summarize_page.json`.

## CLI

Create an automation:

```sh
mycel automation put --domain <domain-uuid> examples/automations/summarize_page.json
```

List automations:

```sh
mycel automation list --domain <domain-uuid>
```

Inspect one definition:

```sh
mycel automation get --domain <domain-uuid> summarize_page
```

Enable or disable:

```sh
mycel automation enable --domain <domain-uuid> summarize_page
mycel automation disable --domain <domain-uuid> summarize_page
```

Delete:

```sh
mycel automation delete --domain <domain-uuid> summarize_page
```

List invocation/run summaries:

```sh
mycel automation runs --domain <domain-uuid> --automation summarize_page --limit 50
```

## Admin API and UI

The daemon exposes client and admin automation services. `mycel-admin` includes an **Automations** tab on the space detail page that lists domain automations, toggles enabled/disabled state, shows definition JSON, lists recent invocations, and opens run detail JSON including inference profile, policy decision, token, provider-request, and action metadata.

A richer visual authoring experience remains future work.

## Operational notes

- Automations run asynchronously after graph commits.
- V1 workers poll pending invocations and record durable run status.
- Self-generated writes are tagged in node metadata and are ignored by the same automation by default.
- V1 only updates a configured field on the changed node.
- LLM generation is resolved through standalone inference profiles declared on automation definitions and workflow LLM steps.
- Automation runs record neutral inference telemetry: profile/model/capability refs, policy decision ID, provider request ID, and token counts.
- Worker controls include `MYCELD_AUTOMATION_WORKER_ENABLED`, `MYCELD_AUTOMATION_WORKER_INTERVAL`, `MYCELD_AUTOMATION_WORKER_BATCH_SIZE`, and `MYCELD_AUTOMATION_WORKER_CONCURRENCY`.
- Safety ceilings include `MYCELD_AUTOMATION_MAX_INPUT_TOKENS` and `MYCELD_AUTOMATION_MAX_OUTPUT_TOKENS`.

## V3 workflows

V3 adds a workflow definition shape for constrained multi-step automations. A workflow is still declarative JSON, but instead of a single `output.actions` block it has ordered/dependent steps:

```json
{
  "id": "enrich_research_note",
  "status": "disabled",
  "on": {"events": ["node.created"], "labels": ["ResearchNote"]},
  "workflow": {
    "steps": [
      {"id": "summarize", "kind": "llm"},
      {"id": "search_related", "kind": "tool", "tool": "debug.echo", "dependsOn": ["summarize"]},
      {"id": "propose_links", "kind": "proposal", "dependsOn": ["search_related"], "approval": "required"}
    ]
  }
}
```

Workflow validation enforces unique step IDs, known step kinds, known dependencies, and acyclic dependency graphs. Initial runtime support persists workflow instances and pending step runs for runnable steps. Proposal and policy records are also persisted internally so later APIs/UI can expose approval queues and budget/policy management.

A full example lives at `examples/automations/research_note_workflow.json`.

V3 schedule and scan triggers are also modeled:

```json
"on": {
  "schedule": {"interval": "1h"},
  "scan": {"gql": "MATCH (n:Page) RETURN n LIMIT 100"}
}
```

Scans must be bounded with `LIMIT`.

## Storage

File-backed automation state is stored under the daemon data directory:

```text
automations/
  definitions/<domain-id>/<automation-id>.json
  invocations/<domain-id>/<yyyy-mm-dd>/<invocation-id>.json
  runs/<domain-id>/<yyyy-mm-dd>/<run-id>.json
  workflow-instances/<domain-id>/<yyyy-mm-dd>/<instance-id>.json
  workflow-steps/<domain-id>/<yyyy-mm-dd>/<step-run-id>.json
  proposals/<domain-id>/<yyyy-mm-dd>/<proposal-id>.json
  policies/<domain-id>.json
  schedule-checkpoints/<domain-id>/<automation-id>.json
```

