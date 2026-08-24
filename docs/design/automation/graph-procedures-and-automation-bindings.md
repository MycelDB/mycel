# Graph procedures and automation bindings

## Status

Proposed design. This document refines the current graph automation model by separating reusable graph work from runtime bindings such as triggers, schedules, scope, and security context.

## Motivation

Current graph automation definitions combine several concerns in one JSON document:

- what work to perform;
- when to run it;
- where it applies;
- which inference profile/model to use;
- which principal owns or authorizes the runtime execution;
- debounce, idempotency, and retry behavior.

This is workable for simple automations, but it becomes ambiguous for platform-managed per-user automations. For example, an operator/admin service may create an automation for a user's domain. If the automation owner is inferred from the creator, background invocations can later run as:

```text
actor_principal_id: automation
on_behalf_of_principal_id: operator/admin
```

That is usually not the desired authorization or audit context. The intended subject is the user whose graph/domain is being processed.

The design goal is to make the reusable procedure and the runtime binding explicit.

## Terminology

### Graph procedure

A reusable, durable definition of graph work. It describes **what to do**.

A procedure contains the graph context assembly, prompt/workflow logic, inference operation defaults, output schema, graph mutations, and local safety ceilings. It is mostly principal-neutral and trigger-neutral.

Examples:

- summarize a PKM page;
- classify a document;
- extract entities from a note;
- propose relationship edges;
- run a multi-step enrichment workflow.

### Graph automation binding

A durable binding that describes **when, where, and for whom** a graph procedure runs.

A binding connects a procedure to graph events, schedules, scans, or manual invocation surfaces. It owns runtime context such as scope, owner principal, on-behalf-of principal, inference profile, debounce, idempotency, and enable/disable state.

### Graph invocation

A durable queued execution request created from a binding. An invocation records one trigger occurrence or one scheduled/manual request.

### Graph run

A concrete attempt to execute an invocation. Runs record rendered input hashes, inference decisions, usage telemetry, output hashes, mutations, errors, and attempt status.

## Conceptual model

```text
GraphProcedure
  reusable logic
        │
        │ referenced by
        ▼
GraphAutomationBinding
  trigger/schedule/manual binding
  scope
  runtime principal
  profile/policy selectors
        │
        │ creates
        ▼
GraphInvocation
  one event/schedule/manual request
        │
        │ attempted by
        ▼
GraphRun
  concrete execution attempt and audit record
```

## Graph procedure shape

Example page summary procedure:

```json
{
  "id": "knot-pkm.page-summary",
  "name": "Summarize PKM page",
  "version": 1,
  "status": "enabled",
  "description": "Summarizes a page from direct child and grandchild page entries.",

  "input": {
    "target": "page",
    "mode": "gql_template",
    "context": {
      "children": {
        "gql": "MATCH (page)-[r:contains]->(entry:pkm.page_entry) RETURN entry, r FETCH FIRST 200 ROWS ONLY",
        "limit": 200
      },
      "grandchildren": {
        "gql": "MATCH (page)-[:contains]->(parent:pkm.page_entry)-[r:contains]->(entry:pkm.page_entry) RETURN parent, entry, r FETCH FIRST 400 ROWS ONLY",
        "limit": 400
      }
    },
    "template": "Summarize this page..."
  },

  "inference": {
    "operation": "summarize",
    "parameters": {
      "responseFormat": "json",
      "maxInputTokens": 12000
    }
  },

  "prompt": "Create a concise, faithful summary. Return only valid JSON.",

  "output": {
    "mode": "json",
    "schema": {
      "type": "object",
      "required": ["summary"],
      "properties": {
        "summary": {
          "type": "object",
          "required": ["text", "status", "version"],
          "properties": {
            "text": {"type": "string"},
            "status": {"type": "string"},
            "version": {"type": "number"}
          }
        }
      }
    },
    "actions": [
      {
        "update_node": {
          "target": "page",
          "set": {
            "properties.summary": "$result.summary"
          }
        }
      }
    ]
  },

  "safety": {
    "ignoreSelfWrites": true,
    "maxActionItems": 5,
    "maxAttempts": 3
  }
}
```

Notes:

- The procedure has no graph trigger.
- The procedure has no space/domain scope.
- The procedure has no durable on-behalf-of principal.
- The procedure declares an inference operation but not necessarily the final profile. A binding may supply or override the profile.

## Graph automation binding shape

Example event binding for a user's PKM page entries:

```json
{
  "id": "knot-pkm.user.a80ce2ef.page-summary.entry-trigger",
  "name": "Summarize PKM pages when entries change",
  "procedure_id": "knot-pkm.page-summary",
  "procedure_version": 1,
  "status": "enabled",

  "scope": {
    "space_id": "af368527-3b7a-4070-87c3-8cd55ac51553",
    "domain_id": "a844c894-6f3b-4b13-bacd-e007bc49e0cb",
    "include_descendants": true
  },

  "trigger": {
    "type": "graph_event",
    "events": ["node.created", "node.updated"],
    "labels": ["pkm.page_entry"],
    "condition": {
      "gql": "MATCH (page:pkm.page)-[:contains*1..2]->(changed:pkm.page_entry) RETURN changed, page"
    }
  },

  "runtime": {
    "actor_principal_id": "automation",
    "owner_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "on_behalf_of_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "inference_profile_id": "01a0300b-fafd-77aa-b491-cff1231d7432"
  },

  "debounce": {
    "duration": "45s",
    "coalesce_by": "page"
  },

  "idempotency": {
    "scope": "target",
    "target": "page",
    "skip_if_output_unchanged": true
  }
}
```

Example schedule binding:

```json
{
  "id": "knot-pkm.user.a80ce2ef.page-summary.nightly",
  "procedure_id": "knot-pkm.page-summary",
  "status": "enabled",
  "scope": {
    "space_id": "af368527-3b7a-4070-87c3-8cd55ac51553",
    "domain_id": "a844c894-6f3b-4b13-bacd-e007bc49e0cb"
  },
  "trigger": {
    "type": "schedule",
    "cron": "0 3 * * *",
    "scan": {
      "gql": "MATCH (page:pkm.page) RETURN page FETCH FIRST 500 ROWS ONLY"
    }
  },
  "runtime": {
    "actor_principal_id": "automation",
    "owner_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "on_behalf_of_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "inference_profile_id": "01a0300b-fafd-77aa-b491-cff1231d7432"
  }
}
```

Example one-time binding:

```json
{
  "id": "knot-pkm.user.a80ce2ef.page-summary.backfill-2026-08-23",
  "procedure_id": "knot-pkm.page-summary",
  "status": "enabled",
  "scope": {
    "space_id": "af368527-3b7a-4070-87c3-8cd55ac51553",
    "domain_id": "a844c894-6f3b-4b13-bacd-e007bc49e0cb"
  },
  "trigger": {
    "type": "one_time_scan",
    "scan": {
      "gql": "MATCH (page:pkm.page) RETURN page FETCH FIRST 500 ROWS ONLY"
    }
  },
  "runtime": {
    "actor_principal_id": "automation",
    "owner_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "on_behalf_of_principal_id": "6e87ad16-b92e-41c3-80ba-3ea8ad2df9ab",
    "inference_profile_id": "01a0300b-fafd-77aa-b491-cff1231d7432"
  }
}
```

## Runtime principal semantics

The runtime context should separate the executor from the subject:

```text
actor_principal_id
  The concrete principal performing execution. For background work this is often
  the built-in automation principal.

owner_principal_id
  The principal that owns/administers the binding. This should not necessarily
  be the principal that created the binding.

on_behalf_of_principal_id
  The authorization, audit, and usage subject for the work. In a per-user PKM
  automation, this is the user's Mycel principal.
```

The runtime should not infer `on_behalf_of_principal_id` from `created_by_principal_id` when a binding explicitly supplies runtime context.

Recommended invocation derivation:

1. Start with binding runtime fields.
2. If an event has an origin principal and the binding permits event-origin override, use it as `on_behalf_of_principal_id`.
3. Otherwise use `binding.runtime.on_behalf_of_principal_id`.
4. Always record the final actor, owner, on-behalf-of, and event origin on the invocation/run.

## Authorization model

Graph automation execution should require explicit grants/policies. The built-in automation actor must not be globally privileged.

For per-user AI generation:

```text
grant grantee_principal_ids:
  - automation

grant allow_on_behalf_of_principal_ids:
  - <user-principal-id>

grant scope:
  space_id: <user-space-id>
  domain_id: <user-domain-id>

grant operations:
  - summarize
```

The inference resolver then evaluates:

```text
actor_principal_id == automation
on_behalf_of_principal_id == <user-principal-id>
scope matches user space/domain
operation matches summarize
profile/model/endpoint/capability constraints match
policy allows the content privacy class
```

This mirrors common delegated execution patterns in other systems:

- PostgreSQL `SECURITY DEFINER` / `SECURITY INVOKER` functions;
- SQL Server `EXECUTE AS`;
- service-account jobs with scoped OAuth grants;
- Kubernetes controllers running as service accounts under RBAC.

## Creation and audit semantics

Bindings should distinguish provisioning actor from runtime owner:

```text
created_by_principal_id
  The principal/API client that created the binding. For platform provisioning,
  this may be an operator/admin service principal.

updated_by_principal_id
  The principal/API client that last updated the binding.

runtime.owner_principal_id
  The durable owner/subject of the binding.
```

An operator can create a binding for a user without becoming the runtime on-behalf principal.

## Invocation and run records

Invocation records should include at least:

```json
{
  "id": "invocation-id",
  "binding_id": "knot-pkm.user.a80ce2ef.page-summary.entry-trigger",
  "procedure_id": "knot-pkm.page-summary",
  "procedure_version": 1,
  "event_id": "event-id",
  "event_type": "node.updated",
  "changed_element_id": "node-id",
  "actor_principal_id": "automation",
  "owner_principal_id": "user-principal-id",
  "on_behalf_of_principal_id": "user-principal-id",
  "event_origin_principal_id": "optional-origin-principal-id",
  "status": "pending"
}
```

Run records should include the same runtime principals plus execution details:

```json
{
  "id": "run-id",
  "invocation_id": "invocation-id",
  "binding_id": "binding-id",
  "procedure_id": "procedure-id",
  "status": "succeeded",
  "target_alias": "page",
  "target_node_id": "node-id",
  "inference_profile_id": "profile-id",
  "policy_decision_id": "decision-id",
  "credential_grant_id": "grant-id",
  "provider_request_id": "provider-request-id",
  "rendered_input_hash": "sha256...",
  "output_hash": "sha256...",
  "mutation_id": "mutation-id",
  "usage": {
    "input_tokens": 149,
    "output_tokens": 61,
    "total_tokens": 210,
    "status": "succeeded"
  }
}
```

## Backward compatibility

The current graph automation definition can be treated as a compatibility form where one document contains both procedure and binding fields.

Compatibility mapping:

```text
current automation id/name/version/status
  -> binding id/name/version/status

current on/condition
  -> binding.trigger

current input/inference/prompt/output/workflow/safety
  -> embedded procedure or generated procedure

current inferred owner_principal_id
  -> binding.runtime.owner_principal_id when no explicit runtime exists
```

Migration options:

1. Continue accepting legacy automation JSON and internally expand it to:
   - an anonymous or generated procedure;
   - a binding referencing that procedure.
2. Add new APIs for first-class procedures and bindings while keeping existing automation APIs as compatibility wrappers.
3. Eventually deprecate legacy combined automations after tooling migrates.

## API sketch

Procedure APIs:

```text
CreateGraphProcedure(domain_id, procedure_json)
UpdateGraphProcedure(domain_id, procedure_id, procedure_json)
GetGraphProcedure(domain_id, procedure_id)
ListGraphProcedures(domain_id)
DeleteGraphProcedure(domain_id, procedure_id)
```

Binding APIs:

```text
CreateGraphAutomationBinding(domain_id, binding_json)
UpdateGraphAutomationBinding(domain_id, binding_id, binding_json)
GetGraphAutomationBinding(domain_id, binding_id)
ListGraphAutomationBindings(domain_id)
EnableGraphAutomationBinding(domain_id, binding_id)
DisableGraphAutomationBinding(domain_id, binding_id)
DeleteGraphAutomationBinding(domain_id, binding_id)
```

Invocation APIs:

```text
ListGraphInvocations(domain_id, binding_id, status, limit)
RetryGraphInvocation(domain_id, invocation_id)
CancelGraphInvocation(domain_id, invocation_id)
```

Manual invocation API:

```text
InvokeGraphProcedure(domain_id, procedure_id, target, runtime_context)
InvokeGraphAutomationBinding(domain_id, binding_id, target_override)
```

## Storage sketch

File-backed storage could evolve to:

```text
automations/
  procedures/<domain-id>/<procedure-id>.json
  bindings/<domain-id>/<binding-id>.json
  invocations/<domain-id>/<yyyy-mm-dd>/<invocation-id>.json
  runs/<domain-id>/<yyyy-mm-dd>/<run-id>.json
  workflow-instances/<domain-id>/<yyyy-mm-dd>/<instance-id>.json
  workflow-steps/<domain-id>/<yyyy-mm-dd>/<step-run-id>.json
  proposals/<domain-id>/<yyyy-mm-dd>/<proposal-id>.json
  policies/<domain-id>.json
  schedule-checkpoints/<domain-id>/<binding-id>.json
```

## Open questions

- Should procedures be global, space-scoped, domain-scoped, or support all three?
- Should bindings reference procedures by immutable version, semver range, or latest enabled version?
- Should `runtime.inference_profile_id` be required on every binding that references an inference procedure, or may procedure defaults resolve it?
- Should event-origin principals ever override binding `on_behalf_of_principal_id`, and how should that be constrained?
- Should a binding be allowed to list multiple on-behalf principals, or should each user/domain have separate bindings?
- How should procedures declare required runtime capabilities so bindings can be validated before enablement?
- How should manual procedure invocation be represented: ephemeral binding, direct invocation, or both?

## Recommendation

Adopt first-class graph procedures and graph automation bindings.

For platform-managed per-user automations, create procedures once and create per-user bindings with explicit runtime context:

```text
actor_principal_id: automation
owner_principal_id: <user-principal-id>
on_behalf_of_principal_id: <user-principal-id>
scope: <user-space/domain>
inference_profile_id: <user-generation-profile>
```

This keeps reusable graph logic separate from trigger/runtime policy and prevents operator-created bindings from accidentally executing on behalf of the operator.
