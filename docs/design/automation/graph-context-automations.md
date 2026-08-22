# Graph context automations

## Status

Initial implementation landed. This document describes graph-context automation
behavior so an automation can summarize or otherwise derive data from a related
graph subtree, not only from the single node that triggered the run.

The motivating example is a daily journal:

```text
(:Journal {date: "2026-08-19"})-[:HAS_ENTRY {position: 1}]->(:JournalEntry)
(:Journal {date: "2026-08-19"})-[:HAS_ENTRY {position: 2}]->(:JournalEntry)
```

When a `JournalEntry` changes, mycel should summarize all entries for the parent
`Journal` and write the result back to the parent `Journal`, for example in
`Journal.properties.summary`.

## Current behavior

The current automation model is centered on the changed element:

```text
graph event
  -> candidate automation selected by `on`
  -> optional/read-only GQL condition evaluated with `changed` bound
  -> input rendered from `changed`
  -> inference call
  -> declared actions, primarily against `changed` or newly created refs
```

This works well for single-node enrichment, such as summarizing one `Page` into
`Page.payload.summaryMarkdown`. It is not sufficient for aggregate derivations
where the trigger node is not the target node and the LLM input must include
multiple related nodes.

## Goals

- Support automations that select a target graph element related to `changed`.
- Support deterministic input rendering from multi-node/multi-row graph context.
- Allow actions to update matched context aliases, not only `changed`.
- Support aggregate idempotency based on the collected context, not only the
  triggering node.
- Support debouncing/coalescing so bursts of entry edits produce one aggregate
  update per target journal.
- Preserve existing fail-closed inference profile, grant, policy, and audit
  behavior.
- Keep graph writes declarative and constrained; the LLM must not receive
  arbitrary write authority.

## Non-goals

- Arbitrary user-supplied server-side code.
- Synchronous transaction hooks.
- Unbounded graph-wide scans.
- Implicit destructive graph repair or merge behavior.
- Replacing semantic generation-rule maintenance or other system maintenance pipelines.

## Proposed model

### Trigger: `on`

`on` remains the cheap event prefilter. It decides whether a graph change should
create an invocation candidate.

Example:

```json
"on": {
  "events": ["node.created", "node.updated"],
  "labels": ["JournalEntry"]
}
```

The trigger should stay intentionally simple and fast: event type, labels, and
future schema-aware field-change predicates. It should not gather context.

### Condition

`condition.gql` remains an optional read-only guard. If omitted, the automation
matches by default and `changed` is bound to the triggering element.

When present, `condition.gql` must be anchored on `changed` and may return
additional aliases. Those aliases become available to later phases.

Example parent selection:

```gql
MATCH (journal:Journal)-[:HAS_ENTRY]->(changed:JournalEntry)
RETURN journal, changed
```

Semantics:

- zero matching rows: skip the invocation with a condition-false status;
- one matching row: proceed and bind returned aliases;
- multiple matching rows: fail closed with an actionable error; explicit fan-out
  remains future work.

### Target aliases

Automation phases should share a single alias environment. At minimum:

- `changed`: the triggering graph element;
- aliases returned by `condition.gql`, such as `journal`;
- `$refs.<name>`: nodes created by earlier actions in the same run.

Aliases should be typed. A node action target must resolve to one node. An edge
action target must resolve to one edge. Ambiguous, missing, or wrong-type aliases
must fail before mutation.

### Context input query

Add an input context query phase for cases where the model input is not only the
changed element.

Implemented shape:

```json
"input": {
  "target": "journal",
  "mode": "gql_template",
  "context": {
    "entries": {
      "gql": "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry, r ORDER BY r.properties.position FETCH FIRST 200 ROWS ONLY",
      "limit": 200
    }
  },
  "template": "..."
}
```

The context query runs after condition aliases are bound. It may reference those
aliases, especially the selected `journal`. The query must be read-only and
bounded in GQL with `FETCH FIRST`; an input-level `limit` can further cap
accepted rows.

For the journal summary use case, the context query collects all entries that
belong to the selected journal, ordered by a relationship property such as
`HAS_ENTRY.position`.

### Multi-row rendering

Input rendering must support deterministic rendering of context collections.
The renderer should provide a small, auditable template language that can:

- access scalar/node/edge fields;
- iterate named context collections;
- preserve deterministic order from the context query;
- render missing fields predictably;
- avoid arbitrary code execution.

Conceptual template:

```text
Summarize this journal day.

Date: {{ journal.properties.date }}
Title: {{ journal.properties.title }}

Entries:
{{#each entries}}
- {{ entry.payload.text }}
{{/each}}
```

The rendered input hash is computed from the final rendered text. Context query
row counts and target IDs are persisted as run diagnostics so idempotency can be
evaluated consistently without storing provider secrets.

### Actions against context aliases

Actions should be able to target aliases returned by the condition, not only
`changed`.

Desired update action:

```json
"actions": [
  {
    "update_node": {
      "target": "journal",
      "set": {
        "properties.summary": "$result.summary"
      }
    }
  }
]
```

Action validation rules:

- `target` must resolve to exactly one node for `update_node`;
- updates must be limited to declared fields/paths;
- schema validation applies before mutation;
- generated writes keep automation metadata for audit and loop prevention;
- if the computed update is identical to existing state, the run should be
  marked skipped/no-op rather than writing.

### Aggregate idempotency

Single-node idempotency is insufficient for aggregate summaries. The idempotency
key should be derived from the selected target plus the collected context.

Conceptual shape:

```json
"safety": {
  "idempotency": {
    "scope": "target",
    "target": "journal",
    "inputHashFields": [
      "journal.properties.date",
      "entries[*].entry.payload.text",
      "entries[*].r.properties.position"
    ],
    "skipIfOutputUnchanged": true
  }
}
```

The engine should persist successful hashes by automation ID/version, target ID,
and input hash. This prevents repeated summaries when unrelated entries trigger
the same journal context without changing the rendered input.

### Debounce and coalescing

Aggregate automations need a way to avoid one LLM call per child edit. The
engine should support debouncing/coalescing by selected target.

Implemented shape:

```json
"safety": {
  "debounce": {
    "duration": "30s",
    "coalesceBy": "journal"
  }
}
```

Semantics:

- multiple invocations selecting the same `journal` within the debounce window
  collapse into one run;
- the run uses the latest committed graph state when it executes;
- invocation records should indicate that they were coalesced into a target run;
- cancellation/disable must prevent pending coalesced work from running.

## End-to-end journal summary example

Conceptual future automation definition:

```json
{
  "id": "summarize_daily_journal",
  "name": "Summarize daily journal",
  "version": 1,
  "status": "enabled",
  "on": {
    "events": ["node.created", "node.updated"],
    "labels": ["JournalEntry"]
  },
  "condition": {
    "gql": "MATCH (journal:Journal)-[:HAS_ENTRY]->(changed:JournalEntry) RETURN journal, changed"
  },
  "input": {
    "target": "journal",
    "mode": "gql_template",
    "context": {
      "entries": {
        "gql": "MATCH (journal)-[r:HAS_ENTRY]->(entry:JournalEntry) RETURN entry, r ORDER BY r.properties.position FETCH FIRST 200 ROWS ONLY",
        "limit": 200
      }
    },
    "template": "Summarize this day.\n\n{{#each entries}}\n- {{ entry.payload.text }}\n{{/each}}"
  },
  "inference": {
    "operation": "chat",
    "profile": "summarize-journal",
    "parameters": {
      "responseFormat": "json",
      "maxOutputTokens": 512
    }
  },
  "prompt": "Return JSON: {\"summary\": string}",
  "output": {
    "mode": "json",
    "schema": {
      "type": "object",
      "required": ["summary"],
      "properties": {
        "summary": {"type": "string"}
      }
    },
    "actions": [
      {
        "update_node": {
          "target": "journal",
          "set": {
            "properties.summary": "$result.summary"
          }
        }
      }
    ]
  },
  "safety": {
    "ignoreSelfWrites": true,
    "idempotency": {
      "scope": "target",
      "target": "journal",
      "inputHashFields": [
        "journal.properties.date",
        "entries[*].entry.payload.text",
        "entries[*].r.properties.position"
      ],
      "skipIfOutputUnchanged": true
    },
    "debounce": {
      "duration": "30s",
      "coalesceBy": "journal"
    }
  }
}
```

A complete disabled example lives in
`examples/automations/summarize_daily_journal.json`.

## Execution semantics

A context automation run should execute as follows:

1. A graph commit emits a change event.
2. `on` selects candidate automations.
3. The engine creates an invocation record.
4. The condition is evaluated in a read transaction with `changed` bound.
5. Returned aliases are validated and stored on the invocation/run metadata.
6. Debounce/coalescing decides whether to run immediately or merge into pending
   work for the same target alias.
7. Context queries run against the latest committed graph state at execution
   time, with target aliases bound.
8. Input is rendered deterministically and hashed.
9. Idempotency checks compare automation/version/target/input hash.
10. Inference resolves through the normal profile, capability, credential grant,
    and policy path.
11. Output is parsed and schema-validated.
12. Declared actions are validated against alias types and schema.
13. Mutations apply in a new transaction and record automation metadata.
14. Run, usage, policy-decision, and audit metadata are persisted.

## Authorization and inference

Context automations should preserve existing authorization boundaries:

- the automation worker runs as the `automation` actor;
- inference requests run on behalf of the automation owner or configured
  principal;
- credential grants must explicitly allow the `automation` actor and, when
  delegated, the on-behalf-of principal;
- inference policies must allow the operation/profile/privacy class;
- graph writes remain constrained by automation definition and schema, not by
  model-generated arbitrary instructions.

## Compatibility

Existing automations continue to work:

- `changed` remains the canonical changed element alias;
- omitted `condition.gql` means the condition passes with `changed` bound;
- single-node input rendering and `update_node target: changed` remain valid;
- existing run records remain readable.

The new context features should be additive. Definitions that use `context`,
`gql_template`, alias action targets, aggregate idempotency, or debounce require
a daemon that supports those features and should fail validation on older daemon
versions.

## Risks and constraints

- **Unbounded scans:** context queries must be bounded or rejected.
- **Ambiguous targets:** multi-row conditions that bind different target nodes
  can cause duplicate or conflicting writes; default behavior should be explicit
  fan-out or fail-closed.
- **Prompt size:** large journals can exceed profile/capability token limits;
  rendering should expose truncation policy and run diagnostics.
- **Write races:** entries can change while a coalesced run is pending; the run
  should summarize the latest committed state and record revision/read metadata.
- **Looping:** updating the parent journal may trigger other automations;
  self-write metadata, depth limits, and idempotency remain required.
- **Auditability:** rendered input hashes, target aliases, context query IDs, and
  selected graph revisions should be persisted without leaking provider secrets.

## Resolved decisions and remaining limitations

- Context query collections live under `input.context`.
- The renderer mode is `gql_template`.
- Debounce lives under `safety.debounce` for the initial implementation.
- Aggregate idempotency currently uses the final rendered input hash and can be
  scoped to the selected target node.
- `update_node.target` supports node aliases. Edge action targets remain future
  work.
- Run records store diagnostics such as target alias/ID, context row counts, and
  coalesced invocation IDs; full context rows are not persisted.
- Multiple condition rows fail closed; fan-out remains future work.

## Suggested implementation tranches

1. **Alias actions:** allow `update_node.target` to reference a condition-returned
   node alias, with validation and tests.
2. **Context input query:** add bounded named GQL context queries that run after
   condition alias binding.
3. **Multi-row renderer:** add deterministic collection rendering for named
   context results.
4. **Aggregate idempotency:** key successful runs by automation/version/target
   and rendered context hash.
5. **Debounce/coalescing:** add target-based delayed execution for bursts of
   related graph changes.
6. **Diagnostics and docs:** expose selected target, context row counts,
   truncation status, and idempotency/coalescing decisions in run detail.
