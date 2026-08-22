# Semantic Maintenance Loaded State

## Status

Implemented loaded-state direction for rule-native semantic maintenance.

## Purpose

Semantic maintenance keeps durable dirty events, dirty work, checkpoints, and
status files in the semantic subsystem. Loaded state avoids scanning every file
for each maintenance mutation by maintaining in-memory indexes rebuilt during
startup and updated synchronously with durable writes.

The loaded state is an implementation detail. Public APIs expose semantic
generation rules, embedding bindings, work items, and physical search-index
status rather than loaded-state internals.

## In-memory indexes

The file-backed maintenance manager maintains indexes for common lookups:

- dirty events by space/domain/time;
- dirty work by status and `earliest_run_at`;
- dirty work by `semantic_rule_id + embedding_binding_key + target_node_id`;
- work items by ID;
- checkpoints by consumer/rule/binding;
- aggregate counters for status responses.

These indexes are rebuilt from authoritative durable files during `Init` and
updated synchronously after append/upsert/cancel/retry operations.

## Work identity

Dirty work coalesces by:

```text
semantic_rule_id + embedding_binding_key + target_node_id
```

Repeated dirty events for the same target update the existing work item, merge
transaction/source metadata, and push out `earliest_run_at` according to the
rule's `SemanticMaintenancePolicy.DirtyCooldown`.

## Mutation flow

```text
append dirty event
  -> durable event file append
  -> dirty-event indexes update

analyze dirty event
  -> load enabled semantic generation rules
  -> select targets and bindings
  -> durable dirty-work upsert
  -> dirty-work indexes update

retry/cancel/process work
  -> durable work mutation
  -> status/counter indexes update
```

Durable files remain authoritative. Loaded-state corruption is handled by
rebuilding indexes from durable state; it must not trigger automatic divergent
repair or destructive reconciliation.

## Status and diagnostics

Status responses derive from loaded counters and expose safe metadata only:

- queue depth by status;
- oldest pending age;
- last dirty/analyzed/worker timestamps;
- sanitized last error category/message;
- rule and binding IDs on work items;
- degraded state when durable files or search-index state cannot be loaded.

Diagnostics do not expose credentials, provider request bodies, raw source text,
embedding vectors, or full provider responses.
