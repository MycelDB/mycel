# Semantic Maintenance Batched Dirty-Work Upserts

## Status

Implemented rule-native batching direction for semantic maintenance.

## Purpose

Dirty analysis can produce many target/binding work updates from one graph
transaction. Batched upserts reduce write amplification while preserving durable,
coalesced maintenance state.

## Coalescing key

Dirty work merges by:

```text
semantic_rule_id + embedding_binding_key + target_node_id
```

The analyzer should emit at most one effective update for each key in a batch.
When multiple dirty events affect the same key, the update merges transaction
metadata and uses the latest `earliest_run_at` implied by the rule's dirty
cooldown.

## Durable flow

```text
analyze dirty events
  -> build per-rule/per-binding target updates
  -> write one durable batched upsert record
  -> apply updates to dirty-work files
  -> update loaded-state indexes synchronously
```

If a batched write fails, the analyzer must leave prior durable state intact and
return an actionable error. It must not partially repair or rewrite unrelated
work items.

## Safety rules

- Batch size is bounded by maintenance policy and operator/admin limits.
- Work identity always includes semantic rule ID and embedding binding key.
- Retry/cancel/process operations still address individual durable work items by
  ID.
- Batching must not hide sanitized error state or queue counters from status
  APIs.
- Batching does not change provider-call idempotency; workers still skip
  unchanged source/binding/vector-space combinations.
