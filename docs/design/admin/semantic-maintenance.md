# Admin Semantic Maintenance API

## Status

Implemented daemon-backed maintenance API for semantic generation rules.

Protobuf source:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/semantic_maintenance.proto
```

## Service

```text
mycel.admin.v1.AdminSemanticMaintenanceService
```

Implemented RPCs:

- `GetSemanticMaintenanceStatus`
- `ListSemanticMaintenanceWork`
- `RetrySemanticMaintenanceWork`
- `CancelSemanticMaintenanceWork`
- `AnalyzeSemanticDirtyWork`
- `ProcessSemanticDirtyWork`
- `BackfillSemanticRule`

Visibility responses expose queue counters, lifecycle timestamps, sanitized error
summaries, work item metadata, semantic rule IDs, embedding binding keys, and
physical maintenance state only. They intentionally do not expose credential
secret values, raw source text, embedding vectors, raw provider request bodies,
or full provider responses.

## Rule and binding scope

Dirty analysis and generation are rule-native. Work coalesces by:

```text
semantic_rule_id + embedding_binding_key + target_node_id
```

`AnalyzeSemanticDirtyWork` may be scoped to a space, domain, semantic rule, and
embedding binding. It applies each rule's trigger, target selector, source
assembly policy, and `SemanticMaintenancePolicy.DirtyCooldown` before enqueuing
or updating work. `EarliestRunAt` remains the durable scheduling boundary.

`BackfillSemanticRule` requires an explicit semantic rule and embedding binding.
Backfill can be limited, forced, and configured to continue on item-level errors,
but it does not automatically choose authoritative nodes or repair divergent
state.

## Physical search-index behavior

Embedding generation writes durable vector records for a rule/binding/target and
updates or invalidates the derived physical search index for that binding. The
`mycel-file` backend stores latest-live search records under per-rule/per-binding
state and can rebuild the physical index from durable vector records.

Search-index state is derived and rebuildable. If a physical index is missing or
degraded, semantic search must either perform a bounded rebuild or fail closed
with a clear warning/error; it must not silently scan unbounded historical vector
records.

## Authorization

Requires an operator bearer token with semantic maintenance/manage capability.
The current `semantic_admin` role grants the required capability set.

## CLI

Daemon-backed commands are used when `--daemon-addr` is supplied:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance status --space-id '<space-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance list --space-id '<space-id>' --rule notes-search --binding search --limit 100

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance retry --space-id '<space-id>' '<work-item-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance cancel --space-id '<space-id>' '<work-item-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance analyze --space-id '<space-id>' --rule notes-search --binding search

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance process --space-id '<space-id>' --limit 10

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule backfill notes-search \
  --space-id '<space-id>' \
  --binding search \
  --force \
  --continue-on-error
```

When a rule argument is a key, daemon mode resolves it within the supplied space.
Domain filters may use domain keys such as `default` or domain UUIDs.

## Notes and limitations

- The default vector/search backend is `mycel-file`.
- Retry returns a work item to pending and clears sanitized error fields; cancel
  marks an item cancelled.
- The worker processes pending dirty work in bounded passes; durable job
  scheduling remains future work.
- Legacy embedding migration is a separate closed-window Admin API path;
  semantic maintenance itself does not read legacy profiles.
