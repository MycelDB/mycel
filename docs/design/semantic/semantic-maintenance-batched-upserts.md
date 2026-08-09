# Semantic Maintenance Batched Dirty-Work Upserts

## Status

Proposed for the semantic maintenance performance tranche on
`refactor_for_performance`.

## Context

Large Logseq-like imports can create thousands of graph dirty events and dirty
semantic targets. The first semantic maintenance loaded-state tranche removed
repeated manager initialization and repeated JSON state rewrites from the hot
path, but large `AnalyzeDirtyWork` runs still emit one semantic maintenance raft
mutation per dirty-work target:

```text
work.upsert
work.upsert
work.upsert
...
```

Each mutation is authoritative raft/WAL traffic for the semantic subsystem. In a
large run this can produce many thousands of raft proposals, raft log appends,
and semantic maintenance apply calls. Diagnostic logs gated by
`MYCEL_COMMIT_TIMING=1` showed the final large-test failure was a context
deadline while waiting for one more `semantic.maintenance.mutation.v1` proposal,
after partition raft had already reached more than twelve thousand committed
entries.

The goal of this design is to preserve raft/WAL authority while reducing proposal
volume by batching dirty-work upserts.

## Goals

- Batch semantic dirty-work upserts produced by `AnalyzeDirtyWork`.
- Keep raft/WAL as the authoritative mutation path.
- Preserve existing semantic maintenance file formats and replay compatibility
  for existing `work.upsert` records.
- Keep batches bounded by count and by the caller's context/deadline.
- Make batch apply deterministic and equivalent to applying the same items one at
  a time in order.
- Avoid recursive raft proposal during raft apply.

## Non-goals

- No automatic repair, restore, merge, rebalance, or authoritative-node
  selection.
- No change to public API contracts.
- No generated API/SDK changes.
- No change to semantic provider execution or embedding processing.
- No replacement of raft storage in this tranche.
- No new independent wall-clock timeout for analyzer runs in the first tranche;
  callers continue to control time through `context.Context`.

## Design

### Batch capability

The storage interface remains source-compatible. A private optional capability is
introduced for managers that can upsert multiple dirty-work items in one call:

```go
type dirtyWorkBatchUpserter interface {
    UpsertDirtyWorkItems(
        ctx context.Context,
        items []domainsemantic.SemanticDirtyWorkItem,
    ) ([]domainsemantic.SemanticDirtyWorkItem, error)
}
```

This capability is implemented by:

- the base file-backed `maintenanceManager`;
- the raft/WAL wrapper `walMaintenanceManager`.

Callers that only know about `MaintenanceManager` can probe for the optional
capability. Fallback remains single-item `UpsertDirtyWorkItem`.

### Mutation kind

The semantic maintenance mutation stream gains a new kind:

```text
work.upsert_batch
```

The mutation payload is:

```go
[]domainsemantic.SemanticDirtyWorkItem
```

Existing mutation kinds remain valid:

```text
dirty_event.append
checkpoint.save
work.upsert
work.complete
work.fail
```

`work.upsert_batch` is committed exactly like other semantic maintenance
mutations:

- WAL mode appends one WAL record and applies it locally.
- Raft mode proposes one raft command to the owning semantic partition group.
- Raft apply uses the cached base maintenance manager, not the raft/WAL wrapper.

### Analyzer batching

`Analyzer.enqueueForEvent` builds dirty-work items for resolved targets and
flushes them in chunks.

Primary bound: item count.

```go
batchSize := configuredMaxBatchSize
if batchSize <= 0 {
    batchSize = 128
}
```

The semantic service passes `MaintenanceConfig.MaxBatchSize` into the analyzer.
For the large synthetic Logseq test this is already tied to the import chunk
size, commonly `200`.

Safety bound: context cancellation/deadline.

The analyzer checks `ctx.Err()` before building/flushing batches and returns the
context error if the caller's deadline expires. No separate timer is introduced in
this tranche.

### File-backed batch apply

The file-backed maintenance manager applies a batch under the manager lock.
For each item, it performs the same normalization and merge rules used by
`UpsertDirtyWorkItem`:

- generate ID when missing;
- set created/updated timestamps;
- reset running/failed fields for pending upserts;
- merge by `(semantic_index_id, target_node_id)`;
- preserve existing item ID and created time;
- increment generation;
- preserve the earliest first graph revision;
- merge source transaction IDs.

Batch apply is deterministic: items are processed in payload order, equivalent
to repeated single-item upserts.

### Work event log

The existing work event log remains the durable mutation stream. It gains support
for a batch record while continuing to read older single-item records:

```json
{"kind":"upsert","item":{...}}
{"kind":"upsert_batch","items":[{...},{...}]}
```

The `workLogRecord` shape is extended with optional `Items`:

```go
type workLogRecord struct {
    Kind  string                               `json:"kind"`
    At    time.Time                            `json:"at"`
    Item  domainsemantic.SemanticDirtyWorkItem `json:"item,omitempty"`
    Items []domainsemantic.SemanticDirtyWorkItem `json:"items,omitempty"`
}
```

Replay rules:

- `upsert`, `claim`, `complete`, and `fail` with `item` continue to work.
- `upsert_batch` replays each `items[]` element in order.
- Corrupt records remain errors, matching current behavior.
- Empty batches are ignored.

This keeps existing persisted files readable and avoids making `state.json` more
authoritative than later event-log records.

### Raft/WAL wrapper behavior

`walMaintenanceManager.UpsertDirtyWorkItems` commits one
`work.upsert_batch` mutation for the batch. It does not apply the items directly
before raft/WAL authority has accepted the mutation.

Pseudo-flow:

```go
func (w *walMaintenanceManager) UpsertDirtyWorkItems(ctx context.Context, items []Item) ([]Item, error) {
    if len(items) == 0 {
        return nil, nil
    }
    err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{
        Kind:    "work.upsert_batch",
        SpaceID: w.spaceID,
        Payload: raw(items),
    })
    if err != nil {
        return nil, err
    }
    return append([]Item(nil), items...), nil
}
```

`applyMaintenanceMutation` handles the new kind against the base manager:

```go
case "work.upsert_batch":
    var items []domainsemantic.SemanticDirtyWorkItem
    _ = json.Unmarshal(r.Payload, &items)
    if batcher, ok := mgr.(dirtyWorkBatchUpserter); ok {
        _, err := batcher.UpsertDirtyWorkItems(ctx, items)
        return err
    }
    for _, item := range items {
        if _, err := mgr.UpsertDirtyWorkItem(ctx, item); err != nil {
            return err
        }
    }
```

The fallback is retained for test doubles and older manager implementations.

## Consistency and failure semantics

- A batch is one raft/WAL mutation.
- If the mutation is not committed, no dirty-work items from the batch are
  authoritative.
- If the mutation is committed and apply begins, the base manager appends one
  `upsert_batch` work-log record and then updates in-memory indexes.
- Replaying the work log reconstructs the same state as the original apply.
- Duplicate raft command handling remains command-ID based and unchanged.
- `operation_id` remains correlation-only metadata and is not used for
  idempotency, authorization, ordering, or replay protection.

## Expected impact

For a large import that previously produced thousands of single-item semantic
maintenance proposals, batching should reduce proposal count approximately by the
configured batch size.

Example with batch size `200`:

```text
10,000 dirty-work upserts -> about 50 raft proposals
```

This should reduce:

- semantic raft proposal wait time;
- raft log append pressure;
- repeated semantic maintenance apply overhead;
- likelihood of exhausting the large import context deadline.

## Diagnostics

Existing `MYCEL_COMMIT_TIMING=1` diagnostics remain the primary validation aid.
After batching, successful large runs should show fewer
`semantic.maintenance.mutation.v1` proposal completions with larger
`payload_bytes`, rather than many thousands of small `work.upsert` proposals.

Useful command:

```sh
MYCEL_COMMIT_TIMING=1 \
MYCEL_RUN_LARGE_LOGSEQ_IMPORT_TEST=1 \
MYCEL_LOGSEQ_IMPORT_TIMEOUT_SECONDS=900 \
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -timeout=20m
```

## Validation plan

- Storage:
  - batch upsert produces the same final dirty-work state as repeated single
    upserts;
  - `upsert_batch` records replay correctly after manager reload;
  - existing `upsert` records still replay correctly.
- Maintenance analyzer:
  - uses batch capability when available;
  - falls back to single upserts when unavailable;
  - respects batch size and context cancellation.
- Semantic service:
  - `work.upsert_batch` WAL/raft apply uses the base maintenance manager;
  - no recursive proposal occurs during raft apply.
- Integration:
  - small synthetic Logseq import passes;
  - large synthetic Logseq import completes within the configured timeout or
    advances to the next bottleneck with clear diagnostics.
