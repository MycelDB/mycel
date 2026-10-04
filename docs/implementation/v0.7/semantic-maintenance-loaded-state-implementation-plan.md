# Semantic maintenance loaded state implementation plan

## Status

Proposed. This plan implements the design in
[Semantic maintenance loaded state](../../design/semantic/semantic-maintenance-loaded-state.md).

## Goal

Reduce semantic maintenance persistence overhead during large graph imports by
keeping one loaded maintenance manager per space and adding in-memory indexes for
common maintenance lookups. The implementation must preserve raft/WAL authority,
existing persisted maintenance files, and recovery behavior.

## Scope

In scope:

- Cache loaded semantic maintenance managers per space in `semanticservice.Module`.
- Add internal indexes to the file-backed maintenance manager:
  - dirty event by transaction ID;
  - dirty work item by semantic index/target node;
  - checkpoint by consumer.
- Stop re-reading the dirty `.ksem` file on every dirty event append.
- Stop reloading work/checkpoint JSON on every raft-applied maintenance mutation.
- Keep current persistence files and write semantics for the first tranche.
- Re-profile the synthetic Logseq import workload.

Out of scope:

- Persisted index format changes.
- Public API changes.
- LLM/embedding provider execution optimization.
- Automatic repair, restore, merge, rebalance, or authoritative-node selection.
- Store idle eviction/LRU unless needed by validation.
- Append-first/periodic-compaction rewrite of work-state persistence, except as a
  follow-up if profiles still show `persistJSON` dominance.

## Constraints

- Authoritative semantic maintenance mutation ordering remains raft/WAL owned.
- Loaded managers are not independently authoritative.
- Raft apply paths must apply committed mutations to the base loaded manager and
  must not propose new raft commands recursively.
- Graph-change callbacks remain callers of maintenance APIs; they do not maintain
  manager indexes directly.
- Each tranche must leave the semantic subsystem functional.

## Current bottleneck summary

After graph adjacency indexing, large synthetic Logseq import runs progress past
graph import and fail during semantic dirty-work analysis. Profiles show time in:

- `semantic.applySemanticRaftCommand`
- `semantic.applySemanticMaintenance`
- `semantic.storage.UpsertDirtyWorkItem`
- `semantic.storage.persistJSON`
- `semantic.storage.loadJSON`
- `semantic.storage.MaintenanceManager.Init`

The current apply path constructs and initializes a new maintenance manager for
many maintenance mutations, causing repeated directory checks and JSON reloads.
Dirty event append also re-reads the full dirty event `.ksem` file to dedupe by
transaction ID.

## Phase 1 — Add maintenance manager loaded indexes

### Tasks

1. Extend the file-backed `maintenanceManager` with internal indexes:

   ```go
   dirtyEvents       []domainsemantic.GraphDirtyEvent
   dirtyEventByTxnID map[uuid.UUID]int

   checkpointByConsumer map[string]int
   workItemByKey        map[dirtyWorkKey]int
   ```

2. Add key type:

   ```go
   type dirtyWorkKey struct {
       semanticIndexID domainsemantic.SemanticIndexID
       targetNodeID    graph.NodeID
   }
   ```

3. In `Init`, after loading persisted state:
   - read dirty events once;
   - populate `dirtyEvents`;
   - build `dirtyEventByTxnID`;
   - build `checkpointByConsumer` from loaded checkpoints;
   - build `workItemByKey` from loaded work state.

4. Keep all indexes private to the storage implementation.

### Tests

Add/extend semantic storage tests for:

- dirty event index rebuilt by `Init`;
- work item index rebuilt by `Init`;
- checkpoint index rebuilt by `Init`.

### Validation

```sh
go test ./internal/semantic/storage
```

## Phase 2 — Optimize dirty event append/list

### Tasks

1. Change `AppendGraphDirtyEvent` to:
   - check `dirtyEventByTxnID` instead of calling `readGraphDirtyEvents`;
   - append one JSON line to `graph-dirty-000001.ksem`;
   - update `dirtyEvents` and `dirtyEventByTxnID` only after append/fsync succeeds.

2. Change `ListGraphDirtyEvents` to return a defensive copy of loaded
   `dirtyEvents`.

3. Preserve idempotency behavior: if an event with the same `TxnID` is already
   loaded, return the existing event.

### Tests

Add/extend tests for:

- append idempotency without duplicated file records;
- list returns persisted events after reopen;
- returned list cannot mutate manager state.

### Validation

```sh
go test ./internal/semantic/storage
```

## Phase 3 — Optimize checkpoint lookup/save

### Tasks

1. Change `GetCheckpoint` to use `checkpointByConsumer`.
2. Change `SaveCheckpoint` to update or append using `checkpointByConsumer`.
3. Update `checkpointByConsumer` only after successful persistence, or carefully
   prepare/persist/publish to avoid in-memory/disk divergence on error.

### Tests

Add/extend tests for:

- get missing checkpoint returns default checkpoint;
- save new checkpoint updates index;
- save existing checkpoint updates index and persisted state;
- behavior survives reopen.

### Validation

```sh
go test ./internal/semantic/storage
```

## Phase 4 — Optimize dirty work upsert lookup

### Tasks

1. Change `UpsertDirtyWorkItem` to use `workItemByKey` instead of scanning
   `dirtyQueue.Items`.
2. Preserve existing merge behavior:
   - retain existing item ID;
   - preserve earliest `FirstGraphRevision`;
   - merge `SourceTxnIDs`;
   - increment generation;
   - reset pending/claim/completion fields as today.
3. Update `workItemByKey` when inserting new items.
4. Ensure delete/complete/fail/claim operations keep the index valid if they
   mutate item positions or remove items in future changes.

### Tests

Add/extend tests for:

- first upsert inserts and indexes item;
- second upsert for same semantic index/target updates existing item;
- source txn IDs and graph revisions merge correctly;
- state survives reopen;
- returned work-item lists are defensive copies if applicable.

### Validation

```sh
go test ./internal/semantic/storage
```

## Phase 5 — Cache base maintenance managers per space

### Tasks

1. Add a per-space base-manager map to `semanticservice.Module`, for example:

   ```go
   maintenanceManagers map[domainspace.SpaceID]storesemantic.MaintenanceManager
   ```

2. Initialize the map in `NewModule` or `Init`.
3. Add helper:

   ```go
   func (m *Module) baseMaintenanceManager(ctx context.Context, spaceID domainspace.SpaceID) (storesemantic.MaintenanceManager, error)
   ```

4. The helper should:
   - validate `spaceID`;
   - return a cached manager if present;
   - otherwise create `storesemantic.NewMaintenanceManager()`;
   - call `Init(ctx, m.maintenanceDir(spaceID), spaceID)` once;
   - cache and return it.

5. Update public `MaintenanceManager` method to use the cached base manager and
   wrap it in `walMaintenanceManager` when raft/WAL is enabled.

6. Avoid caching the wrapper if wrapper behavior can recursively propose raft
   commands during apply. Cache the base manager; construct lightweight wrappers
   as needed.

### Tests

Add semantic service tests for:

- repeated `MaintenanceManager(ctx, spaceID)` calls do not reload state from disk;
- mutations through the returned manager remain persisted;
- distinct spaces get distinct managers.

Where direct reload-count observation is hard, use behavior-focused tests plus a
small fake/spy manager if introducing test seams is low risk.

### Validation

```sh
go test ./internal/semantic/service
```

## Phase 6 — Use cached base manager in raft/WAL apply paths

### Tasks

1. Change `applySemanticMaintenance` from constructing a new manager per mutation
   to using `baseMaintenanceManager`.
2. Ensure apply path receives the base file-backed manager, not the raft/WAL
   wrapper.
3. Confirm these mutation kinds still apply correctly:
   - `dirty_event.append`
   - `checkpoint.save`
   - `work.upsert`
   - `work.complete`
   - `work.fail`
4. Confirm snapshot restore/reload paths reset or rebuild manager state as needed.

### Tests

Add/extend semantic raft tests for:

- repeated maintenance mutations do not require reinitializing from disk;
- dirty events/work/checkpoints converge after raft apply;
- idempotent command replay remains safe.

### Validation

```sh
go test ./internal/semantic/service
```

## Phase 7 — Manager close/reload lifecycle

### Tasks

1. Decide whether to add `Close() error` to `MaintenanceManager` or use optional
   close support:

   ```go
   type ClosableMaintenanceManager interface {
       Close() error
   }
   ```

2. On semantic subsystem stop/close:
   - close cached managers if supported;
   - clear the manager map.

3. On snapshot reload or subsystem reload, ensure cached loaded managers are
   invalidated before restored persisted state is used.

4. Keep no idle eviction in the first tranche unless retained memory validation
   shows a problem.

### Tests

Add/extend tests for:

- close clears manager cache;
- manager state reloads from persisted files after close/reinit;
- snapshot restore paths do not keep stale loaded state.

### Validation

```sh
go test ./internal/semantic/service ./internal/daemon/runtime
```

## Phase 8 — Re-profile synthetic Logseq import

### Small correctness run

```sh
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -v
```

### Large profiling run

```sh
MYCEL_RUN_LARGE_LOGSEQ_IMPORT_TEST=1 \
MYCEL_LOGSEQ_IMPORT_TIMEOUT_SECONDS=900 \
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -timeout=20m \
  -cpuprofile=/tmp/logseq-import.cpu.pprof \
  -memprofile=/tmp/logseq-import.mem.pprof
```

Expected improvements:

- Reduced time in `MaintenanceManager.Init`.
- Reduced time in `loadJSON`.
- Reduced time in `readGraphDirtyEvents`.
- Reduced dirty work queue scan overhead inside `UpsertDirtyWorkItem`.
- The large test should progress beyond the current semantic analysis failure or
  fail at a clearly later bottleneck.

## Phase 9 — Follow-up if persistJSON remains dominant

If profiling still shows `persistJSON` and full work-state rewrites dominating,
create a separate plan for append-first semantic maintenance persistence:

- append every work mutation to an event log;
- update in-memory state immediately after durable append;
- compact/snapshot work-state JSON periodically or by size threshold;
- rebuild from snapshot + log during manager init.

Do not combine that larger persistence-model change with the first loaded-state
tranche unless the simpler cached-manager/index work is insufficient.

## Acceptance criteria

- Dirty event append no longer re-reads the full dirty `.ksem` file per append.
- Dirty work upsert uses an in-memory key index.
- Checkpoint get/save uses an in-memory consumer index.
- Semantic service reuses a loaded base maintenance manager per space.
- Raft apply paths apply to the base loaded manager and do not recursively
  propose raft commands.
- Existing semantic storage/service tests pass.
- Full mycel test suite passes.
- Small synthetic Logseq import test passes.
- Large synthetic Logseq import profile shows reduced semantic manager init/load
  overhead.

## Rollback plan

Rollback should not require persisted data migration because the initial tranche
keeps existing file formats.

1. Revert semantic service manager cache usage.
2. Revert maintenance manager internal indexes to scan/load behavior.
3. Keep persisted `.ksem` and JSON files unchanged.
4. Re-run semantic storage/service tests and synthetic import small test.
