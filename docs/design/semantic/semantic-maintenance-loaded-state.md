# Semantic maintenance loaded state

## Summary

Semantic maintenance should use a loaded, per-space maintenance state managed by
the semantic subsystem instead of constructing a fresh file-backed maintenance
manager for every maintenance mutation. The loaded state keeps in-memory indexes
for dirty events, dirty work items, and checkpoints while continuing to persist
all authoritative state through the existing maintenance files and raft/WAL apply
paths.

This is similar in spirit to the graph adjacency index: keep authoritative state
unchanged, but avoid repeatedly re-reading and scanning growing files for every
small operation.

## Motivation

Large Logseq-shaped imports create many graph commits. Each committed graph
change is converted into semantic maintenance dirty events, then
`AnalyzeDirtyWork` turns those events into dirty work items and checkpoints.

After graph adjacency indexing removed the graph hierarchy bottleneck, large
import profiling showed the next hotspot in semantic maintenance persistence:

- `semantic.applySemanticRaftCommand`
- `semantic.applySemanticMaintenance`
- `semantic.storage.UpsertDirtyWorkItem`
- `semantic.storage.persistJSON`
- `semantic.storage.loadJSON`
- `semantic.storage.MaintenanceManager.Init`

The current raft apply path can construct and initialize a new maintenance
manager for each semantic maintenance command:

```text
semantic maintenance raft command
  -> NewMaintenanceManager
  -> Init
       mkdir/stat
       load checkpoints JSON
       load work-state JSON
       maybe rebuild from work log
  -> apply one mutation
       append dirty event or upsert work/checkpoint
       rewrite state JSON as needed
```

Dirty event append also re-reads the dirty event log for idempotency:

```text
AppendGraphDirtyEvent
  -> read entire graph-dirty-000001.ksem
  -> scan for matching txn_id
  -> append one JSON line
  -> fsync
```

This produces repeated filesystem work and repeated JSON load/serialize costs as
maintenance state grows.

## Goals

- Keep one loaded maintenance manager per space inside the semantic subsystem.
- Avoid reloading checkpoint/work-state JSON for every raft-applied maintenance
  mutation.
- Avoid re-reading the entire dirty-event `.ksem` file for every dirty event
  append.
- Add in-memory indexes for common semantic maintenance lookups.
- Preserve authoritative raft/WAL semantics and persisted maintenance files.
- Preserve operator-driven recovery behavior; no automatic repair or restore.

## Non-goals

- Do not make in-memory state authoritative.
- Do not change semantic index public API contracts.
- Do not require LLM/provider credentials for dirty-work analysis.
- Do not implement automatic repair, merge, rebalance, restore, or
  authoritative-node selection.
- Do not optimize embedding generation/provider calls in this design.
- Do not change graph-change callback semantics.

## Current maintenance storage shape

For each space, semantic maintenance state is stored under:

```text
<data_dir>/graphs/<space_id>/semantic/maintenance/
```

Important files include:

```text
dirty/graph-dirty-000001.ksem
work/state.json
work/events-*.ksem or equivalent work event log
checkpoints.json
```

The dirty `.ksem` file stores semantic graph dirty events derived from committed
graph changes. Work-state JSON stores the current dirty work queue. Checkpoint
JSON stores analyzer progress.

## Proposed lifecycle

The semantic subsystem should cache loaded maintenance managers by space:

```text
semanticservice.Module
  maintenanceManagersBySpace[space_id] -> loaded MaintenanceManager
```

Lifecycle:

```text
first semantic maintenance access for space
  -> create file-backed maintenance manager
  -> Init loads persisted maintenance state
  -> build in-memory indexes
  -> cache manager in semanticservice.Module

subsequent mutation/read for same space
  -> reuse loaded manager
  -> update in-memory state and indexes
  -> persist append/state/checkpoint changes

semantic subsystem stop/reload
  -> close cached managers
  -> clear manager map

next access after restart/reload
  -> reload from persisted files
  -> rebuild indexes
```

The cached manager must be used for both local operations and raft-applied
maintenance commands. Graph-change callbacks should not maintain this state; they
remain an observation/input layer that calls `AppendGraphDirtyEvent`.

## Interface shape

The existing maintenance manager interface should remain mostly stable. Add a
lifecycle method if useful for cached manager cleanup:

```go
package storage

import (
    "context"

    "github.com/google/uuid"
    semantic "github.com/myceldb/mycel/internal/semantic/model"
    space "github.com/myceldb/mycel/internal/space/model"
)

type MaintenanceManager interface {
    Init(ctx context.Context, location string, spaceID space.SpaceID) error
    Close() error

    AppendGraphDirtyEvent(ctx context.Context, event semantic.GraphDirtyEvent) (semantic.GraphDirtyEvent, error)
    ListGraphDirtyEvents(ctx context.Context) ([]semantic.GraphDirtyEvent, error)

    GetCheckpoint(ctx context.Context, consumer string) (MaintenanceCheckpoint, error)
    SaveCheckpoint(ctx context.Context, checkpoint MaintenanceCheckpoint) error

    UpsertDirtyWorkItem(ctx context.Context, item semantic.SemanticDirtyWorkItem) (semantic.SemanticDirtyWorkItem, error)
    ListDirtyWorkItems(ctx context.Context) ([]semantic.SemanticDirtyWorkItem, error)

    ClaimReadyWork(ctx context.Context, in ClaimReadyWorkInput) ([]semantic.SemanticDirtyWorkItem, error)
    CompleteWork(ctx context.Context, id uuid.UUID, result WorkResult) error
    FailWork(ctx context.Context, id uuid.UUID, failure WorkFailure) error
}
```

If adding `Close` to the public internal interface creates too much churn, it can
be introduced as an optional interface first:

```go
type ClosableMaintenanceManager interface {
    Close() error
}
```

## Semantic module provider

The semantic service module should own cached manager lookup:

```go
type MaintenanceManagerProvider interface {
    MaintenanceManager(ctx context.Context, spaceID space.SpaceID) (storage.MaintenanceManager, error)
    CloseMaintenanceManagers(ctx context.Context) error
}
```

Implementation sketch:

```go
type Module struct {
    mu sync.Mutex

    maintenanceManagers map[space.SpaceID]storage.MaintenanceManager
}

func (m *Module) MaintenanceManager(ctx context.Context, spaceID space.SpaceID) (storage.MaintenanceManager, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if mgr := m.maintenanceManagers[spaceID]; mgr != nil {
        return mgr, nil
    }

    base := storage.NewMaintenanceManager()
    if err := base.Init(ctx, m.maintenanceDir(spaceID), spaceID); err != nil {
        return nil, err
    }

    var mgr storage.MaintenanceManager = base
    if m.wal != nil || m.raftGroups != nil {
        mgr = &walMaintenanceManager{
            inner:   base,
            module:  m,
            spaceID: spaceID,
        }
    }

    m.maintenanceManagers[spaceID] = mgr
    return mgr, nil
}
```

Care is needed in raft mode to avoid recursive wrapper behavior during apply. A
common pattern is:

- public/local mutation paths receive the raft/WAL wrapper;
- raft apply paths receive the base loaded manager so applying a committed raft
  command does not propose a new raft command.

That can be modeled with two helper methods:

```go
func (m *Module) MaintenanceManager(ctx context.Context, spaceID space.SpaceID) (storage.MaintenanceManager, error)
func (m *Module) baseMaintenanceManager(ctx context.Context, spaceID space.SpaceID) (storage.MaintenanceManager, error)
```

`MaintenanceManager` returns the wrapper when raft/WAL is enabled.
`baseMaintenanceManager` returns the loaded file-backed manager for apply paths.

## In-memory indexes inside maintenanceManager

The file-backed manager should maintain internal indexes rebuilt during `Init`:

```go
type maintenanceManager struct {
    mu sync.RWMutex

    location string
    spaceID  space.SpaceID

    dirtyEvents       []semantic.GraphDirtyEvent
    dirtyEventByTxnID map[uuid.UUID]int

    checkpoints          maintenanceCheckpointState
    checkpointByConsumer map[string]int

    dirtyQueue    dirtyQueueState
    workItemByKey map[dirtyWorkKey]int
}

type dirtyWorkKey struct {
    semanticIndexID semantic.SemanticIndexID
    targetNodeID    graph.NodeID
}
```

### Dirty event index

`AppendGraphDirtyEvent` should use `dirtyEventByTxnID` for idempotency instead of
re-reading `graph-dirty-000001.ksem` on every append.

Current expensive behavior:

```text
append dirty event
  -> read all dirty events from disk
  -> scan all txn IDs
  -> append event
```

Proposed behavior:

```text
manager Init
  -> read dirty events once
  -> build dirtyEventByTxnID

append dirty event
  -> check dirtyEventByTxnID
  -> append one JSON line
  -> update dirtyEvents and dirtyEventByTxnID
```

`ListGraphDirtyEvents` can return a copy of the loaded `dirtyEvents` slice.

### Dirty work index

`UpsertDirtyWorkItem` should use `workItemByKey` instead of scanning the entire
queue for every upsert.

Key:

```text
semantic_index_id + target_node_id
```

Current expensive behavior:

```text
upsert dirty work
  -> scan dirtyQueue.Items
  -> append work log record
  -> rewrite work-state JSON
```

Proposed first step:

```text
upsert dirty work
  -> lookup workItemByKey
  -> append work log record
  -> update in-memory dirtyQueue
  -> persist state as today
```

This removes the scan and repeated manager reload. A later optimization can
reduce full JSON state rewrites.

### Checkpoint index

`GetCheckpoint` and `SaveCheckpoint` should use a consumer index:

```text
consumer -> checkpoint index
```

This avoids scanning checkpoint slices and keeps save behavior deterministic.

## Persistence model

The first implementation should keep the current persistence files and write
semantics:

- dirty events are appended to `.ksem`;
- work events are appended to the work log;
- work state JSON is persisted on mutations as today;
- checkpoint JSON is persisted on checkpoint updates as today.

This limits risk. The immediate win comes from:

- no repeated manager initialization per mutation;
- no repeated JSON load per mutation;
- no repeated dirty `.ksem` read per append;
- indexed in-memory upsert/checkpoint lookup.

A later design can change persistence to append-first plus periodic compaction if
full JSON rewrites remain the bottleneck.

## Raft/WAL behavior

### Standalone/non-raft mode

```text
semantic mutation
  -> local WAL or direct file manager path, depending current configuration
  -> loaded base manager updates memory + persisted files
```

### Raft mode

```text
local semantic maintenance call
  -> raft/WAL wrapper
  -> propose semantic maintenance raft command
  -> raft apply on each node
  -> base loaded manager applies committed mutation
  -> memory indexes and persisted files update synchronously
```

The loaded manager must not change raft authority. It only avoids repeated disk
reloads and repeated scans while applying the same committed commands.

## Callback behavior

Graph-change callbacks remain inputs to semantic maintenance:

```text
graph commit
  -> graph-change event
  -> semantic dirty appender
  -> AppendGraphDirtyEvent
```

The callback does not maintain the loaded state directly. It calls the semantic
maintenance manager API. The manager owns its in-memory indexes.

## Consistency and recovery

The loaded state is safe because it is always reconstructable:

```text
process restart / subsystem reload / snapshot reload
  -> discard in-memory manager map
  -> reopen managers on demand
  -> read persisted files
  -> rebuild indexes
```

If a write fails after in-memory mutation but before persistence, the operation
must return an error and avoid leaving inconsistent loaded state. Implementations
should prefer this order:

1. validate and canonicalize mutation;
2. prepare new state;
3. persist append/state/checkpoint changes;
4. publish in-memory state update.

For existing code paths that mutate in-memory state before persistence, this
should be reviewed carefully during implementation.

## Open questions

- Should `Close` be added directly to `MaintenanceManager`, or introduced as an
  optional internal interface first?
- Should cached managers have idle eviction, or only close during semantic
  subsystem stop/reload?
- Should work-state JSON still be rewritten on every upsert in the first tranche,
  or should append-first compaction be implemented immediately after manager
  caching?
- Should dirty event idempotency be keyed only by `txn_id`, or by a wider key if
  future commits can share a transaction ID across distinct events?

## Expected impact

For large Logseq-shaped imports, this should reduce:

- repeated filesystem `MkdirAll` / `Stat` / `OpenFile` work;
- repeated `loadJSON` calls;
- repeated dirty `.ksem` reads;
- dirty work queue scans;
- checkpoint scans.

The next profiling run should show reduced time in:

- `semantic.storage.MaintenanceManager.Init`
- `semantic.storage.loadJSON`
- `semantic.storage.readGraphDirtyEvents`
- `semantic.storage.UpsertDirtyWorkItem`

If `persistJSON` remains dominant after this change, the next step is a separate
append-first/periodic-compaction design for semantic maintenance work state.
