# Embedding Generation Implementation Plan

## Status

Implementation plan for the daemon-owned semantic embedding generation pipeline described in [embedding-package.md](../../design/semantic/embedding-package.md).

Initial implementation slices completed:

- Phase 1 complete: `internal/graph/change` neutral commit event/sink package.
- Phase 2 complete: `FileSession` and daemon graph module emit graph-change events after successful commits, preserve old/new context for moves/deletes/reorders, and do not emit for rollback/discard, read-only commits, or no-op commits.
- Sink failures do not fail graph commits; failures are recorded as maintenance-degraded state on the emitting component.
- Phase 3 complete: `store/semantic.MaintenanceManager` owns dirty events, checkpoints, dirty work items, append-only work records, claim leases, completion, and failure state.
- Semantic analyzer/worker use `MaintenanceManager` for operational state and `SpaceManager` for semantic resources.
- Phase 4 complete: `internal/semantic/maintenance.DirtyEventAppender` adapts graph-change events to durable semantic dirty events without depending on `SpaceManager`.
- Phase 5 complete: analyzer uses maintenance checkpoints, semantic source policies, graph/template reads, nearest ancestor-or-self subtree resolution, old/new move context, delete cleanup/refresh handling, and configurable dirty cooldown.
- Phase 6 complete: one-shot worker processing supports configurable batch size, lease duration, worker-pool concurrency, retry/permanent failure classification, exponential backoff, non-forced refreshes for source-hash idempotency, delete/tombstone work, structured failure logging, and daemon config defaults.
- Phase 7 complete: daemon semantic module starts/stops analyzer and worker maintenance loops when enabled, uses configured intervals/batch/worker settings, records loop stats/errors, and shuts down through runtime/module close.

This plan assumes the daemon-only architecture: applications write graph data through `myceld`, and `myceld` owns graph storage, semantic resources, dirty event persistence, background workers, vector records, and accounting.

## Target architecture

```text
Graph/session commit
  -> internal/graph/change.Sink
  -> semantic dirty-event appender
  -> store/semantic.MaintenanceManager append-only dirty log
  -> internal/semantic/maintenance analyzer
  -> MaintenanceManager work queue
  -> internal/semantic/maintenance worker pool
  -> internal/semantic/connectors provider call
  -> internal/semantic/vectorstore advanced record/tombstone
  -> store/accounting usage event
```

Key boundaries:

- `FileSession` emits graph commit notifications through a neutral `internal/graph/change` interface.
- `SpaceManager` remains focused on semantic resources: indexes, grants, policies, decisions, states.
- `MaintenanceManager` owns dirty events, checkpoints, work items, leases, attempts, backoff, and failures.
- `internal/semantic/maintenance` owns analyzer/worker behavior.
- `internal/daemon/modules/semantic` owns lifecycle and dependency wiring.

## Non-goals

- Do not call embedding providers synchronously from graph writes.
- Do not put embedding scheduling into `SpaceManager`.
- Do not reintroduce legacy embedding profile/file-session generation as the primary path.
- Do not make applications responsible for dirty queue or worker execution.

## Phase 1: Graph change notification boundary

### Goal

Add a neutral graph commit event/sink boundary that graph/session code can call without importing semantic packages.

### Tasks

- Add `internal/graph/change` package:
  - `CommittedEvent`
  - `ChangedEdgeRef`
  - `Sink`
  - `MultiSink`
  - no-op sink for tests/defaults
- Include event fields needed for semantic analysis:
  - transaction ID
  - graph revision
  - space ID
  - domain ID
  - created/updated/deleted node IDs
  - added/updated/deleted containment edges
  - old/new parent maps
  - old/new domain maps
  - committed timestamp
- Add tests for `MultiSink` ordering/error behavior.
- Sink failure semantics:
  - graph commits do not fail when a sink fails
  - sink errors are recorded as maintenance-degraded state and should be surfaced through status/Admin APIs

### Acceptance

```sh
go test ./internal/graph/change
```

## Phase 2: Wire graph commits to graphchange sink — complete

### Goal

Make graph mutations notify committed changes without knowing about semantic maintenance.

### Tasks

- Add optional `GraphChangeSink`/`ChangeSink` field to `filesession.Config` or graph module/session construction.
- Extend `FileSession.commitGraphAtRevision` to build a `graphchange.CommittedEvent` before commit while old state is available, then emit it after a successful graph commit.
- Preserve enough old state before mutation for deletes/moves:
  - deleted nodes
  - old parent containment
  - old domain
- Notify the sink synchronously after commit with the committed event.
- Ensure read-only transactions do not emit events.
- Ensure failed/rolled-back transactions do not emit events.
- Add tests for:
  - node create/update/delete events
  - edge add/delete/reorder/move context
  - transaction rollback/discard emits no event
  - read-only/no-op commits emit no event
  - sink failure behavior
  - sink failure is non-fatal and records degraded state

### Acceptance

```sh
go test ./internal/graph/filesession ./internal/daemon/modules/graph
```

## Phase 3: MaintenanceManager persistence — complete

### Goal

Add durable append-only semantic maintenance storage that is separate from `SpaceManager`.

### Tasks

- Add `store/semantic.MaintenanceManager` interface.
- Add data models:
  - `GraphDirtyEvent`
  - `MaintenanceCheckpoint`
  - `SemanticDirtyWorkItem`
  - `ClaimReadyWorkInput`
  - `WorkResult`
  - `WorkFailure`
- Implement file-backed persistence under:

```text
graphs/<space_id>/semantic/maintenance/
  dirty/
    graph-dirty-000001.ksem       # append-only newline JSON dirty-event segment
  work/
    work-000001.ksem              # append-only newline JSON work-state segment
    state.json                    # materialized queue state
  checkpoints.json
```

- Dirty and work segments are append-only single-segment logs in the Phase 3 implementation.
- `state.json` is a materialized view of the latest work-item state and can be rebuilt from `work-000001.ksem`.
- Future hardening can add manifest files and segment rotation:

```text
  dirty/
    manifest.json
    segments/*.kdirty
  work/
    manifest.json
    segments/*.kwork
```
- Add replay/rebuild support for `state.json` from work segments if missing/corrupt.
- Add claim/lease methods:
  - claim ready pending work
  - renew/expire stale claims
  - complete/fail work
- Add tests for:
  - append/list dirty events
  - checkpoint read/write
  - work-item upsert/coalescing
  - claim leases and recovery
  - state rebuild from segments

### Acceptance

```sh
go test ./internal/semantic/storage
```

## Phase 4: Semantic dirty-event appender — complete

### Goal

Convert graph change notifications into durable maintenance dirty events.

### Tasks

- Add `internal/semantic/maintenance.DirtyEventAppender` implementing `graphchange.Sink`.
- It should depend on a `MaintenanceManager` factory/resolver, not `SpaceManager`.
- Convert `graphchange.CommittedEvent` to `store/semantic.GraphDirtyEvent`.
- Keep this path lightweight:
  - no index lookup
  - no policy evaluation
  - no provider calls
  - no vector writes
- Wire daemon semantic module/graph module so graph sessions receive this sink when semantic maintenance is enabled.
- Add tests with real and fake maintenance managers, including full old/new context conversion and missing-manager behavior.

### Acceptance

```sh
go test ./internal/semantic/maintenance ./internal/daemon/modules/semantic ./internal/daemon/modules/graph
```

## Phase 5: Analyzer/coalescer — complete

### Goal

Analyze raw dirty events and upsert debounced target-level semantic work items.

### Tasks

- Add analyzer in `internal/semantic/maintenance`.
- Analyzer inputs:
  - `SpaceManager` for active indexes/source policies and index state
  - `MaintenanceManager` for dirty events/checkpoints/work items
  - graph session/read model for nodes/edges/templates
- For each dirty event batch:
  - use per-index maintenance checkpoints to process each dirty event once
  - load active semantic indexes by space/domain
  - resolve embedding target per index/source policy
  - rely on `MaintenanceManager.UpsertDirtyWorkItem` for target-level coalescing keyed by:

```text
space_id + domain_id + semantic_index_id + target_node_id
```

- Target resolution rules:
  - `self`: target is changed node if it matches index root/template policy
  - `subtree`: nearest ancestor-or-self matching index root/template policy
  - edge move/reorder: analyze old and new containment paths
  - delete: refresh old containing subtree root when available, otherwise enqueue delete/cleanup for the deleted target
- Apply configurable dirty cooldown through work item `earliest_run_at`:

```text
earliest_run_at = analyze_time + semantic.maintenance.dirty_cooldown
```

- Save analyzer checkpoints only after successful work-item upserts.
- Add tests for:
  - self target resolution
  - subtree target resolution
  - dropped irrelevant node
  - move dirties old and new roots
  - delete refreshes old containing subtree root
  - repeated edits coalesce through `MaintenanceManager`
  - cooldown sets `earliest_run_at`
  - checkpoint skip on subsequent analyzer runs

### Acceptance

```sh
go test ./internal/semantic/maintenance
```

## Phase 6: Worker pool — complete

### Goal

Process ready semantic work items asynchronously and idempotently.

### Tasks

- Add worker in `internal/semantic/maintenance`.
- Add daemon config. Default dirty cooldown is 60s and is configurable at the system level:

```yaml
semantic:
  maintenance:
    enabled: true
    dirty_cooldown: 60s
    analyzer_interval: 5s
    worker_interval: 5s
    worker_count: 2
    max_batch_size: 100
    max_concurrent_provider_calls: 4
    max_requests_per_minute: 60
    max_tokens_per_minute: 100000
    provider_defaults:
      max_concurrent_calls: 2
      max_requests_per_minute: 60
      max_tokens_per_minute: 100000
    credential_defaults:
      max_concurrent_calls: 1
      max_requests_per_minute: 30
      max_tokens_per_minute: 50000
```

- Effective throttling is the strictest applicable limit across:

```text
system global limit
  ∩ provider limit
  ∩ model/capability limit
  ∩ credential limit
  ∩ credential grant limit
```

- Worker flow:
  1. claim ready work items with lease
  2. process claimed work through configurable worker-pool concurrency
  3. for refresh work, call semantic backfill with `Force=false` so source-hash idempotency can skip current records
  4. for explicit backfill work, call semantic backfill with `Force=true`
  5. for delete/cleanup work, call semantic vectorstore delete/tombstone
  6. classify failures as retryable/permanent
  7. apply exponential backoff for retryable failures
  8. complete/fail work item through `MaintenanceManager`

Backfill execution owns endpoint/model/capability lookup, policy/grant/credential resolution, source assembly, source-hash checks, connector calls, accounting, and vector upserts.
- Add retry/backoff policy:
  - retryable: rate limit, transient provider/transport errors
  - permanent: invalid config, missing credential, policy denied, source excluded/configuration errors
- Add structured logging for all failures.
- Add tests with fake backfill runner and vectorstore.

### Acceptance

```sh
go test ./internal/semantic/maintenance ./internal/semantic/backfill ./internal/semantic/vectorstore
```

## Phase 7: Daemon lifecycle and config — complete

### Goal

Start analyzer and worker loops as part of `myceld` when semantic maintenance is enabled.

### Tasks

- Extend daemon config with semantic maintenance settings.
- Wire semantic module startup:
  - create maintenance manager resolver
  - create dirty event appender sink
  - register sink with graph/session construction
  - start analyzer loop(s)
  - start worker loop(s)
  - record loop run/error stats
  - shut down cleanly on daemon/runtime close
- Ensure disabled config still records graph data but does not run background workers.
- Add tests for startup/shutdown, runtime close behavior, loop execution, and config defaults.

### Acceptance

```sh
go test ./internal/daemon/config ./internal/daemon/app ./internal/daemon/modules/semantic
```

## Phase 8: Admin visibility and controls

### Goal

Expose maintenance state to operators.

### Tasks

- Add Admin APIs/CLI commands for:
  - list dirty event checkpoints
  - list pending/running/failed work
  - retry failed work
  - cancel work
  - force analyze now
  - force process now
  - summarize worker status
- Include readiness/status fields:
  - queue depth
  - oldest pending item age
  - failed count by category
  - throttle/backoff status
  - last successful worker time
- Add tests for Admin API authorization and CLI output.

### Acceptance

```sh
go test ./internal/daemon/api/admin ./internal/cli/cmd
```

## Phase 9: Cleanup legacy APIs

### Goal

Remove or further isolate legacy embedding profile APIs once semantic maintenance is stable.

### Tasks

- Remove unsupported direct embedding methods from internal session interfaces if no callers remain. **Done.**
- Remove obsolete domain embedding policy APIs/storage. **Done.**
- Remove legacy profile CRUD from `internal/embedding/store`; keep migration-only readers. **Done.**
- Remove `internal/embedding/domain` simple generated-record types when migration/on-disk compatibility no longer needs them.
- Keep `internal/embedding/catalog` and source assembly helpers while semantic migration/backfill use them.
- Update docs to mark v1 MVP embedding generation as replaced by daemon semantic maintenance. **Done.**

### Acceptance

```sh
rg 'GenerateNodeEmbedding|SemanticSearchInput|DomainEmbeddingPolicy|AddProfile|UpdateProfile|DeleteProfile' --glob '*.go'
go test ./...
```

Remaining legacy profile/key hits should be intentional migration/catalog/source compatibility references only.

## Rollout and validation strategy

Use small PRs/commits per phase. After each phase:

```sh
git diff --check
go test ./...
```

For phases that affect daemon behavior, also run daemon integration tests and semantic CLI/Admin tests.

## Decisions

- Sink failure does not fail graph commits.
- Dirty event append happens synchronously after graph commit with explicit failure handling.
- Sink failures mark semantic maintenance degraded and are surfaced through status/Admin APIs.
- Default dirty cooldown is 60s and configurable at the system level.
- Throttling starts conservative and is applied at global, provider, model/capability, credential, and credential-grant scopes.

## Admin API recommendation

Expose operational metadata only. Do not expose credential secret values, raw provider request bodies, raw source text, embedding vectors, or full provider responses.

Recommended Admin APIs:

- `GetSemanticMaintenanceStatus`
- `ListSemanticMaintenanceWork`
- `GetSemanticMaintenanceWork`
- `RetrySemanticMaintenanceWork`
- `CancelSemanticMaintenanceWork`
- `RunSemanticMaintenanceAnalyzer`
- `RunSemanticMaintenanceWorkerOnce`

Recommended status fields:

- `enabled`
- `degraded`
- `degraded_reason`
- `queue_depth_pending`
- `queue_depth_running`
- `queue_depth_failed_retryable`
- `queue_depth_failed_permanent`
- `oldest_pending_age`
- `last_dirty_event_at`
- `last_analyzed_at`
- `last_worker_success_at`
- `last_worker_error_at`
- `throttle_state`

Safe work-item fields:

- `work_item_id`
- `space_id`
- `domain_id`
- `semantic_index_id`
- `target_node_id`
- `action`
- `status`
- `attempt_count`
- `not_before`
- `claimed_until`
- `last_error_category`
- `last_error_message_sanitized`
- `created_at`
- `updated_at`
