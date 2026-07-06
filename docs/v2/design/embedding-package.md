# Embedding Package Design

## Status

Draft design for the daemon-only semantic architecture.

The legacy embedding profile subsystem has been internalized under `internal/embedding`, and the old `internal/embeddingstore` package has been removed. New embedding generation should be driven by daemon semantic indexes, inference credentials/grants, semantic maintenance, and semantic vector stores.

## Goals

- Keep graph writes fast: graph mutation paths must not call embedding providers synchronously.
- Make embedding generation daemon-owned, asynchronous, replayable, and observable.
- Separate low-level embedding utilities from semantic-index orchestration.
- Keep provider catalogs and connector code reusable by semantic provisioning and maintenance.
- Standardize generated vectors on semantic-index-aware vector records.
- Retire legacy embedding profile/file-session generation paths.

## Package layout

Current intended layout:

```text
internal/embedding/
  domain/      legacy embedding profile/key data types
  store/       legacy embedding profile/key metadata store
  catalog/     built-in embedding provider/model catalog
  provider/    low-level embedding provider client helpers
  source.go    graph node/tree source-text assembly helpers

internal/graphchange/
  event and sink interfaces for graph commit notifications

internal/semantic/
  backfill/     semantic-index backfill execution
  maintenance/  dirty work analysis, scheduling, and worker behavior
  search/       semantic query planning and execution
  vectorstore/  semantic-index-aware vector backend
  connectors/   daemon inference connector abstraction
```

### `internal/embedding/domain`

Contains legacy embedding metadata types, such as:

- provider keys
- embedding profiles
- simple embedding records
- source modes: `self`, `subtree`

This package is internal because the daemon-only architecture should not expose provider-key/profile-driven embedding generation as a public API. Public semantic concepts should be represented through Admin/Client API messages and `domain/semantic` resource types.

### `internal/embedding/store`

Stores legacy provider-key/profile metadata. It remains only for migration and compatibility paths that convert old embedding settings into semantic resources.

It is not the target storage for generated semantic vectors.

### `internal/embedding/catalog`

Loads built-in embedding provider/model catalog data. This is useful for:

- validating known providers and embedding models
- provisioning default inference package resources
- mapping legacy provider/model selections during migration

The catalog should remain metadata-only. It should not own credentials, policies, or generated vectors.

### `internal/embedding/provider`

Low-level provider HTTP helpers for embedding endpoints.

Longer term, direct provider calls should flow through the semantic connector layer so usage accounting, credential grants, policy checks, and throttling are consistently applied. This package may remain as a low-level implementation helper, but semantic workers should not bypass daemon inference authorization.

### `internal/embedding/source.go`

Builds source text from graph nodes and trees.

This remains valuable and should be reused by semantic backfill/workers. It is intentionally independent of provider credentials and vector storage.

## Relationship to semantic vector stores

The removed `internal/embeddingstore` package stored legacy simple embedding records under:

```text
graphs/<space_id>/embeddings/
```

The semantic vector backend stores advanced semantic records under semantic indexes:

```text
graphs/<space_id>/semantic/indexes/<semantic_index_id>/
```

The target vector record type is:

```text
domain/semantic.AdvancedEmbeddingRecord
```

These records include semantic provenance:

- semantic index ID
- model endpoint ID
- model ID
- model endpoint capability ID
- credential ID
- credential grant ID
- policy decision ID
- vector store ID
- vector space key
- tombstone/delete records

New embedding generation should write only through `internal/semantic/vectorstore`, not through legacy embedding record storage.

## Manager boundaries

Semantic persistence should be split between resource configuration and operational maintenance state.

### `SpaceManager`

`store/semantic.SpaceManager` should remain focused on semantic resources and relatively stable per-space state:

- semantic indexes
- credential grants
- inference policies
- policy decisions
- index states

It should not become the owner of dirty event queues, worker leases, retry state, or debounce state.

### `MaintenanceManager`

A dedicated `store/semantic.MaintenanceManager` should own dynamic background-processing state:

- append-only graph dirty events
- semantic configuration dirty events
- analyzer checkpoints
- coalesced semantic embedding work items
- worker claims/leases
- attempts, backoff, and last errors

Conceptual interface shape:

```go
type MaintenanceManager interface {
    AppendDirtyEvent(ctx context.Context, event GraphDirtyEvent) error
    ListDirtyEvents(ctx context.Context, in ListDirtyEventsInput) ([]GraphDirtyEvent, error)

    GetCheckpoint(ctx context.Context, consumer string) (MaintenanceCheckpoint, error)
    SaveCheckpoint(ctx context.Context, checkpoint MaintenanceCheckpoint) error

    UpsertWorkItem(ctx context.Context, item SemanticWorkItem) (SemanticWorkItem, error)
    ClaimReadyWork(ctx context.Context, in ClaimReadyWorkInput) ([]SemanticWorkItem, error)
    CompleteWork(ctx context.Context, id uuid.UUID, result WorkResult) error
    FailWork(ctx context.Context, id uuid.UUID, failure WorkFailure) error
}
```

The daemon semantic module should open both managers for a space:

```go
spaceManager := semantic.SpaceManager(ctx, spaceID)
maintenanceManager := semantic.MaintenanceManager(ctx, spaceID)
```

This keeps semantic resource APIs separate from queue/worker mechanics.

## Graph change notification boundary

Graph/session code should not import semantic maintenance packages and should not know how embeddings are scheduled. Instead, graph commits should publish a neutral graph-change event through a narrow sink interface.

Proposed package:

```text
internal/graphchange
```

Conceptual interface:

```go
type Sink interface {
    OnGraphCommitted(ctx context.Context, event CommittedEvent) error
}

type MultiSink []Sink
```

`FileSession` or the daemon graph module depends only on this neutral package. Semantic maintenance then provides an implementation that converts committed graph changes into durable dirty events:

```go
type DirtyEventAppender struct {
    Maintenance storesemantic.MaintenanceManager
}

func (a DirtyEventAppender) OnGraphCommitted(ctx context.Context, ev graphchange.CommittedEvent) error {
    return a.Maintenance.AppendDirtyEvent(ctx, DirtyEventFromGraphCommit(ev))
}
```

This keeps embedding scheduling totally out of `SpaceManager` and keeps graph/session code decoupled from semantic analyzer and worker internals.

### Durability rule

The sink must append a durable dirty event before the graph write is considered fully maintenance-visible. The sink should do only cheap local persistence. It must not:

- resolve semantic indexes
- evaluate policies
- call model endpoints
- generate embeddings
- write vectors

The preferred implementation is a synchronous durable append of the graph dirty event after a successful graph commit. In-memory asynchronous callbacks are not sufficient unless backed by another durable log, because dirty events must survive daemon crashes.

## Embedding generation architecture

Embedding generation is a daemon maintenance pipeline:

```text
graph mutation
  -> graphchange.Sink.OnGraphCommitted
  -> MaintenanceManager appends raw graph dirty event
  -> analyzer/coalescer reads dirty events
  -> analyzer upserts semantic embedding work item
  -> worker pool processes ready work
  -> connector calls provider
  -> vectorstore appends semantic vector record or tombstone
```

Graph mutation paths should do only cheap durable bookkeeping. They should not resolve model endpoints, credentials, policies, or call external providers.

## Raw dirty events

Graph commits notify `internal/graphchange.Sink` for every mutation that can change semantic source text. The semantic sink implementation appends raw dirty events through `MaintenanceManager`, not `SpaceManager`.

Supported event categories:

- node created
- node updated
- node deleted
- edge created
- edge updated
- edge deleted
- subtree moved
- children reordered

Raw events must include enough old/new context to analyze deletes and moves after commit. For example, moving a node between parents can dirty both the old containing source root and the new containing source root.

Conceptual `graphchange.CommittedEvent` fields:

```text
id
txn_id
graph_revision
space_id
domain_id
kind
created_node_ids
updated_node_ids
deleted_node_ids
changed_edges
old_parent_by_node_id
new_parent_by_node_id
old_domain_by_node_id
new_domain_by_node_id
committed_at
```

The persisted maintenance dirty event may use the same shape or a semantic-specific projection, but it should retain enough old/new context for delete and move analysis.

The raw dirty event log should be append-only. A compacted checkpoint/state file may exist beside the append-only segments for efficient analyzer restarts, but the raw event history is the durable source of truth.

## Dirty event analysis

Dirty events are not embedding work yet. The analyzer must decide whether a changed node affects an embedding target for each active semantic index.

Processing steps:

1. Read raw dirty events.
2. Coalesce duplicate node changes, keeping the latest event/context for each affected node.
3. Load active semantic indexes for the relevant space/domain.
4. Evaluate each index source policy.
5. Resolve dirty node/edge changes to semantic embedding targets.
6. Upsert target-level semantic work items with a cooldown.

## Embedding target resolution

An embedding target is the node whose source text will be embedded.

For `self` extraction:

```text
if changed node matches the index root/template policy:
  target = changed node
else:
  drop
```

For `subtree` extraction:

```text
find the nearest ancestor-or-self matching the index root/template policy
if found:
  target = that ancestor/root
else:
  drop
```

For edge moves/reorders, analyze both old and new containment paths when available.

Initial recommendation: use the nearest matching ancestor-or-self. Avoid dirtying every matching ancestor until there is a clear product need, because it can multiply work substantially.

## Semantic embedding work queue

The analyzer upserts a second durable queue of coalesced work items through `MaintenanceManager`. This queue is keyed by:

```text
space_id + domain_id + semantic_index_id + target_node_id
```

Conceptual fields:

```text
id
space_id
domain_id
semantic_index_id
target_node_id
last_dirty_node_id
reason
action                 # refresh, delete, cleanup, backfill
status                 # pending, claimed, running, succeeded, skipped, failed_retryable, failed_permanent
first_dirty_at
updated_at
not_before
attempt_count
last_error
claimed_by
claimed_until
```

Every new dirty event for the same key updates:

```text
updated_at = now
not_before = now + cooldown
dirty_count += 1
last_dirty_node_id = dirty node
```

This is the debounce boundary. Frequent edits to the same note/tree produce one embedding refresh after the content is quiet.

Recommended maintenance storage layout:

```text
graphs/<space_id>/semantic/
  indexes/                 # semantic index vector records and manifests
  grants.json              # space-owned credential grants
  policies.json            # inference policies
  decisions.json           # policy decisions
  states.json              # semantic index states

  maintenance/
    dirty/
      manifest.json
      segments/*.kdirty
    work/
      manifest.json
      segments/*.kwork
      state.json           # materialized queue state for efficient claims
    checkpoints.json
```

The append-only dirty/work segments provide durability and replayability. `state.json` is a materialized view of the latest work-item state and can be rebuilt from segments if necessary.

## Cooldown and worker pool configuration

Semantic maintenance should be controlled by daemon config.

Example configuration shape:

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
```

The worker pool should claim ready work items where:

```text
status = pending
not_before <= now
```

Claims should have a lease (`claimed_until`) so work can recover after daemon restart or worker crash.

## Worker execution

`internal/semantic/maintenance` owns worker behavior. `internal/daemon/modules/semantic` owns daemon lifecycle: reading config, starting/stopping analyzer and worker goroutines, and wiring managers/connectors/vector stores/accounting.

For a ready work item, the worker:

1. Loads the semantic index.
2. Loads endpoint/model/capability/vector-store definitions.
3. Resolves inference policy.
4. Resolves an active credential and valid credential grant.
5. Loads the target node/tree.
6. Assembles source text with the index source policy.
7. Computes source hash.
8. Checks the latest current vector record for the same binding.
9. Skips if the source hash is unchanged.
10. Applies throttling/backoff limits.
11. Calls the embedding provider through the connector.
12. Records token/accounting usage.
13. Appends/upserts an advanced vector record.
14. Marks the work item succeeded/skipped/failed.

If source content no longer exists or policy now excludes it, the worker should tombstone/delete matching vector records.

## Error handling

Workers should classify failures:

```text
configuration_error
authorization_error
rate_limited
provider_error
source_too_small
node_not_found
vector_store_error
internal_error
```

Retryable failures should use exponential backoff. Permanent failures should be recorded and surfaced through maintenance/status APIs.

All failures should be structured logged with:

- space ID
- domain ID
- semantic index ID
- target node ID
- model endpoint ID
- model ID
- credential grant ID when available
- error category
- attempt count

## Token accounting

Provider connectors should return usage data when available:

```text
input_tokens
output_tokens
total_tokens
provider_request_id
```

Embedding generation should append accounting usage events with:

- operation: embedding generation
- space/domain/index IDs
- target node ID
- credential/credential grant IDs
- endpoint/model IDs
- token counts
- estimated cost when catalog/pricing metadata is available

Throttling should be able to use both request counts and token counts.

## FileSession boundary

`FileSession` remains the internal file-backed graph/blob/template/metadata session implementation used by daemon internals.

It should not own embedding generation. Legacy file-session embedding methods should fail with daemon-only guidance or eventually be removed from internal interfaces once callers are migrated.

`FileSession` may notify graph changes when graph commits mutate content or containment. It should depend only on the neutral `internal/graphchange` sink interface, not on `store/semantic`, `SpaceManager`, `MaintenanceManager`, analyzer logic, or worker implementation details.

## Migration notes

Completed cleanup in this branch:

- `domain/embedding` moved to `internal/embedding/domain`.
- `store/embedding` moved to `internal/embedding/store`.
- `internal/embeddingstore` removed.
- legacy direct file-session embedding generation/search now fails with a clear unsupported error.

Remaining direction:

- add `internal/graphchange` event/sink interfaces and wire graph commits to a semantic dirty-event appender
- add `store/semantic.MaintenanceManager` for dirty events, checkpoints, work items, leases, and failures
- make semantic dirty event/work queues explicit and append-only
- wire daemon startup to run analyzer/worker loops when enabled
- add throttling/accounting to provider calls
- expose maintenance status and failure visibility through Admin APIs
- remove legacy embedding profile/key APIs when migration paths no longer need them
