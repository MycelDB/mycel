# Semantic Maintenance Debounce Implementation Plan

## Status

Planned for `debounce_semantic_maintenance`.

## Context

The semantic design already describes a debounce boundary in `docs/design/semantic/embedding-package.md`: graph writes append durable dirty events, the analyzer coalesces those events into target-level work items, and workers only process work after `not_before <= now`. Frequent edits to the same note/tree should produce one semantic refresh after the content is quiet.

A live application fresh-cluster test exposed an error during fast graph content creation followed immediately by a strong graph read:

```text
rpc error: code = Unavailable desc = raft graph strong read for partition 4 failed: partition raft command apply failed: scope=space_partition partition_id=4 space_id="..." record_type=semantic.maintenance.mutation.v1 command_id="semantic-maintenance-...-work.complete-..." handler=semantic: semantic resource not found
```

The cluster later reported healthy and semantic maintenance continued successfully, which points to a semantic-maintenance race/idempotency issue rather than graph data corruption.

## Goals

- Preserve the intended debounce behavior for interactive graph changes.
- Ensure newly-created or actively-edited graph content does not trigger immediate provider/vector processing while it is likely still changing.
- Make semantic maintenance Raft apply idempotent for stale completion/failure commands.
- Prevent semantic-maintenance failures from breaking ordinary graph reads/writes.
- Keep graph writes fast: graph mutation paths must not call embedding providers synchronously.
- Keep semantic search eventually consistent and explicitly allowed to lag graph commits.

## Non-goals

- Removing legacy `internal/embedding/store` or legacy embedding migration support.
- Changing the public semantic search API.
- Reworking provider credentials, inference packages, or vector-store format.
- Making semantic search transactionally current with graph writes.

## Current implementation notes

Current code already has some debounce-related pieces:

- `daemon/config.DefaultSemanticDirtyCooldown = 120 * time.Second`
- `MYCELD_SEMANTIC_DIRTY_COOLDOWN` config loading
- `semantic/maintenance.Analyzer` sets `item.EarliestRunAt = now + DirtyCooldown`
- `store/semantic.MaintenanceManager.UpsertDirtyWorkItem` coalesces work by semantic index and target node
- `ClaimReadyWork` skips items whose `EarliestRunAt` is in the future

The failure class suggests remaining gaps:

- `CompleteWork` returns `ErrNotFound` when the work item is no longer present.
- Raft apply treats that `ErrNotFound` as a hard semantic apply failure.
- A semantic maintenance apply failure can bubble into a strong graph read and surface to application users.
- Upserting a new pending item over an existing running/claimed item may need clearer semantics so active work cannot race with newer dirty state.

## Design principles

### 1. Dirty-event append is immediate and durable

Graph commits should still synchronously append cheap dirty events after the graph mutation commits. This is bookkeeping, not semantic processing.

If dirty-event append fails, graph commits should not fail. The semantic subsystem should mark itself degraded for Admin/status visibility.

### 2. Work coalescing is the debounce boundary

Repeated edits to the same target should update one coalesced work item:

```text
key = space_id + domain_id + semantic_index_id + target_node_id
updated_at = now
not_before / earliest_run_at = now + dirty_cooldown
source_txn_ids merged
last_graph_revision advanced
status = pending when safe
```

### 3. Workers process only quiet work

Workers should claim only pending items where:

```text
earliest_run_at == nil || earliest_run_at <= now
```

Interactive user content should therefore wait at least the configured cooldown before provider calls/vector writes.

### 4. Completion/failure of stale work is idempotent

Semantic maintenance completion/failure commands may arrive late or be replayed. They should be safe when the target work item is already complete, replaced, cancelled, or missing.

For stale commands:

- Do not fail Raft apply.
- Record a debug-level or structured counter if useful.
- Preserve newer pending work if the same target was dirtied again after the worker claimed an older version.

### 5. Background semantic maintenance must not break graph reads

Semantic maintenance is best-effort background work. A stale semantic work mutation should never cause ordinary graph strong reads to fail.

### 6. Empty source content tombstones existing vectors

If source assembly produces empty/whitespace-only content, or content shorter than the effective minimum text length, workers must not call embedding providers. If a current vector record exists for that binding, the worker should append a tombstone/delete record so semantic search does not retain stale hits for content that is no longer semantically indexable.

### 7. Dirty cooldown has a global default and schema-level overrides

The default dirty cooldown should be 120 seconds. It remains configurable globally through daemon configuration and should also be overrideable from schema indexing policy for schema-selected content types.

Initial resolution order:

```text
schema node/edge indexing semantic cooldown override
  -> semantic index/source-policy cooldown override, if added during implementation
  -> daemon semantic maintenance dirty cooldown
  -> built-in default 120s
```

The implementation should prefer a schema-level field on `schema.IndexPolicy` because Mycel already models schema indexing intent there:

```go
type IndexPolicy struct {
    Enabled  bool
    Fields   []string
    FullText bool
    Semantic bool
    // planned: SemanticDirtyCooldown time.Duration or encoded duration string
}
```

## Proposed implementation phases

### Phase 1 — Reproduce and characterize stale completion

Status: complete.

Added focused characterization tests before changing behavior. Phase 2 then updated those tests to assert the corrected no-op/idempotent success behavior:

- `internal/semantic/storage`: `TestMaintenanceManagerCompleteMissingWorkIsIdempotent`
- `internal/semantic/storage`: `TestMaintenanceManagerFailMissingWorkIsIdempotent`
- `internal/semantic/service`: `TestSemanticRaftStateMachineMissingWorkCompleteIsIdempotent`
- `internal/semantic/service`: `TestSemanticRaftStateMachineMissingWorkFailIsIdempotent`

Acceptance criteria:

- Test coverage demonstrates the current stale/missing completion failure path.
- Tests identify expected new behavior as no-op/idempotent where appropriate.

### Phase 2 — Make maintenance completion/failure idempotent

Status: complete.

Updated `store/semantic.MaintenanceManager` behavior so stale terminal mutations do not fail.

Preferred approach:

- Add explicit idempotent methods or options, e.g.:

```go
CompleteWork(ctx, id, result) error
FailWork(ctx, id, failure) error
```

continue to work for existing callers, but treat missing work as success for maintenance Raft apply. If preserving strict storage semantics is desired, make the idempotency decision in `applyMaintenanceMutation`.

Recommended behavior:

- `work.complete` for missing ID: no-op success.
- `work.fail` for missing ID: no-op success.
- `work.complete` for already complete ID: no-op success.
- `work.fail` for already failed/cancelled ID: no-op success unless retry semantics require otherwise.

Acceptance criteria:

- Replayed/stale `work.complete` and `work.fail` mutations do not fail Raft apply.
- Existing retry/cancel/admin behavior remains correct.
- Tests cover missing and already-terminal work IDs.

Implemented tests:

- `TestMaintenanceManagerCompleteMissingWorkIsIdempotent`
- `TestMaintenanceManagerFailMissingWorkIsIdempotent`
- `TestSemanticRaftStateMachineMissingWorkCompleteIsIdempotent`
- `TestSemanticRaftStateMachineMissingWorkFailIsIdempotent`

### Phase 3 — Harden coalescing around running work

Status: complete.

Clarify what happens when new dirty events arrive for a target while the existing item is running.

Current upsert behavior replaces the item for the same index/target. That may erase claim state and can race with worker completion. Decide and implement one of these policies:

#### Option A — Generation-based single item

Add a `generation` or `version` field to work items.

- Upsert increments generation and pushes `earliest_run_at` out.
- Claim records the generation it claimed.
- Complete only marks complete if the claimed generation still matches.
- If generation changed, completion is stale no-op and the newer pending work remains.

#### Option B — Running item plus follow-up pending item

Allow a running item to finish, but when dirty events arrive during running work, create/update a pending follow-up item for the same target.

- This preserves claim semantics cleanly.
- It may require changing the queue key or adding replacement pointers.

Implemented approach: **Option A**. Work items now carry a persisted generation. Upsert increments generation for each new dirty change, claim returns the claimed generation, and worker completion/failure passes that generation back. Terminal commands for older generations are no-op successes that preserve newer pending work.

Acceptance criteria:

- An edit during worker processing does not lose the newer dirty state.
- Late completion from old work does not mark newer pending work complete.
- `earliest_run_at` is pushed out after each new dirty event.

Implemented test:

- `TestMaintenanceManagerStaleGenerationTerminalWorkIsIdempotent`

### Phase 4 — Verify debounce behavior end-to-end

Status: complete.

Add or update tests around analyzer/worker scheduling.

Test scenarios:

1. New graph-content dirty event creates work with `earliest_run_at = now + dirty_cooldown`.
2. Repeated updates before cooldown produce one work item with later `earliest_run_at`.
3. Worker pass before cooldown claims zero items.
4. Worker pass after cooldown claims/processes the item.
5. New dirty event while item is running leaves a pending/updated generation for a later run.

Acceptance criteria:

- Tests prove provider/vector processing does not start before the quiet window.
- Queue status exposes future `not_before` for pending work.

Implemented tests:

- `TestMaintenanceManagerClaimReadyWorkRespectsEarliestRunAt`
- `TestMaintenanceManagerRepeatedUpsertPushesOutCooldown`
- `TestAnalyzerRepeatedDirtyEventsPushOutCooldown`

### Phase 5 — Skip empty or whitespace-only source content

Status: complete.

Semantic workers must not call embedding providers or write vector records for targets whose assembled source text is empty, whitespace-only, or shorter than the semantic index minimum text length.

Current source assembly already trims source text, and backfill currently skips when:

```go
strings.TrimSpace(source.Text) == "" || len(source.Text) < index.SourcePolicy.MinimumTextLength
```

Harden and test this behavior in the worker/debounce path.

Acceptance criteria:

- Empty payload text produces no embedding provider call.
- Whitespace-only payload text produces no embedding provider call.
- Subtree extraction with only empty/whitespace descendant source produces no embedding provider call.
- Work is marked skipped/complete without surfacing an error to ordinary graph operations.
- Existing vector records for a target that becomes empty are tombstoned/deleted so semantic search does not return stale hits.

Implemented test:

- `TestRunnerTombstonesCurrentVectorWhenSourceBecomesEmpty`

### Phase 6 — Prevent semantic maintenance errors from surfacing as graph-read failures

Status: complete.

Audit the semantic partition Raft apply path and strong graph read path.

Hardening options:

- Treat known stale maintenance errors as no-op at apply time.
- Ensure dirty-event append/analyzer/worker failures mark semantic status degraded rather than poisoning normal graph reads.
- Add structured degraded status for semantic maintenance apply anomalies.

Acceptance criteria:

- A stale semantic maintenance command cannot make an unrelated graph strong read fail.
- Admin semantic maintenance status surfaces degraded/anomaly counters if useful.

Implemented through Phase 2 idempotent semantic maintenance terminal mutations. Existing graph commit paths already record graph-change sink errors without failing commits.

### Phase 7 — Configuration and operational guidance

Status: complete.

Confirm the default cooldown is appropriate for interactive graph-content workloads.

Recommended defaults:

```text
MYCELD_SEMANTIC_DIRTY_COOLDOWN=120s
MYCELD_SEMANTIC_ANALYZER_INTERVAL=5s
MYCELD_SEMANTIC_WORKER_INTERVAL=5s
```

Schema-level overrides should support more specific cooldowns for schema-selected content types. For example, highly interactive semantic node types can use a longer cooldown than stable system-generated nodes.

Acceptance criteria:

- Built-in default dirty cooldown is 120 seconds.
- Global config can override the 120-second default.
- Schema indexing policy can override cooldown for matching schema-selected semantic content.
- Config/schema documentation states that semantic indexing is eventually consistent and delayed by dirty cooldown.
- CLI/Admin maintenance status shows `not_before` so operators can see debounced work.

Implemented tests:

- `TestSchemaDirtyCooldownForTargetUsesSemanticIndexingPolicy`
- `TestAnalyzerUsesTargetCooldownOverride`

## Validation commands

Targeted tests:

```sh
go test ./internal/semantic/storage ./internal/semantic/maintenance ./internal/semantic/service
```

Broader semantic tests:

```sh
go test ./internal/semantic/...
```

Daemon/admin tests likely affected:

```sh
go test ./internal/daemon/api/admin ./internal/cli/cmd
```

Full confidence run if time permits:

```sh
go test ./...
```

## Manual validation scenario

1. Start a fresh 3-node raft cluster.
2. Apply inference package and create a semantic index/grant/policy as usual.
3. Create a graph node through a client application or CLI.
4. Immediately perform strong graph reads for that node repeatedly.
5. Confirm no graph read returns semantic maintenance `Unavailable`/`semantic resource not found`.
6. Confirm semantic maintenance status shows pending debounced work with future `not_before`.
7. Wait past cooldown.
8. Confirm worker processes the item and semantic search eventually finds the content.

## Risks

- Treating missing completion as no-op could hide a genuine queue corruption bug if no diagnostics are recorded.
- Generation-based coalescing changes queue semantics and may require snapshot/restore compatibility handling.
- Longer cooldown improves editing UX but delays semantic search freshness.

## Decisions

- Empty or whitespace-only source content must not call embedding providers and must tombstone existing vector records for the same binding.
- `work.complete` and `work.fail` for missing/already-terminal work are idempotent no-op successes, with diagnostics/counters where useful.
- Content changes while a worker is processing an older generation must preserve the newer dirty state; completion of the older generation must not mark the newer generation complete.
- Manual Admin `semantic maintenance process` respects `not_before`/cooldown by default unless an explicit force option is introduced later.
- Explicit operator backfill ignores dirty cooldown.
- Built-in dirty cooldown default is 120 seconds.
- Dirty cooldown is globally configurable and should be overrideable at schema indexing-policy level.
- Semantic maintenance apply failures must not fail ordinary graph reads.

## Remaining design choices

- Whether stale terminal idempotency is implemented directly in storage (`CompleteWork`/`FailWork`) or only in Raft apply (`applyMaintenanceMutation`).
- Whether generation-based coalescing uses an explicit `generation` field or graph revision/timestamps.
- Whether schema DSL/API surfaces should get ergonomic syntax for `IndexPolicy.SemanticDirtyCooldown`; the model-level representation is implemented as `time.Duration`.
- Whether to expose a counter for stale terminal commands.

## Recommended first implementation slice

1. Add tests for missing/stale `work.complete` Raft apply.
2. Make `work.complete` and `work.fail` idempotent for missing/already-terminal work.
3. Add regression test that a stale semantic maintenance completion does not fail apply.
4. Then address generation/coalescing if the create -> immediate strong-read race can still reproduce.
