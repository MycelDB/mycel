# Projection and Materialization Implementation Plan

## Status

Phase 0 is complete and the Phase 1 common metadata tranche is implemented on
`design/projection-materialization-lifecycle` for
[mycel#330](https://github.com/MycelDB/mycel/issues/330). Phase 2 and later
materializer adoption remain pending.

The design contract is in
[`docs/design/projection-materialization.md`](../../design/projection-materialization.md).
The completed Phase 0 inventory and boundary lock are in
[`projection-materialization-inventory.md`](projection-materialization-inventory.md).
This plan deliberately sequences small, verifiable tranches rather than
introducing a generic index framework or rewriting all derived stores at once.

## Invariants to preserve

- Graph transactions remain atomic and are not split to accommodate derived
  work.
- Graph/Raft commits do not wait for providers or derived indexes.
- Revision witnesses and cursors remain opaque; no decoding or blind retry.
- Raft leadership and source-revision fences remain authoritative.
- Derived state is rebuildable or fails closed.
- No generated protobuf or SDK files are hand-edited.
- Diagnostics remain bounded and redacted.

## Phase 0 — Inventory and boundary lock

### Goals

Document the existing source, derived store, checkpoint, worker, and readiness
paths before adding shared code.

### Work

- Inventory lexical, semantic, metadata, automation, reconciliation, and query
  projections.
- For each projection record its authoritative source, scope, identity,
  physical artifact, existing cursor/revision, tombstone behavior, rebuild
  path, and snapshot behavior.
- Identify every current `ready`, `fresh`, `rebuild_required`, lease, and
  generation decision.
- Map cluster ownership and API routing for every derived read.
- Define which state belongs in the owning partition Raft state machine and
  which remains local/rebuildable.

### Exit criteria

- A reviewed projection inventory exists.
- No existing readiness flag is silently reclassified as authoritative.
- The first pilot projection and migration compatibility strategy are approved.

## Phase 1 — Common model and control metadata

### Goals

Introduce shared vocabulary and durable metadata without changing search
behavior.

### Work

- Add internal projection identity, definition, checkpoint, replica-status,
  state, and readiness-reason types.
- Define bounded source-frontier and configuration-generation fields.
- Add a registry owned by the source partition's authoritative state machine.
- Add versioned record types and deterministic replay behavior for registry
  mutations.
- Add status/diagnostic mapping that does not expose vectors, graph payloads,
  provider bodies, credentials, or filesystem paths.
- Add migration/bootstrap behavior for existing projections with unknown
  manifests; they start as `rebuild_required` rather than `ready`.

### Implementation tranche

- Added `internal/projection` identity, definition, frontier, checkpoint,
  replica-status, lifecycle-state, and readiness types.
- Added a deterministic digest-based projection identity; source cursors remain
  opaque and bounded.
- Added a source-partition-owned registry with command-id/hash idempotency,
  authoritative desired frontiers, monotonic source/fencing guards, generation
  invalidation, and snapshots.
- Added versioned projection registry records for definitions, desired frontiers,
  checkpoints, and replica status. The registry is wired into every experimental
  partition Raft composite without owning physical index files.
- Added a runtime registry manager/read-only view and a generic partition
  proposal helper for future materializers.
- Added fail-closed unknown-manifest bootstrap and bounded readiness marshaling.

### Tests

- Registry replay and idempotency.
- Stale-generation and stale-fencing updates rejected.
- Duplicate checkpoint publication is safe.
- Unknown or malformed metadata fails closed.
- Bounded status serialization and redaction-shaped metadata.
- Partition Raft apply, snapshot, and restore round-trip.

### Exit criteria

- Control metadata survives restart and Raft replay through the composite
  state-machine snapshot path.
- Existing projection behavior is unchanged; no materializer or query path is
  switched to the registry until the Phase 2 publication contract is reviewed.

## Phase 2 — Source frontier and publication protocol

### Goals

Make source progress and artifact publication explicit.

### Work

- Define the replayable source adapter: committed change pages plus a
  consistent snapshot/replay boundary where needed.
- Define opaque cursor handling, expiration, gap detection, and rebuild
  fallback.
- Add private artifact generation and manifest validation hooks.
- Add compare-and-swap checkpoint publication keyed by projection, source
  revision, configuration epoch, and fencing epoch.
- Ensure a failed publication leaves the previous active checkpoint intact.
- Define tombstone/delete advancement and safe compaction requirements.

### Tests

- Crash before/after artifact sync and before/after checkpoint publication.
- Older worker completion cannot overwrite a newer checkpoint.
- Cursor gap/expiry requires rebuild.
- Delete/tombstone processing advances the frontier exactly once.
- Concurrent materializers cannot publish conflicting generations.

### Exit criteria

- One projection can publish an atomic, validated checkpoint using the common
  protocol while its existing query path remains unchanged.

## Phase 3 — Semantic projection pilot

### Goals

Adopt the contract for semantic vector/index materialization, including the
known leadership and identity failure modes.

### Work

- Adapt semantic rule/binding definitions to stable projection identities.
- Separate logical semantic configuration from node-local vector/index
  artifacts.
- Replace or wrap projection leases with the common source/configuration/
  fencing checkpoint contract.
- Ensure the built-in `mycel-file` vector-store identity is authoritative and
  stable across nodes; retain a safe legacy-ID mapping only when unambiguous.
- Route strong semantic search through the partition leader and require the
  leader-local materialization checkpoint.
- Map not-ready states to typed, sanitized status details.
- Reuse existing semantic tombstone, rebuild, provider-budget, and source
  revision logic rather than duplicating it.

### Tests

- Three-node leadership change with an already-ready projection.
- Promoted leader with no local baseline remains fenced.
- Restart/replay preserves configuration and source frontiers.
- Legacy default vector-store IDs migrate or fail closed when ambiguous.
- Empty/short source and graph deletion tombstones are not searchable.
- Rebuild cannot publish over newer maintenance work.
- Existing provider-budget and authorization fences remain intact.

### Exit criteria

- Semantic recovery, readiness, and deterministic vector-store identity use the
  common contract.
- Existing semantic reliability and search suites pass without weakened fences.

## Phase 4 — Lexical projection adoption

### Goals

Use the same lifecycle for lexical indexes and new-domain/leadership recovery.

### Work

- Register lexical projection definitions per domain/index scope.
- Convert lexical index replay/checkpoints to the common source frontier.
- Publish lexical segment generations atomically with validated manifests.
- Define tombstone retention and rebuild/compaction interaction.
- Preserve current leader/read-index routing and explicit stale-read policy.

### Tests

- New domain becomes ready after committed writes.
- Restart and retained-volume replay.
- Leadership change replays or rebuilds without operator mutation.
- Segment corruption and missing history fail closed.
- Tombstone-heavy indexes rebuild without resurrecting deleted documents.

### Exit criteria

- Lexical and semantic readiness diagnostics use comparable terminology and
  source-frontier semantics.

## Phase 5 — Other projections

Adopt the contract selectively for:

- metadata/query indexes;
- automation/reconciliation progress views;
- change-stream consumer checkpoints;
- future projection-backed APIs.

Each adoption must identify whether its physical output is local/rebuildable,
Raft-owned durable state, or an external side effect with an outbox contract.
No subsystem should be forced into the model if it does not maintain a derived
view.

## Phase 6 — Snapshot, backup, and restore integration

### Work

- Include projection definitions, configuration generations, desired
  frontiers, and checkpoint manifests in authoritative snapshots/backups.
- Validate physical artifacts against source revision, generation, materializer
  version, and checksums during restore.
- Mark missing/incompatible artifacts `rebuild_required`.
- Add restore tests for empty, partial, stale, corrupt, and newer-than-source
  artifacts.
- Ensure compaction cannot remove source history needed by an active checkpoint
  or lagging replica.

### Exit criteria

- Restore never reports a projection ready without proving compatibility with
  restored authoritative state.

## Phase 7 — Release qualification and operations

### Work

- Add deterministic unit and restart/replay tests for the common lifecycle.
- Add Compose clustered scenarios for leadership change, local-baseline loss,
  tombstone/delete, rebuild, and readiness recovery.
- Add k3d coverage for backup/restore and PVC rejoin where applicable.
- Record source, daemon, harness, scenario, and image identities in release
  artifacts.
- Document operator procedures for inspecting projection state and requesting
  rebuilds without exposing sensitive data.
- Add bounded cleanup and redaction assertions for projection artifacts.

### Exit criteria

- Release gates prove that every started projection scenario reaches
  authoritative finalization and cleanup.
- Functional success cannot override readiness, provenance, cleanup, or
  redaction failures.

## Suggested package boundaries

The first implementation should stay internal and narrow:

```text
internal/projection/
  model.go          # identity, definition, checkpoint, status
  registry.go       # authoritative metadata contract
  raft.go           # source-partition Raft record ownership and snapshots
  frontier.go       # source revision/cursor and gap semantics
  publication.go    # private generation and CAS publication rules
  fencing.go        # ownership/epoch validation
  diagnostics.go    # bounded readiness/status mapping

internal/semantic/  # semantic-specific materializer and storage adapter
internal/search/    # lexical-specific materializer and storage adapter
```

The final package layout may differ. `internal/projection` must not own
semantic algorithms, lexical query behavior, graph authorization, provider
calls, or generated API contracts.

## Operational decisions required before implementation

1. Which source change stream and snapshot boundary are authoritative for each
   projection?
2. Which checkpoint metadata is replicated versus reported as per-node local
   status?
3. What is the minimum freshness contract for each public derived-read API?
4. Which legacy vector-store identities can be migrated without ambiguity?
5. What retention guarantees are required before tombstones and source history
   may be compacted?
6. Which projection failures are retryable `Unavailable` versus operator-action
   `FailedPrecondition`?
7. How are rebuild capacity and provider budgets reserved without blocking graph
   commits?

These decisions should be recorded in design-review changes before Phase 1 is
implemented.
