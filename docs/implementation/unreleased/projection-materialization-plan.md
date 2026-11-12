# Projection and Materialization Implementation Plan

## Status

Phase 0 is complete. Phase 1 and the Phase 2 common frontier/publication
tranche are implemented on `design/projection-materialization-lifecycle` for
[mycel#330](https://github.com/MycelDB/mycel/issues/330). Phase 3 semantic,
Phase 4 lexical, and the in-scope Phase 5 graph/query adoption tranches are
implemented. The initial Phase 6 snapshot/backup catalog tranche is also
implemented; restore preflight and source-retention hardening remain.

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
- Added replayable source-page/snapshot contracts with opaque cursor validation,
  typed expiry/gap/malformed-source rebuild failures, and tombstone entries.
- Added artifact manifest validation and a publication coordinator that activates
  private generations, performs checkpoint CAS, and rolls back on CAS loss.

### Tests

- Registry replay and idempotency.
- Stale-generation and stale-fencing updates rejected.
- Duplicate checkpoint publication is safe.
- Unknown or malformed metadata fails closed.
- Bounded status serialization and redaction-shaped metadata.
- Partition Raft apply, snapshot, and restore round-trip.
- Source cursor expiry/gap, snapshot replay-boundary, tombstone, manifest, CAS,
  activation-failure, and rollback coverage.

### Exit criteria

- Control metadata survives restart and Raft replay through the composite
  state-machine snapshot path.
- The common source/publication protocol is validated independently of existing
  materializers; no materializer or query path is switched until the semantic
  pilot review.

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

### Implemented tranche

- Semantic rule/binding pairs now derive stable, binding-scoped common
  projection identities and use the existing partitioned Raft registry.
- Reliability status publishes common definitions, desired graph frontiers, and
  fenced replica claims; successful local lease recovery publishes a common
  checkpoint through compare-and-swap. Lease certification now also updates
  the common claim after rebuild/backfill completion rather than waiting for a
  subsequent search request.
- Strong semantic search re-certifies the common replica readiness after the
  existing local vector-store lease and rebuild checks. Common failures remain
  typed and content-free.
- The legacy lease remains local artifact evidence during this pilot; it is not
  treated as a replicated artifact or an alternative authority.
- Added a three-node common-registry failover test proving that a promoted
  leader remains fenced until it publishes its own term-fenced replica claim
  and checkpoint.

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

Current validation covers the semantic service, vector store, projection
registry, daemon wiring, race detection, default-store migration, local
restart/recovery, and three-node common-registry failover. Destructive Lab
qualification and the final broader release gate remain separate follow-up
work before closing this phase.

## Phase 4 — Lexical projection adoption

### Goals

Use the same lifecycle for lexical indexes and new-domain/leadership recovery.

### Work

- Register lexical projection definitions per domain/index scope.
- Convert lexical index replay/checkpoints to the common source frontier.
- Publish lexical segment generations atomically with validated manifests.
- Define tombstone retention and rebuild/compaction interaction.
- Preserve current leader/read-index routing and explicit stale-read policy.

### Implemented tranche

- Lexical indexes now use stable `lexical/default` projection identities scoped
  by space and domain, with analyzer/format configuration hashes and common
  desired frontiers, replica claims, and fenced checkpoints.
- Local segment files remain rebuildable artifacts. Graph-commit, rebuild, and
  durable replay paths refresh common metadata only after local cursor/manifest
  progress; replay also republishes a promoted leader's claim when no new graph
  event is needed.
- Search and status require the leader-local common readiness claim and
  checkpoint. Explicit stale reads continue to use the existing freshness
  policy; a stale local artifact cannot bypass common fencing or configuration
  checks.
- Checkpoint/frontier races from retained replicas are ignored only when a
  newer common record already won; common checkpoints never regress during
  promoted-leader catch-up.

### Tests

- New domain becomes ready after committed writes.
- Restart and retained-volume replay.
- Leadership change replays or rebuilds without operator mutation.
- Segment corruption and missing history fail closed.
- Tombstone-heavy indexes rebuild without resurrecting deleted documents.

### Exit criteria

- Lexical and semantic readiness diagnostics use comparable terminology and
  source-frontier semantics.

Current validation covers the service-level corrupt-segment and tombstone-heavy
rebuild cases, plus a three-node clustered new-domain, retained-volume,
leadership-promotion, restart/replay, and common-readiness fail-closed path.
Broader Lab qualification remains separate from this implementation tranche.

## Phase 5 — Other projections

Adopt the contract selectively for:

- metadata/query indexes;
- automation/reconciliation progress views;
- change-stream consumer checkpoints;
- future projection-backed APIs.

### Implemented tranche

- Domain graph checkpoints now publish a stable metadata projection identity
  (`graph-checkpoint`) through the source-partition registry after the local
  checkpoint and persistent-index manifest are written.
- Common graph-checkpoint definitions, desired frontiers, leader-fenced replica
  claims, artifact generations, and CAS checkpoints are wired into clustered
  daemon and integration-test Raft state machines. Follower-local checkpoint
  operations never publish authoritative claims.
- Checkpoint-persisted graph query indexes publish stable, name-scoped
  metadata identities (`query-index:<name>`) with schema/definition
  configuration hashes, desired frontiers, leader-fenced replica claims,
  artifact generations, and checkpoint CAS records. Generated local `idx-*`
  generations are never used as identities.
- Existing graph/query index fallback behavior is unchanged: common records
  describe validated rebuildable artifacts while graph storage remains the
  artifact owner.
- Automation schedule checkpoints, reconciliation operations, and graph replay
  cursors remain durable workflow/consumer state rather than materialized
  artifacts. Their existing automation-Raft replay/snapshot semantics remain
  authoritative, while their wall-clock or binding-aggregated cursors do not
  satisfy the common artifact readiness contract.
- The retained graph-change stream does not own server-side consumer
  checkpoints; callers retain opaque cursors and rebuild after retention gaps.
  Ephemeral adjacency and metadata accelerators likewise remain local,
  rebuildable state without registry entries.

This completes the currently in-scope Phase 5 projection adoptions without
reclassifying workflow state or ephemeral accelerators as materialized
projections. Each adoption identifies whether its physical output is
local/rebuildable, Raft-owned durable state, or an external side effect with an
outbox contract.

## Phase 6 — Snapshot, backup, and restore integration

### Implemented tranche

- Projection registry state remains a child of each partition's composite Raft
  snapshot, so definitions, desired frontiers, checkpoints, replica claims, and
  applied-command hashes restore with the authoritative partition state.
- Backups now record a bounded, deterministic projection catalog in each backup
  manifest. The catalog includes partition snapshot digests, projection
  definitions/checkpoint evidence, and validated local artifact manifests
  without duplicating graph payloads or filesystem paths. Graph checkpoint,
  persisted query-index, lexical, and semantic materializers contribute local
  artifact evidence on followers and leaders; backup validation cross-checks
  generation, source revision, configuration, materializer, and fencing
  evidence against the common checkpoint.
- Clustered empty-storage rejoin coverage proves graph-checkpoint lifecycle
  metadata survives a partition snapshot and is replayed on the rejoined node.
- Existing graph, lexical, semantic, and vector artifact validators remain the
  local-artifact authority: missing, corrupt, stale, or incompatible artifacts
  fail closed rather than making a restored common checkpoint authoritative.

### Remaining Phase 6 hardening

- Expand restore cases for partial/corrupt/stale artifacts and checkpoints
  newer than restored source state.
- Source-history compaction remains subsystem-owned: common lexical and semantic
  artifacts rebuild from authoritative graph snapshots, while graph notification
  cursors are opaque caller-owned state and are not common projection
  watermarks. Retain retention-gap regression coverage rather than coupling
  compaction to local artifact claims.

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
  publication.go    # private generation and checkpoint CAS protocol
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
