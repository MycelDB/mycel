# Projection and Materialization Lifecycle

## Status

Proposed design for [mycel#330](https://github.com/MycelDB/mycel/issues/330).

This document defines a common correctness and lifecycle contract for derived
projections. It does not require lexical search, semantic search, automation,
or metadata indexes to share their algorithms or physical storage.

## Problem

The graph and other user-visible records are authoritative state. Several
other subsystems maintain derived state from that authority:

- lexical search indexes;
- semantic vector records and physical vector indexes;
- semantic reliability and projection-readiness state;
- automation and reconciliation projections;
- metadata and query indexes.

These projections currently have different notions of identity, source
revision, readiness, ownership, checkpoints, snapshots, and recovery. A node
can therefore have locally plausible derived files that are not safe to serve
for the current Raft leader, graph revision, or configuration generation.

The recurring symptoms include projection readiness failures after leadership
changes (#299), node-local default vector-store identity (#326), index lag after
restart, and subsystem-specific snapshot/recovery rules.

## Goals

- Keep graph/user-visible state authoritative and Raft-owned where required.
- Give every derived projection one explicit identity and lifecycle contract.
- Make source progress and published checkpoints comparable across restarts and
  nodes.
- Prevent stale, foreign-term, mismatched-generation, or partially written
  artifacts from being reported as ready.
- Make delete/tombstone processing part of the same ordered source contract.
- Support local, rebuildable physical indexes without treating local files as
  authoritative cluster metadata.
- Provide bounded, sanitized readiness diagnostics.
- Migrate existing materializers incrementally without a flag-day rewrite.

## Non-goals

- A generic implementation of every index or vector algorithm.
- A distributed transaction that waits for projections before committing graph
  data.
- Making every follower projection current before a graph commit succeeds.
- Replacing the existing semantic, lexical, or automation storage formats in
  the first tranche.
- Allowing stale derived results when the API promises authoritative freshness.

## Terminology

### Authoritative source

Durable state whose committed revision defines truth. For graph projections,
this is the owning space-partition Raft state machine and its committed graph
revision.

### Projection

A deterministic or explicitly versioned derived view of authoritative source
data, such as a lexical index or semantic vector stream.

### Materializer

The projection-specific worker and storage adapter that consumes source state or
changes and produces a projection artifact.

### Projection definition

Authoritative metadata describing what a projection means: its stable identity,
scope, kind, configuration generation, and materializer version.

### Materialization replica

One node's physical realization of a projection. A replica may be absent or
behind even when the logical projection definition exists.

### Checkpoint

A durable claim that a particular materialization artifact has been validated
and published through a source revision, configuration generation, and
materializer version.

## Correctness invariants

1. A graph commit never waits for a derived projection to finish.
2. A derived projection never becomes authoritative merely because a local file
   exists.
3. A materializer publishes a checkpoint only after the corresponding artifact
   is complete, durable, and validated.
4. A checkpoint is valid only for its exact source scope, source revision,
   configuration generation, materializer version, and fencing epoch.
5. A newer source revision or configuration generation cannot be overwritten by
   an older worker completion.
6. A delete or tombstone advances the same source frontier as an upsert.
7. A node may serve a derived read only when its local materialization satisfies
   the read's freshness contract.
8. Missing, corrupt, or ambiguous projection state fails closed; it does not
   silently fall back to an unrelated local artifact.

## Logical model

The registry is scoped to the authority that owns the source data. For a graph
projection, its durable control metadata belongs with the owning space
partition rather than with one arbitrary daemon.

Conceptually:

```go
type ProjectionDefinition struct {
    ID                  ProjectionID
    Kind                ProjectionKind // lexical, semantic, automation, metadata
    SpaceID             SpaceID
    DomainID            DomainID
    ConfigurationHash   []byte
    ConfigurationEpoch  uint64
    MaterializerVersion string
    CreatedAt           time.Time
}

type ProjectionCheckpoint struct {
    ProjectionID        ProjectionID
    SourceRevision      uint64
    SourceCursor        string // opaque, bounded, filter-bound when applicable
    ConfigurationEpoch  uint64
    MaterializerVersion string
    FencingEpoch        uint64
    ArtifactGeneration  string
    State               ProjectionState
    PublishedAt         time.Time
}
```

The exact Go types may differ. The important property is that identity and
progress are explicit and opaque cursors/witnesses are never decoded or logged
by callers.

The control metadata has two layers:

- **logical definition and desired frontier:** authoritative and replicated;
- **materialization replica status:** reports what a particular node has
  validated locally.

A global `ready` bit is insufficient in a clustered system. The relevant
leader-local materialization must be ready for leader-routed strong reads.

### Phase 1 concrete decisions

The first implementation keeps the contract internal and narrow:

- `internal/projection` derives stable opaque IDs from kind, source scope, and
  a caller-supplied key; it does not generate IDs independently at daemon
  startup.
- Each source-partition Raft composite owns one projection registry child. The
  child stores definitions, desired source frontiers, published checkpoints,
  and per-node status, while physical artifacts remain local and rebuildable.
- Desired frontiers are authoritative replicated metadata. Per-node status is
  also recorded through the source partition so leadership and restart can see
  the last claim, but it is never sufficient by itself: a materializer must
  validate its local artifact before publishing status, and missing local
  artifacts fail closed.
- Registry records are versioned and partition-scoped. Replayed commands are
  idempotent by command ID and hash; conflicting hashes, stale source
  frontiers, stale fencing epochs, and same-frontier cursor conflicts fail
  closed.
- Configuration changes require a strictly newer configuration epoch and
  invalidate the prior desired frontier and checkpoint. Existing replica
  status is retained only as an explicit `rebuild_required` diagnostic.
- Unknown existing artifacts can be bootstrapped only as
  `rebuild_required`; no local file is promoted to `ready` without a later
  manifest/checkpoint publication.
- Readiness diagnostics contain only bounded IDs, revisions, opaque cursors,
  lifecycle states, and enumerated reasons. They do not carry vectors, graph
  payloads, provider bodies, credentials, or filesystem paths.

This tranche does not yet select the authoritative source adapter or change
lexical/semantic query behavior. Those decisions belong to the Phase 2/3
reviews.

### Phase 2 protocol decisions

The common source/publication layer now fixes the following boundaries without
adopting them into an existing query path:

- Source adapters expose committed pages and a consistent snapshot boundary.
  Cursors are compared only for exact equality or carried forward opaquely;
  they are never decoded or retried blindly.
- Cursor expiry, source gaps, and malformed pages use typed rebuild-required
  failures. Deletes and tombstones are ordinary page entries and therefore
  advance the same frontier as upserts.
- Private artifact manifests prove projection identity, source frontier,
  configuration, materializer version, fencing epoch, generation, format, and
  checksum. They contain no local paths or sensitive payloads.
- Publication activates a validated private generation, then performs an
  authoritative checkpoint CAS. A losing CAS invokes the artifact rollback
  contract, leaving the prior checkpoint active. Recovery must ignore any
  orphan generation whose manifest does not match the authoritative checkpoint.

Physical artifact stores still implement their own durable activation and
rollback mechanics; this common layer only enforces the lifecycle ordering and
fail-closed contract.

## Lifecycle

A projection definition and each materialization replica use the following
bounded state machine:

```text
absent
  -> building
  -> ready
  -> catching_up
  -> ready

ready -> stale -> catching_up
ready -> degraded
building/catching_up/degraded -> rebuild_required
rebuild_required -> building
```

Transitions are monotonic with respect to source/configuration generations.
Older work may finish, but it cannot publish over a newer checkpoint.

Suggested meanings:

- `absent`: no valid artifact has been published;
- `building`: a bounded build is in progress;
- `ready`: the local artifact satisfies the declared freshness contract;
- `catching_up`: an artifact exists but trails the required source frontier;
- `stale`: the artifact is known to be behind or invalid for the current
  configuration;
- `degraded`: progress is blocked by a classified operational or data problem;
- `rebuild_required`: incremental recovery cannot prove correctness.

## Source and checkpoint protocol

The source must provide a replayable committed change stream or a consistent
snapshot plus a replay boundary. A materializer records an opaque source cursor
and source revision only after applying the corresponding changes.

A normal update is:

```text
1. Observe committed source revision R.
2. Materializer reads a consistent source snapshot or change page.
3. Apply changes to a private artifact generation.
4. Validate source/configuration/provenance invariants.
5. Sync the artifact and write its manifest.
6. Compare the expected fencing epoch and configuration epoch.
7. Publish the checkpoint through a compare-and-swap transition.
8. Atomically make the artifact generation active.
```

If any validation or fencing check fails, the private artifact is abandoned or
retained only as diagnostic data; the previous active checkpoint remains in
force.

A source gap, expired cursor, incompatible snapshot, or malformed event must
produce `rebuild_required` rather than an inferred partial result.

## Leadership and fencing

Materializers may run only under an ownership/fencing epoch issued by the
source authority. The epoch is not a replacement for the graph revision; it
prevents an old leader or worker from publishing after ownership changes.

On leadership change, the new leader:

1. loads the authoritative projection definition and desired source frontier;
2. inspects its local artifact manifest and replica checkpoint;
3. verifies source scope, configuration epoch, materializer version, and
   fencing requirements;
4. adopts the artifact only if those checks pass;
5. otherwise catches up or performs a bounded rebuild;
6. exposes `ready` only after local validation succeeds.

This preserves fail-closed projection fences while providing an explicit path to
re-certify or rebuild a safe local baseline.

## Stable identities

Cluster-wide resource identities must be assigned by authoritative metadata,
not generated independently during daemon startup.

This applies to:

- projection definitions;
- built-in vector stores;
- materializer configuration generations;
- durable checkpoint/artifact identities.

Legacy node-local identities may be migrated through an explicit compatibility
mapping when there is exactly one safe source identity. Ambiguous mappings must
remain unavailable rather than being guessed.

## Deletes and tombstones

A source deletion is an ordinary ordered change with a source revision. The
materializer must either remove the derived entry or publish a tombstone in its
own durable history, according to the projection's storage contract.

The checkpoint cannot advance past the deletion until the physical materializer
has applied the deletion/tombstone. Compaction may remove historical tombstones
only after the projection's retention and rebuild rules prove that no required
replay or snapshot install depends on them.

Schema declaration tombstones remain a separate concern. This design covers
projection records and indexes; it does not define dynamic schema-type lifecycle
semantics from #249.

## Read semantics

The API layer must state which freshness contract it requires.

- **Authoritative graph reads:** use the normal Raft/read-index consistency
  contract.
- **Strong derived reads:** route to a node whose local checkpoint is ready
  through the required source revision and configuration epoch.
- **Explicit stale reads:** may use a behind checkpoint only when the API
  explicitly permits and reports the observed frontier.
- **Unavailable derived reads:** return a typed, sanitized readiness error with
  the projection ID/kind, required and observed bounded revisions, state, and a
  retry/rebuild classification.

No response should imply that a projection is current merely because the graph
source is current.

## Snapshots, backup, and restore

Projection control metadata and checkpoint manifests must be included in the
owning Raft snapshot or backup manifest. Physical derived artifacts may be
included when useful, but they remain validated caches rather than authority.

After restore:

- an artifact with matching source revision, configuration epoch, materializer
  version, and checksum may be adopted after validation;
- an artifact with an unknown or incompatible identity is ignored and rebuilt;
- missing physical artifacts produce `rebuild_required`, not false readiness;
- restored checkpoints must not advance beyond restored authoritative source
  state.

This makes restore behavior deterministic without requiring every physical
index format to be preserved forever.

## Common runtime boundary

The shared projection runtime should own only lifecycle and correctness
mechanics. A conceptual interface is:

```go
type Materializer interface {
    Definition(context.Context) (ProjectionDefinition, error)
    Reconcile(context.Context, ReconcileRequest) (ReconcileResult, error)
    Status(context.Context) (ReplicaStatus, error)
    Validate(context.Context) error
}
```

The final interface should remain small. It must not hide projection-specific
queries, vector operations, graph selectors, provider calls, or authorization.
Each materializer owns those details and receives narrow capabilities from the
composition root.

## Migration strategy

The first implementation should add control metadata and adapters around
existing stores rather than rewrite them. Semantic search is a suitable pilot
because it already has generations, rebuild epochs, projection leases, and
readiness diagnostics. Lexical indexing can adopt the same contract afterward.

Existing physical records, tombstones, and indexes remain usable while their
manifests are bootstrapped. Records that cannot be proven compatible are
replayed or rebuilt.

## Risks and trade-offs

- Replicated projection metadata adds control-plane writes and schema surface.
- A per-node replica status model is more explicit but harder to display.
- Strict readiness can temporarily reduce availability during recovery.
- Rebuilds consume resources and require bounded scheduling.
- A generic interface can become a leaky abstraction; keep algorithms and
  storage domain-specific.

These costs are preferable to silently serving a projection whose identity,
revision, or leader ownership cannot be proven.
