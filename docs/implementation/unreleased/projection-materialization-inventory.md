# Projection and Materialization Phase 0 Inventory

## Status

Phase 0 inventory and boundary lock for [mycel#330](https://github.com/MycelDB/mycel/issues/330).

This document records the current implementation as inspected on the
`design/projection-materialization-lifecycle` branch. It is an inventory, not a
claim that the common projection runtime has been implemented.

## Inventory method

The inventory follows committed graph changes, durable stores, checkpoints,
readiness/status APIs, leader ownership, snapshot/restore code, and tombstone
handling. It distinguishes:

- authoritative state;
- derived/rebuildable materialization;
- durable consumer/workflow progress that is not a pure projection;
- source-feed infrastructure used by projections.

## Source-feed infrastructure

### Graph committed-change feed

**Implementation:**

- `internal/graph/change/`
- `internal/graph/notification/module.go`

**Current contract:**

- Graph commits produce `graphchange.CommittedEvent` values containing the
  space/domain scope, committed graph revision, transaction identity, and
  normalized changes.
- The notification module persists per-scope event history and a current
  revision under its own file-backed directory.
- Consumers can register filtered subscriptions or replay after an opaque
  `AfterRevision` value.
- Retention is bounded by event count and age. A consumer whose requested
  revision is no longer retained receives an explicit gap.
- A leader gate can prevent delivery from a non-owner in clustered operation.

**Boundary decision:**

This is source-feed infrastructure, not a projection. It is the leading
candidate for the source adapter used by the common materialization contract,
but its bounded retention means consumers must have a snapshot/rebuild path.
The notification module does not own durable per-consumer materialization
checkpoints; consumers persist their own progress.

## Projection inventory

### 1. Lexical search projection

**Implementation:**

- `internal/search/lexical/service/`
- `internal/search/lexical/index/`
- `internal/search/lexical/storage/`
- `internal/search/lexical/service/module.go`

**Source and scope:**

- Committed graph node create/update/delete changes.
- One local materialization per `space_id/domain_id`.
- Rebuilds use a serialized graph snapshot plus its domain revision.

**Current identity:**

- Implicit filesystem scope and manifest `space_id/domain_id`.
- Segment IDs are local (`seg_*`); there is no common stable projection ID.
- Manifest identity is coupled to analyzer and index-format versions.

**Materialization and progress:**

- `Service.OnGraphCommitted` converts committed graph changes into indexed
  documents.
- `Service.ApplyDocuments` appends/publishes segments and advances a local
  `storage.Cursor.IndexedGraphRevision`.
- `Service.RebuildFromSnapshot` captures a complete domain snapshot while
  graph-change application is serialized for the scope.
- Status states are `fresh`, `stale`, `rebuilding`, `unavailable`, and `error`.
- Status compares `IndexedGraphRevision` with the latest known graph revision.

**Ownership and reads:**

- `service.Coordinator` resolves an owner and forwards search/status to the
  owner node.
- Mutation and rebuild paths reject execution on a non-owner.
- The owner is currently an operational routing concept, not a common
  materialization ownership/fencing record.

**Tombstones and rebuild:**

- Deleted documents are represented in segments with `Deleted=true`.
- Search excludes deleted records; compaction can later rewrite tombstone-heavy
  segments.
- Missing/corrupt manifests or segments make the index unavailable/error.
- A revision gap or cursor-ahead condition requires rebuild.

**Durability and snapshots:**

- Physical segments, manifest, and cursor are local derived files.
- They are not the authoritative graph mutation log or Raft snapshot.
- Graph checkpoints and persistent graph indexes are separate artifacts.

**Gaps relevant to #330:**

- No common projection definition or stable projection ID.
- Cursor, manifest, segment generation, analyzer version, and owner state are
  separate concepts.
- Publication and readiness are local to the lexical service.
- No common configuration epoch or fencing epoch is attached to the cursor.
- Consumer retention gaps are handled by rebuild locally rather than through a
  shared source-frontier protocol.

### 2. Semantic generation and vector-search projection

**Implementation:**

- `internal/semantic/maintenance/`
- `internal/semantic/backfill/`
- `internal/semantic/vectorstore/`
- `internal/semantic/service/reliability.go`
- `internal/semantic/service/projection_lease.go`
- `internal/semantic/service/projection_readiness.go`
- `internal/semantic/service/raft_snapshot.go`

**Source and scope:**

- Committed graph changes become `GraphDirtyEvent` records.
- Semantic analysis selects affected rules, bindings, and target nodes.
- The effective projection scope is generally
  `space_id/domain_id/semantic_rule_id/embedding_binding_key`.
- Provider configuration, access policy, generation, rebuild epoch, and vector
  store identity also affect the materialized result.

**Current identity:**

- Rule ID plus embedding binding key is the primary logical identity.
- `SemanticIndexID` remains as a transitional alias in several runtime/storage
  paths.
- Vector-store ID and vector-space key participate in physical search identity.
- Individual vector records use generated record IDs.
- The built-in `mycel-file` vector-store identity historically exposed the
  node-local identity problem tracked by #326.

**Materialization and progress:**

Progress is represented by several overlapping records:

- maintenance `GraphDirtyEvent` history;
- `MaintenanceCheckpoint` per analyzer/consumer;
- coalesced dirty-work items with first/last graph revisions;
- `SemanticReliabilityState` with generation, rebuild epoch, analyzed-through,
  fresh-through, unresolved ranges, and capacity state;
- `SemanticSearchIndexState` for physical index readiness;
- local `semantic_projection_leases.json` entries containing leader node/term,
  generation, rebuild epoch, and fresh-through revision;
- vector records containing source graph revision, configuration hash,
  generation, rebuild epoch, and tombstone provenance.

**Ownership and reads:**

- Semantic maintenance and search are leader/term fenced in clustered mode.
- Search validates a local projection lease against current leadership,
  generation, rebuild epoch, and freshness.
- Recovery may re-certify a surviving local index, rebuild its physical index,
  or perform bounded graph-truth backfill.
- Readiness diagnostics are typed and sanitized.

**Tombstones and rebuild:**

- Deletes and empty/short source text publish vector tombstones.
- Physical vector search indexes remove tombstoned records from the live set.
- Rebuilds use pinned source revisions and generation/rebuild-epoch checks.
- Older work is prevented from replacing newer semantic state by publication
  fences and transition checks.

**Durability and snapshots:**

- Rule/configuration, maintenance state, reliability state, dirty events,
  checkpoints, and work items have semantic Raft/WAL paths.
- Semantic Raft snapshots explicitly mark vector indexes as
  `derived_rebuild_required`; physical vector indexes are not treated as the
  authoritative snapshot payload.
- Local projection leases are separate file-backed state and are revalidated on
  leadership changes.

**Gaps relevant to #330:**

- The same logical projection is represented by multiple state machines and
  files with no common registry/checkpoint record.
- Logical freshness and local physical readiness are related by convention,
  not one shared materialization contract.
- Configuration hash, semantic generation, rebuild epoch, and leader lease
  overlap but are not one versioned projection identity.
- The authoritative boundary for raw vector records versus rebuildable physical
  indexes must be made explicit.
- Recovery behavior is strong but semantic-specific; it should be the pilot for
  common lifecycle primitives rather than copied into other subsystems.

### 3. Graph checkpoints and persistent graph/query indexes

**Implementation:**

- `internal/graph/storage/checkpoint.go`
- `internal/graph/storage/persistent_index.go`
- `internal/graph/storage/persistent_query_index.go`
- `internal/graph/service/checkpoint.go`
- `internal/graph/service/checkpoint_policy.go`

**Source and scope:**

- Latest graph state for one `space_id/domain_id`.
- Schema-declared property indexes are built from graph state and schema
  definitions.
- Label, tag, adjacency, and query-property indexes are included in persistent
  index sets.

**Current identity:**

- Graph checkpoint directories use generated `chk-*` IDs.
- Persistent index sets use generated `idx-*` IDs.
- Manifests bind artifacts to graph revision, graph checksum, checkpoint ID,
  index format, and checksum algorithm.
- Query indexes use schema/index identities and definition fingerprints.

**Materialization and progress:**

- A graph checkpoint records latest state, revision, checksums, and segment
  offsets.
- A persistent index set is built from a matching graph checkpoint.
- Query index metadata has `building`, `ready`, `failed`, `stale`, and `retired`
  states plus `LastIndexedGraphRevision`.
- The graph checkpoint API exposes local status and explicitly distinguishes
  local checkpoints from Raft snapshots and backups.

**Tombstones and rebuild:**

- Checkpoints represent current live graph state; historical tombstone streams
  are not the checkpoint's source of truth.
- Persistent indexes are rebuilt from the graph checkpoint and validated by
  checksums.
- Historical graph segments remain authoritative until a separately controlled
  compaction phase.

**Boundary decision:**

These are derived graph artifacts and should eventually use the common
checkpoint/manifest vocabulary. They are not the first pilot because their
source is already a graph-local latest-state snapshot and their query-index
lifecycle is narrower than semantic recovery.

### 4. In-memory metadata and adjacency indexes

**Implementation:**

- `internal/graph/metadataindex/index.go`
- `internal/graph/adjacency/`
- graph storage in-memory indexes.

**Source and scope:**

- Current graph nodes/edges for a space/domain.
- Rebuilt from graph storage during load or mutation application.

**Boundary decision:**

These are ephemeral query accelerators, not independently durable projections.
They should follow the same source/readiness principles but should not receive a
registry entry until a durable checkpoint or public freshness contract exists.

### 5. Automation and reconciliation consumer state

**Implementation:**

- `internal/automation/service/graph_recovery.go`
- `internal/automation/service/reconciliation.go`
- `internal/automation/service/reconciliation_worker.go`
- `internal/automation/storage/store.go`
- `internal/automation/service/raft.go`

**Source and scope:**

- Committed graph changes drive automation matching and graph replay.
- Reconciliation discovery is pinned to a graph revision and produces ordinary
  durable invocations.
- Schedule checkpoints and graph replay cursors record consumer progress.

**Current progress and durability:**

- `GraphReplayCursor` tracks the last consumed graph revision per
  space/domain.
- `ScheduleCheckpoint` tracks scheduled automation progress.
- Reconciliation operations persist discovery cursor, source authority,
  versions, counts, status, and cancellation.
- Procedures, bindings, invocations, runs, ledgers, and control barriers are
  durable workflow/audit state with Raft paths in clustered mode.

**Boundary decision:**

Automation has projection-like consumer checkpoints, but invocations, outputs,
and audit records are user-visible durable workflow state, not disposable
materialization. Phase 0 therefore excludes automation from the first physical
projection runtime. Later work may reuse the common opaque frontier, fencing,
and checkpoint primitives for graph consumers.

### 6. Graph-change notification history

**Implementation:**

- `internal/graph/notification/module.go`

This is a retained source log and delivery mechanism, not a projection. It must
be treated as a bounded input to materializers. A consumer must detect a gap and
switch to a consistent snapshot/rebuild path rather than assuming the retained
history is complete.

### 7. Inference and catalog projection readers

**Implementation:**

- `internal/inference/service/projection_reader.go`
- semantic/inference catalog managers.

These methods expose catalog data through a projection-oriented adapter, but the
catalog records themselves are authoritative configuration and access metadata.
They are excluded from the derived-projection registry. Their resolved
configuration fingerprints are inputs to semantic projection generations.

## Authority and durability matrix

| State/artifact | Current authority | Current materialization | Phase 0 classification |
| --- | --- | --- | --- |
| Graph nodes/edges and graph revision | Owning graph Raft/WAL state | Local graph segments and memory indexes | Authoritative source |
| Graph-change history | Notification file history/current revision | Filtered consumer delivery | Source feed |
| Semantic rules/configuration | Semantic/space authoritative stores and Raft paths | Loaded managers | Authoritative configuration |
| Semantic dirty events/work/checkpoints | Semantic maintenance stores and Raft paths | Worker queues and loaded indexes | Durable consumer progress |
| Semantic vector records | File-backed vector store with semantic provenance | Local record/keyed views | Derived materialization boundary to clarify |
| Semantic physical vector index | Local vector-store files | Per-rule/binding search index | Rebuildable projection |
| Semantic projection lease | Local lease file plus Raft-backed reliability state | Search readiness gate | Existing projection readiness; pilot migration target |
| Lexical segments/manifest/cursor | Local lexical storage | Per-domain search segments | Rebuildable projection |
| Graph checkpoints | Local graph storage | Latest-state checkpoint | Rebuildable projection |
| Persistent graph/query indexes | Local graph storage | Checkpoint-bound index set | Rebuildable projection |
| Automation definitions/invocations/runs | Automation stores/Raft paths | Scheduler/worker runtime | Authoritative workflow state |
| Automation replay/schedule checkpoints | Automation stores/Raft paths | Consumer/scheduler progress | Durable consumer progress |
| Metadata/adjacency memory indexes | Graph storage at load/apply | In-memory lookup structures | Ephemeral accelerator |

## Boundary lock

The first common projection runtime will not own:

- graph transaction commit or graph authority;
- semantic rule, provider, credential, or authorization decisions;
- lexical tokenization, ranking, or segment algorithms;
- vector generation, provider calls, or vector-store algorithms;
- automation invocation/output/audit records;
- generated API or SDK contracts;
- schema declaration lifecycle tombstones from #249.

It will own only shared lifecycle mechanics: identity, source/configuration
frontiers, checkpoint publication, fencing, recovery state, and bounded
readiness diagnostics.

## Pilot decision

The **semantic rule/binding projection** is the first pilot.

Reasons:

1. It already contains the richest version of the required concepts:
   semantic generation, configuration hash, rebuild epoch, source freshness,
   leader/term ownership, local-baseline recovery, and typed readiness errors.
2. #299 and #326 demonstrate concrete failures caused by inconsistent identity
   and materialization state.
3. The pilot can wrap existing semantic stores and preserve current provider,
   authorization, budget, tombstone, and atomic publication behavior.
4. Its snapshot path already distinguishes authoritative metadata from derived
   vector indexes.

The first pilot excludes semantic catalog/configuration records themselves. It
covers the derived rule/binding materialization and its local search index.
Lexical search is the second adopter; graph checkpoints/query indexes follow
once the source-frontier and manifest contract is proven.

## Phase 0 decisions and open questions

### Decisions

- Use source-partition ownership for projection control metadata; do not create
  a process-global projection authority.
- Model local materialization status per node/replica; a single global `ready`
  flag is insufficient for leader-routed reads.
- Treat source-history gaps as rebuild conditions, never as permission to infer
  missing changes.
- Keep automation workflow state outside the first projection runtime.
- Preserve existing physical stores during migration through adapters and
  manifest bootstrapping.

### Questions for Phase 1

1. Which semantic records are authoritative durable materialization versus
   rebuildable physical artifacts?
2. Should the common source frontier use graph-change notification revisions,
   graph partition revisions, or both with an explicit mapping?
3. Which checkpoint fields must be replicated and which can remain local status
   reports?
4. What is the minimum strong-read freshness contract for semantic and lexical
   APIs?
5. What compatibility mapping is safe for legacy default vector-store IDs?
6. Which retention guarantees are required before source history or tombstones
   can be compacted?

## Phase 0 exit assessment

- [x] Projection/source inventory recorded.
- [x] Authoritative versus derived boundaries recorded.
- [x] Existing readiness, checkpoint, fencing, snapshot, and tombstone paths
  identified for the pilot and second adopter.
- [x] Semantic rule/binding materialization selected as the pilot.
- [ ] Phase 1 control metadata and source-frontier design reviewed and approved.
