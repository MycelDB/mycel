# Unreleased / Unclassified Implementation Plans

This bucket contains plans that are not yet assigned to a tagged release bucket
or are kept for future cleanup/reference. Review current design and operations
docs before treating any plan here as authoritative current behavior.

## Backup and restore

These plans are complete on the post-v0.6 `for_wednesday` line but are not yet
assigned to a tagged release bucket.

- [Cluster system backup implementation plan](cluster-system-backup-implementation-plan.md) — complete for the initial coordinated backup set path.
- [Cluster system backup raft freeze implementation plan](cluster-system-backup-raft-freeze-implementation-plan.md) — complete through RF6 for raft-storage-safe archive capture and release-gate docs.

## Graph/change notification

- [Graph adjacency index implementation plan](graph-adjacency-index-implementation-plan.md) — derived per-space in-memory adjacency index for faster hierarchy validation and Logseq-shaped imports.
- [Graph-change notification implementation plan](graph-change-notification-implementation-plan.md) — internal committed graph-change model, process-local consumer registrations, projection, replay, and raft-safe notification delivery.
- [add_callbacks parking-lot implementation plan](add-callbacks-parking-lot-implementation-plan.md) — remaining downstream compatibility, examples, integration notes, and final validation work after parking the coordinated graph-change watch/operation-correlation branches.
- [add_callbacks integration notes](add-callbacks-integration-notes.md) — breaking API migration notes, operation ID semantics, downstream compatibility result, example locations, and validation checklist for the coordinated branches.

## Semantic maintenance and inference

- [Standalone inference for graph automations implementation plan](standalone-inference-for-graph-automations-implementation-plan.md) — breaking phased plan for a standalone inference subsystem shared by semantic embeddings/search and graph automations.
- [Semantic maintenance loaded state implementation plan](semantic-maintenance-loaded-state-implementation-plan.md) — per-space loaded maintenance managers and in-memory indexes for dirty events, work items, and checkpoints.

## Query and schema indexes

- [GWL indexes and indexed query execution implementation plan](gwl-indexes-and-indexed-query-execution-implementation-plan.md) — schema-declared node/edge indexes, graph index persistence, synchronous maintenance, backfill, and indexed structured/GQL query planning.
- [REPL space connection and GQL execution implementation plan](repl-space-gql-connect-implementation-plan.md) — psql-like REPL connection state for spaces/domains and convenient GQL execution without repeating IDs.

## Cleanup

- [Legacy file-session and embedding migration cleanup implementation plan](legacy-filesession-and-embedding-cleanup-implementation-plan.md) — staged removal plan for `internal/graph/filesession`, `internal/session/api`, legacy embedding migration, and related compatibility surfaces.

## Identity and access control

- [Unified principal identity implementation plan](unified-principal-identity-implementation-plan.md) — breaking replacement of split administrator/user identity stores with one principal, role-binding, and capability-grant model.
- [Unified principal identity remaining work plan](unified-principal-identity-remaining-work-plan.md) — follow-up checklist for completing the missing UP1-UP11 work after the partial unified-principal branch implementation.

## Admin/UI follow-ups

- [Admin template service and UI implementation plan](admin-template-service-and-ui-implementation-plan.md)
