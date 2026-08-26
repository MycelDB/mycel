# Unreleased / Unclassified Implementation Plans

This bucket contains plans that are not yet assigned to a tagged release bucket
or are kept for future cleanup/reference. Review current design and operations
docs before treating any plan here as authoritative current behavior.

## Backup and restore

These plans are complete on the post-v0.6 `for_wednesday` line but are not yet
assigned to a tagged release bucket.

- [Cluster system backup implementation plan](cluster-system-backup-implementation-plan.md) — complete for the initial coordinated backup set path.
- [Cluster system backup raft freeze implementation plan](cluster-system-backup-raft-freeze-implementation-plan.md) — complete through RF6 for raft-storage-safe archive capture and release-gate docs.
- [Workload-driven system backup/restore test implementation plan](workload-driven-system-backup-restore-test-implementation-plan.md) — implemented rewrite of the destructive K3s backup/restore gate using normal graph workloads, PVC deletion, restore, and convergence verification.

## Graph/change notification and activity

- [Activity events implementation plan](activity-events-implementation-plan.md) — phased plan for a durable operator-facing event stream, Admin APIs, daemon/external emitters, Console Activity page, retention, and export.
- [Graph adjacency index implementation plan](graph-adjacency-index-implementation-plan.md) — derived per-space in-memory adjacency index for faster hierarchy validation and Logseq-shaped imports.
- [Graph-change notification implementation plan](graph-change-notification-implementation-plan.md) — internal committed graph-change model, process-local consumer registrations, projection, replay, and raft-safe notification delivery.
- [add_callbacks parking-lot implementation plan](add-callbacks-parking-lot-implementation-plan.md) — remaining downstream compatibility, examples, integration notes, and final validation work after parking the coordinated graph-change watch/operation-correlation branches.
- [add_callbacks integration notes](add-callbacks-integration-notes.md) — breaking API migration notes, operation ID semantics, downstream compatibility result, example locations, and validation checklist for the coordinated branches.

## Semantic maintenance, inference, and automation

- [Standalone inference for graph automations implementation plan](standalone-inference-for-graph-automations-implementation-plan.md) — breaking phased plan for a standalone inference subsystem shared by semantic embeddings/search and graph automations.
- [Graph context automations implementation plan](graph-context-automations-implementation-plan.md) — phased plan for aggregate/context automations that select a target alias, collect related graph context, render multi-row input, and update the selected graph element.
- [Graph procedures and automation bindings implementation plan](graph-procedures-and-automation-bindings-implementation-plan.md) — phased plan to split reusable graph procedures from trigger/scope/runtime-principal automation bindings while preserving legacy automation definitions.
- [Semantic maintenance loaded state implementation plan](semantic-maintenance-loaded-state-implementation-plan.md) — per-space loaded maintenance managers and in-memory indexes for dirty events, work items, and checkpoints.
- [Intelligence Access model kind implementation plan](intelligence-access-model-kind-implementation-plan.md) — breaking cleanup that replaces model-level workload operations with model kind/category while keeping endpoint capabilities authoritative for workload support.
- [Intelligence Access Raft authority implementation plan](intelligence-access-raft-authority-implementation-plan.md) — phased plan to make Intelligence profiles semantic/Raft-owned and keep standalone inference state as a derived projection in clustered mode.

## Query and schema indexes

- [GWL indexes and indexed query execution implementation plan](gwl-indexes-and-indexed-query-execution-implementation-plan.md) — schema-declared node/edge indexes, graph index persistence, synchronous maintenance, backfill, and indexed structured/GQL query planning.
- [Query expansion implementation plan](query-expansion-implementation-plan.md) — GQL delete/merge, parameters, aliased projections, indexed structured query parity, and path projection.
- [Top query priorities implementation plan](top-query-priorities-implementation-plan.md) — indexed structured multi-hop traversal/path projection, aggregation/result shaping, and predicate/index-pushdown MVP baseline.
- [Top query priorities completion implementation plan](top-query-priorities-completion-implementation-plan.md) — remaining work to make predicate pushdown, semantic/vector execution, broader indexed paths, aggregation, result shaping, diagnostics, SDKs, and Console support production-complete.
- [REPL space connection and GQL execution implementation plan](repl-space-gql-connect-implementation-plan.md) — psql-like REPL connection state for spaces/domains and convenient GQL execution without repeating IDs.

## Clustering and raft reliability

- [Raft disruption test harness implementation plan](raft-disruption-test-harness-implementation-plan.md) — reusable destructive K3s/k3d raft pod-restart pressure harness with disposable cluster lifecycle and artifact capture.

## Cleanup

- [Legacy file-session and embedding migration cleanup implementation plan](legacy-filesession-and-embedding-cleanup-implementation-plan.md) — staged removal plan for `internal/graph/filesession`, `internal/session/api`, legacy embedding migration, and related compatibility surfaces.

## Identity and access control

- [Unified principal identity implementation plan](unified-principal-identity-implementation-plan.md) — breaking replacement of split administrator/user identity stores with one principal, role-binding, and capability-grant model.
- [Unified principal identity remaining work plan](unified-principal-identity-remaining-work-plan.md) — follow-up checklist for completing the missing UP1-UP11 work after the partial unified-principal branch implementation.

## SDKs and downstream integrations

- [SDK error classification and login UX implementation plan](sdk-error-classification-and-login-ux-implementation-plan.md) — cross-repo plan for Rust/Go SDK error classification, structured downstream app errors, and Console login severity handling.

## Admin/UI follow-ups

- [Admin template service and UI implementation plan](admin-template-service-and-ui-implementation-plan.md)
- [mycel-console Intelligence navigation implementation plan](mycel-console-intelligence-navigation-implementation-plan.md) — planned console navigation and page restructure for Intelligence/Access, Automations, and Semantic management.
- [Semantic generation rules implementation plan](semantic-generation-rules-implementation-plan.md) — replacement plan for semantic indexes as constrained graph-reactive embedding rules with fast physical search indexes.
- [SGR0 semantic generation rules API surface plan](semantic-generation-rules-sgr0-api-surface-plan.md) — tranche-specific plan for replacing public semantic-index API terminology with semantic generation rules.
- [SGR1 semantic generation rules domain model plan](semantic-generation-rules-sgr1-domain-model-plan.md) — tranche-specific plan for introducing the internal semantic generation rule model and binding-aware records/work.
- [SGR2 semantic generation rules storage plan](semantic-generation-rules-sgr2-storage-plan.md) — tranche-specific plan for rule-native storage, WAL/raft mutations, and binding-aware maintenance work keys.
- [SGR3 semantic generation rules validation plan](semantic-generation-rules-sgr3-validation-plan.md) — tranche-specific plan for rule validation, selector validation, and bounded GQL selector compilation.
- [SGR4 semantic generation rules analyzer plan](semantic-generation-rules-sgr4-analyzer-plan.md) — tranche-specific plan for rule-native dirty-event analysis and binding-aware work enqueueing.
- [SGR5 semantic generation rules source assembly and embedding generation plan](semantic-generation-rules-sgr5-source-generation-plan.md) — tranche-specific plan for rule/binding source assembly, Intelligence Access embedding resolution, and vector record generation.
- [SGR6 semantic generation rules physical search index plan](semantic-generation-rules-sgr6-physical-search-index-plan.md) — tranche-specific plan for per-rule/per-binding latest-live physical vector search indexes.
- [SGR7 semantic generation rules search planner plan](semantic-generation-rules-sgr7-search-planner-plan.md) — tranche-specific plan for rule-native, binding-aware, fast-index-backed semantic search planning.
- [SGR8 semantic generation rules Admin and Client API plan](semantic-generation-rules-sgr8-admin-client-api-plan.md) — tranche-specific plan for replacing daemon Admin/Client semantic APIs with rule and binding terminology.
- [SGR9 semantic generation rules CLI replacement plan](semantic-generation-rules-sgr9-cli-replacement-plan.md) — tranche-specific plan for replacing `semantic index` CLI commands with rule-native lifecycle, search, maintenance, and backfill commands.
- [SGR10 semantic generation rules Console authoring plan](semantic-generation-rules-sgr10-console-rule-authoring-plan.md) — tranche-specific plan for updating `mycel-console` Intelligence / Semantic to author, validate, monitor, and maintain semantic generation rules.
- [SGR12 semantic generation rules end-to-end validation report](semantic-generation-rules-sgr12-end-to-end-validation-report.md) — validation evidence for the completed semantic generation rules tranche set.
