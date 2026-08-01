# Mycel Documentation

The documentation tree is organized around Mycel architecture, API, operational, and implementation planning topics.

- `design/` contains architecture, API, operational, and design reference documents.
- `implementation/` contains implementation plans and migration/package plans.
- `roadmap/` contains product and subsystem roadmaps, including `roadmap/gql-roadmap.md` for the GQL feature roadmap.
- `makefile_commands.md` summarizes common `make` targets for building, testing, coverage, and running the daemon locally.

Current schema direction:

- `schema-subsystem.md` is the current operator/developer overview for domain-scoped schemas.
- `gql-schema-behavior.md` documents schema-aware GQL validation and modes.
- `design/schema-subsystem.md` records the original design for the domain-scoped Schema subsystem replacing graph templates.
- `implementation/schema-subsystem-implementation-plan.md` breaks the schema/template replacement and Knot PKM refactor into testable tranches.

Current automation direction:

- `design/graph-automations.md` describes event-driven GQL-conditioned AI graph automations across V1, V2, and V3.

Current subsystem/runtime architecture direction:

- `design/subsystem-runtime-architecture.md` describes the target approach: top-level subsystems own service behavior, shared runtime packages define lifecycle/quiesce/health contracts, and the daemon acts as the composition root.
- `design/subsystem-runtime-package-map.md` records the current package map and import-audit state during the migration.
- `implementation/subsystem-runtime-architecture-implementation-plan.md` breaks the migration into functional phases with testing and documentation expectations.
- `implementation/subsystem-service-physical-move-implementation-plan.md` plans the follow-up migration that physically moves service implementations out of `internal/daemon/modules/*`.
- `implementation/runtime-host-service-initialization-implementation-plan.md` plans the migration from concrete daemon runtime initialization to common `internal/runtime.Host` and capability interfaces.

Current clustering direction:

- `design/clustering-replication-reliability.md` is the seed document for the broader clustering and replication reliability effort.
- `design/authoritative-system-raft-cluster-metadata.md` describes the system Raft group as the source of truth for cluster identity, membership, and placement metadata.
- `implementation/authoritative-system-raft-cluster-metadata-implementation-plan.md` breaks the authoritative system Raft metadata work into testable phases.
- `implementation/phase-a-fail-closed-observability-implementation-plan.md` records the completed Phase A fail-closed and observability work from the broader reliability seed document.
- `implementation/phase-b-durable-raft-runtime-audit.md` records the Phase B audit: persistent raft storage, generic snapshot restore for snapshot-capable state machines, system metadata snapshot catch-up tests, and restart/rejoin validation are implemented for V1, while subsystem-specific partition snapshot formats remain a future hardening gap.
- `implementation/phase-b2-subsystem-snapshot-recovery-implementation-plan.md` plans the follow-up subsystem snapshot recovery work needed before broad production raft log compaction/snapshot-only partition catch-up.
- `implementation/phase-b2-subsystem-snapshot-inventory.md` records the B2.0 subsystem snapshot classification and B2.1 composite snapshot contract implications.
- `implementation/phase-d-raft-command-coverage-implementation-plan.md` details explicit raft-mode ownership for every durable subsystem record.
- `implementation/phase-d-raft-record-coverage-inventory.md` tracks the D0 record coverage inventory and current raft-mode classification for WAL record types.
- `implementation/phase-e-leader-session-transaction-routing-implementation-plan.md` records the completed V1 session/transaction home-node routing work and remaining streaming/local-overlay boundaries.
- `implementation/phase-f-read-consistency-model-implementation-plan.md` records the completed V1 read-consistency tranche: raft read-index/strong-read semantics, read-only transaction semantics, read metadata, stale-read rejection, diagnostics, and focused Phase F gates.
- `implementation/phase-f-read-consistency-inventory.md` records the F0 read-consistency contract and graph/query/backend read-path inventory with F1-F7 updates.
- `implementation/phase-g-divergence-detection-repair-implementation-plan.md` records the completed V1 reliability tranche: deterministic graph checksums, cluster consistency reports, real pod-to-pod data-plane gates, forensic diff/export, manual repair workflows, and Phase G gates.
- `implementation/phase-g-divergence-detection-inventory.md` records the G0 inventory of graph storage APIs, raft/admin metadata, import/export capabilities, diff inputs, and current pinned-pod migration evidence.
- `operations/raft-cluster-operations.md` documents operator checks, Phase A/D/E/F/G pre-release validation gates, readiness blockers, client routing behavior, restart/PVC replacement procedures, and cluster ID mismatch recovery guidance.
- `operations/raft-cluster-manual-repair-workflows.md` documents Phase G7 manual recovery workflows for pinned-pod migration, strict-superset evidence, and conflict recovery without automatic merge.
- `operations/raft-cluster-test-matrix.md` lists the Raft-related focused, full-suite, Compose, and K3s tests used to validate cluster behavior before release.
- `design/space-partitioned-raft-clustering.md` describes the space-partitioned `etcd/raft` clustering architecture.
- `implementation/space-partitioned-raft-clustering-implementation-plan.md` records the Raft migration phases.
- `implementation/remove-static-primary-leftovers-implementation-plan.md` tracks final cleanup of legacy static-primary artifacts.

The old `v1/` and `v2/` generation folders have been removed; use the topic-based folders above for current documentation.
