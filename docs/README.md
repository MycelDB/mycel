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
- `operations/raft-cluster-operations.md` documents operator checks, readiness blockers, restart/PVC replacement procedures, and cluster ID mismatch recovery guidance.
- `design/space-partitioned-raft-clustering.md` describes the space-partitioned `etcd/raft` clustering architecture.
- `implementation/space-partitioned-raft-clustering-implementation-plan.md` records the Raft migration phases.
- `implementation/remove-static-primary-leftovers-implementation-plan.md` tracks final cleanup of legacy static-primary artifacts.

The old `v1/` and `v2/` generation folders have been removed; use the topic-based folders above for current documentation.
