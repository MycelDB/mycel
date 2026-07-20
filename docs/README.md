# Mycel Documentation

The documentation tree is organized around the current daemon-oriented architecture.

- `design/` contains architecture, API, operational, and design reference documents.
- `implementation/` contains implementation plans and migration/package plans.
- `makefile_commands.md` summarizes common `make` targets for building, testing, coverage, and running the daemon locally.

Current clustering direction:

- `design/space-partitioned-raft-clustering.md` describes the planned space-partitioned `etcd/raft` clustering architecture.
- `implementation/space-partitioned-raft-clustering-implementation-plan.md` breaks the work into compatibility-preserving phases.
- `implementation/static-primary-clustering-artifact-inventory.md` tracks old static-primary/WAL-replication artifacts that will be removed or replaced once Raft equivalents are live.

The old `v1/` and `v2/` generation folders have been removed; use the topic-based folders above for current documentation.
