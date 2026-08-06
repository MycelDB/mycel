# AGENTS.md

Instructions for AI coding agents working in this repository.

## Project overview

mycel is a daemon-first graph data system written in Go. The daemon owns
identity, sessions, spaces/domains, graph transactions, blobs, schemas,
semantic indexing, automation, backup/restore, and raft clustering.

Use the term **subsystem** for internal domain areas.

## Repository rules

- Keep daemon API adapters under `internal/daemon/api`.
- Service implementations belong under their subsystem packages; the daemon is
  the composition root.
- Do not move daemon API adapters out of `internal/daemon/api`.
- Do not commit generated ANTLR parser output.
- Do not commit generated public SDK/API code unless explicitly approved.
- Do not hand-edit generated protobuf files; update source protobufs and
  generation scripts instead.
- Keep each tranche functional: avoid large rewrites that leave the repo in a
  partially broken state.

## Safety-critical behavior

- System raft metadata is authoritative.
- Raft mode must fail closed until metadata is applied/validated and partition
  groups have started.
- Durable user-visible writes in raft mode must be raft-owned,
  derived/rebuildable, or fail closed.
- Committed reads should preserve the strong/read-index consistency model.
- Automatic raft compaction must remain conservative/off by default unless
  snapshot install, atomic restore, and soak gates are proven.
- Divergence tooling is forensic/read-only first. Do not add automatic divergent
  PVC repair, automatic merge, overwrite, rebalance, or repair behavior.
- User-scoped backup/restore is explicit operator tooling. It must not
  automatically pick an authoritative node or repair split-brain state.
- User backups must not export plaintext passwords or active sessions/tokens.
- Imports should default to dry-run/fail-on-conflict; destructive restore modes
  require explicit future flags.

## Documentation expectations

Docs are organized by intent:

- `docs/README.md` — documentation entrypoint.
- `docs/design/` — current architecture and subsystem design.
- `docs/operations/` — operator procedures, CLI usage, recovery, validation.
- `docs/implementation/` — archival/release-grouped implementation plans.

Operator-facing recovery docs belong under `docs/operations/procedures/`.
Implementation plans are not current operator guidance.

When changing docs, run:

```sh
make docs-check
```

## Common validation commands

For docs-only changes:

```sh
make docs-check
git diff --check
```

For normal code changes:

```sh
make test
git diff --check
```

For raft/clustering-sensitive changes, also consider the relevant phase targets:

```sh
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

Destructive Compose/K3s validation must only be run when explicitly requested:

```sh
make test-compose-cluster
make test-k3s-cluster
make test-compose-user-backup-restore
```

## Generated artifacts

- Protobuf definitions live in `github.com/myceldb/mycel-api`.
- Daemon Go stubs are generated locally under `internal/gen/`.
- ANTLR grammar sources live under `internal/query/gql/`.
- Do not commit generated ANTLR parser output unless explicitly approved.

## Coding style

- Prefer small, reviewable changes.
- Add or update tests with behavior changes.
- Keep errors actionable, especially for operator-facing raft, backup, restore,
  and clustering workflows.
- Avoid adding hidden destructive behavior.
- Prefer explicit flags for risky operations.
- Preserve existing public API behavior unless the task explicitly changes it.

## CLI and operator UX

- Admin/operator commands should be explicit and safe by default.
- Cluster reliability UI and CLI surfaces should expose diagnostics first.
- Do not add force snapshot, force compaction, merge, rebalance, delete,
  overwrite, or repair actions unless explicitly requested and designed.

## Before final response

Summarize:

- files changed
- commands run
- tests/checks passed or not run
- remaining risks or follow-ups
