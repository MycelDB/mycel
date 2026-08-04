# Subsystem Service Physical Move Implementation Plan

## Status

Proposed.

This plan follows the subsystem/runtime compatibility-wrapper migration. The current subsystem service packages exist, but most of them re-export implementations from `internal/daemon/modules/*`. This plan physically moves those implementations into subsystem-owned service packages and removes the daemon module layer.

## Goal

Move implementation ownership from:

```text
internal/daemon/modules/<name>
```

to:

```text
internal/<subsystem>/service
```

while keeping the system functional after every tranche.

## Non-goals

- Changing external gRPC/admin/client APIs.
- Reworking domain models or storage formats.
- Rewriting WAL or Raft semantics.
- Moving daemon API adapters out of `internal/daemon/api`.
- Removing `internal/daemon/runtime` concrete daemon runtime in this plan.

## General rules

For each tranche:

1. Move files with `git mv` where possible.
2. Change package name to `service`.
3. Update imports from old daemon module path to new subsystem service path.
4. Keep service names stable, e.g. `graph`, `space`, `semantic`.
5. Preserve exported manager interfaces and constructors.
6. Preserve WAL record types and Raft state-machine behavior.
7. Run focused tests first, then `go test ./...`.
8. Only delete old daemon module package after all imports are migrated.
9. Update docs if package ownership or caveats change.

## Tranche 1: Move session and changestream

Status: implemented. Session and changestream implementations and tests have been physically moved from `internal/daemon/modules/*` into `internal/session/service` and `internal/changestream/service`. Imports were migrated and the old daemon module directories were removed.

### Rationale

These are smaller services and good first physical moves. They also validate that small subsystems are first-class subsystem owners.

### Target moves

```text
internal/daemon/modules/session/*       -> internal/session/service/*
internal/daemon/modules/changestream/*  -> internal/changestream/service/*
```

### Tasks

1. Move session implementation and tests into `internal/session/service`.
2. Move changestream implementation and tests into `internal/changestream/service`.
3. Update changestream imports to use `internal/session/service` instead of daemon session module.
4. Update daemon API/server/app/tests to import new paths.
5. Remove old daemon module directories when no imports remain.

### Acceptance

```sh
go test ./internal/session/service ./internal/changestream/service ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 2: Move backup and blob

Status: implemented. Backup and blob service implementations and tests have been physically moved from `internal/daemon/modules/*` into `internal/backup/service` and `internal/blob/service`. Imports were migrated and the old daemon module directories were removed.

### Target moves

```text
internal/daemon/modules/backup/* -> internal/backup/service/*
internal/daemon/modules/blob/*   -> internal/blob/service/*
```

### Tasks

1. Move backup runtime service files into `internal/backup/service`.
2. Keep core backup mechanics in `internal/backup`.
3. Move blob service files into `internal/blob/service`.
4. Keep low-level blob storage in `internal/blob/storage`.
5. Update cluster backend payload provider imports.
6. Update WAL/Raft tests and package names.
7. Remove old daemon module directories when no imports remain.

### Acceptance

```sh
go test ./internal/backup/... ./internal/blob/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 3: Move identity user/admin

Status: implemented. User and admin/operator service implementations and tests have been physically moved from `internal/daemon/modules/*` into `internal/identity/service/user` and `internal/identity/service/admin`. The parent `internal/identity/service` package remains as a facade exposing stable identity service aliases and constructors.

### Target moves

Possible target layout:

```text
internal/identity/service/user/*
internal/identity/service/admin/*
```

or, if package size remains manageable:

```text
internal/identity/service/*
```

### Recommendation

Use subpackages if moving both admin/operator and user implementations into one package becomes too large or creates unclear names. Identity may expose more than one runtime service:

```text
user
admin/operator
```

### Tasks

1. Move `internal/daemon/modules/user/*` into identity service ownership.
2. Move `internal/daemon/modules/admin/*` into identity service ownership.
3. Preserve service names `user` and `admin` unless deliberately changed.
4. Update API auth/admin/user/operator imports.
5. Update WAL/Raft state-machine imports.
6. Remove old daemon module directories when no imports remain.

### Acceptance

```sh
go test ./internal/identity/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 4: Move space

Status: implemented. Space service implementation and tests have been physically moved from `internal/daemon/modules/space` into `internal/space/service`. Imports were migrated and the old daemon module directory was removed.

### Target move

```text
internal/daemon/modules/space/* -> internal/space/service/*
```

### Tasks

1. Move space service implementation and tests.
2. Keep `internal/space/model`, `internal/space/storage`, and `internal/space/access` unchanged.
3. Preserve space WAL records and appliers.
4. Preserve space Raft state machine, read routing, idempotency, and metadata behavior.
5. Update daemon API, server, clustering backend reader, and app wiring.
6. Remove old daemon module directory when no imports remain.

### Acceptance

```sh
go test ./internal/space/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 5: Move graph

Status: implemented. Graph service implementation and tests have been physically moved from `internal/daemon/modules/graph` into `internal/graph/service`. Imports were migrated and the old daemon module directory was removed.

### Target move

```text
internal/daemon/modules/graph/* -> internal/graph/service/*
```

### Tasks

1. Move graph service implementation and tests.
2. Keep `internal/graph/model`, `internal/graph/storage`, `internal/graph/query`, and related domain packages unchanged.
3. Update imports to use `internal/session/service` for transaction/session types.
4. Preserve WAL and Raft behavior.
5. Preserve graph-to-semantic dirty-event wiring in daemon composition root unless a separate event bus is introduced later.
6. Update blob ref counter dependencies.
7. Remove old daemon module directory when no imports remain.

### Acceptance

```sh
go test ./internal/graph/... ./internal/blob/... ./internal/semantic/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 6: Move semantic

Status: implemented. Semantic service implementation and tests have been physically moved from `internal/daemon/modules/semantic` into `internal/semantic/service`. Imports were migrated and the old daemon module directory was removed.

### Target move

```text
internal/daemon/modules/semantic/* -> internal/semantic/service/*
```

### Tasks

1. Move semantic service implementation and tests.
2. Decide whether to split foreground manager and maintenance worker into multiple services after the physical move, not during it unless necessary.
3. Keep semantic model/storage/search/vectorstore/maintenance/backfill packages unchanged.
4. Preserve WAL wrappers, accounting, maintenance loops, and Raft behavior.
5. Update graph change sink and daemon API imports.
6. Remove old daemon module directory when no imports remain.

### Acceptance

```sh
go test ./internal/semantic/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Tranche 7: Remove daemon module layer and audit

Status: implemented. The empty `internal/daemon/modules` tree has been removed, imports of `internal/daemon/modules/*` have been eliminated from Go source, and the package-map audit documentation has been updated. Residual subsystem-service imports of `internal/daemon/runtime` and `internal/daemon/config` remain as expected until service initialization is migrated from concrete daemon runtime to common runtime host/capability interfaces.

### Tasks

1. Remove empty `internal/daemon/modules` tree.
2. Run import audits:

```sh
rg 'internal/daemon/modules' internal --glob '*.go'
rg 'internal/daemon' internal/{backup,blob,changestream,graph,identity,semantic,session,space}/service --glob '*.go'
rg 'internal/(daemon|backup|blob|changestream|graph|identity|semantic|session|space)' internal/runtime --glob '*.go'
```

3. Update `docs/design/subsystem-runtime-package-map.md` to remove compatibility-wrapper caveats.
4. Update `docs/implementation/subsystem-runtime-architecture-implementation-plan.md` to mark physical move complete.
5. Update any older docs that still present `internal/daemon/modules/*` as the preferred package location.

### Acceptance

```sh
go test ./...
```

## Risks

- WAL/Raft tests may rely on package-private helpers. Moving files preserves package-private access only if tests move with the package.
- Identity user/admin may be easier to move as separate packages due to size and naming.
- Graph and semantic have cross-subsystem dependencies and should be moved after smaller services validate the pattern.
- Large mechanical import changes can obscure behavior changes; keep tranches small.

## Completion criteria

The migration is complete when:

- no Go source imports `internal/daemon/modules/*`;
- subsystem service implementations live under their subsystem package paths;
- `internal/daemon/modules` is removed or contains only explicitly documented daemon-only code;
- `internal/runtime` has no daemon/subsystem imports;
- `go test ./...` passes.
