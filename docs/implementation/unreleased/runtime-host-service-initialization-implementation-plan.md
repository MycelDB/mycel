# Runtime Host Service Initialization Implementation Plan

## Status

Proposed.

This plan migrates subsystem service initialization from the concrete daemon runtime type:

```go
Init(context.Context, *daemonruntime.Runtime) daemonruntime.InitResult
```

to common runtime host/capability interfaces under:

```text
internal/runtime
```

The physical subsystem service move is already complete: service implementations now live under top-level subsystem packages. The remaining architectural dependency is that those services still import `internal/daemon/runtime` and sometimes `internal/daemon/config` for initialization. This plan removes that dependency while keeping the daemon as the composition root.

## Goals

- Let subsystem services initialize using common runtime interfaces instead of concrete daemon runtime.
- Remove subsystem service imports of `internal/daemon/runtime`.
- Minimize or eliminate subsystem service imports of `internal/daemon/config`.
- Keep daemon's concrete runtime implementation under `internal/daemon/runtime`.
- Preserve existing lifecycle behavior, WAL registration, quiesce registration, and service lookup.
- Keep the system functional after every phase.

## Non-goals

- Moving the concrete daemon runtime out of `internal/daemon/runtime`.
- Moving daemon config loading out of `internal/daemon/config`.
- Redesigning WAL/Raft behavior.
- Changing external APIs.
- Splitting semantic foreground/maintenance services in this plan.

## Current issue

Subsystem services currently live in packages such as:

```text
internal/graph/service
internal/space/service
internal/semantic/service
internal/blob/service
internal/session/service
internal/changestream/service
internal/identity/service/admin
internal/identity/service/user
internal/backup/service
```

but many still initialize with:

```go
func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult
```

This means subsystem service packages import daemon runtime/config packages. That violates the target dependency direction:

```text
internal/runtime
    ↑
internal/{subsystem}/service
    ↑
internal/daemon/app
```

## Target shape

Common runtime defines a base service interface and host/capability interfaces:

```go
package runtime

type Service interface {
    Name() string
    Init(context.Context, Host) InitResult
}

type Host interface {
    Log() *slog.Logger
    DataDir() string
}
```

Additional capability interfaces should be small and optional:

```go
type ServiceLookup interface {
    Service(name string) (Service, bool)
}

type QuiesceRegistrar interface {
    RegisterQuiesceParticipant(quiesce.Participant) error
}

type WALProvider interface {
    WAL() *wal.Manager
    WALRegistry() *wal.Registry
    WALProgress() wal.AppliedLSNStore
    WALWaiter() *wal.ApplyWaiter
    WALCheckpoint() *wal.CheckpointStore
}
```

The concrete daemon runtime implements those interfaces. Subsystem services use type assertions for capabilities they need.

## Config strategy

Avoid passing the full daemon config to subsystem services through the host.

Preferred direction:

1. Add subsystem-specific config structs where needed.
2. Pass subsystem config through constructors when possible.
3. Use host only for runtime resources and cross-cutting capabilities.

Examples:

```go
backupservice.NewModule(backupservice.Config{Policy: cfg.Backup, Version: version})
semanticservice.NewModule(semanticservice.Config{Maintenance: cfg.Semantic.Maintenance})
```

If constructor migration is too broad for a phase, add narrow runtime capability methods as a temporary compatibility step, then remove them later.

## Phase 0: Document migration baseline

Status: implemented. This implementation plan documents the motivation, target shape, phased migration, risks, and completion criteria for moving service initialization to common runtime host/capability interfaces.

## Phase 1: Expand `internal/runtime` contracts

Status: implemented. `internal/runtime.Service` now initializes against `runtime.Host`, and common host/capability interfaces are defined for service lookup, quiesce registration, and WAL access. Unit coverage verifies the common host/service contract.

### Tasks

1. Update `internal/runtime.Service` to include `Init(context.Context, Host) InitResult`.
2. Move common lifecycle/capability aliases out of daemon-specific assumptions.
3. Add small host/capability interfaces needed by existing services:
   - `Host`
   - `ServiceLookup`
   - `QuiesceRegistrar`
   - `WALProvider`
   - optional `LoggerProvider`/`DataDirProvider` if useful
4. Keep capability interfaces minimal.
5. Add tests for zero-value/init result helpers and interface compile assertions.

### Acceptance

```sh
go test ./internal/runtime ./internal/runtime/quiesce
```

## Phase 2: Make daemon runtime implement common host/capabilities

Status: implemented as a compatibility step. The concrete daemon runtime now satisfies common `runtime.Host`, `runtime.QuiesceRegistrar`, and `runtime.WALProvider` interfaces. Daemon service orchestration still uses the daemon-specific `Service.Init(ctx, *Runtime)` compatibility interface until subsystem services are migrated in later phases.

### Tasks

1. Add methods on `internal/daemon/runtime.Runtime` to satisfy common runtime host/capability interfaces.
2. Keep existing daemon runtime service registry behavior.
3. Update daemon runtime orchestration to call `service.Init(ctx, r)` through common runtime interface.
4. If needed, keep a temporary daemon-specific adapter interface during migration.
5. Update daemon runtime tests.

### Acceptance

```sh
go test ./internal/daemon/runtime ./internal/daemon/app
```

## Phase 3: Migrate small services first

Status: implemented. `internal/session/service` and `internal/changestream/service` now initialize with common `runtime.Host`, use `runtime.QuiesceRegistrar`, and no longer import daemon runtime/config packages. Daemon runtime orchestration supports both common runtime initializers and legacy daemon-runtime initializers during the migration window.

### Candidate services

```text
internal/session/service
internal/changestream/service
```

### Tasks

1. Change `Init` signature to `Init(context.Context, runtime.Host) runtime.InitResult`.
2. Replace `daemonruntime.OK/Abort/Continue` with `runtime.OK/Abort/Continue`.
3. Replace direct daemon runtime field access with host/capability assertions.
4. Remove daemon runtime/config imports from these service packages.
5. Update tests to use a lightweight test host where possible.

### Acceptance

```sh
go test ./internal/session/service ./internal/changestream/service ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 4: Migrate blob and backup

Status: implemented for service implementations. `internal/blob/service` and `internal/backup/service` now initialize with common `runtime.Host` and runtime capabilities. Blob uses `runtime.WALProvider` and `runtime.QuiesceRegistrar`; backup uses `runtime.WALProvider`, `runtime.QuiesceCoordinatorProvider`, and constructor-supplied `backupservice.Config` from daemon app wiring. Some blob/backup tests still use concrete daemon runtime fixtures and should be converted to lightweight test hosts in a later test-cleanup pass.

### Tasks

1. Migrate `internal/blob/service` to common runtime initialization.
2. Migrate `internal/backup/service` to common runtime initialization.
3. For backup config currently pulled from daemon config, introduce `backupservice.Config` or a narrow constructor option.
4. Ensure WAL/quiesce/checkpoint dependencies come from capability interfaces.
5. Remove daemon runtime/config imports from blob and backup service packages if possible.

### Acceptance

```sh
go test ./internal/blob/... ./internal/backup/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 5: Migrate identity user/admin

Status: implemented for service implementations. `internal/identity/service/user` and `internal/identity/service/admin` now initialize with common `runtime.Host`, use runtime WAL/quiesce capabilities, and no longer import daemon runtime/config in implementation files. Some identity tests still use concrete daemon runtime/config fixtures and should be converted in a later test-cleanup pass.

### Tasks

1. Migrate `internal/identity/service/user` to common runtime initialization.
2. Migrate `internal/identity/service/admin` to common runtime initialization.
3. Replace daemon config test setup with lightweight runtime host fixtures where possible.
4. Preserve WAL and quiesce behavior through capability interfaces.
5. Keep parent `internal/identity/service` facade stable.

### Acceptance

```sh
go test ./internal/identity/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 6: Migrate space

Status: implemented for service implementation. `internal/space/service` now initializes with common `runtime.Host`, uses runtime WAL/quiesce capabilities, and no longer imports daemon runtime/config in implementation files. Raft partition count is preserved through temporary host config introspection. Space tests still use concrete daemon runtime/config fixtures and should be converted in a later test-cleanup pass.

### Tasks

1. Migrate `internal/space/service` to common runtime initialization.
2. Preserve WAL registration and quiesce registration through runtime capabilities.
3. Handle tests that currently construct concrete daemon runtime by introducing shared test host helpers.
4. Remove daemon runtime/config imports where possible.

### Acceptance

```sh
go test ./internal/space/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 7: Migrate graph

Status: implemented for service implementation. `internal/graph/service` now initializes with common `runtime.Host`, uses runtime WAL/quiesce capabilities, and no longer imports daemon runtime/config in implementation files. Graph tests still use concrete daemon runtime/config fixtures and should be converted in a later test-cleanup pass.

### Tasks

1. Migrate `internal/graph/service` to common runtime initialization.
2. Replace daemon runtime references with common host/capability interfaces.
3. Ensure session type imports use `internal/session/service`.
4. Preserve WAL, quiesce, and change sink behavior.

### Acceptance

```sh
go test ./internal/graph/... ./internal/blob/... ./internal/semantic/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 8: Migrate semantic

Status: implemented for service implementation. `internal/semantic/service` now initializes with common `runtime.Host`, uses runtime WAL/quiesce capabilities, and no longer imports daemon runtime/config in implementation files. Daemon app passes semantic secret and maintenance config through `semanticservice.Config`. Semantic tests still use concrete daemon runtime/config fixtures and should be converted in a later test-cleanup pass.

### Tasks

1. Migrate `internal/semantic/service` to common runtime initialization.
2. Introduce semantic service config if daemon config is still needed directly.
3. Preserve maintenance lifecycle, WAL registration, Raft behavior, and graph dirty-event handling.
4. Remove daemon runtime/config imports where possible.

### Acceptance

```sh
go test ./internal/semantic/... ./internal/daemon/api/... ./internal/daemon/server ./internal/daemon/app
go test ./...
```

## Phase 9: Remove daemon runtime service-interface compatibility

Status: implemented. `internal/daemon/runtime.Service` is now an alias to `internal/runtime.Service`, daemon runtime orchestration calls common `Service.Init(ctx, runtime.Host)` directly, and the temporary legacy daemon-runtime initializer has been removed. Daemon runtime tests have been updated to use common host initialization.

### Tasks

1. Make `internal/daemon/runtime.Service` an alias to `internal/runtime.Service`, or remove it if all call sites use common runtime directly.
2. Update daemon app and server code to import common runtime interfaces where appropriate.
3. Remove temporary adapter interfaces.
4. Update tests and docs.

### Acceptance

```sh
go test ./internal/runtime ./internal/daemon/runtime ./internal/daemon/app ./internal/daemon/api/... ./internal/daemon/server
go test ./...
```

## Phase 10: Final audit and documentation

Status: implemented. Dependency audits confirm subsystem service packages, including tests, no longer import `internal/daemon/runtime` or `internal/daemon/config`, `internal/runtime` does not import daemon/subsystem packages, and removed daemon module/quiesce package imports are absent from Go source. `docs/design/subsystem-runtime-package-map.md` has been updated with the final package map.

### Tasks

1. Run dependency audits:

```sh
rg 'internal/daemon/runtime' internal/{backup,blob,changestream,graph,identity,semantic,session,space}/service --glob '*.go'
rg 'internal/daemon/config' internal/{backup,blob,changestream,graph,identity,semantic,session,space}/service --glob '*.go'
rg 'internal/(daemon|backup|blob|changestream|graph|identity|semantic|session|space)' internal/runtime --glob '*.go'
```

2. Update `docs/design/subsystem-runtime-package-map.md`.
3. Update `docs/design/subsystem-runtime-architecture.md` if the final host interface differs from the proposed one.
4. Mark this implementation plan complete.

### Acceptance

```sh
go test ./...
```

## Risks

- Some services access many fields on concrete daemon runtime. Capability interfaces should be introduced carefully to avoid creating one giant host interface.
- Backup and semantic may need subsystem config constructors before daemon config imports can be removed cleanly.
- Tests may need reusable lightweight host fixtures to avoid depending on daemon runtime.
- Changing the base service interface affects all services and runtime orchestration; migrate in small phases.

## Completion criteria

This migration is complete when:

- subsystem service packages no longer import `internal/daemon/runtime`;
- subsystem service packages no longer import `internal/daemon/config`;
- daemon runtime implements common runtime host/capability interfaces;
- service initialization uses `internal/runtime.Host` or smaller capability interfaces;
- `internal/runtime` imports no daemon or subsystem packages;
- `go test ./...` passes.
