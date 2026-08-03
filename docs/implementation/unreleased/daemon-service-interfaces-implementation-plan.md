# Daemon Service Interfaces Implementation Plan

## Status

Proposed.

This plan implements the cross-cutting service lifecycle interfaces described in [Daemon Service Interfaces Design](../../design/runtime/daemon-service-interfaces.md). It should land before or alongside the quiesce/backup implementation so backup does not introduce ad hoc lifecycle behavior.

> Note: this plan records the earlier daemon-centered service-interface migration. The current target packaging direction is described in [Subsystem Runtime Architecture Implementation Plan](subsystem-runtime-architecture-implementation-plan.md), which builds on this work and moves reusable runtime contracts toward common `internal/runtime` packages.

## Acceptance criteria

- Daemon runtime has explicit base service and optional lifecycle capability interfaces.
- The current `Module` concept is replaced by `Service` as the canonical runtime abstraction.
- Existing module packages continue to initialize and run with minimal churn during the first phase.
- Startable services are started by runtime orchestration.
- Stoppable services are stopped in reverse order during daemon shutdown.
- Quiesce participants can be registered consistently by services.
- Tests cover lifecycle ordering, failure behavior, and shutdown behavior.
- Documentation is updated after implementation.

## Phase 1: Define service lifecycle interfaces and compatibility alias

Status: implemented. `Service` is now the canonical base daemon runtime interface, with `Module` retained as a deprecated compatibility alias. Optional `Starter`, `Stopper`, `StatusReporter`, and `HealthReporter` capability interfaces plus non-sensitive status DTOs are defined in `internal/daemon/runtime/service.go`.

### Goals

Add small runtime-level interfaces and make `Service` the canonical concept without changing runtime behavior yet. Keep `Module` only as a temporary compatibility alias so the conversion can be staged safely.

### Files

```text
internal/daemon/runtime/service.go
internal/daemon/runtime/runtime.go
internal/daemon/runtime/runtime_test.go
```

### Tasks

1. Add `Service` interface with `Name` and `Init`.
2. Add a temporary compatibility alias:

```go
type Module = Service
```

3. Mark `Module` as deprecated in comments and direct new code to use `Service`.
4. Add optional capability interfaces:

```go
type Starter interface { Start(context.Context) error }
type Stopper interface { Stop(context.Context) error }
type StatusReporter interface { Status(context.Context) ServiceStatus }
type HealthReporter interface { Health(context.Context) HealthStatus }
```

5. Add minimal status DTOs that do not expose sensitive details.
6. Document that services should implement only the capabilities they need.
7. Avoid package/directory renames in this phase.

### Unit tests

- Compile-time assertions that existing module implementations satisfy `Service` through the temporary alias.
- Status DTO zero-value tests if helpers are added.

### Acceptance

```sh
go test ./internal/daemon/runtime
```

## Phase 2: Convert runtime naming from modules to services

Status: implemented. `Runtime` now has a canonical ordered service registry (`ServicesByName`, `RegisterService`, `Services`, `Service`, `ServiceAs`) plus `StartServices`/`StopServices`. Historical `Modules`, `Module`, and `ModuleAs` compatibility remain for existing call sites.

### Goals

Move the runtime data model and helper methods from `Module` naming to `Service` naming while preserving compatibility for call sites that still use module terminology.

### Files

```text
internal/daemon/runtime/runtime.go
internal/daemon/app/app.go
internal/daemon/runtime/runtime_test.go
internal/daemon/app/app_test.go
```

### Tasks

1. Add ordered service registry to `Runtime`.
2. Rename or supplement runtime fields:

```go
ServicesByName map[string]Service
serviceOrder   []Service
```

3. Keep temporary compatibility helpers for existing code:

```go
func (rt *Runtime) Module(name string) Service // deprecated compatibility helper, if needed
```

4. Add helper methods:

```go
func (rt *Runtime) RegisterService(s Service) error
func (rt *Runtime) Services() []Service
func (rt *Runtime) Service(name string) (Service, bool)
func (rt *Runtime) StartServices(ctx context.Context) error
func (rt *Runtime) StopServices(ctx context.Context) error
```

5. Ensure `StartServices` calls `Start` only on services implementing `Starter`.
6. Ensure `StopServices` calls `Stop` only on services implementing `Stopper`, in reverse successful-start order.
7. If one service fails to start, stop already-started services in reverse order.
8. Preserve existing lookup behavior needed by API/service wiring while shifting new call sites to `Service` naming.

### Unit tests

- Services start in registration order.
- Services stop in reverse start order.
- Non-startable services are skipped.
- Non-stoppable services are skipped.
- Start failure stops previously started services.
- Stop errors are aggregated or returned according to the chosen policy.
- Service registration rejects duplicate names.
- Deprecated module lookup returns the same service as service lookup during the compatibility window.

### Acceptance

```sh
go test ./internal/daemon/runtime ./internal/daemon/app
```

## Phase 3: Rename daemon app wiring from modules to services

Status: implemented. Daemon app composition now initializes and looks up runtime services through `InitServices`/`ServiceAs`/`Service`, while keeping existing package paths and constructors under `internal/daemon/modules/...`. Runtime close now stops started services before legacy close hooks.

### Goals

Update daemon composition code to use `Service` terminology while leaving package paths under `internal/daemon/modules/...` for now.

### Files

```text
internal/daemon/app/app.go
internal/daemon/runtime/runtime.go
internal/daemon/server/server.go
internal/daemon/api/**
internal/daemon/modules/**
```

### Tasks

1. Update variable names and comments from module/module registry to service/service registry in runtime and app wiring.
2. Register existing graph/blob/semantic/space/user/admin/session/change-stream components as services.
3. Keep package directories unchanged:

```text
internal/daemon/modules/graph
internal/daemon/modules/blob
```

4. Do not rename import paths to `internal/daemon/services` in this phase.
5. Update tests and helper names where practical.

### Unit tests

- Daemon app initializes all expected services.
- Service lookup by name works for existing API adapters.
- No behavior change in module/service construction order.

### Acceptance

```sh
go test ./internal/daemon/app ./internal/daemon/runtime ./internal/daemon/api/...
```

## Phase 4: Migrate existing lifecycle handling

Status: implemented. Semantic maintenance background loops now start through `Start(ctx)` and stop through `Stop(ctx)`. `Init` only configures semantic state. Daemon app wiring initializes services, connects graph-to-semantic sinks, then starts services.

### Goals

Move existing ad hoc background lifecycle behavior onto `Starter`/`Stopper` where appropriate.

### Candidate services

```text
internal/daemon/modules/semantic
internal/daemon/modules/changestream
future internal/daemon/modules/backup
```

### Tasks

1. Identify modules that currently start goroutines during `Init`.
2. Move goroutine startup to `Start(ctx)` where practical.
3. Move shutdown/cancellation to `Stop(ctx)` where practical.
4. Keep passive services unchanged.
5. Ensure daemon app calls `StartServices` after all services are initialized.
6. Ensure daemon close/shutdown calls `StopServices`.

### Unit tests

- Semantic module does not start maintenance loops until `Start`.
- Semantic module stops maintenance loops on `Stop`.
- App initialization still wires graph-to-semantic sinks before services start.
- Runtime close invokes stoppable services.

### Acceptance

```sh
go test ./internal/daemon/modules/semantic ./internal/daemon/app ./internal/daemon/runtime
```

## Phase 5: Remove runtime Module alias usage

Status: implemented. Runtime no longer exposes `Modules`, `Module`, `ModuleAs`, or `InitModules`; app/runtime wiring uses `Service`, `ServiceAs`, and `InitServices`. Service implementation structs may still be named `Module` while package-directory renames remain deferred.

### Goals

Remove active runtime use of the old `Module` name once runtime/app/API wiring uses `Service` terminology.

### Tasks

1. Replace remaining `daemonruntime.Module` type references with `daemonruntime.Service`.
2. Replace comments/docs that describe runtime services as modules unless referring to historical package paths.
3. Remove the runtime-level `type Module = Service` alias once no references remain.
4. Do not rename service implementation structs or `internal/daemon/modules/...` directories unless a later dedicated cleanup decides the churn is worthwhile.

### Unit tests

- `rg 'daemonruntime\.Module|type Module' internal/daemon --glob '*.go'` has no hits if alias is removed, or only the documented alias if retained.
- Full daemon test suite compiles without `Module` references in app/runtime wiring.

### Acceptance

```sh
go test ./internal/daemon/...
```

## Phase 6: Quiesce participant integration point

Status: implemented. `internal/daemon/quiesce` now provides `Participant`, `Lease`, `Coordinator`, status DTOs, and ordered `QuiesceAll` orchestration with reverse release and rollback. `Runtime` constructs a `Quiesce` coordinator, and services can register participants during `Init`.

### Goals

Provide a consistent place for services to register quiesce participants without coupling the runtime base interfaces too tightly to backup.

### Files

```text
internal/daemon/runtime/runtime.go
internal/daemon/quiesce/*
internal/daemon/app/app.go
```

### Tasks

1. Add `Quiesce *quiesce.Coordinator` to `Runtime` after the quiesce package exists.
2. Instantiate the coordinator before service init.
3. Decide registration style:
   - explicit service registration during `Init`, preferred for custom participants
   - optional auto-registration for services implementing `quiesce.Participant`
4. Add runtime/app tests showing participants are registered before backup module starts.

### Unit tests

- Service can register a quiesce participant during `Init`.
- Auto-registration works if implemented.
- Duplicate participant names are rejected by the coordinator.

### Acceptance

```sh
go test ./internal/daemon/runtime ./internal/daemon/quiesce ./internal/daemon/app
```

## Phase 7: Backup service

Status: implemented as a lifecycle skeleton. `internal/daemon/modules/backup` now registers as a daemon service with `Init`, `Start`, and `Stop`; backup policy defaults live in `internal/backup`; daemon backup environment variables load into `daemon/config`. Archive creation and Admin APIs remain in the quiesce/backup implementation phases.

### Goals

Make backup scheduling a daemon service using the new lifecycle interfaces. The implementation may initially live under `internal/daemon/modules/backup` for consistency with existing package layout, but the runtime concept should be `backup service`.

### Files

```text
internal/daemon/modules/backup/module.go
internal/daemon/modules/backup/types.go
internal/daemon/modules/backup/module_test.go
internal/backup/*
```

### Tasks

1. Add backup service implementing `Name`, `Init`, `Start`, and `Stop`.
2. In `Init`, construct backup manager and load policy.
3. In `Start`, start scheduler only when policy is enabled.
4. In `Stop`, stop scheduler cleanly.
5. Expose manager to Admin backup API adapter.
6. Ensure backup service uses `Runtime.Quiesce` to quiesce services before archive creation.

### Unit tests

- Backup service starts scheduler when enabled.
- Backup service does not start scheduler when disabled.
- Backup service stops scheduler on `Stop`.
- Backup service surfaces manager status.

### Acceptance

```sh
go test ./internal/daemon/modules/backup ./internal/backup
```

## Phase 8: Service status API support

Status: implemented for internal runtime/service status collection. `Runtime` can now collect non-sensitive `StatusReporter` and `HealthReporter` output; semantic and backup services report lifecycle status; quiesce coordinator status already reports participant state. Public Admin API exposure remains deferred to the backup/status API phases.

### Goals

Expose safe service and quiesce status for operations.

### Tasks

1. Add runtime helper to collect `StatusReporter` statuses.
2. Add quiesce coordinator status collection.
3. Include participant status in backup status API.
4. Optionally add a daemon/service status Admin API later.

### Unit tests

- Runtime collects statuses only from status-reporting services.
- Sensitive values are not included in status DTOs.
- Backup status includes participant names, active counts, and quiesce state.

### Acceptance

```sh
go test ./internal/daemon/runtime ./internal/daemon/api/admin
```

## Phase 9: Optional package-directory rename decision

Status: implemented as a decision to defer. The runtime abstraction is now `Service`, but package paths remain under `internal/daemon/modules/...` for stability. Renaming directories to `internal/daemon/services/...` is not required for quiesce/backup and would be broad mechanical churn, so it is deferred until there is a separate reason to do it.

### Goals

Decide whether package paths should also move from `internal/daemon/modules/...` to `internal/daemon/services/...`.

### Decision

Defer. The conceptual runtime rename from Module to Service is valuable and implemented. The directory rename is broad mechanical churn and is not required for quiesce/backup.

### Tasks if later approved

1. Move directories:

```text
internal/daemon/modules/graph    -> internal/daemon/services/graph
internal/daemon/modules/blob     -> internal/daemon/services/blob
internal/daemon/modules/semantic -> internal/daemon/services/semantic
```

2. Update imports and package docs.
3. Keep package names stable where possible to reduce churn.

### Acceptance if performed

```sh
go test ./...
git diff --check
```

## Phase 10: Documentation update

### Files to update

```text
docs/design/daemon-service-interfaces.md
docs/implementation/daemon-service-interfaces-implementation-plan.md
docs/design/quiesce-and-backup.md
docs/implementation/quiesce-and-backup-implementation-plan.md
docs/design/daemon-only-boundary.md
README.md
```

### Tasks

1. Mark implemented phases as they land.
2. Document final package locations and lifecycle semantics.
3. Document service registration examples.
4. Document how backup uses service lifecycle and quiesce participants.
5. Update final validation commands.

### Acceptance

```sh
git diff --check
```

## Final validation

Run from `myceldb/mycel`:

```sh
go test ./...
make test
make build
scripts/check-public-surface.sh --workspace /Users/martinbeauvais/Projects/knotbase/Knotbase --strict
git diff --check
```
