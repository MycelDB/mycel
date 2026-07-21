# Subsystem Runtime Architecture Implementation Plan

## Status

Proposed.

This plan implements the architecture described in [Subsystem Runtime Architecture](../design/subsystem-runtime-architecture.md). The migration is intentionally phased so each phase leaves the system compiling, tested, and operational.

## Acceptance criteria

- Shared runtime contracts and quiesce primitives are available outside `internal/daemon`.
- Subsystem service packages can depend on runtime contracts without importing daemon packages.
- A subsystem may expose one or more runtime services when responsibilities have distinct lifecycles, dependencies, health/status, quiesce behavior, or startup ordering.
- The daemon remains the composition root.
- Existing daemon behavior remains functional after every phase.
- Documentation is updated as packages move.
- Tests are updated in the same phase as code changes.
- Compatibility aliases or bridge packages are removed only after call sites are migrated.

## Phase 0: Document and align architecture

### Goals

Create the design and implementation documentation before moving code.

### Tasks

1. Add `docs/design/subsystem-runtime-architecture.md`.
2. Add this implementation plan.
3. Update `docs/README.md` to reference the new design and plan.
4. Review older daemon-service-interface docs and mark them as predecessors or partial context if needed.

### Tests

No code tests required.

### Acceptance

```sh
git diff -- docs/design/subsystem-runtime-architecture.md docs/implementation/subsystem-runtime-architecture-implementation-plan.md docs/README.md
```

## Phase 1: Introduce common runtime package with compatibility bridges

Status: implemented. `internal/runtime` now contains common service lifecycle/status/init types, and `internal/daemon/runtime` aliases the reusable types while retaining the concrete daemon `Runtime` and daemon-specific `Service.Init` compatibility signature.

### Goals

Create `internal/runtime` and move/copy shared interfaces there without changing subsystem behavior.

### Target files

```text
internal/runtime/service.go
internal/runtime/state.go
internal/runtime/health.go
internal/runtime/init.go
internal/runtime/registry.go
internal/runtime/runtime_test.go
```

### Tasks

1. Introduce `internal/runtime` with the current shared service contracts from `internal/daemon/runtime`:
   - `Service`
   - `Starter`
   - `Stopper`
   - `StatusReporter`
   - `HealthReporter`
   - `SnapshotReloadable`
   - `ServiceStatus`
   - `HealthStatus`
   - `InitResult`
   - `InitError`
   - `OK`, `Abort`, `Continue`
2. Keep `internal/daemon/runtime` functional by type-aliasing or forwarding to `internal/runtime` where possible.
3. Do not move daemon's concrete `Runtime` struct yet.
4. Update only low-risk imports if necessary.

### Tests

- Add compile-time tests or ordinary unit tests confirming aliases/forwards behave as expected.
- Existing daemon runtime tests should continue to pass.

### Acceptance

```sh
go test ./internal/runtime ./internal/daemon/runtime
```

## Phase 2: Move quiesce to common runtime

Status: implemented. Quiesce implementation and tests now live under `internal/runtime/quiesce`. The temporary `internal/daemon/quiesce` alias bridge has been removed after internal call sites were migrated to `internal/runtime/quiesce`.

### Goals

Make quiesce a common runtime primitive instead of a daemon-owned primitive.

### Target files

```text
internal/runtime/quiesce/**
```

### Tasks

1. Move quiesce implementation to `internal/runtime/quiesce`.
2. Update subsystem/module imports from daemon quiesce paths to `internal/runtime/quiesce` incrementally.
3. Remove the daemon quiesce compatibility package once no code imports it.
4. Update docs referencing daemon quiesce package.

### Tests

- Move or duplicate quiesce unit tests to `internal/runtime/quiesce`.
- Keep compatibility package covered by compile tests if aliases are used.
- Run backup/quiesce-related tests.

### Acceptance

```sh
go test ./internal/runtime/quiesce ./internal/daemon/runtime ./internal/daemon/modules/...
```

## Phase 3: Decouple service initialization from concrete daemon runtime

Status: partially implemented as a compatibility step. `internal/runtime.Host` and related common host/capability interfaces have been introduced, and the concrete daemon runtime exposes common host methods. Existing daemon services still use `Init(context.Context, *daemonruntime.Runtime)`; changing service initialization signatures is deferred to a later migration phase to keep the system functional.

### Goals

Reduce subsystem dependency on the concrete daemon runtime by introducing a smaller host/capability interface.

### Target files

```text
internal/runtime/host.go
internal/daemon/runtime/runtime.go
internal/daemon/runtime/service.go
internal/daemon/modules/**
```

### Tasks

1. Define a minimal `runtime.Host` interface for service initialization.
2. Add smaller capability interfaces as needed, such as:
   - logger provider
   - data directory provider
   - service lookup provider
   - WAL provider
   - quiesce registrar
3. Make the concrete daemon runtime implement these interfaces.
4. Change `runtime.Service` toward `Init(context.Context, runtime.Host)`.
5. Use compatibility adapters if a full signature migration is too broad for one change.
6. Update services one group at a time while keeping all tests passing.

### Tests

- Update daemon runtime tests for host/capability behavior.
- Add tests that services cannot require concrete daemon runtime when only common host capabilities are needed.
- Existing module/service tests should pass.

### Acceptance

```sh
go test ./internal/runtime ./internal/daemon/runtime ./internal/daemon/modules/...
```

## Phase 4: Establish subsystem service package pattern

Status: implemented as a compatibility-wrapper phase. First-class subsystem service packages now exist for session and changestream, with compile-time tests and daemon app wiring using the new package paths. The implementations currently alias the existing daemon module implementations to keep behavior stable while establishing the target package pattern.

### Goals

Create the target package pattern without moving the largest subsystem first.

### Candidate first subsystems

Recommended candidates:

```text
internal/backup/service
internal/changestream/service
internal/session/service
```

These may be small, but small subsystems are acceptable and should get first-class homes because they are expected to grow.

### Tasks

1. Pick one subsystem as the pilot.
2. Create `internal/<subsystem>/service`.
3. Decide whether the subsystem should expose one service or multiple services. Prefer multiple services when concerns have distinct lifecycle, health/status, quiesce, dependency, or startup-order requirements.
4. Move the service implementation from `internal/daemon/modules/<subsystem>` or create wrappers in the new package.
5. Keep a temporary compatibility package under `internal/daemon/modules/<subsystem>` if needed.
6. Update daemon app wiring to construct the new subsystem service or services.
7. Update API adapters to depend on the same domain-specific manager interfaces as before.
8. Update package comments and docs.

### Tests

- Move subsystem tests with the package or keep compatibility tests in place.
- Add compile-time assertions that the new service implements common runtime interfaces.
- Run subsystem and daemon app tests.

### Acceptance

```sh
go test ./internal/<subsystem>/... ./internal/daemon/app ./internal/daemon/api/...
```

## Phase 5: Migrate identity/admin/user service ownership

Status: implemented as a compatibility-wrapper phase. `internal/identity/service` now exposes user and admin/operator runtime services, manager interfaces, identity types, and Raft state-machine aliases. Daemon app wiring constructs user/admin services through the identity subsystem package while preserving existing behavior.

### Goals

Move user/admin service behavior toward the identity subsystem while preserving existing API behavior.

### Target packages

Possible target:

```text
internal/identity/service
```

or, if clearer:

```text
internal/identity/service/user
internal/identity/service/admin
```

### Tasks

1. Decide whether user and admin are one identity subsystem service or separate identity service packages.
2. Move or wrap `internal/daemon/modules/user` and `internal/daemon/modules/admin` behavior.
3. Keep password, store, WAL, and Raft command ownership with identity service code if they represent identity state.
4. Keep daemon API/protobuf adapters in `internal/daemon/api`.
5. Update daemon app construction and service lookup.
6. Preserve existing service names if external status/health output depends on them.

### Tests

- Move/update user and admin module tests.
- Run auth/admin/user API tests.
- Run WAL and Raft tests for user/admin records.

### Acceptance

```sh
go test ./internal/identity/... ./internal/daemon/api/... ./internal/daemon/app
```

## Phase 6: Migrate blob and backup-related services

Status: implemented as a compatibility-wrapper phase. `internal/blob/service` and `internal/backup/service` now expose subsystem service packages and daemon app wiring constructs blob/backup services through those package paths. The implementations currently alias the existing daemon module implementations to keep behavior stable.

### Goals

Move blob service behavior into the blob subsystem and ensure backup uses common runtime/quiesce interfaces.

### Target packages

```text
internal/blob/service
internal/backup/service
```

### Tasks

1. Move blob metadata, WAL, Raft, replication, and backend payload provider logic into `internal/blob/service` where appropriate.
2. Keep low-level blob storage in `internal/blob/storage`.
3. Move backup daemon module behavior into `internal/backup/service` if not already done in the pilot phase.
4. Ensure quiesce dependencies import `internal/runtime/quiesce`.
5. Update daemon app and cluster backend wiring.

### Tests

- Blob service WAL/Raft tests.
- Backup policy/checkpoint/quiesce tests.
- Cluster backend blob payload provider tests if present.

### Acceptance

```sh
go test ./internal/blob/... ./internal/backup/... ./internal/daemon/app ./internal/daemon/api/...
```

## Phase 7: Migrate space service

Status: implemented as a compatibility-wrapper phase. `internal/space/service` now exposes the space subsystem runtime service, manager types, errors, and Raft state-machine alias. Daemon app, daemon API, and server wiring now import the space service package path while preserving the existing implementation.

### Goals

Move space application behavior into the space subsystem.

### Target package

```text
internal/space/service
```

### Tasks

1. Move space/domain/template/grant service behavior from `internal/daemon/modules/space`.
2. Keep model/storage/access packages under existing `internal/space` subpackages.
3. Move subsystem-specific WAL records and appliers with the service.
4. Move subsystem-specific Raft state machine and command handling with the service.
5. Keep low-level Raft group lifecycle in clustering/daemon infrastructure.
6. Update daemon app, API adapters, clustering backend readers, and tests.

### Tests

- Space service unit tests.
- WAL recovery tests.
- Raft read/write tests.
- API tests for spaces, domains, templates.

### Acceptance

```sh
go test ./internal/space/... ./internal/daemon/api/... ./internal/daemon/app
```

## Phase 8: Migrate graph service

Status: implemented as a compatibility-wrapper phase. `internal/graph/service` now exposes the graph subsystem runtime service, manager types, errors, and Raft state-machine alias. Daemon app, daemon API, and server wiring now import the graph service package path while preserving the existing implementation.

### Goals

Move graph application behavior into the graph subsystem after smaller subsystem patterns are proven.

### Target package

```text
internal/graph/service
```

### Tasks

1. Move graph transaction/node/edge service behavior from `internal/daemon/modules/graph`.
2. Keep graph model/storage/query packages in place.
3. Move graph-specific WAL records and appliers with the service.
4. Move graph-specific Raft state machine/read-routing behavior with the service.
5. Keep semantic dirty-event wiring as daemon composition root wiring unless a clean subsystem event bus abstraction exists.
6. Update blob, semantic, session, API, and cluster backend dependencies to use graph service interfaces.

### Tests

- Graph service tests.
- Graph WAL tests.
- Graph Raft tests.
- Graph API/session tests.
- Cross-subsystem graph-to-semantic dirty event tests.

### Acceptance

```sh
go test ./internal/graph/... ./internal/daemon/api/... ./internal/daemon/app
```

## Phase 9: Migrate semantic service

Status: implemented as a compatibility-wrapper phase. `internal/semantic/service` now exposes the semantic subsystem runtime service, manager types, maintenance/search input and result aliases, and Raft state-machine alias. Daemon app, daemon API, and server wiring now import the semantic service package path while preserving the existing implementation.

### Goals

Move semantic service behavior into the semantic subsystem.

### Target package

```text
internal/semantic/service
```

### Tasks

1. Move semantic manager behavior, maintenance loops, WAL wrappers, accounting, and Raft handling.
2. Decide whether semantic should expose multiple services, for example a foreground semantic manager plus a maintenance/indexing service with separate lifecycle and health.
3. Keep semantic model/storage/search/vectorstore packages in place.
4. Ensure lifecycle loops use common `runtime.Starter` and `runtime.Stopper`.
5. Preserve graph change sink behavior through injected interfaces or daemon wiring.
6. Update API adapters and daemon app wiring.

### Tests

- Semantic service tests.
- Semantic maintenance tests.
- Semantic WAL/Raft tests.
- API tests for semantic admin/client behavior.

### Acceptance

```sh
go test ./internal/semantic/... ./internal/daemon/api/... ./internal/daemon/app
```

## Phase 10: Clean up daemon modules compatibility packages

Status: partially implemented. The daemon quiesce compatibility bridge has been removed. The `internal/daemon/modules/*` implementation packages remain because current subsystem service packages are compatibility wrappers around those implementations; removing them requires physically moving each implementation into its subsystem service package.

### Goals

Remove temporary daemon module packages once all subsystem services have moved.

### Tasks

1. Remove or reduce `internal/daemon/modules/*` packages.
2. If any daemon-only service remains, move it to a clearly named daemon package such as `internal/daemon/services/<name>` or keep it under daemon with documentation explaining why.
3. Remove compatibility aliases for `internal/daemon/runtime` service interfaces if all code imports `internal/runtime`.
4. Remove compatibility aliases for `internal/daemon/quiesce` if all code imports `internal/runtime/quiesce`.
5. Update design docs and implementation plans to reflect final package locations.
6. Update package comments and import examples.

### Tests

- Full Go test suite.
- Build CLI/daemon targets.

### Acceptance

```sh
go test ./...
```

## Phase 11: Documentation and architecture audit

Status: partially implemented. This plan and the architecture docs have been updated for the compatibility-wrapper migration and quiesce bridge removal. A remaining audit should be performed after physical implementation moves remove subsystem-service dependencies on `internal/daemon/modules/*`.

### Goals

Ensure docs reflect the migrated architecture and there are no stale daemon-module assumptions.

### Tasks

1. Update `docs/README.md`.
2. Update or retire `docs/design/daemon-service-interfaces.md`.
3. Update implementation plans that reference `internal/daemon/modules` as the preferred home.
4. Add a package map showing subsystem/service/runtime/daemon boundaries.
5. Run import audits for forbidden dependency directions:
   - subsystem service packages importing `internal/daemon`
   - `internal/runtime` importing daemon or subsystem packages

### Tests/checks

Suggested checks:

```sh
rg 'internal/daemon' mycel/internal/graph mycel/internal/space mycel/internal/semantic mycel/internal/blob mycel/internal/identity mycel/internal/session mycel/internal/changestream mycel/internal/backup
rg 'internal/(graph|space|semantic|blob|identity)' mycel/internal/runtime
```

### Acceptance

```sh
go test ./...
```

## Migration notes

- Keep service names stable during migration to avoid changing status/health output unexpectedly.
- When splitting one subsystem into multiple services, choose stable names that include the subsystem prefix, such as `semantic`, `semantic-maintenance`, or `changestream-delivery`.
- Prefer compatibility aliases and wrappers over broad mechanical churn.
- Move one subsystem at a time.
- Do not move API adapters into subsystem service packages.
- Do not introduce a global runtime config struct.
- Keep daemon app initialization readable; it should become clearer as subsystem services own their own behavior.
