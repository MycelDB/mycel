# Subsystem Runtime Architecture

## Status

Proposed.

This document describes a packaging and runtime architecture for Mycel where graph, space, semantic, blob, identity, session, changestream, backup, and related areas are treated as **subsystems**. Each subsystem owns its domain behavior and may expose a service/manager implementation. A small common runtime package defines lifecycle, health, status, quiesce, and initialization contracts used by both subsystems and the daemon. The daemon remains the composition root: it loads daemon config, constructs shared infrastructure, wires subsystem services together, exposes APIs, and manages process lifecycle.

This direction evolves the existing daemon service interface work. The current code already has useful concepts in `internal/daemon/runtime` and `internal/daemon/quiesce`; the proposed change is to make the reusable parts common runtime concepts rather than daemon-owned concepts.

## Motivation

Mycel currently has top-level subsystem packages such as:

```text
internal/graph
internal/space
internal/semantic
internal/blob
internal/identity
internal/backup
internal/clustering
```

It also has daemon module packages with similar names:

```text
internal/daemon/modules/graph
internal/daemon/modules/space
internal/daemon/modules/semantic
internal/daemon/modules/blob
internal/daemon/modules/user
internal/daemon/modules/admin
internal/daemon/modules/session
internal/daemon/modules/changestream
internal/daemon/modules/backup
```

The daemon module packages mix several responsibilities:

- subsystem application behavior
- storage initialization
- WAL registration and application
- Raft command/state-machine handling
- quiesce admission/drain behavior
- lifecycle hooks and background loops
- API-facing manager methods
- cross-subsystem wiring

This makes ownership unclear. For example, `internal/graph` is the graph domain/library layer, while `internal/daemon/modules/graph` is the daemon-bound graph service. That distinction is valid, but the package layout makes graph behavior appear daemon-owned.

The preferred direction is:

```text
subsystem package     owns subsystem behavior
common runtime        owns shared lifecycle/quiesce/health contracts
myceld daemon         wires subsystems and exposes process/API boundaries
```

## Goals

- Treat graph, space, semantic, blob, identity, session, changestream, backup, and clustering as subsystems.
- Keep small subsystems valid. Session and changestream may be small today but should still have a clear subsystem home because they are expected to grow.
- Move shared runtime interfaces and reusable runtime primitives out of daemon-specific packages.
- Make quiescing a common runtime concept available to subsystem services.
- Keep the daemon as the composition root and process owner.
- Preserve functional behavior after every migration phase.
- Avoid forcing domain packages to import daemon packages.
- Avoid a large framework or one giant service interface.

## Non-goals

- Performing the package migration in one large change.
- Making subsystem services public API packages outside `internal`.
- Moving gRPC/protobuf API adapters into subsystem packages.
- Making all subsystems identical in shape.
- Introducing one global config object consumed by every subsystem.
- Hiding subsystem-specific behavior behind overly generic abstractions.

## Terminology

### Subsystem

A subsystem is a bounded area of Mycel behavior with its own model, storage, service logic, and optional runtime participation.

Examples:

```text
graph
space
semantic
blob
identity
session
changestream
backup
clustering
```

A subsystem can be small. Small size is not a reason to keep it daemon-bound if it represents an application concept that will evolve independently.

A subsystem may expose more than one runtime service. For example, a subsystem can have separate services for foreground API behavior, background maintenance, indexing, replication, or event delivery when those concerns have distinct lifecycles or dependencies. The unit of ownership is the subsystem; the unit of runtime orchestration is the service.

### Service / Manager

A service, often named `Manager` inside a subsystem, owns a coherent runtime/application responsibility for that subsystem. It may implement common runtime lifecycle interfaces and domain-specific manager interfaces consumed by API adapters or other subsystems.

Examples:

```text
internal/graph/service.Manager
internal/semantic/service.IndexManager
internal/semantic/service.MaintenanceService
internal/session/service.Manager
internal/graph/notification.Manager
```

The exact type name can vary, but new subsystem service packages should prefer clear names such as `Manager`, `Service`, `Indexer`, or `Scheduler` over daemon-oriented `Module`.

Do not force a subsystem into exactly one service. Use multiple services when the responsibilities have meaningfully different lifecycles, health/status reporting, dependencies, quiesce behavior, or startup ordering. Keep them together under the subsystem package when they share the same bounded context.

### Runtime

Runtime is the small shared contract layer for component lifecycle and operational coordination. It defines common interfaces and types such as service lifecycle, service state, health, status, initialization results, and quiesce gates/coordinators.

### Daemon

The daemon is the `myceld` process composition root. It owns process-level concerns:

- environment/config loading
- logging setup
- data directory creation
- WAL manager construction
- clustering manager construction
- Raft group lifecycle
- subsystem service construction and dependency injection
- gRPC server startup
- signal handling and shutdown

### Adapter

An adapter translates between a boundary and internal subsystem interfaces.

Examples:

- gRPC request/response adapters under `internal/daemon/api`
- protobuf/domain mappers
- Raft transport and backend adapters
- storage implementations

Adapters should not become the owner of subsystem business behavior.

## Target package shape

Long-term target:

```text
internal/runtime/
  service.go
  state.go
  health.go
  init.go
  registry.go
  quiesce/

internal/graph/
  model/
  storage/
  query/
  service/

internal/space/
  model/
  storage/
  access/
  service/

internal/semantic/
  model/
  storage/
  service/      # may contain multiple semantic services

internal/blob/
  storage/
  service/

internal/identity/
  model/
  storage/
  service/

internal/session/
  api/
  service/

internal/graph/
  notification/

internal/backup/
  service/

internal/daemon/
  app/
  api/
  auth/
  config/
  logging/
  server/
```

During migration, `internal/daemon/modules/*` may remain as compatibility packages or temporary homes. The goal is to move reusable subsystem services into the top-level subsystem packages over time.

## Common runtime package

The common runtime package should define small contracts and reusable primitives. It should be independent of daemon APIs and concrete subsystem implementations.

Candidate interfaces:

```go
package runtime

type Service interface {
    Name() string
    Init(context.Context, Host) InitResult
}

type Starter interface {
    Start(context.Context) error
}

type Stopper interface {
    Stop(context.Context) error
}

type StatusReporter interface {
    Status(context.Context) ServiceStatus
}

type HealthReporter interface {
    Health(context.Context) HealthStatus
}

type SnapshotReloadable interface {
    ReloadAfterSnapshot(context.Context) error
}
```

The current code uses `Init(context.Context, *Runtime)`. That works but gives every service access to the full daemon runtime. The target should be a smaller host/capability surface.

Possible host pattern:

```go
type Host interface {
    Logger() *slog.Logger
    DataDir() string
    Service(name string) (Service, bool)
}
```

Additional capabilities can be separate interfaces:

```go
type WALProvider interface { /* WAL handles needed by services */ }
type QuiesceRegistrar interface { RegisterQuiesceParticipant(quiesce.Participant) }
type ServiceRegistry interface { Service(name string) (Service, bool) }
```

Subsystems should depend only on the host capabilities they need.

## Quiesce as common runtime

Quiescing is a shared operational concept, not a daemon-only concept. The existing `internal/daemon/quiesce` package should become common runtime functionality, for example:

```text
internal/runtime/quiesce
```

Subsystem services can then embed or own a gate. If a subsystem exposes multiple services, each service may register its own participant, or the subsystem may provide a shared participant when quiesce semantics must be coordinated across those services:

```go
type Manager struct {
    gate *quiesce.Gate
}
```

A subsystem may implement quiesce directly or register a gate with the runtime host. The daemon coordinates quiesce across all registered participants, but the admission/drain behavior belongs to each subsystem.

## WAL and Raft placement

WAL and Raft split into shared infrastructure and subsystem-specific behavior.

Shared infrastructure remains in existing packages such as:

```text
internal/wal
internal/clustering/consensus
internal/clustering/routing
```

Subsystem-specific record formats, appliers, and state machines should live with the subsystem service that owns the data:

```text
internal/graph/service/wal.go
internal/graph/service/raft.go
internal/space/service/wal.go
internal/space/service/raft.go
```

The daemon should provide WAL managers, recovery, Raft groups, and transports. Subsystems should own how their mutations are serialized, applied, and proposed.

## Dependency direction

Preferred dependency direction:

```text
internal/runtime
    ↑
internal/{subsystem}/service
    ↑
internal/daemon/app and internal/daemon/api
```

Avoid:

```text
internal/{subsystem}/service -> internal/daemon/...
internal/runtime -> internal/daemon/...
```

The daemon can import everything because it is the composition root. Subsystems should not import daemon packages.

## Configuration approach

Avoid one global common `runtime.Config` struct. It will likely become a dumping ground.

Prefer subsystem-owned config structs:

```go
graphservice.Config
spaceservice.Config
semanticservice.Config
blobservice.Config
```

Daemon config can translate environment-level settings into subsystem configs:

```go
graphSvc := graphservice.New(graphservice.Config{
    DataDir: filepath.Join(cfg.DataDir, "graph"),
    Gate: quiesce.NewGate("graph"),
})
```

Common config primitives are fine, but ownership of subsystem configuration should remain with the subsystem.

## API adapters

Transport/API code should stay daemon-owned:

```text
internal/daemon/api/admin
internal/daemon/api/client
internal/daemon/server
```

These packages should depend on domain-specific interfaces exposed by subsystem services. They should not contain subsystem business behavior.

## Cases where this approach makes less sense

### Pure daemon process code

Packages whose only purpose is process setup, server startup, signal handling, environment parsing, or daemon logging should remain under `internal/daemon`.

### Transport adapters

gRPC/protobuf adapters should stay in `internal/daemon/api` or similar adapter packages. Moving them into subsystem services would couple domain behavior to transport protocols.

### Infrastructure libraries

Packages such as `internal/wal`, `internal/filestore`, and low-level `internal/clustering/consensus` are infrastructure. They should not be forced into subsystem service form unless they grow explicit lifecycle/application behavior.

### Very generic runtime config

A single shared config struct is not recommended. It risks coupling unrelated subsystems and making runtime a second daemon package.

### Low-level Raft mechanics

Subsystems should own state machines and command payload semantics, but not low-level Raft group lifecycle. Raft group construction and transport remain daemon/clustering infrastructure concerns.

### Shared runtime bloat

`internal/runtime` should not import daemon packages, generated API packages, or subsystem packages. If it starts knowing about graph, space, semantic, or gRPC, it has become too powerful.

## Relationship to existing docs

This design supersedes the daemon-only package-placement assumption in `design/daemon-service-interfaces.md` while preserving the useful service lifecycle concepts described there. Future updates should reconcile that document with this subsystem-oriented architecture.
