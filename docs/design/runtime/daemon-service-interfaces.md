# Daemon Service Interfaces Design

## Status

Proposed.

This document defines daemon service lifecycle and capability interfaces for Mycel. These interfaces are cross-cutting daemon concerns used by graph, blob, semantic, backup, identity, space, and other daemon services. The current `Module` concept should become `Service` as the canonical runtime abstraction, with a staged compatibility window.

> Note: this document reflects the earlier daemon-centered service-interface design. The current target architecture is described in [Subsystem Runtime Architecture](subsystem-runtime-architecture.md), which preserves the service lifecycle concepts here but moves reusable runtime contracts and quiesce primitives toward common `internal/runtime` packages so top-level subsystems do not depend on daemon packages.

## Motivation

Mycel already has a module concept:

```go
type Module interface {
    Name() string
    Init(context.Context, *Runtime) InitResult
}
```

As daemon behavior grows, these runtime components need to participate in additional cross-cutting mechanisms:

- lifecycle start/stop
- background scheduler ownership
- quiesce/drain for backup
- health/status reporting
- graceful shutdown

These concerns should be explicit and centrally orchestrated by the daemon runtime instead of being handled ad hoc in each module.

## Goals

- Make `Service` the canonical daemon runtime abstraction.
- Keep daemon service lifecycle simple and explicit.
- Support optional capabilities without forcing no-op methods on every service.
- Give the runtime one place to initialize, start, stop, quiesce, and inspect services.
- Support backup/quiescence as a first-class daemon capability.
- Avoid introducing a large framework or dependency-heavy abstractions.

## Non-goals

- Renaming every existing package path immediately.
- Moving daemon service packages out of `internal/daemon/modules` in the first implementation.
- Exposing service interfaces as public application APIs.
- Replacing domain-specific managers or storage interfaces.

## Terminology

### Service

A daemon service is a runtime-owned component with a stable name and initialization step. Existing daemon modules are services; `Module` should be treated as historical naming during migration.

Examples:

```text
graph
blob
semantic
space
user
admin
session
change-stream
backup
```

### Capability interface

A capability interface is an optional behavior implemented only by services that need it.

Examples:

```text
Starter
Stopper
QuiesceParticipant
StatusReporter
HealthReporter
```

## Package placement

Cross-cutting daemon concerns should live under `internal/daemon/` because they are part of daemon orchestration, not graph/semantic/blob domain logic.

Recommended placement:

```text
internal/daemon/runtime/
  runtime.go          # Runtime, Service registration, orchestration
  service.go          # lifecycle/capability interfaces

internal/daemon/quiesce/
  gate.go             # reusable gate participant
  coordinator.go      # quiesce participant orchestration
  status.go           # status DTOs

internal/daemon/modules/backup/
  module.go           # backup daemon service, scheduler lifecycle
  types.go

internal/backup/
  manager.go          # backup archive/policy implementation
  snapshot.go
  retention.go
```

Rationale:

- `internal/daemon/runtime` owns service lifecycle contracts.
- `internal/daemon/quiesce` owns quiesce-specific types and gates.
- `internal/daemon/modules/backup` wires backup into the daemon lifecycle.
- `internal/backup` owns backup mechanics that are not gRPC/runtime-specific.

Avoid placing these under a top-level generic `internal/service` package. The behavior is daemon runtime behavior and should remain scoped to `myceld`.

The existing service implementation package paths under `internal/daemon/modules/...` are intentionally retained for now. The runtime abstraction is `Service`; renaming directories to `internal/daemon/services/...` is deferred because it is not required for quiesce/backup and would add broad mechanical churn.

## Core lifecycle interfaces

Replace the existing module interface with the base service interface:

```go
type Service interface {
    Name() string
    Init(context.Context, *Runtime) InitResult
}
```

A temporary `Module = Service` alias can be used during migration, but should be removed once runtime/app/API wiring no longer references it. Service implementation structs and package paths may still use `Module` names until a later package cleanup.

Optional lifecycle capabilities:

```go
type Starter interface {
    Start(context.Context) error
}

type Stopper interface {
    Stop(context.Context) error
}
```

Optional status capabilities:

```go
type StatusReporter interface {
    Status(context.Context) ServiceStatus
}

type HealthReporter interface {
    Health(context.Context) HealthStatus
}
```

Quiesce-specific capability remains in the quiesce package:

```go
package quiesce

type Participant interface {
    Name() string
    Quiesce(context.Context, Request) (Lease, error)
    Status() ParticipantStatus
}
```

A service can implement `quiesce.Participant` directly or register a gate/custom participant during initialization.

## Why small optional interfaces

Avoid one large interface like:

```go
type Service interface {
    Name()
    Init()
    Start()
    Stop()
    Quiesce()
    Health()
    Status()
}
```

That would force passive modules to implement no-op methods and would blur unrelated concerns. Small optional interfaces let the runtime detect capabilities:

```go
if starter, ok := svc.(runtime.Starter); ok {
    err := starter.Start(ctx)
}
```

## Runtime orchestration

The runtime should own service ordering and lifecycle.

Recommended startup order:

```text
1. construct runtime and cross-cutting coordinators
2. initialize services in declared dependency order
3. register quiesce participants during or immediately after init
4. start services implementing Starter
5. serve gRPC APIs
```

Recommended shutdown order:

```text
1. stop accepting new daemon API requests
2. stop services implementing Stopper in reverse start order
3. close runtime resources
```

Quiesce registration can be explicit in each service:

```go
func (m *Module) Init(ctx context.Context, rt *runtime.Runtime) runtime.InitResult {
    m.gate = quiesce.NewGate("graph")
    rt.Quiesce.Register(m.gate)
    return runtime.InitResult{Module: m}
}
```

Or runtime can auto-register services that implement `quiesce.Participant`:

```go
if p, ok := svc.(quiesce.Participant); ok {
    rt.Quiesce.Register(p)
}
```

Prefer explicit registration when a module needs to register a custom participant or multiple participants.

## Service status

Status should be intentionally non-sensitive.

Example:

```go
type ServiceStatus struct {
    Name      string
    State     string
    Started   bool
    StartedAt time.Time
    LastError string
}
```

Health can be added later if needed:

```go
type HealthStatus struct {
    Name    string
    Healthy bool
    Reason  string
}
```

Do not expose credential secrets, raw provider payloads, raw source text, embedding vectors, or full provider responses through status APIs.

## Relationship to quiesce and backup

Service lifecycle and quiesce are related but separate:

- lifecycle interfaces answer: how does a daemon component start and stop?
- quiesce interfaces answer: how does a component stop admitting work and drain active operations temporarily?

Backup depends on quiesce but should not own service lifecycle.

```text
Runtime
  owns services and lifecycle

QuiesceCoordinator
  owns temporary drain/admission control

Backup module
  uses QuiesceCoordinator before snapshotting data
```

## Compatibility with existing modules

Current modules can continue to satisfy the base service interface without changes beyond type naming/docs.

Passive modules may only implement:

```go
Name()
Init()
```

Background modules implement:

```go
Name()
Init()
Start()
Stop()
```

Backup-aware modules either:

- register a `quiesce.Gate`, or
- implement/register a custom `quiesce.Participant`.

## Dependency rules

- `internal/daemon/runtime` should avoid importing bounded-context packages.
- `internal/daemon/runtime` may depend on standard library and small daemon runtime DTOs.
- `internal/daemon/quiesce` should avoid importing daemon modules.
- daemon modules may import runtime and quiesce packages.
- backup implementation may import quiesce through daemon wiring, but snapshot/copy helpers should remain mostly independent.

## Future extensions

These interfaces leave room for:

- health aggregation Admin APIs
- metrics registration
- readiness/liveness probes
- maintenance-mode APIs
- coordinated online restore
- dependency-aware service startup

Do not add those until required.
