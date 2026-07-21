# Subsystem Runtime Package Map

## Status

Living audit note.

This package map reflects the current subsystem/runtime migration state after the physical service move and runtime-host initialization migration.

## Target ownership

```text
internal/runtime                 shared lifecycle, status, health, init, host contracts
internal/runtime/quiesce         shared quiesce coordinator, gates, leases, status DTOs

internal/{subsystem}/service     subsystem runtime service package
internal/daemon/app              composition root
internal/daemon/api              transport/API adapters
internal/daemon/server           process server wiring
```

Current subsystem service package paths:

```text
internal/backup/service
internal/blob/service
internal/changestream/service
internal/graph/service
internal/identity/service
internal/identity/service/admin
internal/identity/service/user
internal/semantic/service
internal/session/service
internal/space/service
```

## Completed cleanup

The old daemon module implementation tree has been removed:

```text
internal/daemon/modules/*
```

Subsystem service implementations now live under their subsystem package paths.

The old daemon quiesce package has also been removed. Quiesce now lives under:

```text
internal/runtime/quiesce
```

Subsystem service implementations now initialize through common runtime host/capability interfaces instead of the concrete daemon runtime:

```go
Init(context.Context, runtime.Host) runtime.InitResult
```

The daemon runtime remains the concrete composition-root runtime and implements the common host/capability interfaces needed by subsystem services.

## Current residual daemon dependencies

Subsystem service packages, including tests, should not import:

```text
internal/daemon/runtime
internal/daemon/config
```

Subsystem service tests use lightweight fixtures from `internal/runtime/runtimetest` instead of concrete daemon runtime/config fixtures.

## Runtime dependency state

`internal/runtime` remains independent of daemon and subsystem packages. Current audits should report no daemon/subsystem imports from `internal/runtime`.

## Audit commands

```sh
# Subsystem services and tests should not import daemon runtime/config.
rg 'internal/daemon/(runtime|config)' internal/{backup,blob,changestream,graph,identity,semantic,session,space}/service --glob '*.go'

# Runtime must not import daemon or subsystem packages.
rg 'internal/(daemon|backup|blob|changestream|graph|identity|semantic|session|space)' internal/runtime --glob '*.go'

# Removed daemon module/quiesce package imports should not exist in Go source.
rg 'internal/daemon/(modules|quiesce)' internal --glob '*.go'
```
