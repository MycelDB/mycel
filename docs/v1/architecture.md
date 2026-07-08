# MycelDB architecture (v1 historical)

This page describes the historical embedded-library architecture. It is retained
for context only.

Current MycelDB is daemon-only:

- `myceld` owns the data directory and runtime.
- Applications and tools use Admin/Client gRPC APIs from `mycel-api`, preferably
  through `mycel-go-sdk`.
- The old public Go implementation packages (`engine`, `session`, `domain/*`,
  `store/*`, and `query`) have been removed or moved under `internal/`.
- The `mycel` module now contains daemon/CLI binaries plus internal
  implementation packages; it is not an embeddable application library.

For the active boundary, see:

- [Daemon-only boundary](../v2/design/daemon-only-boundary.md)
- [Internalize Mycel Implementation Packages Plan](../v2/design/internalize-implementation-packages-plan.md)
- [Public Go Surface Audit](../v2/design/public-surface-audit.md)

## Current application-facing contracts

Use these from applications:

```text
github.com/myceldb/mycel-go-sdk
github.com/myceldb/mycel-api/api/proto/... (for SDK/code generation)
```

Do not import these from applications:

```text
github.com/myceldb/mycel/engine
github.com/myceldb/mycel/session
github.com/myceldb/mycel/domain/...
github.com/myceldb/mycel/store/...
github.com/myceldb/mycel/query
```

## Current module shape

```text
mycel/
  cmd/myceld/        daemon entrypoint
  cmd/mycel/         CLI client for daemon APIs
  internal/daemon/   daemon runtime, modules, and gRPC adapters
  internal/domain/   in-process daemon domain records
  internal/store/    daemon persistence managers
  internal/graph/query/    daemon/session query implementation
  internal/session/  internal session API and file-session implementation
```

The v1 embedded flow (`engine.NewEngine(...)`, `OpenSession`, direct graph
session operations) is no longer supported as a public API. Add or extend daemon
Admin/Client APIs when application functionality needs new access.
