# Daemon-Only Boundary

## Status

Daemon-only boundary and phase tracking for removing embedded/library runtime support from Mycel v2.

Mycel is now intended to run as a daemon-owned database process. Applications and tools should communicate with `myceld` through the Admin and Client gRPC APIs. The historical `engine` tree and public `session` packages have been removed; remaining file-session code is internal daemon implementation scaffolding.

## Supported public surfaces

The daemon-only target supports application access through:

- `github.com/myceldb/mycel-api/api/proto/**`: language-independent protobuf service definitions for Admin and Client APIs.
- `github.com/myceldb/mycel-go-sdk`: Go client helpers that generate and wrap Go daemon gRPC clients.

This module supports binaries, not an embedded application library:

- `cmd/myceld`: the daemon process and sole owner of the data directory.
- `cmd/mycel`: operator/user CLI that connects to `myceld` over gRPC.

The former public `domain/**`, `query`, and `store/**` implementation packages have moved under `internal/`; see [Internalize Mycel Implementation Packages Plan](../../implementation/v0.2/internalize-implementation-packages-plan.md). Further internal namespace cleanup is tracked in [Internal Bounded-Context Package Plan](../../implementation/v0.2/internal-bounded-context-package-plan.md).

Daemon service lifecycle/capability interfaces are specified in [Daemon Service Interfaces Design](daemon-service-interfaces.md), with phased delivery tracked in [Daemon Service Interfaces Implementation Plan](../../implementation/v0.3/daemon-service-interfaces-implementation-plan.md). Daemon-owned backup and service quiescing are specified in [Quiesce and Backup Design](../backup-restore/quiesce-and-backup.md), with phased delivery tracked in [Quiesce and Backup Implementation Plan](../../implementation/v0.2/quiesce-and-backup-implementation-plan.md).

## Go package boundary

`github.com/myceldb/mycel` is not an application library. Its importable root package is documentation-only; implementation code is internal to this module.

| Path shape | Status | Notes |
|---|---|---|
| `github.com/myceldb/mycel` | Documentation-only | No runtime constructors or application DTO contracts. |
| `github.com/myceldb/mycel/cmd/...` | Binary entrypoints | Not application imports. |
| `github.com/myceldb/mycel/internal/...` | Daemon implementation | Protected by Go `internal/` visibility. |
| `github.com/myceldb/mycel/domain/...` | Removed/internalized | Use `mycel-api` protobuf messages or SDK types. |
| `github.com/myceldb/mycel/store/...` | Removed/internalized | Daemon persistence implementation only. |
| `github.com/myceldb/mycel/query` | Removed/internalized | Use daemon Query API. |
| `github.com/myceldb/mycel/engine` | Removed | No embedded engine runtime. |
| `github.com/myceldb/mycel/session` | Removed/internalized | Use daemon Session/Graph APIs. |

Enforcement:

```sh
scripts/check-public-surface.sh --workspace /Users/martinbeauvais/Projects/knotbase/Knotbase --strict
```

## Unsupported runtime surfaces

These surfaces are removed, deprecated, or scheduled for removal/internalization:

- `engine.NewEngine`, `engine.Engine`, and the legacy `engine/` tree are removed.
- `session.NewSession`, `session.NewSessionWithStore`, `session/api`, and direct public file-backed graph sessions are removed/internalized.
- Public `domain/**`, `store/**`, and `query` packages as application extension points or DTO contracts. These packages have been internalized.
- CLI paths that open local stores or authenticate against local engine state.
- `MYCELDB_*` embedded runtime configuration as a supported operator interface.

Daemon internals may continue using low-level storage/session packages under `internal/**`. The boundary is not “no reusable code”; it is “no application-owned Mycel runtime.”

## Ownership model

```text
Applications / tools
        |
        | gRPC Admin or Client API
        v
+-----------------------------+
| myceld                      |
|                             |
|  Auth / authorization       |
|  Sessions / transactions    |
|  Graph / blob / query APIs  |
|  Admin APIs                 |
|  Storage modules            |
+-----------------------------+
        |
        v
   MYCELD_DATA_DIR
```

Only `myceld` should read or mutate the Mycel data directory. CLI commands and application code should not instantiate an engine or file session in-process.

This ownership rule includes backups. Operators should use `mycel.admin.v1.AdminBackupService`, SDK helpers that call that service, or `mycel admin backup ...`; applications must not zip or copy `MYCELD_DATA_DIR` directly while the daemon is live. The daemon quiesces work before snapshotting and returns transient `codes.Unavailable` for new non-exempt RPCs, including reads unless explicitly exempted/proven safe, during backup.

## Refactor phases after this boundary

1. **CLI daemon-only cleanup**: remove `EnsureEngine`, embedded auth helpers, and remaining local command paths. Initial cleanup is implemented: the `mycel` binary no longer depends on `engine` or `session` runtime packages, top-level node commands route to daemon graph commands, and embedded-only init/ACL/accounting/legacy embeddings paths now fail with daemon-only guidance.
2. **Engine removal**: implemented. The public wrapper and remaining `engine/internal` legacy scaffold were deleted; daemon modules own runtime behavior directly.
3. **Session public package internalization**: implemented. `session/api` moved to `internal/session/api`; public `session` constructors and type aliases were removed; daemon internals call `internal/graph/filesession` directly.
4. **Config cleanup**: implemented. Active CLI config no longer reads embedded `MYCELDB_*` settings or exposes local runtime flags (`--data-dir`, auth TTLs, blob limits, semantic toggles). The CLI uses `MYCEL_CONFIG` for optional CLI config files and `MYCELD_*` for daemon connection/TLS settings.
5. **Docs and enforcement**: implemented. `scripts/check-daemon-only.sh` is wired into `make test` and `make build`; it fails if the legacy `engine` tree, public `session` package, public embedded imports, legacy `MYCELDB_*` code references, or removed embedded CLI flags are reintroduced.
6. **Public Go implementation surface internalization**: implemented. `scripts/check-public-surface.sh` is wired into `make test` and `make build`; it fails if public `domain/**`, `store/**`, `query`, or other top-level implementation packages are reintroduced.

## Migration rule

When a feature still needs embedded code to function, treat that as technical debt to close by adding or extending daemon APIs. Do not add new public embedded APIs while this refactor is active.
