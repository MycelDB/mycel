# Mycel Daemon Migration Design

## Status

Draft design notes for the `refactor_daemon` branch.

## Summary

Mycel has been refactored from an embedded Go library into a daemon process that can run standalone or as part of a mesh of Mycel daemons. Applications and tools should use daemon Admin/Client gRPC APIs; the public embedded engine runtime is removed, and session/storage/domain/query implementation packages now live under `internal/`. Existing domain, storage, session, metadata, semantic, and auth components remain reusable only inside the daemon implementation while the migration continues to add daemon APIs, connector libraries, connection management, space caching, and eventually replication.

The migration is no longer preserving embedded/library runtime support as a public compatibility target. Each phase should keep daemon/API behavior functional while removing local application-owned runtime entrypoints.

Current implementation status on `refactor_daemon` includes daemon initialization, authenticated Admin Auth/Operator/User APIs, CLI admin/user-management commands over daemon gRPC, the daemon-backed Client AuthService for standard users, daemon-backed Client/Admin space APIs, daemon-backed Client DomainService, daemon-backed Client TemplateService, daemon-backed Client Session/Transaction lifecycle services, the daemon-backed Client GraphService MVP, the daemon-backed Client BlobService MVP, the daemon-backed Client QueryService MVP, the daemon-backed Client ImportExportService with structured graph/template/blob and replace-domain support, the daemon-backed Client MetadataCatalogService MVP, the daemon-backed Client SemanticService MVP, the daemon-backed GraphChangeService with durable replay and graph-change payloads, the daemon-backed AdminDomainService lookup MVP, the daemon-backed AdminSemanticService index configuration/delete MVP, the daemon-backed AdminSemanticMaintenanceService backfill/maintenance MVP, the daemon-backed AdminSemanticMigrationService legacy embedding migration MVP, the daemon-backed AdminInferenceService package/catalog/credentials/grants/policies/soft-lifecycle plus reference-safe hard-delete MVP, and TLS/mTLS transport hardening for daemon gRPC. See:

- [gRPC Admin Auth API](../admin/grpc-admin-auth.md)
- [gRPC Admin List](../admin/grpc-admin-list.md)
- [Admin Domain API](../admin/domain.md)
- [Admin Semantic API](../admin/semantic.md)
- [Admin Inference API](../admin/inference.md)
- [Admin Semantic Maintenance API](../admin/semantic-maintenance.md)
- [Admin Semantic Migration API](../admin/semantic-migration.md)
- [gRPC Client Auth API](../identity/grpc-client-auth.md)
- [Client Space API](../api/space.md)
- [Client Domain API](../api/domain.md)
- [Client Template API](../api/template.md)
- [Client Session and Transaction API](../api/session-transaction.md)
- [Client Graph API](../api/graph.md)
- [Client Blob API](../api/blob.md)
- [Client Query API](../api/query.md)
- [Client Import/Export API](../api/import-export.md)
- [Client Metadata Catalog API](../api/metadata-catalog.md)
- [Client Semantic API](../api/semantic.md)
- [Graph Change Watch API](../api/change-stream.md)
- [Daemon-only boundary](daemon-only-boundary.md)

## Goals

- Remove embedded/library runtime behavior as a supported public interface.
- Introduce and harden a long-running Mycel daemon process.
- Support standalone daemon deployments first.
- Prepare for mesh deployments and graph replication between daemons.
- Define separate API surfaces for client, admin, and private daemon-to-daemon operations.
- Introduce connectors/drivers that help applications use the client API efficiently.
- Improve performance with daemon-side connection management and space/session caching.
- Keep Mycel usable by higher-level systems such as Knot PKM throughout the migration.

## Non-goals for the initial migration

- Do not implement mesh replication before single-daemon mode is stable.
- Do not force browser clients to use raw gRPC directly.
- Do not preserve embedded/library runtime support as a long-term public API; it is deprecated and scheduled for removal/internalization after daemon API parity.

## Target Architecture

```text
Applications / Connectors
        |
        | Client API
        v
+-----------------------------+
| Mycel Daemon                |
|                             |
|  Client API                 |
|  Admin API                  |
|  Private Mesh API           |
|                             |
|  Connection Manager         |
|  Space / Session Cache      |
|  Background Workers         |
|  Internal Runtime Modules   |
|  Internal Storage Components|
+-----------------------------+
        |
        v
   Local Data Directory
```

A daemon may run in one of two broad modes:

- **Standalone**: all data and operations are local to one daemon.
- **Mesh member**: the daemon participates in a trusted mesh and replicates graph state with peers.

## API Surfaces

### Client API

The client API is the standard application-facing interface. It is used by applications and client connectors to manipulate graphs within authorized spaces.

Responsibilities include:

- authentication and session/token handling
- space access checks
- graph/node/edge operations
- transactions or session-like write scopes
- queries and traversal
- metadata operations
- semantic search and semantic index operations where appropriate
- subscriptions/change streams when available

### Admin API

The admin API is operator-facing and should be separated from normal graph/client operations.

Responsibilities include:

- user management
- space management
- access control management
- disk/storage diagnostics
- network configuration
- daemon configuration
- mesh membership management
- backups/import/export
- maintenance jobs
- session cleanup and revocation
- health, readiness, and diagnostics

### Private Mesh API

The private API is daemon-to-daemon only. It is not exposed to normal applications.

Responsibilities may include:

- daemon identity and trust establishment
- peer discovery and membership exchange
- graph replication
- snapshot transfer
- delta/oplog transfer
- conflict detection/resolution
- health and liveness between peers

This API should use stronger daemon identity controls than normal client APIs, likely mTLS or equivalent daemon credentials. Single-daemon gRPC already supports TLS and optional client-certificate verification; future mesh APIs should require daemon identity and mTLS by default.

## Transport Security

Daemon gRPC defaults to plaintext loopback for local development compatibility. Operators can enable TLS by setting both:

```sh
MYCELD_TLS_CERT_FILE=/path/to/server.pem
MYCELD_TLS_KEY_FILE=/path/to/server-key.pem
```

Optional mTLS is enabled with:

```sh
MYCELD_TLS_CLIENT_CA_FILE=/path/to/client-ca.pem
MYCELD_TLS_REQUIRE_CLIENT_CERT=true
```

CLI clients use plaintext by default, or TLS when `--daemon-tls` / `MYCELD_TLS=true` is set. TLS material can be provided with:

```sh
--daemon-tls-ca /path/to/ca.pem
--daemon-tls-server-name localhost
--daemon-tls-client-cert /path/to/client.pem
--daemon-tls-client-key /path/to/client-key.pem
```

`--daemon-tls-insecure-skip-verify` exists only for local testing and should not be used in production. Bearer-token authentication and authorization remain required by Admin and Client APIs even when mTLS is enabled.

## Protocol Direction

The internal/private daemon APIs should use gRPC or an equivalent protobuf-first RPC model.

For public APIs, Mycel should prefer a protobuf-defined API with multiple transports where practical:

- gRPC for service/backend connectors
- Connect or gRPC-Web for browser-compatible generated clients
- JSON/HTTP gateway for debugging, scripting, and broad integration

The recommended source of truth is protobuf service definitions, with generated Go and TypeScript clients/connectors where possible.

## Connectors / Drivers

Connectors are libraries that help applications use the Client API efficiently and idiomatically. They are analogous to SQL drivers.

Examples:

- Go connector
- TypeScript connector
- Python connector
- future language-specific connectors

Responsibilities may include:

- connection pooling
- persistent connections
- authentication and token refresh
- request batching
- retries and backoff
- deadline/cancellation propagation
- local caching where safe
- transactions/session helpers
- query builders
- graph traversal helpers
- streaming result handling
- subscriptions/change notifications
- compression/protocol negotiation

Future connectors should wrap daemon Client/Admin APIs rather than reintroducing an in-process embedded runtime.

## Reusable Existing Components

The daemon should reuse existing Mycel components wherever possible:

- domain models
- daemon module operations
- stores
- graph storage
- internal session abstractions
- metadata indexes
- semantic indexes and maintenance
- auth/session primitives
- CLI logic where appropriate

The main new layer is a daemon runtime around these components, not a rewrite of the storage and graph model.

## New Daemon Runtime Components

### API service layer

Maps network requests to daemon module, internal session, and store operations. Handles validation, authentication, authorization, request limits, and error translation.

### Connection manager

Tracks client, admin, and daemon-to-daemon connections. Handles identity, deadlines, cancellation, heartbeats, rate limits, quotas, and backpressure.

### Space/session cache manager

Keeps frequently used spaces and sessions open for performance. Responsibilities may include:

- open/close lifecycle
- LRU or TTL eviction
- per-space concurrency control
- active reader/writer tracking
- warm metadata/semantic indexes
- dirty-state tracking
- future routing of space ownership/replica reads

### Background workers

Daemon-owned background jobs should eventually include:

- semantic maintenance
- session cleanup
- compaction
- replication sync
- health checks
- accounting rollups
- maintenance scheduling

### Observability

The daemon should provide:

- structured logs
- metrics
- tracing hooks
- health/readiness endpoints
- admin diagnostics

### Resource governance

The daemon should enforce:

- max request size
- query cost limits
- per-user/app quotas
- disk limits
- memory limits
- backpressure under load

## Current Migration Plan

The migration now targets daemon-only behavior rather than embedded/daemon dual mode. See [Daemon-only boundary](daemon-only-boundary.md) for active phase tracking.

Completed milestones include daemon process/runtime setup, authenticated Admin and Client gRPC APIs, daemon-backed CLI commands, TLS/mTLS transport hardening, removal of CLI embedded-engine paths, deletion of the legacy `engine` tree, removal/internalization of the public `session` package, and internalization of `domain`, `store`, and `query` implementation packages.

### Space/session cache manager

Once network mode works, improve daemon performance.

- Cache opened spaces.
- Cache sessions or session-like write contexts where safe.
- Add concurrency controls.
- Add cache metrics and diagnostics.
- Add eviction policies.

### Phase 8: Expand Admin API

Move operational concerns into daemon admin APIs.

- users
- spaces
- access control
- sessions
- storage diagnostics
- semantic maintenance
- backups/import/export
- config/mesh readiness

Adapt CLI commands to call the Admin API where appropriate; embedded CLI paths are not preserved.

### Phase 9: Move background work into daemon

Daemon becomes responsible for scheduled maintenance.

- semantic maintenance
- cleanup
- compaction
- accounting rollups
- recurring diagnostics

Applications should stop triggering daemon-owned maintenance directly once this phase is complete.

### Phase 10: Add private mesh API and replication

Only after single-daemon mode is stable.

- daemon identity
- peer discovery
- mesh membership
- snapshots
- delta/oplog replication
- conflict strategy
- consistency model

### Phase 11: Flip default mode

When daemon mode is stable and consumers are ready:

```text
default: daemon
fallback: none
```

Embedded mode is no longer a supported application runtime.

## Compatibility Principles

- Every phase must leave the repository buildable and tests runnable.
- Embedded runtime entrypoints should not be reintroduced.
- Protocol definitions should be versioned.
- Data format compatibility should be preserved wherever possible.
- Migration should prioritize daemon API/module tests over embedded parity tests.

## Open Questions

- Should protobuf definitions live in this repository or a separate API package?
- Should Connect be the default public transport for browser-compatible clients?
- What is the exact consistency model for replicated graphs?
- What is the daemon identity model for private mesh communication?
- Which operations belong in Client API vs Admin API when they affect both user data and daemon operations?
- How much local caching should connectors perform versus leaving caching entirely daemon-side?
- Which additional daemon Admin APIs are needed before remaining internal scaffolding can be deleted?
