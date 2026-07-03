# Mycel Daemon Migration Design

## Status

Draft design notes for the `refactor_daemon` branch.

## Summary

Mycel is currently primarily consumed as an embedded Go library. The long-term direction is for Mycel to become a daemon process that can run standalone or as part of a mesh of Mycel daemons. Existing engine, domain, storage, session, metadata, semantic, and auth components should remain reusable; the migration adds a daemon runtime, public APIs, connector libraries, connection management, space caching, and eventually replication.

The migration should be gradual. Each phase should leave the existing embedded/library use case functional while introducing daemon capabilities behind stable interfaces.

## Goals

- Preserve the existing embedded/library behavior during migration.
- Introduce a long-running Mycel daemon process.
- Support standalone daemon deployments first.
- Prepare for mesh deployments and graph replication between daemons.
- Define separate API surfaces for client, admin, and private daemon-to-daemon operations.
- Introduce connectors/drivers that help applications use the client API efficiently.
- Improve performance with daemon-side connection management and space/session caching.
- Keep Mycel usable by higher-level systems such as Knot PKM throughout the migration.

## Non-goals for the initial migration

- Do not require all consumers to switch to daemon mode immediately.
- Do not implement mesh replication before single-daemon mode is stable.
- Do not remove embedded mode during the migration.
- Do not force browser clients to use raw gRPC directly.

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
|  Existing Engine Components |
|  Existing Storage Components|
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

This API should use stronger daemon identity controls than normal client APIs, likely mTLS or equivalent daemon credentials.

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

A key migration mechanism is to define a connector abstraction with at least two implementations:

```text
EmbeddedConnector -> existing in-process engine
NetworkConnector  -> daemon Client API
```

This lets consumers move behind the connector abstraction before daemon mode is mandatory.

## Reusable Existing Components

The daemon should reuse existing Mycel components wherever possible:

- domain models
- engine operations
- stores
- graph storage
- session abstractions
- metadata indexes
- semantic indexes and maintenance
- auth/session primitives
- CLI logic where appropriate

The main new layer is a daemon runtime around these components, not a rewrite of the storage and graph model.

## New Daemon Runtime Components

### API service layer

Maps network requests to engine/session/store operations. Handles validation, authentication, authorization, request limits, and error translation.

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

## Gradual Migration Plan

### Phase 1: Design and API boundaries

No behavior change.

- Document daemon architecture.
- Define client, admin, and private API surfaces.
- Define connector model.
- Define migration strategy.

Existing state remains:

```text
Application -> embedded Mycel engine -> storage
```

### Phase 2: Introduce service interfaces

No daemon required yet.

- Extract client-facing engine operations behind service interfaces.
- Extract admin/system operations behind service interfaces.
- Keep current embedded API working.
- Route existing Go calls directly to the same underlying engine implementation.

### Phase 3: Add in-process connector

Still no network dependency.

- Define a connector interface.
- Implement `EmbeddedConnector` over the existing engine.
- Start adapting tests and selected consumers to use the connector abstraction.

Target shape:

```text
Application -> Connector interface -> EmbeddedConnector -> engine -> storage
```

### Phase 4: Add daemon process shell

Introduce a daemon command without requiring consumers to use it.

- Add `mycel daemon` command.
- Load config and data directory.
- Initialize engine and stores.
- Expose health/readiness and minimal diagnostics.
- Keep embedded mode unchanged.

### Phase 5: Add network Client API

Expose core graph/space operations over the network.

- Define protobuf service contracts.
- Implement server handlers over the service interfaces.
- Implement generated or hand-written `NetworkConnector`.
- Add behavior parity tests comparing `EmbeddedConnector` and `NetworkConnector`.

Target shape:

```text
Application -> NetworkConnector -> Mycel daemon -> engine -> storage
```

### Phase 6: Migrate consumers behind connector abstraction

Consumers such as Knot PKM should select Mycel mode by configuration.

Initial default:

```text
embedded
```

Optional daemon mode:

```text
daemon
```

This lets PKM and other consumers continue running while daemon mode matures.

### Phase 7: Add space/session cache manager

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

Adapt CLI commands to call the Admin API where appropriate, while preserving embedded CLI paths during migration.

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
fallback: embedded
```

Embedded mode may remain useful for tests, local tooling, single-process deployments, and libraries.

## Compatibility Principles

- Every phase must leave the repository buildable and tests runnable.
- Existing embedded behavior should remain available until daemon mode is explicitly ready to replace it.
- Protocol definitions should be versioned.
- Data format compatibility should be preserved wherever possible.
- Migration should prioritize parity tests between embedded and daemon behavior.

## Open Questions

- Should protobuf definitions live in this repository or a separate API package?
- Should Connect be the default public transport for browser-compatible clients?
- What is the exact consistency model for replicated graphs?
- What is the daemon identity model for private mesh communication?
- Which operations belong in Client API vs Admin API when they affect both user data and daemon operations?
- How much local caching should connectors perform versus leaving caching entirely daemon-side?
- Should CLI default to embedded access, daemon admin access, or auto-detect?
