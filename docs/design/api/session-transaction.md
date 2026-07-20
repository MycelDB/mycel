# Client Session and Transaction API

## Status

Implemented daemon-oriented Client Session and Transaction lifecycle APIs on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/session.proto
```

This document depends on the cross-cutting access-control model in:

```text
docs/design/access-control.md
```

## Purpose

The daemon Client API separates **sessions** from **transactions**.

- A **session** is a daemon-owned working context scoped to one space and one domain. It is intended to support caching, access-context reuse, space/domain resource pinning, and future watch/subscription contexts.
- A **transaction** is an atomic read or write unit inside a session. Graph and query operations target a transaction id.

This distinction lets connectors keep a daemon-side session warm while executing multiple transactions over time.

## Core model

```text
OpenSession(space_id, domain_id)
  -> session_id

BeginTransaction(session_id, mode)
  -> transaction_id, base_revision

Graph/query operations(transaction_id)

CommitTransaction(transaction_id)
  -> commit_id, committed_revision

CloseSession(session_id)
```

Normal application code should usually interact with connector abstractions rather than manually managing every wire-level field.

Example connector-level shape:

```text
client.withSession(space_id, domain_id, async session => {
  await session.withWriteTransaction(async tx => {
    await tx.createNode(...)
  })
})
```

## SessionService

### Service definition

```protobuf
service SessionService {
  rpc OpenSession(OpenSessionRequest) returns (OpenSessionResponse);
  rpc GetSession(GetSessionRequest) returns (GetSessionResponse);
  rpc HeartbeatSession(HeartbeatSessionRequest) returns (HeartbeatSessionResponse);
  rpc CloseSession(CloseSessionRequest) returns (CloseSessionResponse);
}
```

### Session scope

A session is scoped to exactly one:

```text
space_id + domain_id
```

Transactions inherit this scope. Cross-domain sessions are not supported in v1.

This keeps caching, authorization, locking, domain deletion, and future mesh replication simpler. Cross-domain operations can be added later as explicit higher-level APIs if needed.

### Session purpose

Sessions are intended to support:

- daemon-side cache/context handles
- space/domain resource pinning
- auth/access evaluation caching
- prepared query or traversal state later
- connector-managed connection-like context
- future subscription/watch context
- repeated transactions over a warm daemon context

### Who opens sessions

The API supports explicit session lifecycle operations. In practice, connectors are expected to abstract most session management for applications.

### Session lifetime

Sessions have:

- idle timeout
- max lifetime, if configured by the daemon
- heartbeat/extend support

The daemon may cap or ignore requested timeout/extension values.

### Disconnect behavior

If a connector or client disconnects unexpectedly:

- the daemon may keep the session until idle timeout
- open read transactions expire/close with the session
- open write transactions roll back when the session closes or expires

### Session close behavior

`CloseSession` is a terminal operation and should be safe/idempotent.

Closing a session:

- releases session resources and cache pins
- closes open read transactions
- rolls back open write transactions
- prevents new transactions from being created in that session

### Access token behavior

A session may outlive an individual access token, but every operation requires a valid access token or equivalent authenticated request context. A session handle alone is not authorization.

## TransactionService

### Service definition

```protobuf
service TransactionService {
  rpc BeginTransaction(BeginTransactionRequest) returns (BeginTransactionResponse);
  rpc GetTransaction(GetTransactionRequest) returns (GetTransactionResponse);
  rpc CommitTransaction(CommitTransactionRequest) returns (CommitTransactionResponse);
  rpc RollbackTransaction(RollbackTransactionRequest) returns (RollbackTransactionResponse);
  rpc CloseTransaction(CloseTransactionRequest) returns (CloseTransactionResponse);
}
```

### Transaction modes

Supported v1 modes:

- read-only
- read-write

A read-only transaction represents a consistent read snapshot.

A read-write transaction buffers mutations until commit.

### Transaction scope

A transaction belongs to one session and inherits its:

```text
space_id + domain_id
```

Transactions cannot span multiple domains in v1.

### Revision metadata

`BeginTransaction` returns revision metadata:

```text
base_revision
```

The base revision identifies the domain revision from which the transaction began. This is useful for consistency, diagnostics, connector correctness, optimistic concurrency, cache validation, and future replication/oplog integration.

`CommitTransaction` returns:

```text
commit_id
base_revision
committed_revision
operation_count
commit_time
```

The daemon is authoritative for revision values. Connectors are expected to track these fields and ensure they are used correctly. Normal application code should generally not manage revisions directly.

### Transaction lifecycle

Transactions support:

- begin
- get
- commit
- rollback
- close

Transactions do not have their own heartbeat. Session heartbeat controls the lifetime of the session and therefore of open transactions.

A transaction expires no later than its parent session.

The daemon may still abort or fail a transaction for safety, including:

- session expiration
- deadlock
- excessive lock wait
- resource pressure
- conflict detection

### Transaction timeout

Transactions piggyback on the parent session timeout.

When `HeartbeatSession` extends a session, open transactions may remain valid for that extended session lifetime. There is no separate `HeartbeatTransaction` method in v1.

### Transaction concurrency

A session may have multiple active transactions.

The daemon is responsible for locking and concurrency control. Recommended v1 semantics:

- read-only transactions are snapshot reads
- read-write transactions buffer mutations
- commits are serialized per domain
- commit checks the transaction base revision and detects conflicts
- conflicting commits fail with `ABORTED`
- connectors may retry safe transactions

Nested transactions are not supported in v1.

### Graph/query operation target

Graph and query APIs should use:

```text
transaction_id
```

not `session_id`.

This ensures every graph/query operation belongs to an explicit atomic read or write unit.

### Commit semantics

`CommitTransaction` is valid only for read-write transactions.

Committing a read-write transaction:

- applies buffered mutations
- creates a commit/replication unit
- advances the domain revision
- closes the transaction

Read-only transactions are closed, not committed.

### Rollback and close semantics

`RollbackTransaction` is valid for read-write transactions and discards pending mutations.

`CloseTransaction` is safe for connectors:

- read-only transaction: closes/releases snapshot resources
- read-write transaction with uncommitted mutations: rolls back and closes

`CloseTransaction` should be idempotent from a client perspective.

## Authorization

All session and transaction operations require an authenticated caller.

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| Open session | `domain.read` and/or `graph.read` for the target domain |
| Begin read-only transaction | `graph.read` |
| Begin read-write transaction | `graph.write` |
| Commit transaction with deletes | delete operations require appropriate delete capabilities during graph operation or commit validation |
| Rollback transaction | caller owns/controls the transaction context |
| Close session/transaction | caller owns/controls the session/transaction context |

The exact enforcement point for delete capabilities may be graph-operation specific, but delete is not included in write.

## Audit and replication

The current daemon implementation provides in-memory session and transaction lifecycle state. Graph/query operations do not yet attach to transaction ids; that is the next migration slice.

A committed read-write transaction is the natural audit and replication unit.

Commit records should include:

- commit id
- session id
- transaction id
- space id
- domain id
- actor principal
- base revision
- committed revision
- operation count
- commit time

Future mesh replication can use committed transaction metadata as part of an oplog or delta stream.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| caller cannot access space/domain | `NOT_FOUND` or `PERMISSION_DENIED` |
| malformed id | `INVALID_ARGUMENT` |
| session not found or expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| transaction not found or expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| commit on read-only transaction | `FAILED_PRECONDITION` |
| rollback on read-only transaction | `FAILED_PRECONDITION` |
| nested transaction attempted | `FAILED_PRECONDITION` |
| transaction conflict | `ABORTED` |
| lock/resource pressure | `RESOURCE_EXHAUSTED` or `ABORTED` |
| service unavailable | `UNAVAILABLE` |

For normal Client API callers, returning `NOT_FOUND` for inaccessible sessions/transactions can avoid leaking existence.

## Mesh implications

Sessions and open transactions are daemon-local runtime state. They are not themselves replicated as durable graph state.

Committed read-write transactions, however, produce durable graph changes and commit metadata that must replicate across the mesh when the affected space/domain is replicated.

Domain revisions must be meaningful enough for daemons and connectors to reason about consistency and conflict handling.

## CLI

The CLI now uses daemon gRPC and standard-user credentials for session and transaction lifecycle commands:

```sh
./bin/mycel -u alice -p '<password>' session open --space-id '<space-id>' --domain-id '<domain-id>'
./bin/mycel -u alice -p '<password>' session get '<session-id>'
./bin/mycel -u alice -p '<password>' session heartbeat '<session-id>'
./bin/mycel -u alice -p '<password>' session close '<session-id>'

./bin/mycel -u alice -p '<password>' transaction begin '<session-id>' --mode read-write
./bin/mycel -u alice -p '<password>' transaction get '<transaction-id>'
./bin/mycel -u alice -p '<password>' transaction commit '<transaction-id>'
./bin/mycel -u alice -p '<password>' transaction rollback '<transaction-id>'
./bin/mycel -u alice -p '<password>' transaction close '<transaction-id>'
```

`session open` can resolve the default domain when `--domain-id` is omitted:

```sh
./bin/mycel -u alice -p '<password>' session open --space-id '<space-id>'
```

## Current implementation notes

- `SessionService` and `TransactionService` are registered on the Client API and require user bearer tokens.
- Session ownership is tied to the authenticated user principal.
- `OpenSession` validates that the caller can see the target space/domain through the daemon space module.
- Session/transaction lifecycle state is in-memory and therefore reset by daemon restart.
- Read-write transaction commit persists staged daemon `GraphService` operations first, then advances daemon lifecycle revision metadata with the graph `operation_count`.
- Closing a session closes read-only transactions and rolls back active read-write transactions.

## Open questions

- Should the API expose session-level statistics such as active transaction count or cache status?
- Should the daemon expose a transaction conflict detail type later, beyond gRPC `ABORTED`?
- Should commit metadata include a hash/digest for future replication integrity checks?
- Should connectors expose revisions to advanced users or keep them entirely internal by default?
