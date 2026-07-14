# Sessions (v1 historical)

This page describes the former embedded Go `session` package. That public
package has been removed/internalized.

Current applications should not import `github.com/myceldb/mycel/session` or use
file-backed sessions directly. Open sessions and transactions through the
`myceld` Client gRPC APIs, using `mycel-api` protobuf clients or
`mycel-go-sdk` helpers.

Current graph/session concepts are exposed through daemon APIs:

- Client `SessionService` opens, heartbeats, and closes daemon sessions.
- Client `GraphService` applies graph operations inside daemon transactions.
- Client `TemplateService`, `QueryService`, `BlobService`, and related services
  expose application-facing operations.

Daemon-internal session implementation code lives under `internal/session/` and
is not an application API.

See also:

- [Daemon-only boundary](../../v2/design/daemon-only-boundary.md)
- [API graph design](../../v2/design/api/graph.md)
- [API session/transaction design](../../v2/design/api/session-transaction.md)
