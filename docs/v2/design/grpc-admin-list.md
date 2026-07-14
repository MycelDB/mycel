# gRPC Admin List

## Status

Implemented v2 daemon gRPC Admin Operator API slice on the `refactor_daemon` branch.

## Purpose

The daemon Admin API exposes bootstrap/admin operator accounts over gRPC so clients can inspect and manage daemon admins without reading daemon store files directly.

The user-facing CLI command is:

```sh
mycel admin list
```

The command talks to the daemon gRPC API. During the migration from embedded/library workflows to daemon-first workflows, this command name remains stable while implementation details move behind gRPC.

The command must authenticate as a daemon operator before listing administrators:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin list
```

## API surface

The daemon implements `mycel.admin.v1.AdminOperatorService` methods for:

```text
ListOperators
GetOperator
FindOperator
CreateOperator
UpdateOperator
DisableOperator
EnableOperator
DeleteOperator
SetOperatorPassword
ListOperatorRoles
GrantOperatorRole
RevokeOperatorRole
ListOperatorCapabilities
GrantOperatorCapability
RevokeOperatorCapability
ListOperatorSessions
RevokeOperatorSession
RevokeOperatorSessions
```

Although the CLI uses the word `admin`, the v2 Admin API models daemon administrators as **operators**. The admin module persists a compact operator record and maps it to protobuf `Operator` messages.

## Mapping

```text
AdminSummary.ID        -> Operator.operator_id
AdminSummary.Username  -> Operator.username
AdminSummary.Email     -> Operator.email
AdminSummary.State     -> Operator.state
AdminSummary.CreatedAt -> Operator.create_time
AdminSummary.UpdatedAt -> Operator.update_time
```

The store intentionally does not persist `display_name` in this slice. It persists optional email, active/disabled/deleted state, update time, role grants, and direct capability grants. Session RPCs are implemented as empty/no-op placeholders because daemon access tokens are short-lived and not persisted.

Password hashes are never returned by this API.

`ListOperators` is protected by gRPC bearer-token authentication. Anonymous calls return `Unauthenticated`.

## Transport

The daemon listens on a configurable gRPC address:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MYCELD_GRPC_ADDR` | `127.0.0.1:9091` | Address for the daemon gRPC listener. |

The default is loopback-only because transport security is not complete yet.

The CLI first calls `AdminAuthService.LoginOperator`, then calls `ListOperators` with gRPC metadata:

```text
authorization: Bearer <access-token>
```

The CLI resolves the daemon address from:

1. `--daemon-addr`
2. `MYCELD_GRPC_ADDR`
3. `127.0.0.1:9091`

## Pagination

`ListOperators` supports simple offset-based pagination:

- empty `page_token` starts at offset `0`
- non-empty `page_token` must be a non-negative integer offset
- `page_size <= 0` uses the default page size
- page size is capped by the daemon
- `next_page_token` is the next integer offset when more results exist

## Current limitations

- Transport security is not complete; TLS/mTLS is not configured yet.
- Loopback-only default is a safety constraint, not a complete security boundary.
- Authorization is currently coarse: system admins may mutate other operators; authenticated operators may read operator records and change their own password.
- Operator auth sessions are not persisted; session list/revoke RPCs return empty/no-op results while access tokens remain short-lived.
- The direct file-backed admin module remains the source of truth until richer daemon metadata storage is introduced.

## Validation expectations

Tests cover:

- protobuf service mapping from `AdminSummary` to `Operator`
- pagination and invalid page tokens
- no password/hash leakage
- gRPC server registration
- anonymous gRPC calls fail with `Unauthenticated`
- system-admin checks for mutating operator operations
- last active system admin protection
- CLI `mycel admin ...` commands using login plus authenticated gRPC
- CLI daemon address resolution via `--daemon-addr` and `MYCELD_GRPC_ADDR`
