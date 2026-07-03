# gRPC Admin List

## Status

Implemented initial v2 daemon gRPC endpoint on the `refactor_daemon` branch.

## Purpose

The first daemon Admin API operation exposes the current daemon bootstrap/admin accounts over gRPC so clients can list existing daemon admins without reading daemon store files directly.

The user-facing CLI command is:

```sh
mycel admin list
```

The command talks to the daemon gRPC API. During the migration from embedded/library workflows to daemon-first workflows, this command name remains stable while implementation details move behind gRPC.

## API surface

The daemon implements the existing protobuf service:

```text
mycel.admin.v1.AdminOperatorService.ListOperators
```

Although the CLI uses the word `admin`, the v2 Admin API models daemon administrators as **operators**. The bootstrap admin module currently stores a narrower `AdminSummary`, which maps to the protobuf `Operator` message.

## Mapping

```text
AdminSummary.ID        -> Operator.operator_id
AdminSummary.Username  -> Operator.username
AdminSummary.CreatedAt -> Operator.create_time
state                  -> OPERATOR_STATE_ACTIVE
```

The initial store does not yet track display name, email, disabled state, deleted state, update time, roles, capabilities, or sessions. Those fields/operations remain future Admin API work.

Password hashes are never returned by this API.

## Transport

The daemon listens on a configurable gRPC address:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MYCELD_GRPC_ADDR` | `127.0.0.1:9091` | Address for the daemon gRPC listener. |

The default is loopback-only because daemon admin authentication/authorization has not been implemented yet.

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

- No daemon admin auth/authz yet.
- Loopback-only default is a safety constraint, not a complete security boundary.
- Only `ListOperators` is implemented; other `AdminOperatorService` methods return unimplemented.
- TLS/mTLS is not configured yet.
- The direct store-backed admin module remains the source of truth until richer operator storage is designed.

## Validation expectations

Tests cover:

- protobuf service mapping from `AdminSummary` to `Operator`
- pagination and invalid page tokens
- no password/hash leakage
- gRPC server registration
- CLI `mycel admin list` using gRPC
- CLI daemon address resolution via `--daemon-addr` and `MYCELD_GRPC_ADDR`
