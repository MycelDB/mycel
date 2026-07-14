# gRPC Admin Authentication

## Status

Implemented for daemon Admin APIs. Admin authentication now uses the standard
short-lived access-token plus durable refresh-token/session lifecycle.

The protobuf contract is defined in `mycel.admin.v1.AdminAuthService` in
`mycel-api` and consumed by SDKs/daemon implementations through locally
generated stubs.

## Problem

Admin API operations must not be callable anonymously. Even read-only
operations such as listing daemon administrators expose sensitive operational
information and must require an authenticated daemon operator.

Long-running operator clients also cannot depend on a single short-lived access
token remaining valid forever. Services such as Knot PKM keep daemon clients in
memory for days, so they need automatic access-token renewal instead of process
restarts.

## Approach

The daemon uses the standard gRPC bearer-token pattern with refresh sessions:

1. The client calls public `LoginOperator` with operator username/password.
2. The daemon verifies the password against the admin/operator store.
3. The daemon creates a durable operator auth session and returns:
   - a short-lived access token
   - an access-token expiry timestamp
   - a high-entropy refresh token
4. The client sends the access token on protected RPCs using gRPC metadata:

```text
authorization: Bearer <access-token>
```

5. A daemon interceptor validates the access token before protected service
   handlers run.
6. Authenticated principal information, including the operator auth-session ID,
   is attached to `context.Context` for service-level checks.
7. Before expiry, or after one expired-token `Unauthenticated` response, SDKs
   call public `RefreshOperator` with the refresh token.
8. The daemon rotates the refresh token, updates the session, and returns a new
   access token and refresh token.
9. `LogoutOperator` revokes the current operator auth session, or an explicitly
   supplied session when authorized.

Unauthenticated requests return:

```text
grpc status: Unauthenticated
```

Authorization failures return:

```text
grpc status: PermissionDenied
```

## API

The daemon exposes:

```text
mycel.admin.v1.AdminAuthService.LoginOperator
mycel.admin.v1.AdminAuthService.RefreshOperator
mycel.admin.v1.AdminAuthService.LogoutOperator
mycel.admin.v1.AdminAuthService.WhoAmI
```

`LoginOperator` and `RefreshOperator` are public. `WhoAmI`, `LogoutOperator`,
and all other Admin API operations require a bearer access token unless a method
is explicitly exempted for daemon maintenance semantics.

`LoginOperatorResponse` and `RefreshOperatorResponse` include:

```text
access_token
access_token_expire_time
operator
refresh_token
```

The refresh token is optional in the protobuf contract so future browser or HTTP
transports can carry it in an HttpOnly cookie. gRPC SDK/service clients should
store and pass it explicitly.

## Token model

### Access tokens

Access tokens are daemon-local, HMAC-signed, short-lived tokens.

Current properties:

- token payload contains operator ID, username, principal kind, auth-session ID,
  issued time, and expiry
- token signature uses a daemon-local random signing key generated at daemon
  startup
- tokens expire after `MYCELD_ACCESS_TOKEN_TTL` when configured for the daemon,
  or the default access-token TTL otherwise
- tokens are not persisted
- tokens become invalid on daemon restart because the signing key changes

The default TTL is intended to stay short. Increasing it is not the preferred
fix for long-running services; use refresh sessions instead.

### Refresh sessions

Operator refresh sessions are persisted under the daemon admin data directory.
The daemon stores only refresh-token hashes, never raw refresh tokens.

Refresh-session behavior:

- created during `LoginOperator`
- associated with the authenticated operator
- contains coarse client metadata supplied by `OperatorClientInfo`
- has idle and absolute expiries
- rotates the refresh token on every successful `RefreshOperator`
- records consumed token hashes to detect token reuse
- revokes a token family on detected refresh-token reuse
- is revoked by `LogoutOperator`, operator session revoke APIs, operator disable,
  operator delete, and password-change paths that request session revocation

If a refresh token is missing, invalid, reused, expired, or revoked, the daemon
returns `Unauthenticated`.

## SDK behavior

The Go and Rust SDKs maintain access-token expiry and refresh-token state for
both Client and Admin clients.

Expected SDK behavior:

- store access-token expiry and refresh token returned by login/refresh
- refresh proactively shortly before access-token expiry
- retry once on expired-token `Unauthenticated`
- rotate stored refresh token after every refresh response
- avoid recursive refresh attempts for login/refresh/logout RPCs
- expose logout helpers that revoke the current auth session and clear local
  token state

Raw generated service clients still receive bearer metadata from SDK
interceptors, but callers that bypass SDK convenience helpers must ensure they
call refresh helpers themselves when needed.

## Quiesce behavior

`LoginOperator`, `RefreshOperator`, and `WhoAmI` are quiesce-exempt so operators
and long-running services can maintain auth continuity while the daemon blocks
normal mutating work for backup/quiesce operations.

## CLI behavior

The CLI can still log in per command:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin list
```

An authenticated operator can change their own password:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<current-password>' admin password set --new-password '<new-password>'
```

Operator lifecycle and session-management commands use the same daemon operator
login flow. Examples:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin get --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin create --operator-username '<username>' --new-password '<password>' [--email '<email>'] [--role system-admin]
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin session list --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin session revoke --operator-id '<id>' --session-id '<auth-session-id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin session revoke-all --operator-id '<id>'
```

## Security notes

The daemon still defaults to a loopback-only gRPC listener:

```text
127.0.0.1:9091
```

Do not bind the daemon Admin API to a non-loopback interface in production
without transport security. TLS/mTLS is recommended for remote administration.

The bootstrap password is logged once during standalone initialization. Logs
must be protected because they contain the initial operator credential.

Refresh tokens are bearer secrets. SDKs and service callers should keep them in
process memory or a secure secret store, never logs or user-visible telemetry.

## Testing expectations

Tests cover:

- valid access-token issue and verification
- tampered and expired access-token rejection
- gRPC interceptor requiring bearer metadata
- `LoginOperator` success, refresh-token issuance, and bad-password rejection
- `RefreshOperator` access-token renewal and refresh-token rotation
- refresh-token reuse rejection and session/family revocation behavior
- `LogoutOperator` revoking the current session
- operator session list/revoke APIs
- password/disable/delete paths that revoke operator sessions
- `RefreshOperator` public/quiesce-exempt server wiring
- SDK auto-refresh and one retry on expired-token `Unauthenticated`
- long-running PKM daemon runtime signup/onboarding after cached daemon tokens expire
