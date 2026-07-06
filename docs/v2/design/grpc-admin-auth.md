# gRPC Admin Authentication

## Status

Implemented initial daemon Admin API authentication on the `refactor_daemon` branch.

## Problem

Admin API operations must not be callable anonymously. Even read-only operations such as listing daemon administrators expose sensitive operational information and must require an authenticated daemon operator.

## Approach

The daemon uses the standard gRPC bearer-token pattern:

1. The client calls a public login RPC with operator username/password.
2. The daemon verifies the password against the admin/operator store.
3. The daemon returns a short-lived access token.
4. The client sends the token on protected RPCs using gRPC metadata:

```text
authorization: Bearer <access-token>
```

5. A daemon unary interceptor validates the token before protected service handlers run.
6. Authenticated principal information is attached to `context.Context` for service-level checks.

Unauthenticated requests return:

```text
grpc status: Unauthenticated
```

Future authorization failures should return:

```text
grpc status: PermissionDenied
```

## API

The daemon exposes:

```text
mycel.admin.v1.AdminAuthService.LoginOperator
mycel.admin.v1.AdminAuthService.WhoAmI
```

`LoginOperator` is public. `WhoAmI` and all Admin API operations require a bearer token.

Authorization is currently coarse: only active operators can log in; active `system_admin` operators may create, update, disable, enable, soft-delete, grant roles/capabilities, and change another operator's password. Operators with user-management capabilities may manage standard users through `AdminUserService`. Any authenticated operator can list/read operators and change their own password.

## Token model

The initial implementation issues daemon-local, HMAC-signed, short-lived access tokens.

Current properties:

- token payload contains operator ID, username, creation time, issued time, and expiry
- token signature uses a daemon-local random signing key generated at daemon startup
- tokens expire after a short TTL
- tokens are not persisted
- tokens become invalid on daemon restart because the signing key changes

This is acceptable for the first daemon API slice because the CLI logs in for each admin command. Later REPL/session behavior can keep the token in memory for the process lifetime.

Future hardening can add:

- persisted signing key or keyring-backed secrets
- refresh sessions for operators
- token revocation lists
- mTLS for operator-to-daemon administration
- finer-grained capability checks beyond the current system-admin gate

## CLI behavior

`mycel admin list` now authenticates before calling the protected list RPC:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin list
```

An authenticated operator can change their own password:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<current-password>' admin password set --new-password '<new-password>'
```

A system admin can change another operator's password by passing `--operator-id`.

Additional operator lifecycle commands include:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin get --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin find --operator-username '<username>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin create --operator-username '<username>' --new-password '<password>' [--email '<email>'] [--role system-admin]
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin update --operator-id '<id>' --email '<email>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin disable --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin enable --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin delete --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin role list --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin role grant --operator-id '<id>' --role space-admin
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin role revoke --operator-id '<id>' --grant-id '<grant-id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin capability list --operator-id '<id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin capability grant --operator-id '<id>' --capability operator-manage
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin capability revoke --operator-id '<id>' --grant-id '<grant-id>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<password>' admin session list --operator-id '<id>'
```

The CLI list flow is:

1. call `AdminAuthService.LoginOperator`
2. receive access token
3. call `AdminOperatorService.ListOperators` with `authorization: Bearer <token>` metadata

Standard user-management commands use the same daemon operator login flow:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' user add --user-username alice --new-password '<password>'
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' user list
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' user delete '<user-id>'
```

The CLI password-change flow is:

1. call `AdminAuthService.LoginOperator` with the current password
2. call `AdminAuthService.WhoAmI` with the access token
3. call `AdminOperatorService.SetOperatorPassword` for the requested operator ID, defaulting to the authenticated operator ID

The command rejects missing `--username/-u` or `--password/-p`.

## Security notes

The daemon still defaults to a loopback-only gRPC listener:

```text
127.0.0.1:9091
```

This is intentional until TLS/mTLS and richer admin authorization are implemented. Do not bind the daemon Admin API to a non-loopback interface in production without transport security.

The bootstrap password is logged once during standalone initialization. Logs must be protected because they contain the initial operator credential.

## Testing expectations

Tests cover:

- valid token issue and verification
- tampered and expired token rejection
- gRPC interceptor requiring bearer metadata
- `LoginOperator` success and bad-password rejection
- `AdminOperatorService` and `AdminUserService` methods requiring an authenticated context
- gRPC server rejecting anonymous admin calls
- CLI requiring credentials and rejecting bad passwords
- CLI login plus authenticated admin/operator management over gRPC
- self-service and system-admin password change over gRPC
- old password rejection and new password acceptance after a password change
