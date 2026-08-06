# Auth refresh release notes

## Summary

Mycel daemon authentication now supports the standard short-lived access-token
plus durable refresh-token lifecycle for both Client and Admin callers.

This fixes long-running service failures such as:

```text
rpc error: code = Unauthenticated desc = authorization token is expired
```

without increasing access-token TTLs or restarting dependent services.

## Admin API changes

`mycel.admin.v1.AdminAuthService` now supports:

```text
LoginOperator
RefreshOperator
LogoutOperator
WhoAmI
```

`LoginOperator` and `RefreshOperator` return:

```text
access_token
access_token_expire_time
refresh_token
```

Operator access tokens include an auth-session ID. `LogoutOperator` revokes the
current operator session by default.

## Client API behavior

The existing Client auth refresh/session model remains the normal path for user
callers. SDK clients now keep access-token expiry and refresh-token state so
long-running callers can renew automatically.

## SDK behavior

Updated SDKs should:

- store access-token expiry and refresh token returned by login/refresh
- refresh proactively before access-token expiry
- retry once on expired-token `Unauthenticated`
- rotate the stored refresh token after every refresh
- expose logout helpers that revoke the current auth session

Raw generated clients receive bearer metadata, but direct callers that bypass SDK
helpers may need to call refresh helpers themselves.

## Daemon configuration

`myceld` accepts:

```text
MYCELD_ACCESS_TOKEN_TTL=15m
```

The default remains short-lived. Prefer refresh sessions over increasing the TTL
for services that stay online for long periods.

## Migration notes

- `mycel-api` remains proto-only; generated bindings belong to consuming repos.
- Consumers should regenerate local stubs from `mycel-api`.
- Go and Rust SDK users should update to versions containing auto-refresh
  support before running long-lived daemons/services.
- Existing short-lived access tokens remain valid until expiry, but clients need
  a refresh token from a fresh login to renew automatically.

## Validation highlights

Validation added for this release includes:

- operator login refresh-token issuance
- operator refresh-token rotation and reuse rejection
- operator logout/session revocation
- SDK proactive refresh and expired-token retry
- PKM signup/onboarding after cached daemon access tokens expire
