# Admin Auth API

## Status

Superseded by unified principal authentication.

The daemon no longer exposes a separate `mycel.admin.v1.AdminAuthService`. Authentication for admin and data-plane APIs is handled by:

```text
mycel.common.v1.AuthService
```

Admin identity management is handled by:

```text
mycel.admin.v1.AdminPrincipalService
```

## Current model

All daemon callers authenticate as principals. A principal can be human, service, or system. Access to system-management APIs is controlled through role bindings, capability grants, and scoped authorization checks.

This means:

- login/refresh/logout/whoami use `mycel.common.v1.AuthService`;
- admin APIs accept the same bearer access token as client/data-plane APIs;
- system administration is a property of a principal's roles/capabilities, not a separate operator identity species;
- delegated session creation and session management for other principals belongs to `AdminPrincipalService`.

## Public/quiesce behavior

`AuthService.Login` and `AuthService.Refresh` are public at the access-token layer because they validate primary credentials or refresh-token credentials. `AuthService.WhoAmI` is quiesce-exempt for diagnostics but still requires authentication.

## Historical note

Older designs in this repository used `AdminAuthService.LoginOperator`, `RefreshOperator`, and `LogoutOperator`. Those RPCs were removed during unified principal identity cleanup. Use the common auth service instead.
