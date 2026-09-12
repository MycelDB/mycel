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

## Bootstrap system-admin principals

Standalone daemons create a bootstrap human system-admin principal when no active system admin exists. If no password is configured, standalone mode generates a one-time password and writes it to the daemon log so an operator can recover initial access.

Mesh deployments are stricter by default: a mesh daemon must not generate a human administrator implicitly. Generated mesh admins would create surprising privileged credentials in clustered environments and are inappropriate for production-style deployments.

Mesh mode can still create a bootstrap system-admin principal when the operator explicitly configures bootstrap credentials with `MYCELD_BOOTSTRAP_ADMIN_USERNAME` and `MYCELD_BOOTSTRAP_ADMIN_PASSWORD`. This is required for disposable local mesh clusters, including reliability-test environments such as `mycel-lab`, where Mycel Console and CLI need a predictable operator identity to inspect cluster status, raft groups, spaces, users, and other authenticated admin views. Because the credentials are explicit operator input, the daemon logs only that configured bootstrap credentials were used; it does not log the plaintext password.

The intended safety boundary is:

- standalone with no configured password may generate a one-time bootstrap admin password;
- mesh with configured bootstrap credentials creates that configured system-admin principal;
- mesh without configured bootstrap credentials does not create a generated human admin.

## Historical note

Older designs in this repository used `AdminAuthService.LoginOperator`, `RefreshOperator`, and `LogoutOperator`. Those RPCs were removed during unified principal identity cleanup. Use the common auth service instead.
