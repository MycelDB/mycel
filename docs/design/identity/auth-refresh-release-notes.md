# Auth Refresh Sessions Release Notes

## Status

Historical note superseded by unified principal identity.

The current public authentication surface is:

```text
mycel.common.v1.AuthService
```

The current admin identity-management surface is:

```text
mycel.admin.v1.AdminPrincipalService
```

## Current behavior

`AuthService` supports login, refresh, logout, whoami, caller-owned session listing, session revocation, and revoking all other sessions for the authenticated principal. Access tokens include the authenticated `principal_id` and auth-session ID. Refresh sessions are durable and rotate refresh tokens on refresh.

System-management authorization is handled by principal role bindings and capability grants; there is no separate admin/operator auth service.

## Historical note

Earlier daemon slices exposed separate admin/operator auth RPCs. Those RPCs were removed during unified principal identity cleanup.
