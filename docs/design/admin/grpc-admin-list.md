# Admin Principal List API

## Status

Current admin identity listing uses unified principals.

The daemon exposes principal listing and lookup through:

```text
mycel.admin.v1.AdminPrincipalService
```

Callers authenticate through:

```text
mycel.common.v1.AuthService
```

## Current behavior

`AdminPrincipalService` supports listing, getting, finding, creating, updating, disabling/enabling, deleting, password changes, role bindings, capability grants, and auth-session management for principals.

All records use `principal_id`. System-management access is authorized by role bindings and capability grants on the authenticated principal.

## Historical note

Older designs referred to `AdminOperatorService`, `Operator.operator_id`, and `AdminAuthService.LoginOperator`. Those surfaces were removed during unified principal identity cleanup.
