# Admin Operators

## Status

Superseded by unified principal identity.

Mycel no longer models system admins/operators as a separate identity store or public service. The old `mycel.admin.v1.AdminOperatorService` has been replaced by:

```text
mycel.admin.v1.AdminPrincipalService
```

Authentication uses:

```text
mycel.common.v1.AuthService
```

## Current model

System administration is represented by role bindings and capability grants on principals. A principal with the appropriate system-management capabilities can create principals, assign roles, grant capabilities, manage sessions, and perform other admin operations.

Canonical fields use `principal_id`; old `operator_id` terminology should not be used for current APIs.

## Historical note

This document used to describe a split admin/operator model. That model was removed during unified principal identity cleanup. See:

- `../identity/unified-principal-access-control.md`
- `../../implementation/unreleased/unified-principal-identity-implementation-plan.md`
