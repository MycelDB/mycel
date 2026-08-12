# Admin Users

## Status

Superseded by unified principal identity.

Mycel no longer models daemon users as a separate identity store or public admin service. The old `mycel.admin.v1.AdminUserService` has been replaced by:

```text
mycel.admin.v1.AdminPrincipalService
```

Authentication uses:

```text
mycel.common.v1.AuthService
```

## Current model

All daemon-managed identities are principals. Human application users, service accounts, and system/bootstrap identities share one principal store and one auth session model.

Application profile data should live in application spaces. It may reference the daemon `principal_id`, but profile attributes, retention, and product-specific validation are not part of the daemon identity model.

## Historical note

This document used to describe a split user/admin model. That model was removed during unified principal identity cleanup. Use `principal_id` and `AdminPrincipalService` for current APIs.
