# Unified Principal Identity Implementation Plan

## Status

Proposed. This is a breaking replacement for the current split standard-user and
administrator/operator identity model. The target deployment model is a fresh
mycel environment reset; no compatibility migration from existing
`admins/` or `users/` data directories is required.

Design reference:
[Unified principal identity and access-control model](../../design/identity/unified-principal-access-control.md).

## Problem Statement

mycel currently stores and manages administrators/operators and standard users as
different identity species:

```text
<data-dir>/admins/admins.json
<data-dir>/admins/sessions/
<data-dir>/users/users.json
<data-dir>/users/sessions/
```

The code mirrors this split:

- `internal/identity/service/admin`
- `internal/identity/service/user`
- `identity.admin.put.v1`
- `identity.user.put.v1`
- access tokens with `OperatorID` or `UserID`
- admin APIs that require operator principals
- client APIs that require user principals
- space APIs that store `OwnerUserID` and grant access to users

This makes system management a storage-model distinction instead of an
authorization decision. It also duplicates authentication/session/lifecycle code
and makes service accounts awkward.

The new model makes all actors principals. A principal is a system administrator
only when it has system-scoped roles or capabilities.

## Goals

- Replace split user/admin logical storage with one authoritative principal
  store.
- Replace operator/user token identities with one `principal_id`-based token.
- Represent administration through system-scoped roles and capabilities.
- Attach access to any principal: human, service, or reserved system actor.
- Make client and admin service authorization capability-based.
- Update space ownership and grants from user-specific to principal-specific.
- Remove old split identity modules and record types from active code.
- Keep each implementation phase functional.
- Avoid legacy migration and compatibility scaffolding.

## Non-goals

- No migration from existing `admins/` or `users/` stores.
- No old `AdminOperatorService` / `AdminUserService` compatibility layer unless a
  later product decision explicitly asks for one.
- No OIDC/SAML integration in this tranche.
- No groups, teams, organizations, explicit deny rules, or fine-grained
  node/subtree ACLs.
- No automatic repair or authoritative-principal selection.
- No generated internal protobuf/parser artifacts committed unless explicitly
  approved.

## Implementation Principles

- **One identity catalog.** Humans, service accounts, and reserved system actors
  are all principals.
- **Capabilities are authoritative.** Roles are built-in bundles that expand to
  capabilities.
- **Scopes are explicit.** System, space, and domain scopes are the initial
  authorization scopes.
- **Fail closed.** Unknown principals, disabled principals, missing grants,
  malformed scopes, and missing authorizers deny access.
- **Soft delete.** Principal and grant history is retained for audit/reference.
- **Last-admin invariant.** The daemon must not commit a state that leaves no
  active login-enabled system administrator.
- **Subsystem authority remains.** Identity WAL/raft/snapshots remain the
  authoritative source for principal and grant records.

## Target File Layout

```text
<MYCELD_DATA_DIR>/identity/store.json
<MYCELD_DATA_DIR>/identity/sessions/refresh_sessions.json
```

`store.json` contains:

```json
{
  "principals": [],
  "role_bindings": [],
  "capability_grants": []
}
```

## Phase UP1: Public API and Naming Decision

### Tasks

1. Decide whether this tranche renames public admin/user services immediately or
   stages the protobuf change behind internal code. Because compatibility is not
   required, the preferred answer is immediate public API cleanup.
2. Update `mycel-api` protobufs:
   - replace operator/user principal identifiers with `principal_id`;
   - add `Principal`, `PrincipalKind`, `PrincipalState`, `RoleBinding`,
     `CapabilityGrant`, and `AccessScope` shapes as needed;
   - replace `AdminOperatorService` and `AdminUserService` with
     `AdminPrincipalService`;
   - update `AuthPrincipal` to expose `principal_id`, `username`, and kind;
   - update `common/v1/access.proto` to remove `PRINCIPAL_TYPE_OPERATOR` as a
     distinct authorization species;
   - rename user/operator capabilities to identity/principal capabilities.
3. Regenerate public generated code in `mycel`, `mycel-go-sdk`, and any other
   public SDK that depends on changed protobufs.
4. Update generated SDK helper surfaces only enough to compile; ergonomic helpers
   can follow later.

### Acceptance

- `mycel-api` defines one principal-oriented identity/admin surface.
- Generated public SDK code compiles.
- No public auth response requires callers to choose between `user_id` and
  `operator_id`.

## Phase UP2: Unified Principal Model and Store

### Tasks

1. Add a new identity service package, for example:

   ```text
   internal/identity/service/principal
   ```

2. Define canonical records:

   ```go
   type Principal struct {
       ID           string
       Username     string
       Email        string
       DisplayName  string
       Kind         PrincipalKind // human | service | system
       State        PrincipalState // active | disabled | deleted
       LoginEnabled bool
       PasswordHash string
       CreatedAt    time.Time
       UpdatedAt    time.Time
       CreatedBy    string
   }

   type RoleBinding struct {
       ID          string
       PrincipalID string
       Role        string
       Scope       AccessScope
       State       GrantState
       Reason      string
       CreatedBy   string
       CreatedAt   time.Time
       RevokedBy   string
       RevokedAt   time.Time
   }

   type CapabilityGrant struct {
       ID          string
       PrincipalID string
       Capability  string
       Scope       AccessScope
       State       GrantState
       Reason      string
       CreatedBy   string
       CreatedAt   time.Time
       RevokedBy   string
       RevokedAt   time.Time
   }
   ```

3. Implement a file-backed store at `<data-dir>/identity/store.json` with atomic
   write behavior matching the current stores.
4. Enforce uniqueness for:
   - principal ID;
   - case-insensitive username for login-enabled human/service principals;
   - case-insensitive email when present, if product requirements choose unique
     email.
5. Add CRUD/update methods:
   - list/get/find principals;
   - create/update/disable/enable/delete principal;
   - set password hash;
   - put/revoke role binding;
   - put/revoke capability grant;
   - list grants by principal and by scope.
6. Add last-system-admin invariant checks to lifecycle and grant updates.
7. Add comprehensive store tests.

### Acceptance

- A fresh identity store can create, read, update, disable, and soft-delete
  principals.
- Role bindings and direct grants can be added/revoked.
- The store rejects states that remove the last active login-enabled system
  administrator.
- No current admin/user store package is needed by the new model tests.

## Phase UP3: Unified Auth Sessions and Token Payload

### Tasks

1. Update `internal/daemon/auth/token.go`:
   - replace `OperatorID` and `UserID` with `PrincipalID`;
   - replace `PrincipalKindOperator/User` with principal kind values such as
     `human`, `service`, and `system`;
   - preserve `AuthSessionID`, `Username`, timestamps, HMAC signing, and TTL.
2. Update refresh-session storage to key sessions by `principal_id`.
   - The existing `internal/identity/storage/session` package can be reused if
     its owner fields are generalized.
   - Replace `UserID`/`UserRef` naming with `PrincipalID`/`PrincipalRef` or a
     neutral owner field.
3. Implement unified auth methods on the principal manager:
   - `AuthenticatePrincipal`;
   - `CreateAuthSession`;
   - `RefreshAuthSession`;
   - `ListPrincipalSessions`;
   - `RevokePrincipalSession`;
   - `RevokePrincipalSessions`.
4. Reject login for principals that are disabled, deleted, not login-enabled, or
   missing usable credentials.
5. Update token/interceptor tests.

### Acceptance

- Login/refresh/logout works for a normal human principal.
- Login/refresh/logout works for a human principal with system-admin role using
  the same auth path.
- Tokens contain one `principal_id` and no user/operator-specific ID.
- Refresh-token rotation and reuse detection still pass tests.

## Phase UP4: Identity WAL, Raft, Snapshot, and Bootstrap

### Tasks

1. Add unified WAL record types:

   ```text
   identity.principal.put.v1
   identity.role_binding.put.v1
   identity.capability_grant.put.v1
   ```

2. Add raft state machine handling for unified identity records.
3. Add idempotency/dedupe coverage equivalent to current user/admin modules.
4. Add snapshot/reload support containing principals, role bindings,
   capability grants, and applied progress.
5. Implement bootstrap behavior:
   - if no active system administrator exists in standalone fresh data, create a
     login-enabled human principal with system-scoped `system.admin`;
   - in clustered mode, fail closed until authoritative identity metadata is
     available or explicit bootstrap configuration is applied.
6. Remove active registration of `identity.admin.put.v1` and
   `identity.user.put.v1` once callers have moved to unified records.

### Acceptance

- WAL replay restores principals and grants.
- Raft apply restores principals and grants on followers.
- Snapshot/reload restores principals and grants.
- Fresh standalone startup creates one usable system administrator when needed.
- Last-system-admin invariant is preserved after replay/reload.

## Phase UP5: Unified Authorization Service

### Tasks

1. Add a shared authorizer interface:

   ```go
   type Authorizer interface {
       Authorize(ctx context.Context, principalID string, capability string, scope AccessScope) error
       EffectiveAccess(ctx context.Context, principalID string, scope AccessScope) (EffectiveAccess, error)
   }
   ```

2. Implement capability evaluation:
   - direct grants;
   - role bindings expanded through built-in role definitions;
   - owner-derived capabilities;
   - reserved system-principal capabilities.
3. Define built-in roles and role-to-capability mappings in one package.
4. Replace current operator-only `HasCapability(ctx, operatorID, capability)`
   checks with principal/scoped authorization.
5. Add tests for:
   - system admin grants;
   - identity admin grants;
   - space viewer/editor/admin/owner roles;
   - service principal roles;
   - missing grant denial;
   - scope mismatch denial.

### Acceptance

- All authorization tests evaluate capabilities against `principal_id`.
- Admin/control operations can be authorized without checking for an operator
  principal kind.
- A principal with no system role cannot call system-management operations.

## Phase UP6: Admin Principal Service and CLI

### Tasks

1. Replace daemon admin user/operator services with `AdminPrincipalService`.
2. Implement principal lifecycle RPCs:
   - list/get/find/create/update/disable/enable/delete;
   - set password;
   - list/revoke sessions;
   - create delegated session;
   - grant/revoke roles;
   - grant/revoke direct capabilities.
3. Use the unified authorizer for every method.
4. Replace or rewrite CLI commands:
   - `mycel principal ...` for lifecycle and grants;
   - optional `mycel auth ...` for self auth/session operations;
   - remove old `mycel admin create` operator-specific and `mycel user ...`
     code paths unless intentionally kept as aliases before release.
5. Update admin docs and CLI tests.

### Acceptance

- A system-admin principal can create a normal principal.
- A system-admin or identity-admin principal can grant scoped roles.
- A normal principal cannot manage other principals.
- Delegated session creation requires `identity.session.delegate`.
- CLI tests pass with unified principal commands.

## Phase UP7: Client Auth and Data-Plane Services

### Tasks

1. Refactor client `AuthService` to authenticate through the principal manager.
2. Replace helpers such as `userPrincipalFromContext` with neutral principal
   helpers.
3. Update client services to use `principal.PrincipalID`:
   - session service;
   - domain service;
   - graph service;
   - query service;
   - blob service;
   - metadata catalog;
   - change stream;
   - semantic search;
   - import/export.
4. Replace checks that only require a `PrincipalKindUser` with capability checks
   on the requested resource scope.
5. Update route forwarding principal serialization:
   - remove forwarded `OperatorID`/`UserID`;
   - forward `PrincipalID`, `Kind`, username, auth-session ID, and created time.
6. Update client-service tests.

### Acceptance

- A principal with space graph/query capabilities can open sessions and query.
- A principal without those capabilities is denied.
- A system-admin principal can also use data-plane APIs only when it has the
  necessary data-plane capabilities or owner/access grants.
- Route forwarding preserves `principal_id` across nodes.

## Phase UP8: Space, Domain, Session, and Access Refactor

### Tasks

1. Replace user-specific space service model fields:

   ```text
   OwnerUserID -> OwnerPrincipalID
   GrantSpaceUser -> GrantSpacePrincipal
   EffectiveAccess(userID, ...) -> EffectiveAccess(principalID, ...)
   ```

2. Update `internal/space/model.Space` ownership fields from `identity.UserID` to
   principal identifier.
3. Update ACL storage records from `UserID` to `PrincipalID`.
4. Rename WAL/raft records where appropriate:

   ```text
   space.acl.grant.v1 -> space.access.grant.v1
   ```

   Since compatibility is not required, old names can be removed rather than
   supported.
5. Update session service types:

   ```text
   UserID -> PrincipalID
   ```

   This includes route records and origin metadata wiring where the field is an
   authenticated actor identifier.
6. Update semantic service fields such as `ActorPrincipalID` that currently use
   `identity.UserID`.
7. Update all tests and fixtures that create spaces/sessions with user IDs.

### Acceptance

- Spaces are created with a principal owner.
- Space access grants target principals.
- Sessions and transactions are scoped to a principal.
- Existing graph-change origin metadata records the authenticated principal ID.
- Space/domain/client API tests pass with principal IDs.

## Phase UP9: Remove Split Identity Code

### Tasks

1. Delete active code packages after callers are migrated:
   - `internal/identity/service/admin`
   - `internal/identity/service/user`
2. Delete old store-specific tests or port useful cases to the principal package.
3. Remove daemon runtime module wiring for separate admin/user modules.
4. Remove old bootstrap admin code paths in favor of principal bootstrap.
5. Remove old API mapping helpers that convert `Admin`, `User`, `OperatorID`, or
   `UserID`.
6. Remove old generated internal protobuf references after public API changes are
   regenerated.
7. Run broad dead-code searches for:

   ```text
   OperatorID
   UserID
   PrincipalKindOperator
   PrincipalKindUser
   OwnerUserID
   GrantSpaceUser
   identity.admin.put.v1
   identity.user.put.v1
   ```

   Some `UserID` references may remain only in historical docs or external API
   compatibility if explicitly approved; otherwise they should be gone from
   active code.

### Acceptance

- Active runtime has one identity module.
- Active code has no dependency on old admin/user identity service modules.
- New fresh data directories do not create `admins/` or `users/` identity stores.
- Dead-code searches show no old split identity path in active implementation.

## Phase UP10: Documentation and Operational Updates

### Tasks

1. Update design docs:
   - `docs/design/identity/access-control.md`;
   - `docs/design/api/auth.md`;
   - `docs/design/identity/grpc-client-auth.md`;
   - `docs/design/admin/grpc-admin-auth.md`;
   - `docs/design/admin/operator.md`;
   - `docs/design/admin/user.md`;
   - space/domain API docs.
2. Mark old operator/user docs as superseded or delete/rewrite them before
   release.
3. Update operations docs and CLI examples to use principal terminology.
4. Update SDK README examples.
5. Update backup/restore and import/export docs where identity owner references
   change from user to principal.

### Acceptance

- Documentation consistently describes one principal model.
- Admin/control-plane authority is described as roles/capabilities, not a
  separate administrator store.
- `make docs-check` passes.

## Phase UP11: Validation Matrix

### Required tests

Run and keep passing:

```sh
MYCEL_API_ROOT=../mycel-api make test
go test ./internal/identity/... ./internal/space/... ./internal/session/... ./internal/daemon/api/admin ./internal/daemon/api/client
go test ./internal/daemon/app ./internal/clustering/... ./internal/semantic/...
make docs-check
git diff --check
cd ../mycel-api && make test
cd ../mycel-go-sdk && MYCEL_API_ROOT=../mycel-api make test
cd ../mycel-rust-sdk && MYCEL_API_ROOT=/Users/martinbeauvais/Projects/knotbase/Knotbase/myceldb/mycel-api cargo test
```

### New integration tests

Add tests proving:

1. fresh bootstrap creates one system-admin principal;
2. normal principal login/refresh/logout;
3. system-admin principal login through the same auth path;
4. principal can be both a normal data-plane actor and a system administrator;
5. admin operation denied without required system capability;
6. space read/write denied without scoped role;
7. space read/write allowed with scoped role;
8. last-system-admin disable/delete/grant-revoke is rejected;
9. delegated session creation requires `identity.session.delegate`;
10. raft/WAL replay restores principals and grants;
11. route forwarding preserves principal identity;
12. service principal can perform a narrowly scoped automation/semantic-style
    operation if granted the correct role.

## Rollout Plan

Because compatibility is not required, rollout is a branch-based breaking change:

1. Create a dedicated branch from `develop`.
2. Apply phases UP1-UP11 in order.
3. Keep the code compiling at the end of each phase or small subphase.
4. Regenerate and commit public API/SDK generated code when API contracts change.
5. Do not include old-store migration code.
6. Reset local/dev/test environments after merge.
7. Tag the next API/SDK/mycel releases only after all downstream repos compile
   against the principal model.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Large blast radius across auth, sessions, space, API, SDKs | Execute in phases; keep tests green after each phase; use mechanical renames where possible. |
| Accidentally granting system powers to normal principals | Deny by default; require explicit system-scoped roles; test missing-grant denial. |
| Locking out all administrators | Enforce last-system-admin invariant in store and manager before commit. |
| Ambiguous scopes | Centralize scope matching and role expansion in one authorizer package. |
| Service principals over-permissioned | Add narrow built-in service roles and tests for least-privilege scopes. |
| Public API churn | Accept as intentional breaking change before shipping; regenerate SDKs in the same tranche. |

## Final Acceptance Criteria

The work is complete when:

- one principal identity store is authoritative for humans, services, and system
  actors;
- old administrator and standard-user stores are gone from active runtime;
- tokens carry `principal_id` only;
- admin and client APIs authorize by scoped capabilities;
- system administration is represented by roles/capabilities;
- spaces and access grants target principals;
- tests cover bootstrap, auth, authorization, grants, last-admin protection,
  data-plane access, raft/WAL replay, and route forwarding;
- docs and SDK examples use principal terminology;
- fresh environment startup works without compatibility migration.
