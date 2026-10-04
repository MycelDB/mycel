# Unified Principal Identity Remaining Work Plan

Status: draft follow-up plan for the `unified_principal_identity` branch.

This plan captures the work still missing after the partial implementation of
[Unified principal identity implementation plan](unified-principal-identity-implementation-plan.md).
It is intentionally scoped to the remaining work across UP1-UP11, not a new
identity design.

## Current Baseline

The branch currently has partial unified-principal plumbing:

- `internal/identity/service/principal` exists with a file-backed principal,
  role-binding, capability-grant, session, WAL, Raft, and snapshot foundation.
- Daemon startup registers one active identity subsystem module and wraps it with
  compatibility adapters for legacy operator/user API paths.
- Token payloads and client auth responses are moving toward `principal_id`.
- Public protobufs include principal-oriented additions such as
  `AdminPrincipalService`, `PrincipalType`, principal capabilities, and
  `owner_principal_id`.
- A daemon-side `AdminPrincipalService` foundation is present and focused admin,
  daemon app/server, and CLI command package tests pass.

The branch is not complete because active code still contains old split-identity
terminology and behavior, including `internal/identity/service/admin`,
`internal/identity/service/user`, `OperatorID`, `UserID`, `OwnerUserID`, and
`GrantSpaceUser` in runtime paths.

## Non-goals and Constraints

- No compatibility or migration for old `admins/` and `users/` stores is
  required. Fresh data directories are acceptable.
- Do not keep the split admin/user storage model for active daemon runtime.
- System management is modeled as roles/capabilities on principals.
- Do not hand-edit generated protobuf outputs. Update `mycel-api` protos and
  regenerate.
- Do not commit generated ANTLR/internal generated code unless explicitly
  approved.
- Keep each tranche compiling and leave mycel functional.
- Use subsystem terminology in documentation.

## RM1: Close Public API and SDK Surface

Maps to remaining UP1 and UP6.

### Tasks

1. Decide final public breaking surface for this branch:
   - keep `AdminPrincipalService` as the canonical admin identity service;
   - remove or explicitly mark legacy `AdminOperatorService` and
     `AdminUserService` as temporary aliases only if deletion in this branch is
     not yet possible;
   - ensure public request/response fields use `principal_id` rather than
     `user_id` or `operator_id` for identity semantics.
2. Finish protobuf cleanup in `mycel-api`:
   - replace `PrincipalKind`/`PrincipalType` naming inconsistencies if any;
   - remove deprecated `PRINCIPAL_TYPE_USER` and `PRINCIPAL_TYPE_OPERATOR` from
     new APIs unless intentionally retained only for old APIs pending deletion;
   - rename legacy user/operator capabilities in public surfaces to
     identity/principal capabilities.
3. Regenerate public generated API/SDK code:
   - `mycel-go-sdk`;
   - `mycel-rust-sdk`;
   - mycel internal protobufs from `mycel-api`.
4. Update SDK helper APIs:
   - add principal CRUD/session/grant helpers;
   - replace `GrantSpaceUser` helpers with `GrantSpacePrincipal`;
   - keep any legacy helpers only as short-lived aliases with deprecation
     comments if needed to keep tests compiling during the refactor.

### Acceptance

- Public identity API presents one principal-oriented model.
- SDK builds/tests do not require callers to choose between admin/operator/user
  identity species.
- Public auth responses expose `principal_id`, username, and principal kind/type.

## RM2: Harden Principal Store, Sessions, WAL, Raft, and Bootstrap

Maps to remaining UP2-UP4.

### Tasks

1. Add missing principal store tests:
   - CRUD;
   - duplicate username/email handling;
   - soft delete and disabled filtering;
   - role binding grant/revoke;
   - direct capability grant/revoke;
   - list by principal and by scope if exposed;
   - last active login-enabled `system.admin` invariant.
2. Add session storage tests for principal-keyed refresh sessions:
   - create/list by principal;
   - refresh-token rotation;
   - consumed-token reuse detection;
   - revoke one/revoke all;
   - old `UserID`/`OperatorID` session paths absent from new active code.
3. Verify and test WAL replay for:
   - `identity.principal.put.v1`;
   - `identity.role_binding.put.v1`;
   - `identity.capability_grant.put.v1`;
   - `identity.principal.session.put.v1` if session WAL/Raft remains active.
4. Verify and test Raft apply/idempotency/snapshot restore for the identity
   subsystem.
5. Confirm bootstrap behavior:
   - standalone fresh data creates exactly one login-enabled human principal with
     system-scoped `system.admin`;
   - clustered/Raft startup fails closed until authoritative identity metadata is
     available or explicit bootstrap configuration is applied.
6. Remove active registration/callers for legacy identity WAL record types:
   - `identity.admin.put.v1`;
   - `identity.user.put.v1`.

### Acceptance

- Principal store/session tests cover the new model without importing old
  admin/user service packages.
- WAL replay and Raft snapshot restore recover principals, grants, and sessions.
- Fresh standalone startup uses `<data-dir>/identity/store.json` and
  `<data-dir>/identity/sessions/refresh_sessions.json` only.

## RM3: Complete Scoped Principal Authorization

Maps to remaining UP5.

### Tasks

1. Define one internal authorizer interface around principal IDs and scopes:
   - `Authorize(ctx, principalID, capability, scope)`;
   - `EffectiveAccess(ctx, principalID, scope)`.
2. Normalize capability names in one package and remove duplicated mapping logic
   from admin/client services where possible.
3. Ensure built-in roles cover:
   - `system.admin`;
   - `identity.admin`;
   - `space.admin`;
   - `space.owner`, `space.editor`, `space.viewer`;
   - service-principal roles for automation/import/semantic maintenance;
   - backup/cluster/semantic administrative roles.
4. Replace active operator-only authorization checks:
   - `HasCapability(ctx, operatorID, ...)`;
   - `principal.Kind == operator/user` checks;
   - unscoped system checks when a scoped capability is required.
5. Add authorization tests for allow/deny behavior across system, space, and
   domain scopes.

### Acceptance

- Authorization decisions use `principal_id` and scoped capabilities.
- A normal principal cannot perform management operations without grants.
- A system-admin principal is not implicitly a data-plane actor unless the
  required data-plane capability/space grant is effective.

## RM4: Finish Admin Principal Service and CLI

Maps to remaining UP6.

### Tasks

1. Complete daemon `AdminPrincipalService` tests for every method:
   - list/get/find/create/update/disable/enable/delete;
   - set password;
   - create/list/revoke sessions;
   - grant/revoke role bindings;
   - grant/revoke direct capability grants;
   - unauthorized caller denial.
2. Add CLI commands:
   - `mycel principal list|get|find|create|update|disable|enable|delete`;
   - `mycel principal password set`;
   - `mycel principal session list|revoke|revoke-all|create`;
   - `mycel principal role grant|list|revoke`;
   - `mycel principal capability grant|list|revoke`.
3. Remove or convert old CLI commands:
   - `mycel admin create` operator-specific paths;
   - `mycel user ...` paths;
   - old flag names such as `--operator-id`, `--user-id`, and
     `--owner-user-id`, except temporary deprecated aliases where needed during
     the branch.
4. Update CLI tests to exercise the principal commands as the canonical path.

### Acceptance

- CLI tests pass using principal terminology.
- Delegated session creation requires `identity.session.delegate`.
- Principal lifecycle/grants fail closed without the required identity
  capability.

## RM5: Refactor Client Auth and Data-plane Services

Maps to remaining UP7.

### Tasks

1. Refactor client `AuthService` to use the principal manager directly, not the
   user compatibility adapter.
2. Replace helper names and checks:
   - `userPrincipalFromContext` -> principal-neutral helper;
   - `spaceUserPrincipalFromContext` -> scoped authenticated principal helper;
   - remove active reliance on `PrincipalKindUser`/`PrincipalKindOperator`.
3. Update all data-plane services to pass `principal_id`:
   - session;
   - domain;
   - graph;
   - query;
   - blob;
   - metadata catalog;
   - change stream;
   - semantic search;
   - import/export;
   - automation client/admin boundaries where applicable.
4. Replace data-plane access checks with scoped authorization/effective access.
5. Refactor route forwarding to serialize only:
   - `principal_id`;
   - kind/type;
   - username;
   - auth-session ID;
   - created time.
6. Update client service tests for principal actors and deny cases.

### Acceptance

- A principal with scoped graph/query permissions can use data-plane APIs.
- A principal without scoped permissions is denied.
- Forwarded requests preserve `principal_id` across nodes.

## RM6: Refactor Space, Domain, Session, ACL, and Origin Models

Maps to remaining UP8.

### Tasks

1. Replace active space service names and types:
   - `OwnerUserID` -> `OwnerPrincipalID`;
   - `GrantSpaceUser` -> `GrantSpacePrincipal`;
   - `EffectiveAccess(userID, ...)` -> `EffectiveAccess(principalID, ...)`.
2. Replace `identity.UserID` ownership/access fields with principal identifiers
   in:
   - `internal/space/model`;
   - space storage;
   - ACL storage;
   - access grants;
   - admin/client mapping helpers.
3. Rename WAL/Raft space access records where compatibility-free removal is
   acceptable:
   - `space.acl.grant.v1` or legacy `GrantSpaceUser` record names to
     `space.access.grant.v1` / principal-oriented payloads.
4. Replace session service fields:
   - `OpenSessionInput.UserID` -> `PrincipalID`;
   - `GraphSession.UserID` -> `PrincipalID`;
   - transaction and route records likewise.
5. Update graph-change origin metadata to record the authenticated principal ID
   where the field is actor identity.
6. Update semantic service fields that currently use `identity.UserID` for actor
   identity to principal identifiers.
7. Port tests and fixtures from user IDs to principal IDs.

### Acceptance

- Spaces are owned by principals.
- Space access grants target principals.
- Sessions and transactions are scoped to principals.
- Graph-change origin metadata records the authenticated principal ID.

## RM7: Remove Split Identity Runtime and Dead Code

Maps to remaining UP9.

### Tasks

1. Delete active runtime dependencies on:
   - `internal/identity/service/admin`;
   - `internal/identity/service/user`.
2. Remove compatibility adapters once all callers have moved to the principal
   manager.
3. Delete or port old admin/user service tests.
4. Remove old split store bootstrap paths and directory creation.
5. Delete stale API mapping helpers for `Admin`, `User`, `OperatorID`, and
   `UserID` after public API/CLI cleanup.
6. Run and resolve active-code searches for:
   - `identity/service/admin`;
   - `identity/service/user`;
   - `identity.admin.put.v1`;
   - `identity.user.put.v1`;
   - `PrincipalKindOperator`;
   - `PrincipalKindUser`;
   - `OperatorID`;
   - `UserID`;
   - `OwnerUserID`;
   - `GrantSpaceUser`.

### Acceptance

- Active runtime has one identity subsystem module.
- Fresh data directories do not create `admins/` or `users/` identity stores.
- Old split-identity packages are gone or retained only in explicitly marked
  historical tests/docs outside active runtime.

## RM8: Documentation and Operational Cleanup

Maps to remaining UP10.

### Tasks

1. Update identity docs:
   - access control;
   - auth;
   - admin auth;
   - operator/user docs replaced or marked superseded;
   - space/domain access docs.
2. Update operations docs and CLI examples:
   - principal lifecycle;
   - role/capability grants;
   - bootstrap credentials;
   - backup/restore references to owner principal IDs.
3. Update SDK READMEs and examples.
4. Update implementation plan status notes to point from the original UP1-UP11
   plan to this remaining-work plan.

### Acceptance

- Documentation describes one principal model.
- mycel management authority is described as roles/capabilities on principals.
- `make docs-check` passes.

## RM9: Final Validation Matrix

Maps to remaining UP11.

Run at minimum:

```sh
cd mycel
MYCEL_API_ROOT=../mycel-api make test
go test ./internal/identity/... ./internal/space/... ./internal/session/... ./internal/daemon/auth ./internal/daemon/api/admin ./internal/daemon/api/client
go test ./internal/daemon/app ./internal/daemon/server ./internal/clustering/... ./internal/semantic/...
make docs-check
git diff --check

cd ../mycel-api
make test
git diff --check

cd ../mycel-go-sdk
MYCEL_API_ROOT=../mycel-api make test
git diff --check

cd ../mycel-rust-sdk
MYCEL_API_ROOT=/Users/martinbeauvais/Projects/knotbase/Knotbase/myceldb/mycel-api cargo test
git diff --check
```

Add or update integration tests proving:

1. fresh standalone bootstrap creates one system-admin principal;
2. normal principal login/refresh/logout;
3. system-admin principal uses the same auth path;
4. one principal can be both a data-plane actor and system administrator;
5. admin operation denied without required capability;
6. space read/write denied without scoped role;
7. space read/write allowed with scoped role;
8. last-system-admin disable/delete/grant-revoke is rejected;
9. delegated session creation requires `identity.session.delegate`;
10. WAL replay restores principals and grants;
11. Raft snapshot restore restores principals and grants;
12. route forwarding preserves `principal_id`;
13. service principal can perform a narrowly scoped automation or semantic
    maintenance-style operation when granted the correct role.

## Suggested Execution Order

1. RM2 store/session/WAL/Raft hardening, while compatibility adapters still make
   the daemon easy to validate.
2. RM4 complete `AdminPrincipalService` and principal CLI.
3. RM5/RM6 data-plane and space/session/access refactor.
4. RM1 final public API/SDK deletion/cleanup once active code has moved.
5. RM7 remove old split identity packages and compatibility adapters.
6. RM8 docs.
7. RM9 full validation.
