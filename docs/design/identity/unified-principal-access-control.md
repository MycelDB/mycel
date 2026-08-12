# Unified Principal Identity and Access-Control Model

## Status

Proposed breaking design for the next mycel identity subsystem iteration. This
is intended for a fresh environment reset; no compatibility migration from the
current split administrator/user stores is required.

## Summary

mycel should use one logical identity model for all actors:

```text
principal + role bindings + direct capability grants + scoped authorization
```

A standard application user and a system administrator are both principals.
System management is not a separate identity species; it is represented by roles
and capabilities granted at system scope. A principal with the `system.admin`
role can administer mycel. A principal without system-management capabilities is
a normal data-plane user.

This replaces the current split model:

```text
admins/admins.json        -> Admin records and operator grants
users/users.json          -> User records
admins/sessions/          -> operator refresh sessions
users/sessions/           -> user refresh sessions
```

with one identity subsystem store:

```text
identity/store.json       -> principals, role bindings, capability grants
identity/sessions/        -> refresh sessions keyed by principal_id
```

The physical store can evolve later, but the logical model should remain one
principal catalog.

## Existing Code Review

The current implementation intentionally separates administrator/operator
identity from standard user identity.

| Area | Current code | Current behavior |
|---|---|---|
| Administrator subsystem | `internal/identity/service/admin` | Stores `Admin` records, operator roles, direct capability grants, operator sessions, and bootstrap system admin state. |
| Standard user subsystem | `internal/identity/service/user` | Stores simpler `User` records and user auth sessions. No roles or capabilities are attached to users. |
| Storage | `internal/identity/service/admin/store.go`, `internal/identity/service/user/store.go` | Two separate JSON stores: `<data-dir>/admins/admins.json` and `<data-dir>/users/users.json`. |
| Sessions | `internal/identity/storage/session` | Shared session machinery, but mounted separately under `admins/sessions` and `users/sessions`. |
| WAL/raft records | `identity.admin.put.v1`, `identity.user.put.v1` | Administrator and user records have different authoritative record types and state-machine plumbing. |
| Access tokens | `internal/daemon/auth/token.go` | Token payload distinguishes `PrincipalKindOperator` with `OperatorID` from `PrincipalKindUser` with `UserID`. |
| Client login | `internal/daemon/api/client/auth_service.go` | Authenticates only standard users through the user manager. |
| Admin login | `internal/daemon/api/admin/auth_service.go` | Authenticates only operators through the admin manager. |
| User management | `internal/daemon/api/admin/user_service.go` | Operators with user-management capabilities manage standard users. |
| Operator management | `internal/daemon/api/admin/operator_service.go` | System admins manage operators, roles, capabilities, and operator sessions. |
| Space ownership/access | `internal/space/service/types.go` | Uses user-centric fields such as `OwnerUserID`, `GrantSpaceUser`, and `EffectiveAccess(ctx, userID, ...)`. |
| Public access model | `mycel.common.v1.access.proto` | Has `PRINCIPAL_TYPE_USER`, `PRINCIPAL_TYPE_OPERATOR`, and `PRINCIPAL_TYPE_SYSTEM`. |

This model works, but it duplicates identity lifecycle paths and makes
administrator behavior a separate storage concern rather than an authorization
outcome.

## Problem

The split model has several long-term costs:

1. **Duplicated lifecycle code**: administrators and users both need password
   hashing, login, refresh sessions, disable/delete, password reset, and session
   revocation.
2. **Two identity catalogs**: the daemon must answer questions such as "who is
   this actor?" by first checking whether the token contains an operator ID or a
   user ID.
3. **Artificial product boundary**: a human who uses data-plane APIs and also
   manages mycel must either have two records or be represented as an operator
   only for admin APIs.
4. **Service account ambiguity**: automation workers, semantic maintenance,
   import jobs, and future application service accounts need capabilities, but
   neither `Admin` nor `User` is the right conceptual model.
5. **Space ownership is too user-specific**: ownership and access grants should
   attach to any eligible principal, not only to records in the user store.
6. **Divergence from common database practice**: systems such as PostgreSQL use
   one principal/role catalog and represent administrator authority through
   privileges rather than a separate administrator table.

## Goals

- Replace split administrator/user stores with one principal model.
- Represent system management through roles and capabilities.
- Use scoped role bindings and capability grants for system, space, and domain
  authorization.
- Support human principals, service principals, and reserved system principals.
- Make access tokens carry one principal identifier.
- Make all authorization decisions capability-based.
- Preserve the existing principle that roles are convenience bundles; effective
  capabilities are authoritative.
- Support fresh-environment reset with no legacy compatibility or migration
  scaffolding.
- Keep profile/business data out of daemon identity records; applications can
  still model rich profiles as graph data.

## Non-Goals

- No compatibility layer for the old admin/user stores.
- No automatic migration from `<data-dir>/admins` or `<data-dir>/users`.
- No external OIDC/SAML integration in this tranche.
- No groups, teams, or organizations in the first implementation.
- No explicit deny rules in the first implementation.
- No fine-grained node/subtree ACL model in this tranche.
- No automatic repair or authoritative-principal selection if identity data is
  corrupted or divergent.

## Target Model

### Principal

A principal is any actor that can authenticate, own resources, or receive access.

```go
type Principal struct {
    ID           string
    Username     string
    Email        string
    DisplayName  string
    Kind         PrincipalKind // human | service | system
    State        PrincipalState // active | disabled | deleted
    LoginEnabled bool
    PasswordHash string // optional; omitted for non-password principals
    CreatedAt    time.Time
    UpdatedAt    time.Time
    CreatedBy    string
}
```

Recommended principal kinds:

| Kind | Meaning |
|---|---|
| `human` | Interactive person. Can be a normal application user, an administrator, or both depending on grants. |
| `service` | Application, automation worker, importer, semantic maintenance actor, or integration. May authenticate through service credentials in a later tranche. |
| `system` | Reserved internal daemon/system actor. Not normally login-enabled. |

Important rules:

- `Kind` is descriptive and operational. It is not enough for authorization.
- A human principal becomes an administrator only by receiving system-scoped
  management capabilities or a role that expands to them.
- A service principal can receive narrowly scoped capabilities for automation,
  imports, semantic maintenance, or backup work.
- Disabled and deleted principals cannot authenticate.
- Deleted principals are soft-deleted so audit/history references remain valid.

### Role Binding

A role binding grants a built-in role to a principal at a scope.

```go
type RoleBinding struct {
    ID          string
    PrincipalID string
    Role        string
    Scope       AccessScope
    State       GrantState // active | revoked
    Reason      string
    CreatedBy   string
    CreatedAt   time.Time
    RevokedBy   string
    RevokedAt   time.Time
}
```

Roles are convenience bundles. The daemon expands active role bindings into
capabilities and enforces capabilities, not role names.

### Capability Grant

A direct capability grant gives one explicit capability to a principal at a
scope.

```go
type CapabilityGrant struct {
    ID          string
    PrincipalID string
    Capability  string
    Scope       AccessScope
    State       GrantState // active | revoked
    Reason      string
    CreatedBy   string
    CreatedAt   time.Time
    RevokedBy   string
    RevokedAt   time.Time
}
```

Capability names should be stored internally as canonical strings such as
`identity.principal.create` or `graph.read`. Public protobuf APIs can expose
built-in capabilities as enums or strings, but the store should not require a
schema migration every time a new capability is added.

### Access Scope

```go
type AccessScope struct {
    Type     string // system | space | domain
    SpaceID  string
    DomainID string
}
```

Scope rules:

- `system` scope authorizes daemon/system/control-plane operations.
- `space` scope authorizes operations on one space and, where defined, its
  domains.
- `domain` scope authorizes one domain inside a space.
- Future finer scopes can be added later, but the first implementation should
  stay with system/space/domain.

### Auth Session

Refresh sessions should be associated with `principal_id`, not with separate
operator/user IDs.

```go
type RefreshSession struct {
    ID          uuid.UUID
    PrincipalID string
    Status      string
    Metadata    RefreshSessionMetadata
    CreatedAt   time.Time
    LastUsedAt  time.Time
    IdleExpiresAt     time.Time
    AbsoluteExpiresAt time.Time
}
```

The existing refresh-token rotation and reuse-detection behavior should be kept.
Only the owner key changes from user/operator-specific IDs to `principal_id`.

## Built-In Roles

Initial built-in roles should be small and explicit. Suggested roles:

### System roles

| Role | Scope | Purpose |
|---|---|---|
| `system.admin` | system | Full mycel administration. Bootstrap principals receive this role. |
| `identity.admin` | system | Manage principals, credentials, sessions, role bindings, and direct capability grants. |
| `space.admin` | system | Create/archive/delete spaces and manage space-level access. |
| `cluster.operator` | system | Inspect/manage clustering and subsystem health. |
| `backup.operator` | system | Manage backup/quiesce/restore operations. Restore remains offline/operator-driven. |
| `semantic.admin` | system or space | Configure semantic providers, grants, policies, indexes, and maintenance. |
| `automation.admin` | system or space | Manage graph automation definitions, runs, and policies. |
| `audit.reader` | system | Read audit and diagnostic information. |

### Space roles

| Role | Scope | Effective capabilities |
|---|---|---|
| `space.owner` | space | Read/update/manage access, domain read/create/update/delete, graph/blob/query/metadata read/write/delete where allowed. |
| `space.admin` | space | Manage access and mutable space/domain settings, but not necessarily transfer ownership. |
| `space.editor` | space | Read/write graph, query, metadata, blob write, semantic search where enabled. |
| `space.viewer` | space | Read space/domain/graph/blob/metadata and run allowed read queries. |

### Service roles

| Role | Scope | Purpose |
|---|---|---|
| `automation.worker` | space or domain | Run automation actions with explicit graph/query capabilities. |
| `semantic.maintenance` | space or domain | Analyze/process semantic dirty work and update semantic subsystem records. |
| `import.worker` | space or domain | Import graph/blob data for a bounded scope. |

## Capability Names

The first implementation should replace user/operator-specific capability names
with principal/identity names. Suggested capability namespaces:

### Identity capabilities

| Capability | Meaning |
|---|---|
| `identity.principal.read` | List/find/read principal records. |
| `identity.principal.create` | Create principals. |
| `identity.principal.update` | Update mutable principal metadata. |
| `identity.principal.disable` | Disable/enable principals. |
| `identity.principal.delete` | Soft-delete principals. |
| `identity.credential.set` | Set or rotate password/credential material. |
| `identity.session.manage` | List/revoke sessions for other principals. |
| `identity.session.delegate` | Create delegated sessions for another principal. |
| `identity.grant.manage` | Grant/revoke roles and direct capabilities. |

### System/control capabilities

| Capability | Meaning |
|---|---|
| `daemon.configure` | Change daemon configuration where supported. |
| `cluster.read` | Inspect cluster/subsystem status. |
| `cluster.manage` | Manage cluster metadata and operational controls. |
| `backup.manage` | Manage backups and quiesce operations. |
| `audit.read` | Read audit/diagnostic information. |

### Data-plane capabilities

Existing data-plane concepts remain valid, but they should attach to any
principal:

- `space.read`
- `space.update`
- `space.manage_access`
- `space.create`
- `space.archive`
- `space.delete`
- `domain.read`
- `domain.create`
- `domain.update`
- `domain.delete`
- `graph.read`
- `graph.write`
- `graph.delete`
- `query.run`
- `blob.read`
- `blob.write`
- `blob.delete`
- `metadata.read`
- `metadata.write`
- `semantic.search`
- `semantic.manage`
- `automation.manage`
- `automation.run`

## Authorization Evaluation

Every protected daemon operation should evaluate:

```text
principal + requested capability + requested scope -> allow/deny
```

Evaluation algorithm:

1. Verify the access token signature and expiry.
2. Resolve the authenticated `principal_id`.
3. Require the principal to exist and be active.
4. For session-bound operations, require the auth session to be active when the
   subsystem has access to the session store; otherwise revocation takes effect
   no later than refresh/access-token expiry.
5. Build effective capabilities from:
   - active direct capability grants;
   - active role bindings expanded through built-in role definitions;
   - ownership-derived capabilities for the requested resource;
   - reserved system-principal capabilities for internal daemon work.
6. Match capability and scope.
7. Deny by default.

No explicit deny rules are included in the first implementation.

### Last System Administrator Rule

The daemon must reject operations that would leave mycel without at least one
active, login-enabled principal with an active system-scoped `system.admin` role
or equivalent full system capability.

This applies to:

- disabling the last system administrator;
- deleting the last system administrator;
- revoking the last system administrator role;
- revoking direct capabilities if direct grants are used to satisfy the invariant;
- disabling login for the last system administrator;
- setting an unusable credential for the last system administrator.

## API Shape

Because compatibility is not required, the API should be simplified instead of
wrapping the old operator/user split.

### Auth API

Use one auth service for all login-capable principals:

```protobuf
service AuthService {
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Refresh(RefreshRequest) returns (RefreshResponse);
  rpc Logout(LogoutRequest) returns (LogoutResponse);
  rpc WhoAmI(WhoAmIRequest) returns (WhoAmIResponse);
  rpc ListAuthSessions(ListAuthSessionsRequest) returns (ListAuthSessionsResponse);
  rpc RevokeAuthSession(RevokeAuthSessionRequest) returns (RevokeAuthSessionResponse);
  rpc RevokeOtherAuthSessions(RevokeOtherAuthSessionsRequest) returns (RevokeOtherAuthSessionsResponse);
}
```

`WhoAmI` should return:

```protobuf
message AuthPrincipal {
  string principal_id = 1;
  string username = 2;
  PrincipalKind kind = 3;
}
```

The old split fields should be removed from token payloads and public auth
responses:

```text
operator_id
user_id
PrincipalKindOperator
PrincipalKindUser
```

### Admin Principal API

Replace `AdminOperatorService` and `AdminUserService` with one principal
management service:

```protobuf
service AdminPrincipalService {
  rpc ListPrincipals(ListPrincipalsRequest) returns (ListPrincipalsResponse);
  rpc GetPrincipal(GetPrincipalRequest) returns (GetPrincipalResponse);
  rpc FindPrincipal(FindPrincipalRequest) returns (FindPrincipalResponse);
  rpc CreatePrincipal(CreatePrincipalRequest) returns (CreatePrincipalResponse);
  rpc UpdatePrincipal(UpdatePrincipalRequest) returns (UpdatePrincipalResponse);
  rpc DisablePrincipal(DisablePrincipalRequest) returns (DisablePrincipalResponse);
  rpc EnablePrincipal(EnablePrincipalRequest) returns (EnablePrincipalResponse);
  rpc DeletePrincipal(DeletePrincipalRequest) returns (DeletePrincipalResponse);
  rpc SetPrincipalPassword(SetPrincipalPasswordRequest) returns (SetPrincipalPasswordResponse);
  rpc ListPrincipalRoles(ListPrincipalRolesRequest) returns (ListPrincipalRolesResponse);
  rpc GrantPrincipalRole(GrantPrincipalRoleRequest) returns (GrantPrincipalRoleResponse);
  rpc RevokePrincipalRole(RevokePrincipalRoleRequest) returns (RevokePrincipalRoleResponse);
  rpc ListPrincipalCapabilities(ListPrincipalCapabilitiesRequest) returns (ListPrincipalCapabilitiesResponse);
  rpc GrantPrincipalCapability(GrantPrincipalCapabilityRequest) returns (GrantPrincipalCapabilityResponse);
  rpc RevokePrincipalCapability(RevokePrincipalCapabilityRequest) returns (RevokePrincipalCapabilityResponse);
  rpc ListPrincipalSessions(ListPrincipalSessionsRequest) returns (ListPrincipalSessionsResponse);
  rpc RevokePrincipalSession(RevokePrincipalSessionRequest) returns (RevokePrincipalSessionResponse);
  rpc RevokePrincipalSessions(RevokePrincipalSessionsRequest) returns (RevokePrincipalSessionsResponse);
  rpc CreatePrincipalSession(CreatePrincipalSessionRequest) returns (CreatePrincipalSessionResponse);
}
```

Authorization examples:

| Operation | Required capability |
|---|---|
| List/get/find principals | `identity.principal.read` |
| Create principal | `identity.principal.create` |
| Update/disable/delete principal | `identity.principal.update` or specific lifecycle capability |
| Set another principal's password | `identity.credential.set` |
| Manage another principal's sessions | `identity.session.manage` |
| Create delegated session | `identity.session.delegate` |
| Grant/revoke roles/capabilities | `identity.grant.manage` |

A principal can always manage its own current auth session through the Auth API
without an admin capability.

### Client/Data APIs

Client graph/session/query/blob APIs should stop checking for a user principal
specifically. They should require an authenticated active principal and the
appropriate scoped capability.

Examples:

```text
OpenSession(space_id, domain_id) requires domain.read and graph/query capability depending on use.
Graph read operations require graph.read on the target scope.
Graph write operations require graph.write on the target scope.
Delete operations require graph.delete/blob.delete/domain.delete as applicable.
```

## Space Ownership and Access

Space ownership should move from user-specific fields to principal references.

Current shapes such as:

```go
OwnerUserID identity.UserID
GrantSpaceUser(ctx, spaceID, userID, role)
EffectiveAccess(ctx, userID, space)
```

should become:

```go
OwnerPrincipalID string
GrantSpacePrincipal(ctx, spaceID, principalID, role)
EffectiveAccess(ctx, principalID, scope)
```

A space owner is a principal with ownership accountability. Owner-derived
capabilities can be implemented as an implicit `space.owner` role at that space
scope. Ownership is still distinct from a normal revocable grant: transferring
ownership should be an explicit operation, not merely revoking a role binding.

System-owned spaces should be owned by a reserved system principal, not by a
special owner enum that bypasses the principal model.

## Storage and Replication

### File layout

Initial file-backed layout:

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

The store should maintain lookup indexes by:

- principal ID;
- normalized username;
- normalized email when present;
- active grants by principal;
- grants by scope.

### WAL/raft records

Replace split records:

```text
identity.admin.put.v1
identity.user.put.v1
```

with unified records:

```text
identity.principal.put.v1
identity.role_binding.put.v1
identity.capability_grant.put.v1
```

Soft deletes and revocations can be represented as `put` records with state set
to `deleted` or `revoked`. This keeps replay and snapshots simple and preserves
history.

The identity subsystem's raft/WAL state remains authoritative for principals and
grants. Refresh sessions are authentication state; they may remain daemon-local
unless mycel later requires cluster-wide refresh-session roaming.

### Snapshots

Identity subsystem snapshots should include:

- principals;
- role bindings;
- capability grants;
- applied WAL/raft progress;
- enough metadata to validate the last-system-admin invariant on restore.

Snapshots must not include plaintext passwords or refresh tokens.

## Bootstrap

On a fresh standalone data directory, mycel should create one bootstrap human
principal when no active system administrator exists.

Bootstrap principal:

```text
kind: human
login_enabled: true
role: system.admin at system scope
```

Credential source:

1. configured bootstrap username/password when present;
2. generated development credential for standalone development only, logged once
   with the same security warning as today.

In clustered mode, system raft metadata and identity subsystem readiness should
fail closed until an active system administrator exists or explicit bootstrap
configuration is applied. The daemon should not invent authoritative principals
on arbitrary nodes.

## Service Accounts and Internal Actors

Service principals should be first-class records. Examples:

- automation worker;
- semantic maintenance worker;
- import worker;
- backup worker;
- application integration.

Service principals should receive narrowly scoped roles or direct capabilities.
For example:

```text
principal: automation-worker
kind: service
role: automation.worker at domain scope
capabilities: graph.read, graph.write, query.run for that domain
```

Internal daemon work can use reserved system principals. These principals should
not be login-enabled and should not use password credentials.

## Security Requirements

- Password and refresh-token plaintext must never be persisted.
- Refresh tokens remain rotated and reuse-detected.
- Access tokens remain short-lived.
- Audit logs must not include passwords, password hashes, refresh-token
  plaintext, or refresh-token hashes.
- Authorization must check capabilities on every protected RPC.
- Login must reject disabled/deleted principals and principals with
  `login_enabled=false`.
- Role/capability grant changes must be auditable.
- The last-system-administrator invariant must be enforced before committing
  lifecycle or grant changes.

## Implementation Plan

1. **Model/store**
   - Add `internal/identity/service/principal` or replace the current user/admin
     modules with a unified identity manager.
   - Add store tests for principal uniqueness, role/capability grants,
     revocation, and last-system-admin validation.

2. **WAL/raft**
   - Add unified identity raft/WAL records.
   - Remove `identity.admin.put.v1` and `identity.user.put.v1` from active code.
   - Add snapshot coverage for the unified identity subsystem.

3. **Auth tokens and sessions**
   - Change token payload to `principal_id`.
   - Replace `PrincipalKindOperator/User` with `PrincipalKindHuman/Service/System`
     or a neutral principal kind enum.
   - Reuse the existing refresh-session store with `principal_id` ownership.

4. **Public protobuf/API**
   - Replace user/operator identity APIs with a principal API.
   - Update common access protobuf types to remove `PRINCIPAL_TYPE_OPERATOR` and
     standardize on unified principal kinds.
   - Rename user/operator capability enums or replace them with string
     capability identifiers.
   - Regenerate public SDKs when API contracts change.

5. **Daemon service authorization**
   - Introduce a shared authorizer:

     ```go
     Authorize(ctx, principalID, capability, scope) error
     ```

   - Replace operator-only and user-only context helpers with principal helpers.
   - Refactor admin services to require capabilities instead of operator kind.
   - Refactor client services to require scoped capabilities instead of user kind.

6. **Space/domain access**
   - Replace user-specific owner/grant APIs with principal-based APIs.
   - Store owner as `principal_id`.
   - Compute owner-derived capabilities as implicit scoped access.

7. **Delete old split code**
   - Remove `internal/identity/service/admin` and
     `internal/identity/service/user` after replacement.
   - Remove separate admin/user session directories from runtime initialization.
   - Remove old admin/user CLI commands or rewrite them as principal commands.

8. **Docs/tests**
   - Update identity, admin auth, admin user/operator, client auth, and access
     control docs to point to the unified model.
   - Add integration tests for:
     - bootstrap system admin;
     - normal login;
     - system-admin login;
     - principal with both data-plane and system roles;
     - scoped space access;
     - denied admin operation without system role;
     - last-system-admin protection;
     - delegated session creation;
     - raft/WAL replay of principals and grants.

## Acceptance Criteria

The unified identity model is accepted when:

- there is one authoritative principal store for humans, services, and reserved
  system actors;
- a principal can be both a normal data-plane user and an administrator through
  role/capability grants;
- tokens carry `principal_id` rather than user/operator-specific IDs;
- admin/control-plane services authorize by capabilities, not by operator kind;
- client/data-plane services authorize by scoped capabilities, not by user kind;
- spaces are owned by principals, not only by users;
- old admin/user stores and active modules are removed;
- fresh standalone bootstrap creates exactly one active system administrator when
  needed;
- tests prove login, authorization, grant management, last-admin protection,
  and raft/WAL replay.
