# Admin Operator API

## Status

Implemented initial daemon-oriented Admin Operator API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/operator.proto
```

This document depends on:

```text
docs/design/grpc-admin-auth.md
docs/design/admin/user.md
```

## Purpose

`AdminOperatorService` manages system admins/operators.

Operators are fundamentally different from standard users. Standard users are managed by `AdminUserService`; operators are managed by this service.

Operators administer the daemon/control plane. They may have built-in operator roles and/or direct admin capability grants.

## Scope

`AdminOperatorService` includes:

- list operators
- get operator
- find operator by username/email
- create operator
- update operator display metadata
- disable/enable operator
- soft-delete operator
- set operator password
- list/grant/revoke operator roles
- list/grant/revoke operator capabilities
- list/revoke operator auth sessions

`AdminOperatorService` does not include:

- standard user creation
- standard user management
- standard user space access grants
- Client API auth/session APIs
- mesh peer identity
- system internal principal creation

## Users vs operators

Standard users and operators are separate principal types.

- Standard users use product/client functionality and can own/use spaces.
- Operators administer the daemon/system/control plane.

Creating an operator does not create a standard user and does not grant normal space access.

## Service definition

```protobuf
service AdminOperatorService {
  rpc ListOperators(ListOperatorsRequest) returns (ListOperatorsResponse);
  rpc GetOperator(GetOperatorRequest) returns (GetOperatorResponse);
  rpc FindOperator(FindOperatorRequest) returns (FindOperatorResponse);

  rpc CreateOperator(CreateOperatorRequest) returns (CreateOperatorResponse);
  rpc UpdateOperator(UpdateOperatorRequest) returns (UpdateOperatorResponse);

  rpc DisableOperator(DisableOperatorRequest) returns (DisableOperatorResponse);
  rpc EnableOperator(EnableOperatorRequest) returns (EnableOperatorResponse);
  rpc DeleteOperator(DeleteOperatorRequest) returns (DeleteOperatorResponse);

  rpc SetOperatorPassword(SetOperatorPasswordRequest) returns (SetOperatorPasswordResponse);

  rpc ListOperatorRoles(ListOperatorRolesRequest) returns (ListOperatorRolesResponse);
  rpc GrantOperatorRole(GrantOperatorRoleRequest) returns (GrantOperatorRoleResponse);
  rpc RevokeOperatorRole(RevokeOperatorRoleRequest) returns (RevokeOperatorRoleResponse);

  rpc ListOperatorCapabilities(ListOperatorCapabilitiesRequest) returns (ListOperatorCapabilitiesResponse);
  rpc GrantOperatorCapability(GrantOperatorCapabilityRequest) returns (GrantOperatorCapabilityResponse);
  rpc RevokeOperatorCapability(RevokeOperatorCapabilityRequest) returns (RevokeOperatorCapabilityResponse);

  rpc ListOperatorSessions(ListOperatorSessionsRequest) returns (ListOperatorSessionsResponse);
  rpc RevokeOperatorSession(RevokeOperatorSessionRequest) returns (RevokeOperatorSessionResponse);
  rpc RevokeOperatorSessions(RevokeOperatorSessionsRequest) returns (RevokeOperatorSessionsResponse);
}
```

## Operator model

Protobuf shape:

```protobuf
message Operator {
  string operator_id = 1;
  string username = 2;
  string display_name = 3; // not persisted in the initial daemon store
  string email = 4;        // optional, persisted
  OperatorState state = 5;
  google.protobuf.Timestamp create_time = 6;
  google.protobuf.Timestamp update_time = 7;
}
```

Supported states:

```protobuf
enum OperatorState {
  OPERATOR_STATE_UNSPECIFIED = 0;
  OPERATOR_STATE_ACTIVE = 1;
  OPERATOR_STATE_DISABLED = 2;
  OPERATOR_STATE_DELETED = 3;
}
```

## CreateOperator

Creates a system admin/operator. The current implementation requires a password in the request; it does not generate or log credentials for non-bootstrap operators.

```protobuf
message CreateOperatorRequest {
  string username = 1;
  string display_name = 2;
  string email = 3;
  optional string password = 4;
  repeated OperatorRole roles = 5;
  repeated InitialOperatorCapabilityGrant capability_grants = 6;
  bool disabled = 7;
}
```

Response:

```protobuf
message CreateOperatorResponse {
  Operator operator = 1;
  repeated OperatorRoleGrant role_grants = 2;
  repeated OperatorCapabilityGrant capability_grants = 3;
  repeated Capability effective_capabilities = 4;
}
```

Behavior:

- creates an operator, not a standard user
- does not create spaces
- does not grant normal space access
- hashes password before storage if supplied
- never returns password plaintext or password hash
- creates an active operator by default unless `disabled` is true
- assigns initial built-in operator roles
- assigns optional direct capability grants
- computes effective capabilities
- requires the caller to be an active `SYSTEM_ADMIN`

Authorization in this slice:

```text
active SYSTEM_ADMIN operator
```

## Operator roles

Operator roles are built-in admin/control-plane role bundles. They are distinct from space roles such as owner/admin/writer/reader.

Recommended built-in roles:

| Role | Purpose |
| --- | --- |
| `SYSTEM_ADMIN` | Full daemon/system administration. |
| `USER_ADMIN` | Manage standard users. |
| `SPACE_ADMIN` | Create/archive/delete spaces and manage space-level admin operations. |
| `SEMANTIC_ADMIN` | Configure semantic indexes, providers, credentials/policies, maintenance, and backfill. |
| `STORAGE_ADMIN` | Inspect/manage storage, compaction, and disk maintenance. |
| `MESH_ADMIN` | Manage mesh membership/configuration. |
| `AUDIT_READER` | Read audit events. |

Roles are convenience bundles. The daemon enforces effective capabilities.

## Direct capability grants

Operators may receive direct capability grants in addition to roles.

Direct grants are useful when a role bundle is too broad.

Grant shape:

```protobuf
message OperatorCapabilityGrant {
  string capability_grant_id = 1;
  string operator_id = 2;
  Capability capability = 3;
  AccessScope scope = 4;
  string reason = 5;
  string granted_by_operator_id = 6;
  google.protobuf.Timestamp create_time = 7;
}
```

## Role and capability management

`GrantOperatorRole` and `GrantOperatorCapability` add new grants.

`RevokeOperatorRole` and `RevokeOperatorCapability` revoke existing grants by grant id.

The response returns updated effective capabilities where useful so admin UIs can refresh state without an extra call.

Authorization in this slice:

```text
active SYSTEM_ADMIN operator
```

The daemon rejects revoking the final active `SYSTEM_ADMIN` role.

## UpdateOperator

Updates mutable operator display metadata.

Mutable fields in the current implementation:

- `email`

`display_name` is present in the protobuf but not persisted in the initial daemon store. Username is immutable.

Authorization in this slice:

```text
active SYSTEM_ADMIN operator
```

## DisableOperator and EnableOperator

Disabling an operator:

- prevents new operator login
- preserves identity for audit and historical references
- rejects disabling the last active system admin

Authorization in this slice:

```text
active SYSTEM_ADMIN operator
```

## DeleteOperator

`DeleteOperator` is a soft delete in v1.

Behavior:

- marks the operator deleted
- prevents login
- optionally revokes sessions
- preserves identity for audit/history

Deletion rejects attempts that would remove the last active system admin/operator.

Authorization in this slice:

```text
active SYSTEM_ADMIN operator
```

## SetOperatorPassword

Sets/resets an operator password.

Behavior:

- hashes password before storage
- never returns password plaintext or password hash
- optionally revokes sessions
- audits the password change without logging the password

Authorization in this slice:

```text
self-service for authenticated operator, or active SYSTEM_ADMIN for another operator
```

## Operator auth sessions

Operator session management is separate from standard user session management.

Current behavior:

- `LoginOperator` creates a durable operator auth session and returns a short-lived access token plus refresh token.
- `RefreshOperator` rotates the refresh token, updates the session, and returns a new access token.
- `LogoutOperator` revokes the current operator auth session by default.
- `ListOperatorSessions` returns persisted operator auth sessions with coarse client metadata.
- `RevokeOperatorSession` revokes one operator auth session.
- `RevokeOperatorSessions` revokes active sessions for an operator.
- Disable/delete/password-change operations revoke sessions when requested.

Session responses must expose coarse metadata only and must never expose refresh token plaintext, refresh token hashes, consumed refresh-token hashes, raw request metadata hashes, or secrets.

## Bootstrap

The first operator cannot be created through an authenticated admin call unless an operator already exists.

Current bootstrap behavior: standalone daemon initialization creates one active `admin` operator with a random password and a `SYSTEM_ADMIN` role when the admin store is empty. The plaintext password is logged once with an explicit change-password warning and is never stored as plaintext.

## Authorization

Current coarse authorization mapping:

| Operation | Required authorization |
| --- | --- |
| `ListOperators`, `GetOperator`, `FindOperator`, `ListOperatorRoles`, `ListOperatorCapabilities`, operator session list/revoke RPCs | Any authenticated active operator |
| `SetOperatorPassword` for self | Authenticated active operator |
| Create/update/disable/enable/delete another operator, grant/revoke roles, grant/revoke capabilities, set another operator password | Active `SYSTEM_ADMIN` operator |

Role/capability values are persisted and effective capabilities are returned, but enforcement currently uses the active `SYSTEM_ADMIN` role as the coarse mutation gate.

Future versions may split `operator.manage` into finer capabilities:

- `operator.read`
- `operator.update`
- `operator.disable`
- `operator.delete`
- `operator.password.set`
- `operator.role.manage`
- `operator.capability.manage`
- `operator.session.manage`

## Audit requirements

Audit events should be emitted for:

- create operator
- update operator
- disable operator
- enable operator
- delete operator
- set operator password
- grant operator role
- revoke operator role
- grant operator capability
- revoke operator capability
- revoke operator session
- revoke all operator sessions
- bootstrap first operator

Audit records must not include:

- passwords
- password hashes
- refresh token plaintext
- refresh token hashes
- secrets

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid operator auth | `UNAUTHENTICATED` |
| missing operator capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| duplicate username/email | `ALREADY_EXISTS` |
| operator not found | `NOT_FOUND` |
| username update attempted | `FAILED_PRECONDITION` |
| deleting/disabling last system admin or revoking final `SYSTEM_ADMIN` role | `FAILED_PRECONDITION` |
| bootstrap disabled | `FAILED_PRECONDITION` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Operator identities, role grants, and capability grants are security-critical control-plane metadata. In mesh mode, they must replicate consistently enough for every daemon to enforce the same admin authorization model.

Operator auth sessions may be local or replicated depending on future auth/session architecture, but disabled/deleted operator state must be enforced consistently across the mesh.

## Current CLI commands

All commands authenticate with root flags `-u/--username` and `-p/--password`; command-specific usernames use `--operator-username` to avoid colliding with the login username.

```sh
mycel -u admin -p '<password>' admin list
mycel -u admin -p '<password>' admin get --operator-id '<id>'
mycel -u admin -p '<password>' admin find --operator-username '<username>'
mycel -u admin -p '<password>' admin create --operator-username '<username>' --new-password '<password>' [--email '<email>'] [--role system-admin]
mycel -u admin -p '<password>' admin update --operator-id '<id>' --email '<email>'
mycel -u admin -p '<password>' admin disable --operator-id '<id>'
mycel -u admin -p '<password>' admin enable --operator-id '<id>'
mycel -u admin -p '<password>' admin delete --operator-id '<id>'
mycel -u admin -p '<password>' admin password set [--operator-id '<id>'] --new-password '<password>'
mycel -u admin -p '<password>' admin role list --operator-id '<id>'
mycel -u admin -p '<password>' admin role grant --operator-id '<id>' --role space-admin
mycel -u admin -p '<password>' admin role revoke --operator-id '<id>' --grant-id '<grant-id>'
mycel -u admin -p '<password>' admin capability list --operator-id '<id>'
mycel -u admin -p '<password>' admin capability grant --operator-id '<id>' --capability operator-manage
mycel -u admin -p '<password>' admin capability revoke --operator-id '<id>' --grant-id '<grant-id>'
mycel -u admin -p '<password>' admin session list --operator-id '<id>'
mycel -u admin -p '<password>' admin session revoke --operator-id '<id>' --session-id '<session-id>'
mycel -u admin -p '<password>' admin session revoke-all --operator-id '<id>'
```

## Open questions

- Should direct operator capability grants be required to be system-scoped initially?
- Should `SYSTEM_ADMIN` grant/revoke require multi-party approval in production deployments later?
- What durable operator refresh-session model should replace the current short-lived-token-only placeholder?
