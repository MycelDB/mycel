# Admin User API

## Status

Draft design for the daemon-oriented Admin User API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/admin/v1/user.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
```

## Purpose

`AdminUserService` manages standard users in the Mycel daemon.

System admins/operators are fundamentally different from standard users and are not created through this service. Admin/operator identity and role management should be designed as a separate Admin API service.

## Scope

`AdminUserService` includes:

- list standard users
- get a standard user
- find a standard user by username or email
- create a standard user
- update standard user display metadata
- disable/enable a standard user
- soft-delete a standard user
- set a standard user's password
- list/revoke that user's auth sessions

`AdminUserService` does not include:

- system admin/operator creation
- admin role assignment
- admin capability assignment
- space creation
- automatic personal space creation
- space access grants
- signup/onboarding flows

Space access grants belong to a future `AdminAccessService`. Public signup remains a higher-level product concern currently handled by Knot PKM.

## Service definition

```protobuf
service AdminUserService {
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  rpc FindUser(FindUserRequest) returns (FindUserResponse);

  rpc CreateUser(CreateUserRequest) returns (CreateUserResponse);
  rpc UpdateUser(UpdateUserRequest) returns (UpdateUserResponse);

  rpc DisableUser(DisableUserRequest) returns (DisableUserResponse);
  rpc EnableUser(EnableUserRequest) returns (EnableUserResponse);
  rpc DeleteUser(DeleteUserRequest) returns (DeleteUserResponse);

  rpc SetUserPassword(SetUserPasswordRequest) returns (SetUserPasswordResponse);

  rpc ListUserSessions(ListUserSessionsRequest) returns (ListUserSessionsResponse);
  rpc RevokeUserSession(RevokeUserSessionRequest) returns (RevokeUserSessionResponse);
  rpc RevokeUserSessions(RevokeUserSessionsRequest) returns (RevokeUserSessionsResponse);
}
```

## User model

A user is a standard application/product principal.

There is no `UserKind` in v1. Admins/operators and system principals are modeled separately.

Recommended shape:

```protobuf
message User {
  string user_id = 1;
  string username = 2;
  string display_name = 3;
  string email = 4;
  UserState state = 5;
  google.protobuf.Timestamp create_time = 6;
  google.protobuf.Timestamp update_time = 7;
}
```

Supported states:

```protobuf
enum UserState {
  USER_STATE_UNSPECIFIED = 0;
  USER_STATE_ACTIVE = 1;
  USER_STATE_DISABLED = 2;
  USER_STATE_DELETED = 3;
}
```

## CreateUser

Creates a standard user.

```protobuf
message CreateUserRequest {
  string username = 1;
  string display_name = 2;
  string email = 3;
  optional string password = 4;
  bool disabled = 5;
}
```

Behavior:

- creates a standard user only
- does not create a space
- does not assign admin roles
- does not assign admin capabilities
- does not assign space access
- hashes password before storage if password is supplied
- never returns password plaintext or password hash
- creates an active user by default unless `disabled` is true
- audits the user creation

Required capability:

```text
user.create
```

## ListUsers

Lists standard users.

Supports pagination and optional inclusion of disabled/deleted users.

Required capability:

```text
user.manage
```

A future capability split may introduce `user.read`.

## GetUser and FindUser

`GetUser` retrieves a user by id.

`FindUser` retrieves a user by username or email.

Required capability:

```text
user.manage
```

## UpdateUser

Updates mutable user display metadata.

Mutable fields in v1:

- `display_name`
- `email`

Username is immutable in v1.

Required capability:

```text
user.manage
```

## DisableUser and EnableUser

Disabling a user:

- prevents new login
- optionally revokes existing auth sessions
- retains user identity for ownership, audit, and reference integrity

Enabling a user restores login eligibility.

Required capability:

```text
user.manage
```

## DeleteUser

`DeleteUser` is a soft delete in v1.

Behavior:

- marks the user deleted
- prevents login
- optionally revokes sessions
- preserves identity for audit and historical references
- does not purge user data immediately

Deletion should be rejected if the user owns spaces unless a future Admin Space API defines and supplies an ownership transfer/archive policy.

Required capability:

```text
user.manage
```

## SetUserPassword

Sets or resets a standard user's password.

Behavior:

- hashes password before storage
- never returns password plaintext or password hash
- optionally revokes existing sessions
- audits the password change without logging the password

Required capability:

```text
user.manage
```

A future capability split may introduce `user.password.set`.

## Auth session management

Admin user session management includes:

- list auth sessions for a user
- revoke one auth session
- revoke all auth sessions for a user

These operations are user-wide administrative equivalents of client-owned session management.

Session summaries expose coarse metadata only and must not expose refresh token plaintext, refresh token hashes, raw request metadata hashes, or secrets.

Required capability:

```text
user.manage
```

A future capability split may introduce `user.session.manage`.

## Users vs admins/operators

Standard users and system admins/operators are separate concepts.

This service does not create admins. A future service should manage operators/admins and their roles/capabilities, for example:

```text
AdminOperatorService
```

This separation avoids implying that a normal user can become an operator simply by adding a kind/role through the standard user API.

## Users vs space access

Creating a user does not imply space creation or space access.

Space access grants are separate and should be handled by a future `AdminAccessService`.

This keeps user identity lifecycle separate from collaboration/authorization over spaces.

## Authorization

Current coarse capability mapping:

| Operation | Required capability |
| --- | --- |
| CreateUser | `user.create` |
| All other methods | `user.manage` |

Future versions may split `user.manage` into finer capabilities:

- `user.read`
- `user.update`
- `user.disable`
- `user.delete`
- `user.password.set`
- `user.session.manage`

## Audit requirements

Audit events should be emitted for:

- create user
- update user
- disable user
- enable user
- delete user
- set password
- revoke user session
- revoke all user sessions

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
| missing/invalid admin auth | `UNAUTHENTICATED` |
| missing admin capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| duplicate username/email | `ALREADY_EXISTS` |
| user not found | `NOT_FOUND` |
| username update attempted | `FAILED_PRECONDITION` |
| delete blocked by owned spaces | `FAILED_PRECONDITION` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Standard user identity state must replicate across the mesh if multiple daemons enforce authentication/authorization for the same deployment.

User auth sessions may be local or replicated depending on future auth/session architecture, but user disabled/deleted state must be enforced consistently across the mesh.

## Open questions

- Should email be required or optional?
- Should password be required for all standard users, or can passwordless users exist for external identity providers later?
- Should user deletion support an explicit owned-space policy once Admin Space API is designed?
- Should session summaries be moved to a shared/common proto once Admin and Client Auth APIs are implemented together?
