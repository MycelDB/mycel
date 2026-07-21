# Admin User API

## Status

Implemented initial daemon-oriented Admin User API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/user.proto
```

This document depends on:

```text
docs/design/grpc-admin-auth.md
```

## Purpose

`AdminUserService` manages standard user identities in the Mycel daemon.

The daemon user identity is intentionally minimal. Application/business profile attributes such as display name, email, avatar, locale, preferences, onboarding state, or app-specific metadata belong in application spaces as graph data.

System admins/operators are separate principals and are not created through this service. Admin/operator identity, roles, and capabilities are managed by `AdminOperatorService`.

## Scope

`AdminUserService` includes:

- list standard users
- get a standard user
- find a standard user by username
- create a standard user
- disable/enable a standard user
- soft-delete a standard user
- set a standard user's password
- list/revoke that user's auth sessions

`AdminUserService` does not include:

- display names
- email addresses
- user profile attributes
- system admin/operator creation
- admin role assignment
- admin capability assignment
- space creation
- automatic personal space creation
- space access grants
- signup/onboarding flows

## Service definition

```protobuf
service AdminUserService {
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  rpc FindUser(FindUserRequest) returns (FindUserResponse);
  rpc CreateUser(CreateUserRequest) returns (CreateUserResponse);
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

```protobuf
message User {
  string user_id = 1;
  string username = 2;
  UserState state = 3;
  google.protobuf.Timestamp create_time = 4;
  google.protobuf.Timestamp update_time = 5;
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

The domain identity model uses `identity.User.Username` as the immutable login/unique username. Legacy embedded user store decoding tolerates old `Ref`/`Username` JSON keys but drops old presentation metadata.

## Behavior

`CreateUser`:

- creates a standard user only
- requires a username and password in the current daemon-local implementation
- hashes the password with bcrypt before storage
- creates an active user by default unless `disabled` is true
- does not create a space, assign admin roles/capabilities, or grant space access
- never returns password plaintext or password hashes

`DisableUser` / `EnableUser`:

- disabled users cannot log in once Client API auth is backed by this module
- disable can revoke existing refresh sessions

`DeleteUser`:

- is a soft delete (`USER_STATE_DELETED`)
- can revoke existing refresh sessions
- does not purge user-owned data immediately
- owned-space transfer/archive enforcement remains a follow-up once daemon space/admin APIs are wired in

`SetUserPassword`:

- bcrypt-hashes the new password
- never returns password plaintext or password hashes
- can revoke existing refresh sessions

`ListUserSessions`, `RevokeUserSession`, and `RevokeUserSessions` use the daemon user module's durable refresh-session store and expose only coarse session metadata.

## Module/store

The daemon initializes a dedicated user module:

```text
internal/daemon/modules/user
```

It stores daemon-managed standard users and their refresh sessions under:

```text
<MYCELD_DATA_DIR>/users/users.json
<MYCELD_DATA_DIR>/users/sessions/refresh_sessions.json
```

User records include:

- id
- username
- active/disabled/deleted state
- bcrypt password hash
- create/update timestamps

Admin API summaries intentionally omit password hashes.

## Authorization

All AdminUserService methods require daemon operator bearer-token authentication.

Current coarse capability mapping:

| Operation | Required capability |
| --- | --- |
| `CreateUser` | `CAPABILITY_USER_CREATE` |
| All other methods | `CAPABILITY_USER_MANAGE` |

The initial operator roles grant these capabilities as follows:

| Operator role | User-management capabilities |
| --- | --- |
| `SYSTEM_ADMIN` | `CAPABILITY_USER_CREATE`, `CAPABILITY_USER_MANAGE` |
| `USER_ADMIN` | `CAPABILITY_USER_CREATE`, `CAPABILITY_USER_MANAGE` |

Direct operator capability grants are also honored.

Future versions may split `user.manage` into finer capabilities:

- `user.read`
- `user.disable`
- `user.delete`
- `user.password.set`
- `user.session.manage`

## CLI commands

Existing `mycel user ...` commands now call daemon gRPC. Root flags `-u/--username` and `-p/--password` are daemon operator credentials:

```sh
mycel -u admin -p '<operator-password>' user add --user-username alice --new-password '<password>'
mycel -u admin -p '<operator-password>' user list [--include-disabled] [--include-deleted]
mycel -u admin -p '<operator-password>' user get --user-id '<id>'
mycel -u admin -p '<operator-password>' user find --user-username alice
mycel -u admin -p '<operator-password>' user disable --user-id '<id>' [--revoke-sessions]
mycel -u admin -p '<operator-password>' user enable --user-id '<id>'
mycel -u admin -p '<operator-password>' user delete '<id>' [--revoke-sessions]
mycel -u admin -p '<operator-password>' user password set --user-id '<id>' --new-password '<password>' [--revoke-sessions]
mycel -u admin -p '<operator-password>' user session list --user-id '<id>' [--include-inactive]
mycel -u admin -p '<operator-password>' user session revoke --user-id '<id>' --session-id '<session-id>'
mycel -u admin -p '<operator-password>' user session revoke-all --user-id '<id>'
```

`--ref` remains as a deprecated alias for `--user-username` on add/find for compatibility with the old embedded CLI vocabulary.

## Users vs profile data

User profile and business attributes should be represented in application spaces. For example, an app can create a profile node keyed by the daemon `user_id` or `username` and define its own fields, validation, access control, and retention behavior.

This keeps daemon identity lifecycle separate from application data modeling.

## Users vs admins/operators

Standard users and system admins/operators are separate concepts.

This service does not create admins. Operators/admins and their roles/capabilities are managed through:

```text
AdminOperatorService
```

## Users vs space access

Creating a user does not imply space creation or space access.

Space access grants belong to a future daemon Admin Access API. This keeps user identity lifecycle separate from collaboration/authorization over spaces.

## Audit requirements

Audit events should be emitted for:

- create user
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

Full audit emission is still future hardening for this daemon slice.

## Error model

The implementation uses standard gRPC status codes.

| Condition | gRPC status |
| --- | --- |
| missing/invalid admin auth | `UNAUTHENTICATED` |
| missing admin capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| duplicate username | `ALREADY_EXISTS` |
| user/session not found | `NOT_FOUND` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Standard user identity state must replicate across the mesh if multiple daemons enforce authentication/authorization for the same deployment.

User auth sessions may be local or replicated depending on future auth/session architecture, but user disabled/deleted state must be enforced consistently across the mesh.

## Open questions

- Should passwordless users be allowed later for external identity providers?
- Should user deletion support an explicit owned-space policy once Admin Space API is designed?
- Should session summaries be moved to a shared/common proto once Admin and Client Auth APIs are implemented together?
