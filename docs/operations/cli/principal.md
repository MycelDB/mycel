# `mycel principal`

Manage daemon principals through `mycel.admin.v1.AdminPrincipalService`.

Authentication mode: **principal auth with admin identity capabilities**. The caller logs in through the common auth service using `--username` / `--password`; the daemon authorizes each operation from the caller's role bindings and capability grants.

Use `--output json` for scripting. Without `--output json`, commands print a compact human-readable table or status line.

## Output object reference

### Principal

Most lifecycle commands return a `Principal` object in JSON mode.

| Field | Meaning |
| --- | --- |
| `principal_id` | Stable daemon identity ID. Use this for role, capability, session, space ownership, and automation references. |
| `username` | Login/lookup name when the principal has one. System principals may omit it. |
| `display_name` | Optional display label. |
| `email` | Optional email lookup field. |
| `type` | Principal type enum: `PRINCIPAL_TYPE_HUMAN`, `PRINCIPAL_TYPE_SERVICE`, or `PRINCIPAL_TYPE_SYSTEM`. |
| `state` | Lifecycle state: `PRINCIPAL_STATE_ACTIVE`, `PRINCIPAL_STATE_DISABLED`, or `PRINCIPAL_STATE_DELETED`. |
| `login_enabled` | Whether password login is enabled for this principal. |
| `create_time` | Principal creation timestamp. |
| `update_time` | Last principal update timestamp. |

### AuthSessionSummary

Session-list commands return `AuthSessionSummary` objects in JSON mode.

| Field | Meaning |
| --- | --- |
| `auth_session_id` | Durable refresh-session ID. |
| `create_time` | Session creation time. |
| `last_seen_time` | Last observed use/refresh time. |
| `expire_time` | Absolute session expiration time. |
| `current` | Whether the session is the current caller session when known. |
| `client` | Client metadata supplied at login/session creation. Includes fields such as `name`, `version`, `platform`, and `device_label`. |
| `state` | Session state: active, revoked, or expired. |

### PrincipalRoleGrant

Role commands return grants with:

| Field | Meaning |
| --- | --- |
| `role_grant_id` | Grant ID used for revocation. |
| `principal_id` | Principal receiving the role. |
| `role` | Role name, for example `system.admin` or `space.admin`. |
| `scope` | Scope for the grant. Current CLI role grant defaults to system scope. |
| `reason` | Optional audit reason. |
| `granted_by_principal_id` | Principal that created the grant. |
| `create_time` | Grant creation timestamp. |

### PrincipalCapabilityGrant

Capability commands return grants with:

| Field | Meaning |
| --- | --- |
| `capability_grant_id` | Grant ID used for revocation. |
| `principal_id` | Principal receiving the capability. |
| `capability` | Capability enum, for example `CAPABILITY_IDENTITY_PRINCIPAL_UPDATE`. |
| `scope` | Scope for the grant. Current CLI capability grant defaults to system scope. |
| `reason` | Optional audit reason. |
| `granted_by_principal_id` | Principal that created the grant. |
| `create_time` | Grant creation timestamp. |

## `principal list`

List principals.

```sh
mycel --daemon-addr 127.0.0.1:9091 \
  --username admin --password '<password>' \
  principal list
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--include-disabled` | Include disabled principals. |
| `--include-deleted` | Include deleted principals. |

Text output: table with `Principal ID`, `Username`, `Display Name`, `Email`, `Type`, `State`, and `Login` columns.

JSON output: array of `Principal` objects.

## `principal get`

Get one principal by ID.

```sh
mycel principal get --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output: `principal: <principal-id>`.

JSON output: one `Principal` object.

## `principal find`

Find one principal by username or email.

```sh
mycel principal find --principal-username alice
mycel principal find --email alice@example.com
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-username` | Username lookup. |
| `--email` | Email lookup. |

Exactly one lookup path is used. If both are supplied, username takes precedence.

Text output: `principal: <principal-id>`.

JSON output: one `Principal` object.

## `principal create`

Create a principal.

```sh
mycel principal create \
  --principal-username alice \
  --new-password '<alice-password>' \
  --login-enabled \
  --role space.admin
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-username` | Principal username. Required for `human` and `service` principals. |
| `--display-name` | Optional display name. |
| `--email` | Optional email. |
| `--new-password` | Initial password. Required when `--login-enabled` is true. |
| `--type` | `human`, `service`, or `system`. Defaults to `human`. |
| `--login-enabled` | Enable password login. Defaults to true. |
| `--disabled` | Create the principal disabled. |
| `--role` | Initial role binding. Repeatable. |

Text output: `principal created: <principal-id>`.

JSON output: the created `Principal` object. Initial role grants are applied by the daemon but are not printed by this CLI command unless you separately run `principal role list`.

## `principal update`

Update mutable principal fields.

```sh
mycel principal update \
  --principal-id <principal-id> \
  --display-name 'Alice Example' \
  --email alice@example.com
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--principal-username` | Set username. |
| `--display-name` | Set display name. |
| `--email` | Set email. |
| `--type` | Set type: `human`, `service`, or `system`. |
| `--login-enabled` | Set password-login enabled/disabled. |

At least one mutable field flag is required.

Text output: `principal updated: <principal-id>`.

JSON output: updated `Principal` object.

## `principal disable`

Disable a principal.

```sh
mycel principal disable --principal-id <principal-id> --reason 'offboarding' --revoke-sessions
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--reason` | Optional audit reason. |
| `--revoke-sessions` | Revoke the principal's auth sessions while disabling. |

Text output: `principal disabled: <principal-id>`.

JSON output: updated `Principal` object with disabled state.

## `principal enable`

Enable a disabled principal.

```sh
mycel principal enable --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output: `principal enabled: <principal-id>`.

JSON output: updated `Principal` object with active state.

## `principal delete`

Delete a principal.

```sh
mycel principal delete --principal-id <principal-id> --revoke-sessions
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--revoke-sessions` | Revoke the principal's auth sessions while deleting. |

Text output: `principal deleted: <principal-id>`.

JSON output: updated `Principal` object with deleted state.

## `principal password set`

Set a principal password.

```sh
mycel principal password set \
  --principal-id <principal-id> \
  --new-password '<new-password>' \
  --revoke-sessions
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--new-password` | Required replacement password. |
| `--revoke-sessions` | Revoke existing auth sessions after changing the password. |

Text output: `principal password changed: <principal-id>`.

JSON output: updated `Principal` object. Password hashes and plaintext passwords are never returned.

## `principal session list`

List auth sessions for a principal.

```sh
mycel principal session list --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--include-inactive` | Include revoked and expired sessions. |

Text output: `principal sessions: <count>`.

JSON output: array of `AuthSessionSummary` objects.

## `principal session create`

Create a delegated auth session for a principal.

```sh
mycel principal session create --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output includes the new session ID and refresh token:

```text
principal session created: <auth-session-id>
refresh_token: <refresh-token>
```

JSON output: object with:

| Field | Meaning |
| --- | --- |
| `access_token` | Short-lived bearer token for the delegated session. |
| `access_token_expire_time` | Expiration timestamp for `access_token`. |
| `refresh_token` | Durable refresh token. Store securely; plaintext is only returned at creation/rotation time. |
| `principal` | Delegated `Principal` object. |
| `auth_session_id` | Durable auth-session ID. |

## `principal session revoke`

Revoke one auth session for a principal.

```sh
mycel principal session revoke \
  --principal-id <principal-id> \
  --session-id <auth-session-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--session-id` | Required auth-session ID. |

Text output: `principal session revoked`.

JSON output: empty object `{}`.

## `principal session revoke-all`

Revoke all auth sessions for a principal.

```sh
mycel principal session revoke-all --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output: `principal sessions revoked: <count>`.

JSON output: object with `revoked_count`, the number of sessions revoked.

## `principal role list`

List role grants for a principal.

```sh
mycel principal role list --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output: `principal role grants: <count>`.

JSON output: object with:

| Field | Meaning |
| --- | --- |
| `grants` | Array of `PrincipalRoleGrant` objects. |
| `effective_roles` | Effective role names after normalization and active-grant filtering. |

## `principal role grant`

Grant a role to a principal.

```sh
mycel principal role grant \
  --principal-id <principal-id> \
  --role space.admin \
  --reason 'grant space administration'
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--role` | Required role name. |
| `--reason` | Optional audit reason. |

Text output: `principal role granted: <role-grant-id>`.

JSON output: object with:

| Field | Meaning |
| --- | --- |
| `grant` | Created `PrincipalRoleGrant`. Use `grant.role_grant_id` to revoke it later. |
| `effective_capabilities` | Capability enum values effective after the role grant. |

## `principal role revoke`

Revoke a role grant.

```sh
mycel principal role revoke \
  --principal-id <principal-id> \
  --grant-id <role-grant-id> \
  --reason 'remove space administration'
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--grant-id` | Required role grant ID. |
| `--reason` | Optional audit reason. |

Text output: `principal role revoked`.

JSON output: object with `effective_capabilities`, the capability enum values remaining after revocation.

## `principal capability list`

List direct capability grants for a principal.

```sh
mycel principal capability list --principal-id <principal-id>
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |

Text output: `principal capability grants: <count>`.

JSON output: object with:

| Field | Meaning |
| --- | --- |
| `grants` | Array of `PrincipalCapabilityGrant` objects. |
| `effective_capabilities` | Capability enum values effective from both roles and direct grants. |

## `principal capability grant`

Grant a direct capability to a principal.

```sh
mycel principal capability grant \
  --principal-id <principal-id> \
  --capability identity-principal-update \
  --reason 'temporary identity maintenance'
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--capability` | Required capability. CLI accepts kebab-case shortcuts such as `identity-principal-update` and maps them to the public enum. |
| `--reason` | Optional audit reason. |

Text output: `principal capability granted: <capability-grant-id>`.

JSON output: object with:

| Field | Meaning |
| --- | --- |
| `grant` | Created `PrincipalCapabilityGrant`. Use `grant.capability_grant_id` to revoke it later. |
| `effective_capabilities` | Capability enum values effective after the grant. |

## `principal capability revoke`

Revoke a direct capability grant.

```sh
mycel principal capability revoke \
  --principal-id <principal-id> \
  --grant-id <capability-grant-id> \
  --reason 'temporary access no longer needed'
```

Flags:

| Flag | Meaning |
| --- | --- |
| `--principal-id` | Required principal ID. |
| `--grant-id` | Required capability grant ID. |
| `--reason` | Optional audit reason. |

Text output: `principal capability revoked`.

JSON output: object with `effective_capabilities`, the capability enum values remaining after revocation.

## Related docs

- [CLI index](README.md)
- [Standalone start procedure](../procedures/standalone-start.md)
- [Unified principal identity design](../../design/identity/unified-principal-access-control.md)
