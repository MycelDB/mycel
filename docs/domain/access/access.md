# Access Control

MycelDB access control is governed by allow/grant rules stored in `meta/access.json`.

The access file contains two classes of rules:

- system rules: global roles for system administration
- space rules: per-space permissions for graph access and space administration

```json
{
  "system_rules": [
    {
      "id": "rule-uuid",
      "user_id": "user-uuid",
      "roles": ["superuser"]
    }
  ],
  "space_rules": [
    {
      "id": "rule-uuid",
      "space_id": "space-uuid",
      "user_id": "user-uuid",
      "permissions": ["read"]
    }
  ]
}
```

## System roles

Supported system roles:

- `superuser`
- `user_admin`
- `operator`

### `superuser`

Full system authority.

A superuser can:

- create spaces
- grant and revoke system roles
- grant and revoke space access
- administer/read/write any space
- import templates for any space
- manage users when user-management APIs are exposed
- operate the system when lifecycle APIs are exposed

### `user_admin`

User-management authority.

A user admin can manage users when user-management APIs are exposed. This role does not automatically grant access to spaces.

### `operator`

System operation authority.

An operator can perform system lifecycle/operation actions when those APIs are exposed. This role does not automatically grant access to spaces.

## Last-superuser invariant

The system must retain at least one `superuser`.

Access management rejects revoking or downgrading the last superuser rule.

## Space permissions

Supported space permissions:

- `read`
- `write`
- `admin`

Space permissions are hierarchical:

```text
admin => write => read
```

So:

- `read` allows read operations only.
- `write` allows read and write operations.
- `admin` allows read, write, template management, and access management for the space.

`superuser` bypasses per-space rules and can administer/read/write any space.

## Space owner access

When a space is created, the creator receives an `admin` access rule for that space.

Ownership is not an unremovable bypass. Another admin may remove the original owner's admin rule as long as at least one admin remains for the space.

## Last-admin invariant

Every space must retain at least one admin user.

Access management rejects revoking or downgrading a space rule if doing so would leave a space with no admin rules.

## Sessions

Read-only users may open sessions. Write operations fail inside read-only sessions.

Session operation requirements:

- `GetNode` requires `read`.
- `AddNode`, `AddEdge`, and `AddGraph` require `write`.

## Deny rules

Deny rules are not currently supported. All rules are allow/grant rules.
