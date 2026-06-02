# Space Access Control

Space access is governed by allow/grant rules stored in `meta/access.json`.

Each rule grants permissions to one user for one space.

```json
{
  "id": "rule-uuid",
  "space_id": "space-uuid",
  "user_id": "user-uuid",
  "permissions": ["read"]
}
```

## Permissions

Supported permissions:

- `read`
- `write`
- `admin`

Permissions are hierarchical:

```text
admin => write => read
```

So:

- `read` allows read operations only.
- `write` allows read and write operations.
- `admin` allows read, write, template management, and access management.

## Space owner access

When a space is created, the creator receives an `admin` access rule for that space.

Ownership is not an unremovable bypass. Another admin may remove the original owner's admin rule as long as at least one admin remains for the space.

## Last-admin invariant

Every space must retain at least one admin user.

Access management rejects revoking or downgrading a rule if doing so would leave a space with no admin rules.

## Sessions

Read-only users may open sessions. Write operations fail inside read-only sessions.

Session operation requirements:

- `GetNode` requires `read`.
- `AddNode`, `AddEdge`, and `AddGraph` require `write`.

## Deny rules

Deny rules are not currently supported. All rules are allow/grant rules.
