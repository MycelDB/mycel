# User Structures

This document describes user identity types exposed by `martinbeauvais.com/mbgit/knotbase/knotdb/model`.

## Identity model
Each user has two identifiers:

- **Internal**: `UserID` (UUID, immutable)
- **External**: `UserRef` (string, unique, immutable)

`UserRef` can hold values like email, username, or identity-provider subject.

## `UserStatus`
- `pending`
- `active`
- `paused`
- `revoked`

## `User`
| Field | Type | Description |
|---|---|---|
| `ID` | `UserID` | Internal stable system key. |
| `Ref` | `UserRef` | External unique identifier. |
| `Email` | `*string` | Optional email attribute. |
| `Username` | `*string` | Optional username attribute. |
| `Status` | `UserStatus` | User lifecycle state. |

## `UserInput`
| Field | Type | Required | Description |
|---|---|---:|---|
| `ID` | `*UserID` | No | Optional caller-provided UUID. |
| `Ref` | `UserRef` | Yes | External unique identifier. |
| `Email` | `*string` | No | Optional email attribute. |
| `Username` | `*string` | No | Optional username attribute. |
| `Status` | `UserStatus` | Yes | Initial lifecycle state. |

## User management APIs

`CreateUser` requires a caller with the `users:manage` system permission.

`DeleteUser` is a hard delete. It removes the user record, removes access rules for the user, and deletes every space owned by the user including each space's metadata, ACLs, templates, graph nodes, and graph edges.

The last `superuser` cannot be deleted.
