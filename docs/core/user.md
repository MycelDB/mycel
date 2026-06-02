# User Structures

This document describes user identity types exposed by `knot_db/model`.

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
