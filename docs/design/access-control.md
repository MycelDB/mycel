# Access Control Design

## Status

Draft design for the daemon-oriented Mycel access-control model on the `refactor_daemon` branch.

This document defines cross-cutting authorization concepts used by the Client API, Admin API, daemon runtime, and future mesh replication. Service-specific API documents should reference this document rather than redefining rights and roles independently.

The protobuf source of truth for shared access-control types is:

```text
api/proto/mycel/common/v1/access.proto
```

## Summary

Mycel authorization is capability-based. Roles are built-in convenience bundles of capabilities for v1. The daemon enforces effective capabilities, not role names.

Space ownership is an identity/accountability concept. It is related to access control but is not the same as an access grant.

Initial v1 assumptions:

- A space has exactly one owner.
- Initial owner types are `user` and `system`.
- Groups, teams, organizations, ownership transfer, and custom roles are future work.
- Apps/service accounts are treated as users for this initial model.
- User capabilities are generally scoped to a space.
- Admin capabilities may be system-level or space-scoped.
- System capabilities are used by the daemon/system itself for internal operations.
- Space creation is admin-only.
- Space archival/deletion is admin-only.
- Hard deletion is a two-step lifecycle: archive first, then delete the archived space.
- Delete capabilities are separate from write capabilities.
- No explicit deny rules in v1.
- Access metadata must replicate across the mesh.

## Principles

1. **Capabilities are authoritative**

   Authorization checks evaluate capabilities. Roles are only presets that expand to capabilities.

2. **Roles simplify common grants**

   Built-in roles make common collaboration cases easy, but the daemon should still compute and enforce effective capabilities.

3. **Ownership is identity**

   Ownership identifies the accountable principal for a space. It is not merely a role assignment.

4. **Delete is distinct from write**

   A principal that can write graph data is not automatically allowed to delete graph data, blobs, domains, or spaces.

5. **Control plane and data plane are separate**

   Client graph access is distinct from admin operations such as creating users, creating spaces, archiving spaces, deleting spaces, and configuring daemon/system behavior.

6. **No explicit deny rules in v1**

   Effective permissions are the union of applicable grants and built-in ownership/admin rules. Explicit deny rules are deferred.

7. **Access metadata is replicated**

   In mesh mode, access rules and ownership metadata must replicate so daemons enforce the same authorization model.

## Principal Types

### v1 principal types

- `user`
- `operator`
- `system`

`operator` identifies a system admin/operator principal. Operators are distinct from standard users and are managed through Admin Operator APIs.

### Future principal types

- `group`
- `team`
- `organization`

Apps and service accounts are treated as users in the initial version. A separate service-account principal type may be introduced later if needed.

## Capability Buckets

Mycel has three broad buckets of capabilities.

### User capabilities

User capabilities are associated with a user/app when accessing a given space. These are the capabilities used by the Client API for normal application operations.

Examples:

- read graph data in a space
- write graph data in a space
- upload blobs in a space
- manage templates in a space
- run semantic search in a space

### Admin capabilities

Admin capabilities authorize control-plane operations. They can be system-level or space-scoped.

Examples:

- create a user
- create/manage an operator
- create a space
- archive a space
- delete an archived space
- grant/revoke space access
- inspect daemon/system state

### System capabilities

System capabilities are tied to the daemon/system itself and are used for internal or operational work.

Examples:

- compact a space
- run internal maintenance
- manage system-owned spaces
- replicate access metadata
- perform backup/restore tasks

System capabilities are not normal user collaboration grants.

## Ownership

A space has exactly one owner in v1.

Supported owner types:

- `user`
- `system`

Unsupported in v1:

- group-owned spaces
- team-owned spaces
- organization-owned spaces
- multiple owners
- ownership transfer through Client API

Ownership is about identity and accountability. The owner may receive effective owner capabilities, but ownership itself is not modeled as a normal revocable role grant.

### User-owned spaces

A user-owned space is owned by one user. Apps are treated as users in the initial model.

### System-owned spaces

System-owned spaces are reserved for daemon/system use. If present, they are visible only to:

- the system itself
- system administrators

System-owned spaces are not returned to normal users from Client API space listing unless a future design explicitly adds such behavior.

Potential uses for system-owned spaces include:

- persistent daemon/system metadata
- backup coordination metadata
- mesh metadata
- audit/event data, if this storage approach is selected later

## Capability Scope

User capabilities are generally scoped to a space in v1.

The model should leave room for more specific scopes later, including:

- domain scope
- node/subtree scope
- blob scope
- template scope

However, v1 should avoid implementing fine-grained scopes unless an immediate product requirement needs them.

Admin capabilities may be either:

- system-level, such as `space.create` or `user.create`
- space-scoped, such as managing access within a specific space

## Initial Capability Set

Capability names are design-level names. Final protobuf/API enum names may use uppercase enum naming conventions.

### Space capabilities

| Capability | Meaning |
| --- | --- |
| `space.read` | See the space and its basic metadata. |
| `space.update` | Update mutable space metadata/settings, where allowed. |
| `space.manage_access` | Grant/revoke allowed access for the space. |
| `space.archive` | Archive the space. Admin capability. |
| `space.delete` | Permanently delete an archived space. Admin capability. |
| `space.create` | Create a new space. System-level admin capability. |

Space creation is admin-only in v1.

### Operator capabilities

| Capability | Meaning |
| --- | --- |
| `operator.create` | Create a system admin/operator. |
| `operator.manage` | Manage existing operators, including state, roles, capabilities, passwords, and sessions. |

Operators are distinct from standard users. Operator roles are admin/control-plane role bundles and are not the same as space roles.

Space destruction is admin-only in v1 and uses a two-step lifecycle:

1. archive the space
2. delete the archived space

A non-archived space cannot be hard-deleted.

### Domain capabilities

| Capability | Meaning |
| --- | --- |
| `domain.read` | List/read domains in a space. |
| `domain.create` | Create a domain in a space. |
| `domain.update` | Update domain metadata/settings. |
| `domain.delete` | Delete a domain. |

Domain deletion is a distinct capability.

### Graph capabilities

| Capability | Meaning |
| --- | --- |
| `graph.read` | Read nodes/edges and run graph reads in a space/domain. |
| `graph.write` | Create/update graph data. Does not imply delete. |
| `graph.delete` | Delete graph data. |

Delete is not included in write.

### Template capabilities

| Capability | Meaning |
| --- | --- |
| `template.read` | Read templates. |
| `template.manage` | Create/update/delete templates. |

Template management is part of the Client API but requires `template.manage`.

### Blob capabilities

| Capability | Meaning |
| --- | --- |
| `blob.read` | Read/download blobs. |
| `blob.write` | Upload/create/update blobs. |
| `blob.delete` | Delete blobs. |

Delete is not included in write.

### Metadata capabilities

| Capability | Meaning |
| --- | --- |
| `metadata.read` | Read metadata/tags/properties. |
| `metadata.write` | Create/update metadata/tags/properties. |

### Query and semantic capabilities

| Capability | Meaning |
| --- | --- |
| `query.run` | Run structured graph/metadata queries. |
| `semantic.search` | Run semantic search over permitted data. |

Semantic index creation/configuration is not a Client API capability. It belongs to the Admin API.

### Session capabilities

Graph operations are expected to run through daemon-owned graph sessions. Session authorization can be derived from graph capabilities:

| Operation | Required capability |
| --- | --- |
| Begin read session | `graph.read` |
| Begin write session | `graph.write` |
| Commit write session with deletes | `graph.delete` for delete operations |

Session-specific capability names may be introduced later if the model needs them.

## Built-in Roles

Roles are v1 built-in presets. Custom roles are future work.

The daemon should expand roles into capabilities and enforce capabilities.

### owner

The owner is the canonical owner identity. Ownership may imply owner capabilities, but ownership is not merely a role grant.

Suggested effective capabilities:

- `space.read`
- `space.update`
- `space.manage_access`
- `domain.read`
- `domain.create`
- `domain.update`
- `domain.delete`
- `graph.read`
- `graph.write`
- `graph.delete`
- `template.read`
- `template.manage`
- `blob.read`
- `blob.write`
- `blob.delete`
- `metadata.read`
- `metadata.write`
- `query.run`
- `semantic.search`

Space creation, archival, and hard deletion remain admin/control-plane operations in v1.

### admin

A space admin can administer collaboration and structure inside a space, but is not the owner.

Suggested effective capabilities:

- `space.read`
- `space.update`
- `space.manage_access`
- `domain.read`
- `domain.create`
- `domain.update`
- `domain.delete`
- `graph.read`
- `graph.write`
- `graph.delete`
- `template.read`
- `template.manage`
- `blob.read`
- `blob.write`
- `blob.delete`
- `metadata.read`
- `metadata.write`
- `query.run`
- `semantic.search`

A space admin does not receive system-level admin capabilities such as `space.create`, `space.archive`, or `space.delete` unless separately granted through the Admin API.

### writer

A writer can edit graph data and related content but cannot manage access or destructive structural operations by default.

Suggested effective capabilities:

- `space.read`
- `domain.read`
- `graph.read`
- `graph.write`
- `template.read`
- `blob.read`
- `blob.write`
- `metadata.read`
- `metadata.write`
- `query.run`
- `semantic.search`

Not included:

- `space.manage_access`
- `domain.create`
- `domain.update`
- `domain.delete`
- `graph.delete`
- `template.manage`
- `blob.delete`
- `space.archive`
- `space.delete`

### reader

A reader can read and query permitted data.

Suggested effective capabilities:

- `space.read`
- `domain.read`
- `graph.read`
- `template.read`
- `blob.read`
- `metadata.read`
- `query.run`
- `semantic.search`

## Access Grants

An access grant associates a principal with a role and/or capabilities at a scope.

For v1, normal grants should be space-scoped and role-based:

```text
principal -> space -> role
```

The daemon computes effective capabilities from:

- ownership
- built-in role grants
- admin/system capability grants
- future direct capability grants, if introduced

Direct custom capability grants may be useful later, but built-in role grants should be sufficient for the initial release.

## Access Management Rules

Access management is controlled by `space.manage_access` for a given space, plus system-level admin rules.

Recommended v1 behavior:

- A space admin can grant/revoke `reader` and `writer` grants.
- A system admin can grant/revoke `admin` grants.
- No Client API operation can revoke or replace the owner.
- Ownership transfer is future work or Admin API only.
- Collaborator self-removal may be added later, but is not required for v1.

This keeps day-to-day collaboration simple while reducing uncontrolled privilege escalation.

## Effective Capability Reporting

Client APIs should report effective capabilities where useful, especially on space discovery APIs.

`ListSpaces` and `GetSpace` should return enough information for clients to enable/disable UI and avoid attempting unauthorized operations.

A response should include concepts like:

```text
space_id
name
owner principal
roles held by caller
effective capabilities
archived state
```

Clients should treat effective capabilities as advisory for UI. The daemon still enforces authorization on every request.

## Client API and Admin API Boundary

### Client API

The Client API is for normal application use inside authorized spaces.

Client-facing operations include:

- list/get spaces visible to the caller
- list/get domains in authorized spaces
- begin graph sessions
- read/write/delete graph data subject to capabilities
- manage templates subject to `template.manage`
- read/write/delete blobs subject to blob capabilities
- run semantic search subject to `semantic.search`

### Admin API

The Admin API owns control-plane operations.

Admin-facing operations include:

- create users
- manage users
- create spaces
- archive spaces
- delete archived spaces
- assign/revoke space admins
- manage system-owned spaces
- configure semantic indexes
- daemon/system maintenance
- mesh management

## Audit Requirements

Access-control and lifecycle changes should be auditable.

Events to audit include:

### Auth events

- login success/failure
- refresh success/failure
- logout
- refresh token reuse detection
- auth session revoke

### Access-control events

- grant access
- revoke access
- change role
- change capability grant, if direct grants are introduced

### Space lifecycle events

- create space
- archive space
- delete archived space
- restore archived space, if supported later

### System-owned space events

- create system-owned space
- modify system-owned space visibility/access

### Admin events

- create user
- disable user
- delete user
- reset credentials
- grant admin capability
- revoke admin capability

### Future mesh/security events

- peer added
- peer removed
- replication policy changed
- access metadata replicated/applied

Audit records should include:

- event id
- timestamp
- actor principal
- action
- target type
- target id
- optional space id
- optional domain id
- result
- sanitized reason/error
- redacted metadata

Audit records must not include:

- passwords
- refresh token plaintext
- refresh token hashes
- raw API keys
- raw secrets

Open storage question: audit events may be stored in a system-owned space, or in a separate append-only audit store. A dedicated append-only store may provide stronger tamper-resistance later.

## Mesh Implications

Access rules, ownership metadata, and relevant access-control audit metadata must replicate across the mesh.

Mesh replication must preserve enough ordering/consistency for daemons to enforce current access rules. The detailed consistency model is future design work, but access metadata cannot remain purely local once a space is replicated.

## Future Work

- groups/teams/organizations
- custom roles
- direct custom capability grants in public APIs
- domain-scoped grants
- node/subtree-scoped grants
- ownership transfer
- collaborator self-removal
- explicit deny rules
- service-account principal type
- tamper-resistant audit storage
- detailed mesh consistency model for access metadata
