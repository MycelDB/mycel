# Client Space API

## Status

Draft design for the daemon-oriented Client Space API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/space.proto
```

This document depends on the cross-cutting access-control model in:

```text
docs/v2/design/access-control.md
```

## Purpose

`SpaceService` is the client-facing API for discovering and inspecting spaces visible to the authenticated caller.

In daemon mode, a space is the top-level user-visible boundary for graph data. Since the Client API is domain-aware, clients use `SpaceService` first to find accessible spaces, then use `DomainService` and graph/session APIs inside a selected space.

## Scope

The Client `SpaceService` includes:

- listing spaces visible to the authenticated caller
- retrieving one visible space by id
- returning the caller's effective capabilities for each returned space
- returning coarse space metadata needed by applications and connectors

The Client `SpaceService` does **not** include:

- space creation
- space archival
- space hard deletion
- system-owned space management
- global storage management
- mesh placement/replication controls
- assigning or revoking space admins

Those operations belong to the Admin API.

## Service definition

Initial recommended Client API surface:

```protobuf
service SpaceService {
  rpc ListSpaces(ListSpacesRequest) returns (ListSpacesResponse);
  rpc GetSpace(GetSpaceRequest) returns (GetSpaceResponse);
}
```

This intentionally keeps Client `SpaceService` small. Creation, archival, deletion, and administrative access management are control-plane operations.

## Methods

### ListSpaces

Lists spaces visible to the authenticated caller.

A space is visible when the caller has effective `space.read` for that space or is otherwise authorized by admin/system policy.

The response should include:

- space id
- display name
- owner principal
- archived state
- timestamps
- template usage policy
- caller roles, if any
- caller effective capabilities

`ListSpaces` should support pagination from the beginning so the API can scale beyond small personal deployments.

Suggested request fields:

```protobuf
message ListSpacesRequest {
  int32 page_size = 1;
  string page_token = 2;
  bool include_archived = 3;
}
```

Suggested response fields:

```protobuf
message ListSpacesResponse {
  repeated SpaceSummary spaces = 1;
  string next_page_token = 2;
}
```

### GetSpace

Returns one visible space by id.

The caller must have effective `space.read` or equivalent admin/system authorization. Normal users should not be able to retrieve system-owned spaces unless explicitly authorized by system/admin policy.

Suggested request fields:

```protobuf
message GetSpaceRequest {
  string space_id = 1;
}
```

Suggested response fields:

```protobuf
message GetSpaceResponse {
  Space space = 1;
}
```

## Space resource shape

The exact protobuf may be refined later, but the design-level resource should contain:

```protobuf
message Space {
  string space_id = 1;
  string name = 2;
  Principal owner = 3;
  SpaceState state = 4;
  google.protobuf.Timestamp create_time = 5;
  google.protobuf.Timestamp update_time = 6;
  EffectiveAccess caller_access = 7;
  SpaceTemplateUsage template_usage = 8;
}
```

A `SpaceSummary` may initially be identical to `Space`. If later space detail grows, `SpaceSummary` can remain a smaller listing representation.

## Template usage policy

Template usage is a space-level policy selected when the space is created.

Space creation is an Admin API operation. Client `SpaceService` exposes the selected policy as space metadata so clients and connectors can apply the correct template behavior.

Supported policies:

```protobuf
enum SpaceTemplateUsage {
  SPACE_TEMPLATE_USAGE_UNSPECIFIED = 0;
  SPACE_TEMPLATE_USAGE_OPTIONAL = 1;
  SPACE_TEMPLATE_USAGE_MANDATORY = 2;
}
```

### Optional template usage

Nodes may omit `template_id`.

When deleting a template, `TemplateService.DeleteTemplate` may allow explicit detach behavior that clears `template_id` from active nodes referencing the deleted template.

### Mandatory template usage

Nodes must have `template_id`.

Template deletion is blocked while active nodes reference the template. Referencing objects must first be migrated to another template or deleted/archived.

If archived nodes reference a template, the template should be archived rather than hard-deleted so archived data remains readable/interpretable.

## Space identity and ownership

A space has exactly one owner in v1.

Supported v1 owner types:

- `user`
- `system`

Unsupported in v1:

- multiple owners
- group/team/organization owners
- ownership transfer through Client API

Ownership is identity/accountability. It is not merely an access grant.

## System-owned spaces

System-owned spaces are reserved for daemon/system use.

Client `ListSpaces` and `GetSpace` should hide system-owned spaces from normal users. System-owned spaces are visible only to:

- the system itself
- system administrators

Potential system-owned space uses include daemon metadata, backup coordination, mesh metadata, and possibly audit/event data.

## Effective capabilities

`ListSpaces` and `GetSpace` should return effective capabilities for the authenticated caller.

Clients can use these capabilities to enable or disable UI actions, but capabilities returned by `SpaceService` are advisory for the client. The daemon must still enforce authorization on every request.

Examples of capabilities that may appear:

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

The source of truth for capabilities is `docs/v2/design/access-control.md`.

## Roles

`SpaceService` may also return the caller's built-in roles for convenience:

- `owner`
- `admin`
- `writer`
- `reader`

Roles are convenience bundles only. The daemon enforces capabilities, not role names.

## Space state

Recommended v1 states:

```protobuf
enum SpaceState {
  SPACE_STATE_UNSPECIFIED = 0;
  SPACE_STATE_ACTIVE = 1;
  SPACE_STATE_ARCHIVED = 2;
}
```

Hard-deleted spaces are not returned by Client `SpaceService`.

Space archival and hard deletion are Admin API operations. Hard deletion requires the space to already be archived.

## No explicit OpenSpace operation

The Client API should not expose an explicit `OpenSpace` method.

Clients should pass `space_id` to later APIs, such as `DomainService`, `SessionService`, and graph/query services. The daemon is responsible for opening, caching, and evicting space resources internally.

This keeps the public API stateless at the space-selection level while still allowing daemon-side performance optimizations such as:

- space cache warm-up
- LRU/TTL eviction
- active session pinning
- metadata/index cache management

## Authorization

All `SpaceService` methods require an authenticated caller.

Suggested authorization rules:

| Method | Required authorization |
| --- | --- |
| `ListSpaces` | authenticated caller; returns only spaces with effective `space.read` or admin/system visibility |
| `GetSpace` | effective `space.read` for requested space, or admin/system visibility |

System-owned spaces are hidden from normal callers even if the caller can otherwise list user-owned spaces.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| caller cannot see requested space | `NOT_FOUND` or `PERMISSION_DENIED` |
| malformed space id | `INVALID_ARGUMENT` |
| requested space does not exist | `NOT_FOUND` |
| page token is invalid | `INVALID_ARGUMENT` |
| service unavailable | `UNAVAILABLE` |

For normal Client API callers, returning `NOT_FOUND` for inaccessible spaces can avoid leaking existence. Admin API may use more explicit `PERMISSION_DENIED` diagnostics.

## Client/Admin API boundary

### Client SpaceService

Client-facing:

- list visible spaces
- get visible space details
- report caller effective capabilities

### Admin SpaceService

Admin-facing:

- create space
- archive space
- delete archived space
- restore archived space, if supported
- manage system-owned spaces
- manage space admins
- inspect all spaces
- manage space-level storage/maintenance settings
- configure mesh placement/replication policy later

## Mesh implications

Space metadata, ownership metadata, state, and access metadata must replicate across the mesh.

A daemon serving `ListSpaces` or `GetSpace` must be able to compute effective capabilities from replicated access metadata. The detailed consistency model is future work, but the API should assume that space visibility and access decisions are mesh-relevant and not purely local.

## Open questions

- Should `SpaceSummary` and `Space` be separate protobuf messages from the start, or should v1 use one `Space` message for both list and get responses?
- Should archived spaces be hidden by default from `ListSpaces`? Current recommendation: yes, unless `include_archived` is true.
- Should Client API expose a read-only list of space access grants, or should all access inspection be Admin API? Current recommendation: defer until access-management APIs are designed.
- Should user-owned personal/default space discovery be a Mycel convention or a PKM-level convention? Current recommendation: PKM-level for now.
