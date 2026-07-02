# Client Domain API

## Status

Draft design for the daemon-oriented Client Domain API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/domain.proto
```

This document depends on the cross-cutting access-control model in:

```text
docs/design/access-control.md
```

## Purpose

`DomainService` is the client-facing API for listing, inspecting, creating, updating, and deleting domains inside a space.

The Client API is domain-aware. Client graph sessions and graph operations operate inside an explicit space/domain context.

## Scope

The Client `DomainService` includes:

- listing domains in an authorized space
- retrieving one domain
- creating a domain
- updating domain metadata
- deleting a domain and all graph content contained in it

The Client `DomainService` does **not** include:

- mesh placement or replication policy configuration
- system-owned space management
- system/internal domain management for normal users
- graph node/edge mutation directly; those happen through graph sessions/services

## Service definition

```protobuf
service DomainService {
  rpc ListDomains(ListDomainsRequest) returns (ListDomainsResponse);
  rpc GetDomain(GetDomainRequest) returns (GetDomainResponse);
  rpc CreateDomain(CreateDomainRequest) returns (CreateDomainResponse);
  rpc UpdateDomain(UpdateDomainRequest) returns (UpdateDomainResponse);
  rpc DeleteDomain(DeleteDomainRequest) returns (DeleteDomainResponse);
}
```

## Methods

### ListDomains

Lists domains visible to the authenticated caller in a space.

Required capability:

```text
domain.read
```

Normal clients should not see system/internal domains by default. The request may include `include_system`, but that flag should only take effect for callers with admin/system visibility.

Suggested request fields:

```protobuf
message ListDomainsRequest {
  string space_id = 1;
  int32 page_size = 2;
  string page_token = 3;
  bool include_system = 4;
}
```

Suggested response fields:

```protobuf
message ListDomainsResponse {
  repeated Domain domains = 1;
  string next_page_token = 2;
}
```

### GetDomain

Returns a domain by id.

Required capability:

```text
domain.read
```

For normal clients, requesting a hidden system/internal domain should return `NOT_FOUND` or `PERMISSION_DENIED` according to the error-leakage policy.

### CreateDomain

Creates a new non-system domain in a space.

Required capability:

```text
domain.create
```

Domains are flat within a space. There is no domain hierarchy.

The domain name must be unique within the space among non-deleted domains.

### UpdateDomain

Updates mutable domain metadata.

Required capability:

```text
domain.update
```

Mutable fields:

- description
- non-default, non-system domain name

The default domain's name cannot be changed.

### DeleteDomain

Deletes a domain.

Required capability:

```text
domain.delete
```

Deleting a domain is destructive and cascades to all graph content contained in the domain. This includes the domain's graph data such as nodes, edges, and domain-scoped graph state.

The default domain cannot be deleted.

System/internal domains cannot be deleted by normal clients.

## Domain model

A domain is a flat partition inside a space.

Recommended fields:

```protobuf
message Domain {
  string space_id = 1;
  string domain_id = 2;
  string name = 3;
  string description = 4;
  DomainState state = 5;
  bool default = 6;
  bool system = 7;
  google.protobuf.Timestamp create_time = 8;
  google.protobuf.Timestamp update_time = 9;
  mycel.common.v1.EffectiveAccess caller_access = 10;
}
```

A domain has no parent domain.

## Default domain

Every space has a default domain.

Default domain rules:

- created automatically with the space
- named `default`
- cannot be renamed
- cannot be deleted
- visible to authorized normal clients

The default domain gives simple applications a stable domain to use while preserving the domain-aware API model.

## System/internal domains

System/internal domains are supported.

Visibility rules:

- hidden from normal `ListDomains` by default
- `include_system` may be requested, but only authorized admin/system callers should receive system/internal domains
- inaccessible system/internal domains should not leak implementation details to normal callers

System/internal domains are intended for daemon/system use and future internal features.

## Domain deletion semantics

Domain deletion is hard deletion in v1, not archival.

Deleting a domain deletes all content contained by that domain. The operation should be treated as a destructive graph operation and audited.

Deletion constraints:

- default domain cannot be deleted
- system/internal domains require admin/system authorization
- active graph sessions in the domain should be handled safely by the daemon, for example by rejecting deletion while active write sessions exist or by coordinating session invalidation

The exact active-session coordination behavior is an implementation-plan concern.

## Authorization

| Method | Required capability |
| --- | --- |
| `ListDomains` | `domain.read` |
| `GetDomain` | `domain.read` |
| `CreateDomain` | `domain.create` |
| `UpdateDomain` | `domain.update` |
| `DeleteDomain` | `domain.delete` |

`domain.delete` is sufficient authority for deleting the domain and its contained graph data. The daemon does not require separate `graph.delete` for the cascade unless a later design changes this rule.

## Effective capabilities

Domain responses may include `caller_access` so clients can determine which domain actions should be enabled in the UI.

As with spaces, this is advisory for clients. The daemon enforces authorization on every request.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| caller cannot see requested space/domain | `NOT_FOUND` or `PERMISSION_DENIED` |
| malformed space/domain id | `INVALID_ARGUMENT` |
| duplicate domain name | `ALREADY_EXISTS` |
| default domain rename attempted | `FAILED_PRECONDITION` |
| default domain delete attempted | `FAILED_PRECONDITION` |
| active sessions block deletion | `FAILED_PRECONDITION` |
| service unavailable | `UNAVAILABLE` |

For normal Client API callers, returning `NOT_FOUND` for inaccessible domains can avoid leaking existence. Admin API may use more explicit `PERMISSION_DENIED` diagnostics.

## Mesh implications

Domain metadata must replicate across the mesh.

A domain may become a useful future replication, placement, policy, or routing boundary. The v1 API should therefore treat domain identity and metadata as stable replicated state.

Deleting a domain must replicate as a destructive domain-level operation so other daemons remove the same domain and contained graph data.

## Open questions

- Should domain delete reject when read sessions are active, or only when write sessions are active?
- Should the daemon provide a preview/count endpoint for domain delete before deletion?
- Should system/internal domain creation be Admin API only, or internal daemon-only?
