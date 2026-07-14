# Client Domain API

## Status

Implemented daemon-oriented Client Domain API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/domain.proto
```

This document depends on the cross-cutting access-control model in:

```text
docs/v2/design/access-control.md
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

Returns a domain by id or stable key.

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

The domain key must be unique within the space among non-deleted domains. The CLI preserves the existing `domain add KEY --name ...` shape by sending both key and display name.

### UpdateDomain

Updates mutable domain metadata.

Required capability:

```text
domain.update
```

Mutable fields:

- description
- non-default, non-system domain name
- `discovery_mode`
- `search_mode`
- `semantic_mode`
- `read_only`

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
  string key = 11;
  DomainDiscoveryMode discovery_mode = 12;
  DomainSearchMode search_mode = 13;
  DomainSemanticMode semantic_mode = 14;
  bool read_only = 15;
}
```

A domain has no parent domain. Mycel preserves the existing stable domain `key` concept for CLI compatibility and human-readable references.

## Domain policy

Domain policy separates discovery, graph search/query, semantic behavior, and mutability.

### Discovery mode

`discovery_mode` controls broad domain listing and browsing:

- `normal`: appears in ordinary domain listing/browsing.
- `explicit_only`: hidden from ordinary discovery/listing, but accessible by explicit domain id/key.
- `hidden`: hidden from ordinary discovery and inaccessible through normal visible-domain APIs unless the caller has admin/system visibility.

### Search mode

`search_mode` controls graph query/search behavior:

- `normal`: included in ordinary broad query/search.
- `explicit_only`: excluded from broad query/search, but allowed when the caller explicitly targets the domain.
- `disabled`: not queryable/searchable through normal user search APIs.

### Semantic mode

`semantic_mode` controls semantic indexing and semantic search:

- `normal`: indexed and included in ordinary semantic search.
- `explicit_only`: indexed and semantically searchable only when explicitly targeted.
- `disabled`: not semantically indexed and not semantically searchable.

### Read-only domains

`read_only = true` rejects normal client/user write transactions and graph mutations. Admin/import paths may still replace or update read-only domains.

Common policy presets:

| Use case | discovery_mode | search_mode | semantic_mode | read_only |
| --- | --- | --- | --- | --- |
| Normal user content | `normal` | `normal` | `normal` | `false` |
| Private app/user metadata | `explicit_only` | `disabled` | `disabled` | `false` |
| Product manual/help corpus | `explicit_only` | `explicit_only` | `explicit_only` | `true` |

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

## CLI

The CLI now uses daemon gRPC and standard-user credentials for domain commands:

```sh
./bin/mycel -u alice -p '<password>' domain list --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' domain show default --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' domain add notes --space-id '<space-id>' --name Notes
./bin/mycel -u alice -p '<password>' domain update --space-id '<space-id>' --domain-id '<domain-id>' --description 'Updated'
./bin/mycel -u alice -p '<password>' domain delete '<domain-id>' --space-id '<space-id>'
```

`domain show` can resolve by key or by `--domain-id`. Domain creation currently requires effective space admin access, matching the previous embedded engine behavior.

## Current implementation notes

- Domains are stored in the daemon space module's metadata store under `<MYCELD_DATA_DIR>/meta/domains.json`.
- `DomainService` is registered on the Client API and uses user bearer tokens from Client `AuthService`.
- Default domains cannot be deleted or renamed.
- Domain delete removes domain metadata and domain embedding policies. Graph-content cascade will be hardened further when daemon graph/session services are migrated.

## Open questions

- Should domain delete reject when read sessions are active, or only when write sessions are active?
- Should the daemon provide a preview/count endpoint for domain delete before deletion?
- Should system/internal domain creation be Admin API only, or internal daemon-only?
