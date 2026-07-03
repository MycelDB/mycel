# Client Semantic API

## Status

Draft design for the daemon-oriented Client Semantic API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/semantic.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/graph.md
```

## Purpose

`SemanticService` is the client-facing API for semantic search over graph data.

The Client API owns using semantic search. The Admin API owns semantic infrastructure and operations, including:

- semantic index creation
- semantic index configuration
- provider/model/vector-store setup
- credential grants and policies
- semantic maintenance
- backfill controls

## Scope

`SemanticService` includes:

- listing semantic indexes available to the caller for a space/domain
- running semantic search over committed semantic index state

`SemanticService` does not include:

- semantic index creation/configuration
- credential management
- provider/model configuration
- maintenance/backfill commands
- graph mutation
- query-language hybrid retrieval

## Service definition

```protobuf
service SemanticService {
  rpc ListSemanticIndexes(ListSemanticIndexesRequest) returns (ListSemanticIndexesResponse);
  rpc SemanticSearch(SemanticSearchRequest) returns (SemanticSearchResponse);
}
```

## Transaction scoping

`SemanticService` is not transaction-scoped in v1.

Semantic search operates over committed semantic index state and may lag behind the latest graph writes. Requests use:

```text
space_id + domain_id
```

rather than `transaction_id`.

Response warnings may report stale/building/unavailable index conditions.

## ListSemanticIndexes

Lists searchable semantic indexes available to the authenticated caller for a space/domain.

Suggested request:

```protobuf
message ListSemanticIndexesRequest {
  string space_id = 1;
  string domain_id = 2;
  int32 page_size = 3;
  string page_token = 4;
}
```

Suggested response:

```protobuf
message ListSemanticIndexesResponse {
  repeated SemanticIndex indexes = 1;
  string next_page_token = 2;
}
```

Returned index metadata must be safe for normal clients.

Do not expose:

- API keys
- raw credential ids
- secrets
- raw policy internals
- provider configuration that could leak sensitive deployment details

Safe fields include:

- index id
- key
- display name
- description
- space id
- domain id
- model display label
- vector store display label
- index state

## SemanticSearch

Runs semantic search over committed index state.

Suggested request:

```protobuf
message SemanticSearchRequest {
  string space_id = 1;
  string domain_id = 2;
  optional string semantic_index_id = 3;
  string query = 4;
  int32 limit = 5;
  optional double min_score = 6;
}
```

If `semantic_index_id` is omitted, the daemon uses the default searchable semantic index for the domain when one is available.

Suggested response:

```protobuf
message SemanticSearchResponse {
  repeated SemanticSearchResult results = 1;
  repeated string warnings = 2;
}
```

Result shape:

```protobuf
message SemanticSearchResult {
  string node_id = 1;
  double score = 2;
  Node node = 3;
  repeated string matched_chunk_ids = 4;
  string snippet = 5;
}
```

Including `Node` is a convenience for clients. The daemon must still enforce graph visibility.

## Filters

v1 does not include complex semantic filters.

Graph/metadata filtering belongs to `QueryService`; semantic search belongs to `SemanticService`. Hybrid retrieval that composes graph predicates with semantic search may be designed later.

## Maintenance behavior

Client `SemanticSearch` does not guarantee maintenance/backfill execution.

The daemon may opportunistically maintain indexes, but maintenance/backfill controls are Admin API responsibilities.

Search responses may include warnings such as:

- index is stale
- index is building
- vector space unavailable
- provider skipped
- credentials unavailable

Warnings must not include secrets, API keys, or sensitive credential material.

## Authorization

Semantic search requires:

```text
semantic.search
graph.read
```

`semantic.search` authorizes semantic retrieval. `graph.read` is required because results expose graph nodes.

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| ListSemanticIndexes | `semantic.search` |
| SemanticSearch | `semantic.search` and `graph.read` |

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| missing semantic capability | `PERMISSION_DENIED` |
| missing graph read capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| semantic index not found | `NOT_FOUND` |
| no default semantic index | `FAILED_PRECONDITION` |
| index disabled/unavailable | `FAILED_PRECONDITION` |
| provider unavailable | `UNAVAILABLE` |
| query/limit too large | `RESOURCE_EXHAUSTED` |
| service unavailable | `UNAVAILABLE` |

Non-fatal partial failures should be returned as warnings where safe.

## Mesh implications

Semantic index configuration is durable metadata and should replicate according to Admin API/mesh rules.

Semantic index contents may be rebuilt locally or replicated depending on future mesh policy. Client semantic search should not assume that every daemon has equally fresh vector state.

A daemon should report stale/building/unavailable index conditions through safe warnings or status fields.

## Open questions

- Should hybrid graph+semantic retrieval become a QueryService extension or a separate service later?
- Should semantic result snippets be daemon-generated, index-generated, or connector-generated?
- Should result chunks expose offsets/ranges in addition to chunk ids?
- Should clients be able to request no embedded `Node` payload for lighter responses?
