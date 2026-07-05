# Client Semantic API

## Status

Implemented daemon-oriented Client Semantic API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/semantic.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/graph.md
```

## Purpose

`SemanticService` is the client-facing API for semantic search over graph data.

The current daemon implementation uses the existing semantic metadata stores under `meta/` and `graphs/<space-id>/semantic/`, the embedded `mycel-file` vector backend, and the existing semantic search planner. Inline encrypted secrets can be decrypted by the daemon when `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` is configured.

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

## CLI

Daemon-backed Client SemanticService commands:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p '<password>' \
  semantic index list --space-id '<space-id>' --domain personal-pkm

./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p '<password>' \
  semantic search --space-id '<space-id>' --domain personal-pkm --index notes-search --text 'query text'
```

`semantic search` currently uses daemon gRPC when `--daemon-addr` is supplied; embedded legacy behavior is retained for local semantic maintenance/backfill tests and workflows until the admin-side semantic operations are moved behind daemon APIs.

## Current implementation notes

- `ListSemanticIndexes` validates caller graph/domain visibility and returns safe display metadata only.
- `SemanticSearch` validates caller graph/domain visibility, resolves an explicit semantic index or all enabled search indexes in the domain, runs the existing semantic search planner, and loads committed graph nodes for returned hits.
- Search is not transaction-scoped and may lag graph commits until semantic maintenance/backfill has generated vector records.
- Search warnings are returned for safe non-fatal conditions such as missing grants/policies, provider failures, or stale node references.
- Admin-side semantic index list/upsert now has an AdminSemanticService MVP. Inference provisioning, credentials, policies, maintenance, and backfill remain embedded CLI/store workflows for now.

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

If `semantic_index_id` is omitted, the daemon uses all enabled searchable semantic indexes for the domain in the current MVP. A future default-index selector may narrow that behavior.

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
