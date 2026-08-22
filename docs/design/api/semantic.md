# Client Semantic API

## Status

Implemented daemon-oriented Client Semantic API for semantic generation rules.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/semantic.proto
```

This document depends on:

```text
docs/design/api/graph.md
```

## Purpose

`SemanticService` is the client-facing API for semantic search over graph data.
Clients search enabled semantic rule bindings that are visible to the caller.
Operators manage rule definitions, Intelligence Access resources, maintenance,
and backfill through Admin APIs.

The current daemon implementation uses semantic metadata under
`graphs/<space-id>/semantic/`, durable vector records, derived physical
per-rule/per-binding search indexes, and the daemon semantic search planner.
Inline encrypted secrets are resolved only through Intelligence Access and are
never returned by the Client API.

## Scope

`SemanticService` includes:

- listing searchable semantic generation rules and embedding bindings available
  to the caller for a space/domain;
- running semantic search over committed, fast-index-backed rule binding state.

`SemanticService` does not include:

- semantic rule creation/configuration;
- credential, profile, provider, or vector-store management;
- maintenance/backfill commands;
- graph mutation;
- query-language hybrid retrieval.

## Service definition

```protobuf
service SemanticService {
  rpc ListSemanticRules(ListSemanticRulesRequest) returns (ListSemanticRulesResponse);
  rpc SemanticSearch(SemanticSearchRequest) returns (SemanticSearchResponse);
}
```

## CLI

Daemon-backed Client SemanticService commands:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p '<password>' \
  semantic rule list --space-id '<space-id>' --domain personal-pkm

./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p '<password>' \
  semantic search --space-id '<space-id>' --domain personal-pkm --rule notes-search --binding search --text 'query text'
```

`semantic search` uses daemon gRPC. Local embedded search workflows are not
supported application interfaces.

## Current implementation notes

- `ListSemanticRules` validates caller graph/domain visibility and returns safe
  rule and binding display metadata only.
- `SemanticSearch` validates caller graph/domain visibility, resolves an
  explicit rule/binding or all enabled searchable bindings in the domain,
  resolves query embeddings through Intelligence Access, searches physical
  per-binding vector indexes, and loads committed graph nodes for returned hits.
- Search is not transaction-scoped and may lag graph commits until semantic
  maintenance/backfill has generated vector records.
- Search warnings are returned for safe non-fatal conditions such as missing
  grants/policies, provider failures, degraded physical search indexes, or stale
  node references.
- Admin-side semantic rule, Intelligence Access provisioning, credentials,
  policies, maintenance, and backfill operations are daemon Admin API
  responsibilities.

## Transaction scoping

`SemanticService` is not transaction-scoped in v1.

Semantic search operates over committed rule binding state and may lag behind the
latest graph writes. Requests use:

```text
space_id + domain_id
```

rather than `transaction_id`.

Response warnings may report stale, building, missing, or degraded search-index
conditions.

## ListSemanticRules

Lists searchable semantic generation rules and bindings available to the
authenticated caller for a space/domain.

Request shape:

```protobuf
message ListSemanticRulesRequest {
  string space_id = 1;
  string domain_id = 2;
  int32 page_size = 3;
  string page_token = 4;
}
```

Response shape:

```protobuf
message ListSemanticRulesResponse {
  repeated SemanticGenerationRuleSummary rules = 1;
  string next_page_token = 2;
}
```

Returned metadata must be safe for normal clients.

Do not expose:

- API keys
- raw credential ids
- secrets
- raw policy internals
- provider configuration that could leak sensitive deployment details
- raw source text or embedding vectors

Safe fields include:

- semantic rule id
- key
- display name
- description
- space id
- domain id
- enabled/state
- embedding binding keys and purposes
- safe Intelligence Access profile/vector-store labels
- derived search-index state and record counts

## SemanticSearch

Runs semantic search over committed rule binding state.

Request shape:

```protobuf
message SemanticSearchRequest {
  string space_id = 1;
  string domain_id = 2;
  string semantic_rule_id = 3;
  string embedding_binding_key = 4;
  string query = 5;
  int32 limit = 6;
  optional double min_score = 7;
}
```

If `semantic_rule_id` is omitted, the daemon uses all enabled searchable rule
bindings for the domain that the caller is allowed to read. If
`embedding_binding_key` is supplied, `semantic_rule_id` should identify the rule
that owns the binding.

Response shape:

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
  string semantic_rule_id = 6;
  string embedding_binding_key = 7;
  string vector_record_id = 8;
}
```

Including `Node` is a convenience for clients. The daemon must still enforce
graph visibility after vector candidates are selected.

## Physical search-index behavior

Semantic search must use physical per-rule/per-binding search indexes or bounded
rebuilds. Historical vector records, tombstones, and old revisions are durable
for provenance/rebuild purposes but are not scanned on every query.

If a physical search index is missing or corrupt, the daemon may rebuild it
synchronously only within a bounded limit. Otherwise it returns a degraded
warning or `FAILED_PRECONDITION`; it must not silently fall back to an unbounded
historical vector scan.

Physical search indexes are derived state and may be deleted/rebuilt from
durable vector records. Purging a semantic rule removes the durable records and
the derived search-index state for that rule.

## Filters

v1 does not include complex semantic filters.

Graph/metadata filtering belongs to `QueryService`; semantic search belongs to
`SemanticService`. Hybrid retrieval that composes graph predicates with semantic
search may be designed later.

## Maintenance behavior

Client `SemanticSearch` does not guarantee maintenance/backfill execution.
Maintenance/backfill controls are Admin API responsibilities.

Search responses may include warnings such as:

- rule binding is disabled or stale
- physical search index is missing, building, or degraded
- vector space is unavailable
- provider skipped or denied by policy
- credentials unavailable

Warnings must not include secrets, API keys, or sensitive credential material.

## Authorization

Semantic search requires:

```text
semantic.search
graph.read
```

`semantic.search` authorizes semantic retrieval. `graph.read` is required because
results expose graph nodes.

Capability mapping:

| Operation | Required capability |
| --- | --- |
| ListSemanticRules | `semantic.search` |
| SemanticSearch | `semantic.search` and `graph.read` |

## Error model

The protobuf does not define custom error messages. Implementations should use
standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| missing semantic capability | `PERMISSION_DENIED` |
| missing graph read capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| semantic rule not found | `NOT_FOUND` |
| no enabled searchable binding | `FAILED_PRECONDITION` |
| rule/binding disabled or unavailable | `FAILED_PRECONDITION` |
| physical search index missing/degraded beyond bounded rebuild | `FAILED_PRECONDITION` |
| provider unavailable | `UNAVAILABLE` |
| query/limit too large | `RESOURCE_EXHAUSTED` |
| service unavailable | `UNAVAILABLE` |

Non-fatal partial failures should be returned as warnings where safe.

## Mesh implications

Semantic generation rules are durable metadata and should replicate according to
Admin API/mesh rules.

Durable vector records and derived physical search indexes may be rebuilt locally
or replicated depending on future mesh policy. Client semantic search should not
assume that every daemon has equally fresh vector state.

A daemon should report stale/building/unavailable search-index conditions
through safe warnings or status fields.

## Open questions

- Should hybrid graph+semantic retrieval become a QueryService extension or a
  separate service later?
- Should semantic result snippets be daemon-generated, rule-generated, or
  connector-generated?
- Should result chunks expose offsets/ranges in addition to chunk ids?
- Should clients be able to request no embedded `Node` payload for lighter
  responses?
