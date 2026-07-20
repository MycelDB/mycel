# Client Metadata Catalog API

## Status

Implemented daemon-oriented Client Metadata Catalog API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/metadata_catalog.proto
```

This document depends on:

```text
docs/design/access-control.md
docs/design/api/session-transaction.md
docs/design/api/query.md
```

## Purpose

`MetadataCatalogService` is a small transaction-scoped Client API for metadata discovery.

The current daemon implementation scans the active graph transaction snapshot, including staged writes in read-write transactions.

It lists known canonical tags and custom property names visible in a transaction snapshot.

Metadata filtering/search belongs in `QueryService`, not this service, so graph relationships and metadata predicates can be composed in a single query.

## Scope

`MetadataCatalogService` includes:

- list known tags
- list known custom property names
- counts for how many nodes currently expose each tag/property name

`MetadataCatalogService` does not include:

- finding nodes by tag/property
- structured metadata search
- graph traversal
- metadata mutation
- semantic search

Metadata writes happen through `GraphService` by updating node props. Metadata search/filtering happens through `QueryService`.

## Service definition

```protobuf
service MetadataCatalogService {
  rpc ListTags(ListTagsRequest) returns (ListTagsResponse);
  rpc ListPropertyNames(ListPropertyNamesRequest) returns (ListPropertyNamesResponse);
}
```

## CLI

Daemon-backed metadata catalog commands:

```sh
./bin/mycel -u alice -p '<password>' metadata tags --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' metadata properties --transaction-id '<tx-id>'
```

Text output is tab-separated `name` and `node_count`. JSON output returns the protobuf response shape.

## Current implementation notes

- `ListTags` normalizes tag values with the existing graph metadata helpers.
- `ListPropertyNames` normalizes custom property names from the canonical `properties` node prop.
- Results are sorted by descending `node_count`, then ascending name.
- Pagination is offset-token based and capped by daemon policy.
- Property value listing remains a future API extension because the current proto only defines tag and property-name summaries.

## Transaction scoping

Every request includes:

```text
transaction_id
```

The transaction determines:

- space
- domain
- read snapshot/base revision
- authorization context

The service should read from the transaction snapshot. Ideally this includes staged writes in a read-write transaction. If the initial implementation can only read committed metadata indexes, that should be documented as an implementation limitation rather than changing the API shape.

## ListTags

Lists known canonical tags visible in the transaction snapshot.

Suggested request:

```protobuf
message ListTagsRequest {
  string transaction_id = 1;
  int32 page_size = 2;
  string page_token = 3;
}
```

Suggested response:

```protobuf
message ListTagsResponse {
  repeated TagSummary tags = 1;
  string next_page_token = 2;
}
```

Each tag summary includes:

```protobuf
message TagSummary {
  string name = 1;
  int64 node_count = 2;
}
```

`name` is the canonical normalized tag name.

## ListPropertyNames

Lists known canonical custom property names visible in the transaction snapshot.

Suggested request:

```protobuf
message ListPropertyNamesRequest {
  string transaction_id = 1;
  int32 page_size = 2;
  string page_token = 3;
}
```

Suggested response:

```protobuf
message ListPropertyNamesResponse {
  repeated PropertySummary properties = 1;
  string next_page_token = 2;
}
```

Each property summary includes:

```protobuf
message PropertySummary {
  string name = 1;
  int64 node_count = 2;
}
```

`name` is the canonical normalized custom property name.

## Authorization

All metadata catalog operations require a valid access token and an active readable transaction.

Suggested capabilities:

```text
metadata.read
graph.read
```

`metadata.read` authorizes metadata catalog discovery. `graph.read` is generally required because metadata describes graph nodes in the transaction's domain.

## Relationship to QueryService

`QueryService` owns metadata filtering/search predicates, such as:

- has tag
- property exists
- property equals

This lets callers compose graph traversal with metadata constraints in one query, for example:

```text
nodes under Project X
AND tag = urgent
AND property status = active
```

`MetadataCatalogService` only helps clients discover which tags/property names exist.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| transaction not found or expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| missing metadata capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| page token is invalid | `INVALID_ARGUMENT` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Metadata catalog results are derived from graph node props in a transaction snapshot. Catalog requests are not replicated.

Committed graph mutations and replicated metadata indexes determine what catalog results a daemon can return for a replicated domain.

## Open questions

- Should future catalog APIs expose common values for a property name?
- Should tag/property summaries include last-used timestamps?
- Should catalog methods support prefix filtering for autocomplete?
- Should result counts include only nodes visible to the caller if future fine-grained node/subtree access is added?
