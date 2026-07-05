# Client Blob API

## Status

Implemented daemon-oriented Client Blob API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/blob.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/graph.md
```

## Purpose

`BlobService` is the client-facing API for raw blob content and blob metadata.

The current daemon implementation stores raw content under:

```text
<MYCELD_DATA_DIR>/blobs/<space-id>
```

and metadata under:

```text
<MYCELD_DATA_DIR>/blob_meta/<space-id>.json
```

Blob-backed graph node creation belongs to `GraphService` because it creates graph state inside a transaction. `BlobService` itself does not mutate the graph.

## Existing model alignment

Current Mycel graph nodes support optional blob references:

```go
type Node struct {
    BlobRef *BlobID
    Content string
}
```

A node has inline text content or blob content, never both.

Current blob-backed node creation is graph-aware (`AddBlobNode`) because it uploads binary content and creates a graph node that references the stored blob.

The daemon API preserves this split:

- `BlobService`: raw blob upload/download/get/delete
- `GraphService.CreateBlobNode`: transaction-scoped graph node creation with streamed blob content

## Scope

`BlobService` includes:

- upload raw blob content
- download raw blob content
- get blob metadata
- delete unreferenced blob content

`BlobService` does not include:

- graph node creation
- graph node deletion
- graph mutation
- semantic indexing
- template validation

## Service definition

```protobuf
service BlobService {
  rpc UploadBlob(stream UploadBlobRequest) returns (UploadBlobResponse);
  rpc DownloadBlob(DownloadBlobRequest) returns (stream DownloadBlobResponse);
  rpc GetBlob(GetBlobRequest) returns (GetBlobResponse);
  rpc DeleteBlob(DeleteBlobRequest) returns (DeleteBlobResponse);
}
```

## Blob model

A blob represents content-addressed binary content in a space.

Recommended fields:

```protobuf
message Blob {
  string blob_id = 1;
  string space_id = 2;
  string digest = 3;
  int64 size_bytes = 4;
  string mime_type = 5;
  string declared_mime_type = 6;
  string original_filename = 7;
  google.protobuf.Timestamp create_time = 8;
}
```

The daemon may deduplicate content by digest. The API exposes digest/blob metadata without requiring clients to know storage internals.

## UploadBlob

Uploads raw blob content.

The request is client-streaming:

```protobuf
message UploadBlobRequest {
  oneof part {
    UploadBlobMetadata metadata = 1;
    bytes chunk = 2;
  }
}
```

The first message should contain metadata, followed by one or more chunk messages.

`UploadBlob` creates raw blob content but does not create a graph node. This supports upload-then-attach workflows and low-level blob storage use cases.

Requires:

```text
blob.write
```

## DownloadBlob

Downloads raw blob content.

The response is server-streaming:

```protobuf
message DownloadBlobResponse {
  oneof part {
    Blob blob = 1;
    bytes chunk = 2;
  }
}
```

The response stream should include metadata followed by content chunks.

Requires:

```text
blob.read
```

## GetBlob

Returns blob metadata only.

Requires:

```text
blob.read
```

## DeleteBlob

Deletes raw blob content only when it is unreferenced.

Direct blob deletion must fail when graph nodes still reference the blob.

Referenced blob cleanup should be handled by graph/node deletion semantics, not direct `DeleteBlob`.

Requires:

```text
blob.delete
```

## GraphService.CreateBlobNode

Blob-backed graph node creation belongs to `GraphService`.

`CreateBlobNode`:

- is transaction-scoped
- streams binary content
- stores the blob
- creates a graph node referencing that blob
- enforces graph/template rules
- auto-populates blob metadata props where appropriate

Requires:

```text
graph.write
blob.write
```

See:

```text
docs/v2/design/api/graph.md
```

## CLI

Daemon-backed raw blob commands:

```sh
./bin/mycel -u alice -p '<password>' blob upload --space-id '<space-id>' --mime-type text/plain ./note.txt
./bin/mycel -u alice -p '<password>' blob get --space-id '<space-id>' '<blob-id>'
./bin/mycel -u alice -p '<password>' blob download --space-id '<space-id>' '<blob-id>' --output-file ./note.txt
./bin/mycel -u alice -p '<password>' blob delete --space-id '<space-id>' '<blob-id>'
```

`blob upload` is raw blob upload only. Transaction-scoped graph attachment is available through `GraphService.CreateBlobNode` and the CLI command:

```sh
./bin/mycel -u alice -p '<password>' graph blob-node create --transaction-id '<tx-id>' --mime-type image/png ./image.png
```

## Current implementation notes

- Client and server streaming RPCs are protected by the same bearer-token auth as unary client RPCs.
- Raw blob IDs are SHA-256 content addresses; the API `digest` field is exposed as `sha256:<blob-id>`.
- Uploads are deduplicated by content address.
- `DeleteBlob` checks daemon graph storage and rejects blobs with live graph references.
- `GraphService.CreateBlobNode` uploads blob content, creates a blob-backed graph node in the transaction overlay, and auto-populates blob metadata props.

## Browser/Connect-Web considerations

Backend/service clients can use native gRPC streaming.

Browser clients using Connect-Web may need transport-specific support for large uploads/downloads. A future JSON/HTTP or multipart gateway can be added without changing the daemon's internal blob model.

## Authorization

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| UploadBlob | `blob.write` |
| DownloadBlob | `blob.read` |
| GetBlob | `blob.read` |
| DeleteBlob | `blob.delete` |
| GraphService.CreateBlobNode | `graph.write` and `blob.write` |

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| missing blob capability | `PERMISSION_DENIED` |
| malformed id/metadata | `INVALID_ARGUMENT` |
| blob not found | `NOT_FOUND` |
| referenced blob delete attempted | `FAILED_PRECONDITION` |
| upload too large | `RESOURCE_EXHAUSTED` |
| unsupported content type | `INVALID_ARGUMENT` |
| storage unavailable | `UNAVAILABLE` |

## Mesh implications

Blob metadata and content availability must replicate according to the space/domain replication policy.

Graph nodes reference blobs by blob id/ref. Replication must ensure blob content is available wherever replicated graph state references it, or expose clear degraded/unavailable states.

## Open questions

- Should raw unreferenced blobs have a retention window before garbage collection?
- Should `DeleteBlob` support a dry-run/reference count response?
- Should the daemon expose chunk-size recommendations to connectors?
- Should browser clients use a separate multipart gateway for upload/download?
