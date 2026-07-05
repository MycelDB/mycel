# Client Import/Export API

## Status

Implemented daemon-oriented Client Import/Export API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/import_export.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/session-transaction.md
docs/v2/design/api/graph.md
docs/v2/design/api/blob.md
docs/v2/design/api/template.md
```

## Purpose

`ImportExportService` provides client/application data portability for graph/domain data.

The current daemon implementation supports transaction-scoped structured Mycel stream export/import of graph nodes, edges, optional templates, optional blob payloads, and `REPLACE_DOMAIN` mode. Raw JSON/NDJSON gRPC chunk formats and semantic-index export remain future hardening slices.

It is distinct from Admin API backup/restore. Admin backup/restore may include users, access grants, daemon configuration, mesh metadata, semantic credentials, and other operational state. Client import/export is for application data inside authorized spaces/domains.

## Logseq import direction

Mycel should not directly own Logseq-specific semantics.

The Logseq importer should remain responsible for:

```text
Logseq files/assets
  -> parse/normalize Logseq
  -> map to Mycel-native records
  -> call ImportExportService.ImportDomain
```

This keeps Mycel generic while allowing `knot_pkm_importer` to use the daemon Client API.

Recommended Logseq importer flow:

```text
OpenSession(space_id, domain_id)
BeginTransaction(read_write)
ImportDomain(transaction_id, stream)
CommitTransaction(transaction_id)
```

If import fails, the importer rolls back the transaction.

## Scope

`ImportExportService` includes:

- export domain graph data from a transaction snapshot
- import domain graph data into a read-write transaction
- Mycel-native structured import/export streams
- Mycel-native JSON/NDJSON encoded streams
- optional templates
- optional blobs
- stable external identities for importer upsert/re-import workflows

`ImportExportService` does not include:

- Logseq parsing
- users
- access grants
- auth sessions
- admin configuration
- mesh configuration
- semantic credentials
- semantic index content/configuration
- full daemon backup/restore

## Service definition

```protobuf
service ImportExportService {
  rpc ExportDomain(ExportDomainRequest) returns (stream ExportDomainResponse);
  rpc ImportDomain(stream ImportDomainRequest) returns (ImportDomainResponse);
}
```

## CLI

The daemon-backed CLI provides JSON document wrappers around the structured gRPC stream:

```sh
./bin/mycel -u alice -p '<password>' export domain \
  --transaction-id '<read-tx-id>' \
  --file domain.json \
  --include-templates \
  --include-blobs

./bin/mycel -u alice -p '<password>' import domain \
  --transaction-id '<write-tx-id>' \
  --file domain.json \
  --mode append \
  --include-templates \
  --include-blobs
```

The JSON document shape is intentionally simple for the MVP:

```json
{
  "format": "mycel-domain-json-v1",
  "manifest": { "space_id": "...", "domain_id": "..." },
  "templates": [
    { "key": "note", "version": "1.0.0", "display_name": "Note" }
  ],
  "blob_metadata": [
    { "import_blob_id": "...", "declared_mime_type": "text/plain", "size_bytes": 12 }
  ],
  "blob_chunks": [
    { "import_blob_id": "...", "chunk": "base64..." }
  ],
  "nodes": [
    { "node_id": "...", "content": "A", "props": { "tags": ["test1"] } }
  ],
  "edges": [
    { "edge_id": "...", "from_node_id": "...", "to_node_id": "...", "kind": "contains", "props": { "order": 0 } }
  ]
}
```

`import domain` defaults to preserving supplied node/edge ids so exported documents can be imported into another domain while preserving containment edge endpoints.

## Current implementation notes

- `ExportDomain` currently supports `DOMAIN_EXPORT_FORMAT_MYCEL_STREAM`.
- `ImportDomain` currently supports `DOMAIN_IMPORT_FORMAT_MYCEL_STREAM`.
- Node and edge records are supported.
- Template records are supported when `include_templates` is set.
- Blob metadata/chunk records are supported when `include_blobs` is set.
- `APPEND`, basic `UPSERT`, and `REPLACE_DOMAIN` modes are supported for graph records.
- Raw JSON chunks, NDJSON chunks, and semantic-index export are not yet implemented.
- Import mutates only the transaction overlay; callers still commit or roll back through `TransactionService`.
- Export reads the active transaction snapshot, including read-your-writes for read-write transactions.

## Transaction scoping

Both import and export are transaction-scoped.

Export uses a readable transaction:

```text
ExportDomain(transaction_id)
```

Import uses a read-write transaction:

```text
ImportDomain(metadata.transaction_id)
```

The transaction determines:

- space
- domain
- snapshot/base revision for export
- mutation buffer for import
- authorization context

Commit/rollback remains separate and is handled by `TransactionService`.

## ExportDomain

Exports graph/domain data from a transaction snapshot.

Request:

```protobuf
message ExportDomainRequest {
  string transaction_id = 1;
  DomainExportFormat format = 2;
  DomainExportOptions options = 3;
}
```

Response stream:

```protobuf
message ExportDomainResponse {
  oneof part {
    DomainExportManifest manifest = 1;
    ImportExportRecord record = 2;
    bytes chunk = 3;
  }
}
```

Structured records are used for `DOMAIN_EXPORT_FORMAT_MYCEL_STREAM`.

Raw chunks are used for JSON/NDJSON or other encoded payload formats.

## ImportDomain

Imports Mycel-native data into a read-write transaction.

Request stream:

```protobuf
message ImportDomainRequest {
  oneof part {
    ImportDomainMetadata metadata = 1;
    ImportExportRecord record = 2;
    bytes chunk = 3;
  }
}
```

The first message should contain import metadata, followed by records or chunks according to format.

Response:

```protobuf
message ImportDomainResponse {
  ImportSummary summary = 1;
}
```

## Formats

Supported design-level formats:

```text
MYCEL_STREAM
MYCEL_JSON
MYCEL_NDJSON
```

`MYCEL_STREAM` uses structured protobuf records.

`MYCEL_JSON` and `MYCEL_NDJSON` use raw chunks containing encoded Mycel-native payloads.

Logseq is intentionally not a Mycel import format. Source-specific conversion belongs to application importers.

## Import modes

Supported v1 import modes:

### APPEND

Create new records. Duplicate ids or duplicate import identities fail.

### UPSERT

Create or update records by supplied ids or stable import identities.

This is the preferred long-term mode for repeatable imports such as Logseq re-imports.

### REPLACE_DOMAIN

Clear the domain and then import the stream.

This is destructive and requires delete capabilities in addition to write capabilities.

## Import/export records

A structured import/export record may contain:

- template definition
- node
- edge
- blob metadata
- blob chunk

Each record may also include optional import identity:

```protobuf
message ImportIdentity {
  string source_system = 1;
  string source_id = 2;
}
```

Import identity lets an importer map stable source objects to Mycel objects. For example, the Logseq importer can map a source page/block id to a stable Mycel node for upsert/re-import behavior.

## Blobs

Import/export can include blobs when requested.

Blob records use an import-local blob id to associate metadata and chunks:

```text
BlobImportMetadata.import_blob_id
BlobImportChunk.import_blob_id
```

Graph nodes can reference blob ids in their node payloads. The importer/daemon is responsible for resolving import-local blob ids into stored blob ids during import.

Raw blob upload/download remains in `BlobService`.

Blob-backed node creation remains in `GraphService.CreateBlobNode` for normal interactive graph mutation.

## Templates

Import/export can include templates when requested.

Template import requires:

```text
template.manage
```

Template export requires:

```text
template.read
```

Template policy and lifecycle rules are defined in:

```text
docs/v2/design/api/template.md
```

## Semantic indexes

Client ImportExportService does not export semantic index configuration or semantic index content in v1.

Semantic index configuration, provider credentials, policies, maintenance, and backups are Admin API concerns.

If a client requests semantic index export through `include_semantic_indexes`, the daemon should return a clear unsupported/failed-precondition error.

## Authorization

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| Export graph data | `graph.read` |
| Import append/upsert graph data | `graph.write` |
| Import replace domain | `graph.write` and `graph.delete` |
| Export templates | `template.read` |
| Import templates | `template.manage` |
| Export blobs | `blob.read` |
| Import blobs | `blob.write` |

Delete is not included in write.

## Dry run

`DomainImportOptions.dry_run` validates the import stream and reports counts/warnings without mutating the transaction.

Dry run is useful for import previews and importer diagnostics.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| transaction not found/expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| export from non-readable transaction | `FAILED_PRECONDITION` |
| import into non-write transaction | `FAILED_PRECONDITION` |
| missing capability | `PERMISSION_DENIED` |
| malformed stream | `INVALID_ARGUMENT` |
| duplicate id/import identity in append mode | `ALREADY_EXISTS` |
| replace requested without delete capability | `PERMISSION_DENIED` |
| unsupported semantic export request | `FAILED_PRECONDITION` |
| import too large | `RESOURCE_EXHAUSTED` |
| storage unavailable | `UNAVAILABLE` |

## Mesh implications

Import mutations occur inside a transaction. Once committed, the transaction becomes normal durable graph state and replicates according to mesh replication rules.

Export reads from the daemon's transaction snapshot. In mesh mode, different daemons may expose different freshness depending on replication state.

## Open questions

- Should JSON/NDJSON schemas be formally versioned separately from protobuf messages?
- Should export support server-side compression negotiation?
- Should import support a mapping report from source ids to assigned Mycel ids?
- Should `REPLACE_DOMAIN` require an additional confirmation token or Admin API approval?
- Should blob chunks carry per-chunk checksums, or is whole-blob digest sufficient?
