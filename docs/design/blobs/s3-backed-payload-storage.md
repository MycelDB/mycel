# S3-backed blob payload storage

## Status

Target design for issue #6, implemented on the S3 blob storage feature branch.
The default local filesystem backend remains the compatibility baseline.

See also:

- [Client blob API](../api/blob.md)
- [Blob design index](README.md)
- [S3 blob storage operations procedure](../../operations/procedures/s3-blob-storage.md)
- [S3-backed blob payload storage implementation plan](../../implementation/unreleased/s3-backed-blob-payload-storage-implementation-plan.md)

## Summary

Mycel stores blob metadata, graph references, WAL records, Raft logs, indexes,
and subsystem state on node-local or per-node block storage. S3-backed blob
payload storage moves only the large immutable blob bytes to S3. Public blob and
blob-node APIs continue to expose Mycel blob IDs, digest, size, MIME metadata,
and graph references; they do not expose bucket/key details.

The design supports two payload backends:

- `local` — the existing per-space content-addressed filesystem store;
- `s3` — a shared S3 object store for new blob payload bytes.

Existing metadata without an explicit payload descriptor is interpreted as
`local` so old data remains readable.

## Goals

- Keep local filesystem blob storage working by default.
- Store new blob payload bytes in S3 when configured.
- Preserve client-visible `BlobService` and blob-node semantics.
- Keep blob IDs content-addressed as the SHA-256 hex digest of payload bytes.
- Commit Mycel metadata only after payload bytes are durably stored.
- Allow any authorized node in a Raft cluster to read S3-backed payloads without
  pod-to-pod payload copying.
- Keep referenced-blob delete protection unchanged.
- Avoid making S3 the store for WAL, Raft logs, graph state, metadata, schemas,
  semantic indexes, automation state, or backups by default.

## Non-goals

- No public externally-owned object-reference API.
- No automatic migration of existing local blob payloads to S3.
- No storage of WAL, Raft logs, indexes, graph state, or subsystem metadata in
  S3.
- No static S3 access-key/secret-key configuration owned by Mycel. Operators use
  the AWS SDK default credential chain.
- No automatic bucket lifecycle, retention, replication, or encryption policy
  creation.

## Current local blob model

The local backend stores blob payloads under the daemon data directory:

```text
<data-dir>/blobs/<space-id>/objects/<aa>/<sha256-hex>
<data-dir>/blobs/<space-id>/tmp/<staged-write>
<data-dir>/blob_meta/<space-id>.json
```

Blob metadata is committed through the blob subsystem WAL path or, in Raft mode,
through partition-scoped blob metadata commands. Blob payload bytes themselves
are not placed in Raft logs or snapshots. In local multi-node Raft mode, a
follower that applies blob metadata may fetch the payload from a peer through the
internal cluster backend and verify the size/checksum before exposing metadata.

## Payload descriptor

Blob metadata may include an internal payload descriptor. The descriptor is
replicated with blob metadata and is used by the service to open, verify, and
clean up payload bytes.

```text
backend: local | s3
space_id: <space-id>
blob_id: <sha256-hex>
size_bytes: <size>
checksum_algorithm: sha256
checksum_hex: <sha256-hex>
s3_bucket: <bucket, for s3>
s3_key: <object-key, for s3>
s3_region: <region, optional>
s3_etag: <etag observed at write, optional>
```

Compatibility rule: if `payload` is absent, the descriptor is synthesized as a
local descriptor from the existing `space_id`, `blob_id`, `digest`, and
`size_bytes` fields.

The public API mapping omits the descriptor, so clients continue to see the
stable blob fields only.

## Configuration

Blob payload backend is selected at daemon startup:

```sh
MYCELD_BLOB_BACKEND=local|s3
MYCELD_BLOB_S3_BUCKET=<bucket>        # required for s3
MYCELD_BLOB_S3_PREFIX=<prefix>        # optional
MYCELD_BLOB_S3_REGION=<region>        # optional but recommended
MYCELD_BLOB_S3_KMS_KEY_ID=<kms-key>   # optional SSE-KMS
```

LocalStack and S3-compatible testing can also use:

```sh
MYCELD_BLOB_S3_ENDPOINT_URL=<endpoint-url>
MYCELD_BLOB_S3_FORCE_PATH_STYLE=true|false
```

Authentication uses the AWS SDK default credential chain. In production that
should normally be an EC2 instance profile, ECS task role, or EKS IRSA/web
identity role. Standard AWS environment variables and profiles remain available
for developer testing, but Mycel does not define separate access-key settings.

## S3 object layout

Objects are deterministic and content-addressed:

```text
<prefix>/spaces/<space-id>/objects/<aa>/<sha256-hex>
```

`<aa>` is the first two characters of the SHA-256 hex digest. The prefix is
trimmed of leading/trailing slashes. Because keys are derived from a validated
space ID and the content digest, user-provided filenames never affect S3 object
paths.

## Upload flow

1. The blob service validates `space_id` and reader presence.
2. The service reads a small prefix for MIME sniffing, then passes the full
   stream to the configured payload backend.
3. The S3 backend stages the stream to a local temporary file while computing
   SHA-256 and byte size.
4. The blob ID is the SHA-256 hex digest.
5. The S3 backend uploads the staged file with content length and SHA-256
   checksum metadata where supported.
6. The S3 backend verifies object visibility and expected size with
   `HeadObject`.
7. Only after the payload backend reports success does the blob service commit
   Mycel blob metadata through WAL or Raft.
8. If metadata commit fails after S3 upload, the object is an orphan and must be
   handled by later safe garbage collection or operator cleanup.

Duplicate payloads remain safe because storage is content-addressed. If metadata
for the blob already exists, Mycel preserves original digest, size, and creation
time while refreshing client-declared metadata such as declared MIME type and
original filename.

## Download and metadata flow

`GetBlob` resolves Mycel blob metadata and confirms payload availability through
the descriptor. For S3-backed blobs this is a `HeadObject` check against the
stored bucket/key and expected size.

`OpenBlob` opens the payload through the descriptor backend and streams bytes
through the existing `DownloadBlob` API. The normal public response still begins
with blob metadata followed by payload chunks.

## Delete and garbage collection

Referenced-blob protection remains authoritative: a blob with graph references
must not be deleted directly.

For local blobs, payload deletion remains strict before metadata removal. If the
local delete fails unexpectedly, the metadata delete fails so local metadata does
not point at uncertain local state.

For S3-backed blobs, metadata is removed first and the S3 `DeleteObject` is
best-effort afterward. A transient S3 delete failure must not block Raft apply or
make deleted metadata reappear. The daemon logs bucket/key context for later
cleanup. Safe S3 orphan garbage collection is a follow-up operator feature and
must compare committed Mycel metadata with object keys before deleting objects.

## Raft and cluster behavior

Blob metadata remains partition-Raft-owned. Blob payload bytes remain outside
Raft logs and snapshots.

Local payload descriptors keep the existing peer-fetch behavior: followers must
materialize and checksum-verify local payload bytes before exposing metadata.
Multi-node local payload replication continues to require configured backend peer
addresses and cluster identity.

S3 payload descriptors change only payload availability. A node applying S3 blob
metadata validates that the S3 object is visible with the expected size. It does
not fetch payload bytes from another pod and does not materialize a local copy in
`objects/`. Reads from any node use S3 directly, provided the node has IAM access
to the configured bucket/key.

Raft snapshots include blob metadata and descriptors, not payload bytes. Snapshot
restore must fail closed if descriptor validation cannot prove payload
availability.

## Import/export and backup implications

Domain export with `include_blobs=true` streams blob bytes through the public
blob path, independent of whether the source payload is local or S3. Domain
import reuploads payload bytes through the destination daemon and therefore uses
that daemon's configured backend.

System backups that archive node-local data directories do not automatically
include S3 object bytes. In S3 mode, operators must protect the bucket with the
same care as block storage: versioning, encryption, access logging, replication,
lifecycle policy, and separate backup/restore procedures where required.

## Security and IAM

Recommended IAM permissions are scoped to one bucket/prefix:

- `s3:PutObject`
- `s3:GetObject`
- `s3:HeadObject`
- `s3:DeleteObject`

If a future GC scanner lists objects, it will also need `s3:ListBucket` scoped to
the prefix. If SSE-KMS is enabled, the daemon role also needs the appropriate KMS
permissions for the configured key, typically encrypt/decrypt and data-key
operations.

Bucket/key details are internal operational metadata. They should appear only in
operator logs, diagnostics, and storage files, not in normal client blob or graph
responses.

## Failure semantics

| Failure | Behavior |
| --- | --- |
| Invalid backend config | Daemon startup fails with an actionable configuration error. |
| S3 upload fails | Upload fails; Mycel metadata is not committed. |
| S3 upload succeeds but metadata commit fails | Client receives an error; object may be orphaned for future GC. |
| S3 object missing at `GetBlob`/`OpenBlob` | Blob read fails as not found/unavailable rather than returning corrupt data. |
| S3 delete fails after metadata delete | Delete remains successful; daemon logs best-effort cleanup failure. |
| Node lacks IAM after metadata replication | Reads fail on that node until IAM/config is fixed; metadata remains authoritative. |

## Future work

- Safe orphan garbage collection for S3 prefixes.
- Explicit local-to-S3 migration/backfill tooling that updates metadata through
  the normal WAL/Raft path.
- Optional read-time checksum verification wrappers for S3 downloads.
- S3 object version ID capture and version-specific reads/deletes when bucket
  versioning semantics require it.
- Admin/CLI diagnostics for blob backend health and S3 descriptor counts.
- K3s/EKS deployment examples with IRSA and bucket policy templates.
