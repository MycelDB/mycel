# S3-backed blob payload storage implementation plan

## Status

In progress on the issue #6 feature branch. The first branch implementation
covers the core local/S3 backend split, daemon configuration, S3 upload/download
/delete plumbing, WAL/Raft metadata descriptors, fake-S3 tests, opt-in
integration-test hooks, and operator documentation.

Before merge, use this plan as the review checklist for hardening, additional
Raft-specific coverage, and release validation.

Design: [S3-backed blob payload storage](../../design/blobs/s3-backed-payload-storage.md)

Operations: [S3 blob payload storage](../../operations/procedures/s3-blob-storage.md)

## Problem

Blob payload bytes are currently stored in node-local content-addressed files.
In multi-node clusters, metadata is Raft-owned but payload bytes must be fetched
between pods/nodes so every node that applies blob metadata can serve the blob.
For AWS deployments, S3 is a better durability and sharing boundary for large
immutable payload bytes.

## Goals

- Keep the public `BlobService` and blob-node APIs source-compatible.
- Keep local filesystem storage as the default backend.
- Add an S3 backend for new payload uploads.
- Store only payload bytes in S3; keep metadata, graph state, WAL, Raft logs,
  indexes, and subsystem state on node-local/block storage.
- Ensure metadata is committed only after payload bytes are durable.
- Allow any authorized Raft node to read S3-backed payloads without pod-to-pod
  payload copying.
- Preserve referenced-blob delete protection.
- Add tests and docs sufficient for an operator to configure and validate S3.

## Non-goals

- No automatic migration of existing local payloads to S3 in this tranche.
- No bucket/key exposure in normal client APIs.
- No static AWS access-key settings owned by Mycel.
- No automatic object lifecycle, replication, KMS, or bucket-policy management.
- No replacement of backup archives, WAL, Raft storage, graph storage, semantic
  storage, or metadata storage with S3.

## Compatibility stance

Existing metadata without a payload descriptor is treated as local. The current
local backend data layout remains readable. New S3-backed metadata adds an
internal `payload` descriptor and keeps the existing public blob fields
unchanged.

Switching `MYCELD_BLOB_BACKEND` to `s3` affects new uploads only. Mixed local and
S3 descriptors are valid. A future migration command can copy old local payloads
to S3 and update metadata through the normal WAL/Raft path.

## Tranche 1 — Configuration and daemon wiring

Status: implemented in the initial branch.

1. Add daemon blob config fields:
   - `MYCELD_BLOB_BACKEND=local|s3`, default `local`;
   - `MYCELD_BLOB_S3_BUCKET`;
   - `MYCELD_BLOB_S3_PREFIX`;
   - `MYCELD_BLOB_S3_REGION`;
   - `MYCELD_BLOB_S3_KMS_KEY_ID`;
   - `MYCELD_BLOB_S3_ENDPOINT_URL`;
   - `MYCELD_BLOB_S3_FORCE_PATH_STYLE`.
2. Validate startup config:
   - unknown backend fails startup;
   - S3 backend requires a bucket;
   - local backend requires no S3 settings.
3. Pass daemon config into `blobservice.NewModule` from the app composition root.
4. Log selected backend without logging credentials.

Validation:

```sh
go test ./internal/daemon/config -count=1
```

## Tranche 2 — Payload backend abstraction

Status: implemented in the initial branch.

1. Add a module-level payload dispatch boundary that supports:
   - put;
   - exists/head;
   - open;
   - delete.
2. Preserve the existing local `internal/blob/storage.Store` implementation and
   wrap it as the default backend.
3. Add `PayloadDescriptor` fields for backend, checksum, size, and S3 location.
4. Extend `BlobMeta` with optional internal `Payload` descriptor.
5. Synthesize a local descriptor for legacy metadata with no payload descriptor.
6. Keep public proto/API mapping unchanged.

Validation:

```sh
go test ./internal/blob/service -run 'TestModuleUploadGetOpenDeleteBlob|TestModuleWALBlobMetadataMutationsAppendAndApply' -count=1
```

## Tranche 3 — S3 payload store

Status: implemented in the initial branch.

1. Use AWS SDK for Go v2 and the default credential chain.
2. Stage incoming streams to `<data-dir>/blobs/_s3_staging` while computing:
   - SHA-256 digest;
   - byte size.
3. Use deterministic object keys:

   ```text
   <prefix>/spaces/<space-id>/objects/<aa>/<sha256-hex>
   ```

4. Upload with `PutObject` using content length and SHA-256 checksum where
   supported.
5. Set content type when detected.
6. Apply optional SSE-KMS when `MYCELD_BLOB_S3_KMS_KEY_ID` is configured.
7. Verify post-write visibility and size with `HeadObject`.
8. Open payloads with `GetObject` and map missing keys to blob not found.
9. Delete payloads with `DeleteObject`.
10. Cover S3 behavior with a fake S3 client so normal tests do not need AWS
    credentials.

Validation:

```sh
go test ./internal/blob/service -run 'TestS3PayloadStorePutOpenDelete|TestModuleS3DeleteIsBestEffortAfterMetadataDelete' -count=1
```

Optional live/S3-compatible validation:

```sh
MYCELD_TEST_S3_BUCKET=mycel-test-blobs \
MYCELD_TEST_S3_REGION=us-east-1 \
go test ./internal/blob/service -run TestS3BlobBackendIntegration -count=1
```

## Tranche 4 — Blob service upload/read/delete semantics

Status: implemented in the initial branch; GC remains future work.

1. Upload:
   - detect MIME type from the leading bytes;
   - store payload through selected backend;
   - commit metadata only after backend success;
   - preserve existing metadata when duplicate content already exists.
2. Get/read:
   - resolve metadata first;
   - validate payload existence through descriptor;
   - stream from local file or S3 object through existing APIs.
3. Delete:
   - preserve referenced-blob rejection;
   - keep local payload deletion strict;
   - delete S3 metadata first, then best-effort `DeleteObject`;
   - log S3 delete failures for future cleanup.
4. Future work:
   - safe S3 orphan garbage collection;
   - optional read-time checksum wrapper;
   - object version ID capture when version-aware deletes are required.

Validation:

```sh
go test ./internal/blob/service -count=1
```

## Tranche 5 — WAL, Raft, and snapshot integration

Status: partially implemented; add one more S3-specific Raft regression before
merge if possible.

1. Include payload descriptors in `blob.meta.put.v1` records.
2. Preserve compatibility with older `payload_descriptor` records by copying the
   descriptor into `BlobMeta.Payload` during apply when needed.
3. Keep local payload Raft behavior unchanged:
   - local payloads are fetched from peers before metadata is exposed;
   - multi-node local payload writes fail closed if peer backend addresses or
     cluster identity are unavailable.
4. For S3 descriptors:
   - do not require remote peer payload fetch;
   - validate S3 object availability through `HeadObject` before exposing
     metadata;
   - allow every node with IAM access to stream from S3 directly.
5. Keep Raft snapshots metadata-only but include payload descriptors inside blob
   metadata.
6. Snapshot restore must fail closed if payload availability cannot be proven.

Recommended remaining tests:

- S3-backed `blob.meta.put.v1` apply succeeds on a node with an empty local
  `objects/` directory and shared fake S3 object.
- S3-backed snapshot restore validates `HeadObject` and does not materialize a
  local payload copy.
- S3 `HeadObject` failure during apply/snapshot restore fails closed with an
  actionable error.

Validation:

```sh
go test ./internal/blob/service -run 'TestBlobRaft|TestS3' -count=1
make test-phase-d
```

## Tranche 6 — Documentation and operator guidance

Status: implemented/expanded by this doc tranche.

1. Add current-design documentation under `docs/design/blobs/`.
2. Add/update operator procedure for S3 configuration and IAM guidance.
3. Link design and operations docs from their indexes.
4. Document migration stance: S3 affects new uploads only.
5. Document backup stance: system backups do not automatically copy S3 objects.
6. Document LocalStack/S3-compatible endpoint settings for test environments.

Validation:

```sh
make docs-check
git diff --check
```

## Tranche 7 — Release validation checklist

Before opening/merging the PR:

1. Run docs checks:

   ```sh
   make docs-check
   git diff --check
   ```

2. Run focused tests:

   ```sh
   go test ./internal/blob/service ./internal/daemon/config -count=1
   ```

3. Run Raft-sensitive tests:

   ```sh
   make test-phase-d
   ```

4. If AWS or LocalStack is available, run the opt-in integration test.
5. Start a daemon with default config and verify local uploads still work.
6. Start a daemon with S3 config and verify upload, get, download, and delete.
7. In a multi-node test, verify an S3-backed blob uploaded through one node can
   be read from another without local payload copying.
8. Review logs for bucket/key leakage boundaries:
   - acceptable in operator logs and diagnostics;
   - not acceptable in normal client API responses.
9. Review docs for IAM minimum permissions and backup implications.

## Future follow-ups

- `mycel blob migrate-to-s3` or an admin maintenance command for explicit
  local-to-S3 migration.
- Safe S3 orphan garbage collector with dry-run, prefix scoping, metadata
  comparison, and deletion receipts.
- Admin/CLI diagnostics for backend config, descriptor counts, failed cleanup,
  and S3 health.
- Read-time checksum verification for S3 streams, configurable by cost/latency
  tolerance.
- S3 object version ID support.
- EKS IRSA and K3s/secret examples in deployment docs.
- Backup/restore procedures that optionally snapshot or verify S3 object sets
  alongside local daemon data archives.
