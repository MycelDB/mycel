# Object-store blob payload storage

Mycel can store blob payload bytes in an S3-compatible object store, including AWS S3, MinIO, and LocalStack. This moves large immutable blob content out of node-local disks while keeping graph state, WAL, Raft logs, indexes, and blob metadata on local/block storage.

The default remains local file storage.

## Configuration

Preferred configuration for object-store-backed new uploads:

```sh
export MYCELD_BLOB_BACKEND=object_store
export MYCELD_BLOB_OBJECT_STORE_PROVIDER=s3-compatible
export MYCELD_BLOB_OBJECT_STORE_BUCKET=mycel-prod-blobs
export MYCELD_BLOB_OBJECT_STORE_REGION=us-east-1
```

Optional:

```sh
export MYCELD_BLOB_OBJECT_STORE_PREFIX=clusters/prod-a
export MYCELD_BLOB_OBJECT_STORE_KMS_KEY_ID=alias/mycel-blobs
export MYCELD_BLOB_OBJECT_STORE_ENDPOINT_URL=http://127.0.0.1:4566
export MYCELD_BLOB_OBJECT_STORE_FORCE_PATH_STYLE=true
```

`MYCELD_BLOB_OBJECT_STORE_PROVIDER` currently supports `s3-compatible`. When it is omitted, `s3-compatible` is assumed for `object_store` and legacy `s3` backends.

Use `MYCELD_BLOB_OBJECT_STORE_ENDPOINT_URL` and `MYCELD_BLOB_OBJECT_STORE_FORCE_PATH_STYLE=true` for MinIO, LocalStack, or S3-compatible endpoints that require path-style requests.

## MinIO example

```sh
export MYCELD_BLOB_BACKEND=object_store
export MYCELD_BLOB_OBJECT_STORE_PROVIDER=s3-compatible
export MYCELD_BLOB_OBJECT_STORE_BUCKET=mycel-dev-blobs
export MYCELD_BLOB_OBJECT_STORE_REGION=us-east-1
export MYCELD_BLOB_OBJECT_STORE_ENDPOINT_URL=http://minio:9000
export MYCELD_BLOB_OBJECT_STORE_FORCE_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

Create the bucket before starting the daemon, for example:

```sh
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc mb local/mycel-dev-blobs
```

## Compose defaults

The local compose cluster in `tests/compose/cluster` starts MinIO by default, creates the configured bucket, and configures each `myceld` node with:

```sh
MYCELD_BLOB_BACKEND=object_store
MYCELD_BLOB_OBJECT_STORE_PROVIDER=s3-compatible
MYCELD_BLOB_OBJECT_STORE_BUCKET=mycel-compose-blobs
MYCELD_BLOB_OBJECT_STORE_ENDPOINT_URL=http://minio:9000
MYCELD_BLOB_OBJECT_STORE_FORCE_PATH_STYLE=true
```

Override `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MYCELD_BLOB_OBJECT_STORE_BUCKET`, or the `MYCELD_BLOB_OBJECT_STORE_*` variables when invoking compose targets if a different local object-store setup is needed.

## Backward-compatible aliases

Existing S3-specific settings remain supported:

```sh
export MYCELD_BLOB_BACKEND=s3
export MYCELD_BLOB_S3_BUCKET=mycel-prod-blobs
export MYCELD_BLOB_S3_REGION=us-east-1
export MYCELD_BLOB_S3_PREFIX=clusters/prod-a
export MYCELD_BLOB_S3_KMS_KEY_ID=alias/mycel-blobs
export MYCELD_BLOB_S3_ENDPOINT_URL=http://127.0.0.1:4566
export MYCELD_BLOB_S3_FORCE_PATH_STYLE=true
```

When both generic object-store variables and legacy `MYCELD_BLOB_S3_*` variables are set, the generic `MYCELD_BLOB_OBJECT_STORE_*` values take precedence.

## Credentials

Runtime authentication uses the AWS SDK default credential chain because the current provider is S3-compatible. Prefer IAM roles for EC2/ECS/EKS or web identity for Kubernetes. Mycel does not provide custom object-store access-key or secret-key environment variables.

For local testing, use standard AWS SDK mechanisms, such as:

```sh
export AWS_PROFILE=mycel-dev
# or, for MinIO/LocalStack-style tests only:
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
```

## Object layout

Object keys are deterministic and content-addressed:

```text
<prefix>/spaces/<space-id>/objects/<sha256-fanout>/<sha256-hex>
```

Blob IDs remain the SHA-256 hex digest of the payload bytes, and the public blob API is unchanged.

## Migration behavior

Enabling object-store storage affects new uploads only. Existing local blob metadata without an object-store payload descriptor continues to read from local storage. This change does not automatically migrate existing local blobs to object-store storage.

Legacy S3 payload descriptors remain readable. New `object_store` uploads use the same S3-compatible object layout and descriptor fields.

## Delete behavior

For object-store-backed blobs, Mycel deletes metadata first and then attempts object deletion on a best-effort basis. A transient object-store delete failure does not fail the blob delete after metadata has been removed; the daemon logs a warning with the bucket/key for later cleanup.

Local blob payload deletion remains strict before metadata removal.

## Minimal AWS IAM permissions

For AWS S3, grant the daemon principal access only to the configured bucket/prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:HeadObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::mycel-prod-blobs/clusters/prod-a/*"
    }
  ]
}
```

If `MYCELD_BLOB_OBJECT_STORE_KMS_KEY_ID` or legacy `MYCELD_BLOB_S3_KMS_KEY_ID` is set, also allow the required KMS operations for that key.

## Integration tests

Unit tests use a fake S3-compatible client and require no AWS credentials. The opt-in integration test is skipped unless `MYCELD_TEST_S3_BUCKET` is set:

```sh
MYCELD_TEST_S3_BUCKET=mycel-test-blobs \
MYCELD_TEST_S3_REGION=us-east-1 \
go test ./internal/blob/service -run TestS3BlobBackendIntegration -count=1
```

For MinIO, LocalStack, or other S3-compatible endpoints:

```sh
MYCELD_TEST_S3_BUCKET=mycel-test-blobs \
MYCELD_TEST_S3_REGION=us-east-1 \
MYCELD_TEST_S3_ENDPOINT_URL=http://127.0.0.1:4566 \
MYCELD_TEST_S3_FORCE_PATH_STYLE=true \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
go test ./internal/blob/service -run TestS3BlobBackendIntegration -count=1
```
