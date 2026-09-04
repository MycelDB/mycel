# S3 blob payload storage

Mycel can store blob payload bytes in S3 for AWS deployments. This moves large immutable blob content out of node-local disks while keeping graph state, WAL, Raft logs, indexes, and blob metadata on local/block storage.

The default remains local file storage.

## Configuration

Required for S3-backed new uploads:

```sh
export MYCELD_BLOB_BACKEND=s3
export MYCELD_BLOB_S3_BUCKET=mycel-prod-blobs
export MYCELD_BLOB_S3_REGION=us-east-1
```

Optional:

```sh
export MYCELD_BLOB_S3_PREFIX=clusters/prod-a
export MYCELD_BLOB_S3_KMS_KEY_ID=alias/mycel-blobs
export MYCELD_BLOB_S3_ENDPOINT_URL=http://127.0.0.1:4566
export MYCELD_BLOB_S3_FORCE_PATH_STYLE=true
```

Use `MYCELD_BLOB_S3_ENDPOINT_URL` and `MYCELD_BLOB_S3_FORCE_PATH_STYLE=true` for LocalStack or S3-compatible endpoints that require path-style requests.

## Credentials

Runtime authentication uses the AWS SDK default credential chain. Prefer IAM roles for EC2/ECS/EKS or web identity for Kubernetes. Mycel does not provide custom S3 access-key or secret-key environment variables.

For local testing, use standard AWS SDK mechanisms, such as:

```sh
export AWS_PROFILE=mycel-dev
# or, for LocalStack-style tests only:
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

Enabling S3 affects new uploads only. Existing local blob metadata without an S3 payload descriptor continues to read from local storage. This change does not automatically migrate existing local blobs to S3.

## Delete behavior

For S3-backed blobs, Mycel deletes metadata first and then attempts S3 object deletion on a best-effort basis. A transient S3 delete failure does not fail the blob delete after metadata has been removed; the daemon logs a warning with the bucket/key for later cleanup.

Local blob payload deletion remains strict before metadata removal.

## Minimal IAM permissions

Grant the daemon principal access only to the configured bucket/prefix:

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

If `MYCELD_BLOB_S3_KMS_KEY_ID` is set, also allow the required KMS operations for that key.

## Integration tests

Unit tests use a fake S3 client and require no AWS credentials. The opt-in integration test is skipped unless `MYCELD_TEST_S3_BUCKET` is set:

```sh
MYCELD_TEST_S3_BUCKET=mycel-test-blobs \
MYCELD_TEST_S3_REGION=us-east-1 \
go test ./internal/blob/service -run TestS3BlobBackendIntegration -count=1
```

For LocalStack/S3-compatible endpoints:

```sh
MYCELD_TEST_S3_BUCKET=mycel-test-blobs \
MYCELD_TEST_S3_REGION=us-east-1 \
MYCELD_TEST_S3_ENDPOINT_URL=http://127.0.0.1:4566 \
MYCELD_TEST_S3_FORCE_PATH_STYLE=true \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
go test ./internal/blob/service -run TestS3BlobBackendIntegration -count=1
```
