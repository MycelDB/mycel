# mycel

[![CI](https://github.com/MycelDB/mycel/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/MycelDB/mycel/actions/workflows/ci.yml)
[![Docker](https://img.shields.io/docker/v/myceldb/mycel?label=docker)](https://hub.docker.com/r/myceldb/mycel)
[![License](https://img.shields.io/github/license/MycelDB/mycel)](LICENSE)

Daemon and CLI for mycel graph spaces.

Module path: `github.com/myceldb/mycel`.

## Runtime model

mycel v2 is daemon-first and is being refactored to be daemon-only:

```text
myceld owns the data directory
mycel and applications talk to myceld over gRPC
```

Do not embed Mycel by opening an engine or file-backed sessions in an application process. Applications should use `mycel-go-sdk` and the language-independent protobuf contracts in `mycel-api`; the `mycel` module contains daemon binaries and internal implementation packages only. The historical public `engine`, `session`, `domain`, `query`, and `store` implementation packages have been removed or internalized.

See [`docs/design/daemon-only-boundary.md`](docs/design/daemon-only-boundary.md) for the boundary and removal plan.

## Go package boundary

Applications should import:

```text
github.com/myceldb/mycel-go-sdk
github.com/myceldb/mycel-api/api/proto/... (for non-Go SDK/code generation)
```

Applications should not import implementation packages from this module:

```text
github.com/myceldb/mycel/domain/...
github.com/myceldb/mycel/store/...
github.com/myceldb/mycel/query
github.com/myceldb/mycel/engine
github.com/myceldb/mycel/session
```

Those packages are removed or internalized. The root `github.com/myceldb/mycel` package is documentation-only.

## Layout

**Supported application-facing surfaces:**

- `github.com/myceldb/mycel-api/api/proto/`: language-independent protobuf service definitions.
- `github.com/myceldb/mycel-go-sdk`: Go daemon client helpers and SDK-owned generated client stubs.

**Supported binaries in this module:**

- `cmd/myceld/`: daemon entrypoint and owner of local storage.
- `cmd/mycel/`: CLI client for daemon Admin and Client APIs.

**Daemon implementation internals:**

- `internal/daemon/`: runtime, modules, auth, and gRPC service adapters.
- `internal/graph/model/`: in-process graph records and schema-era graph model types.
- `internal/graph/storage/`: graph persistence used by `myceld`.
- `internal/schema/`: domain schema model, storage, DSL parsing, validation, and service logic.
- `internal/graph/change/`: neutral graph commit events/sinks used by semantic maintenance.
- `internal/blob/storage/`: blob persistence used by `myceld`.
- `internal/graph/query/` and `internal/graph/metadataindex/`: graph query planning/evaluation and metadata indexing internals.
- `internal/session/service/`: daemon client session and transaction lifecycle service.
- `internal/semantic/model/` and `internal/semantic/storage/`: semantic index and vector metadata internals.
- `internal/inference/model/`, `internal/inference/storage/`, and `internal/inference/service/`: standalone inference catalog, credential, policy, connector, and neutral usage telemetry internals.
- `internal/space/model/`, `internal/space/access/`, and `internal/space/storage/`: space/access models and space/domain/ACL persistence.
- `internal/identity/model/`, `internal/identity/auth/`, and `internal/identity/storage/`: identity/auth models and user/session persistence.

## Protobuf generation

`myceld` and the `mycel` CLI generate the Go gRPC/protobuf stubs they need from the sibling `mycel-api` checkout. Generated files are written under `internal/gen/` and are ignored by git.

```sh
make generate-proto
```

By default this reads `../mycel-api/api/proto`. Set `MYCEL_API_ROOT=/path/to/mycel-api` to use another checkout.

`make test`, `make build`, `make run-cli`, and `make run-daemon` run generation first.

## Build

```sh
make build
```

This builds:

- `bin/myceld`
- `bin/mycel`

## Run the daemon

```sh
make start
```

Defaults:

```text
MYCELD_DATA_DIR=~/mycel_data
MYCELD_GRPC_ADDR=127.0.0.1:9091
```

Daemon logs are written to `MYCELD_DATA_DIR/log/myceld.log` and use size-based
rotation. The default maximum active-file size is 100 MiB, with seven numeric
rotated files retained as `myceld.log.1` through `myceld.log.7`. Configure the
limits with `MYCELD_LOG_MAX_BYTES` and `MYCELD_LOG_MAX_FILES`. Rotation occurs
before a write would exceed the active-file limit and also runs at startup when
the existing active file is oversized.

Stop it with:

```sh
make stop
```

## Blob payload storage

Blob payload bytes are stored locally by default under `MYCELD_DATA_DIR`:

```text
MYCELD_BLOB_BACKEND=local
```

For AWS S3, MinIO, LocalStack, or another S3-compatible endpoint, new blob payloads can be stored in an object store while graph state, WAL, Raft logs, indexes, and blob metadata remain on local/block storage:

```text
MYCELD_BLOB_BACKEND=object_store
MYCELD_BLOB_OBJECT_STORE_PROVIDER=s3-compatible
MYCELD_BLOB_OBJECT_STORE_BUCKET=mycel-prod-blobs
MYCELD_BLOB_OBJECT_STORE_PREFIX=clusters/prod-a
MYCELD_BLOB_OBJECT_STORE_REGION=us-east-1
```

Object-store credentials are loaded with the AWS SDK default credential chain because the current provider is S3-compatible (for example IAM role, web identity, shared config/profile, or environment credentials). Mycel does not define custom object-store access-key environment variables. Optional MinIO/local-test settings are `MYCELD_BLOB_OBJECT_STORE_ENDPOINT_URL` and `MYCELD_BLOB_OBJECT_STORE_FORCE_PATH_STYLE=true`. For production cluster backups that omit external blob payload bytes, configure non-secret coordination refs such as `MYCELD_BLOB_OBJECT_STORE_CREDENTIAL_REF`, `MYCELD_BLOB_OBJECT_STORE_BACKUP_POLICY_REF`, and especially `MYCELD_BLOB_OBJECT_STORE_RECOVERY_POINT_REF`; the recovery point must identify the object-store snapshot/version that contains every referenced payload. Legacy `MYCELD_BLOB_BACKEND=s3` and `MYCELD_BLOB_S3_*` daemon settings are no longer accepted for startup, though old `s3` payload descriptors remain readable. Existing local blobs remain local; enabling object-store storage affects new uploads only.

See [`docs/operations/procedures/s3-blob-storage.md`](docs/operations/procedures/s3-blob-storage.md).

## Graph checkpoints

Domain graph checkpoints are local derived storage artifacts used to speed graph store open/recovery. They are not Raft snapshots or backups. Operators can create or inspect them with:

```sh
bin/mycel cluster graph-checkpoint create --space-id <space-id> --domain-id <domain-id>
bin/mycel cluster graph-checkpoint status --space-id <space-id> --domain-id <domain-id>
```

Automatic local checkpointing is disabled by default. Configure it with:

```text
MYCELD_GRAPH_CHECKPOINT_AUTO_ENABLED=false
MYCELD_GRAPH_CHECKPOINT_AUTO_INTERVAL=1m
MYCELD_GRAPH_CHECKPOINT_AUTO_REVISIONS=10000
MYCELD_GRAPH_CHECKPOINT_AUTO_TIMEOUT=30s
```

When enabled, `myceld` checks opened local domain stores on `MYCELD_GRAPH_CHECKPOINT_AUTO_INTERVAL` and writes a checkpoint after `MYCELD_GRAPH_CHECKPOINT_AUTO_REVISIONS` tail revisions since the latest checkpoint. Each write attempt is bounded by `MYCELD_GRAPH_CHECKPOINT_AUTO_TIMEOUT`.

See [`docs/operations/cli/cluster.md`](docs/operations/cli/cluster.md).

## Graph transaction admission limits

Graph transactions remain atomic: the daemon rejects an oversized transaction rather than silently splitting it. The defaults are intentionally bounded and can be adjusted through existing daemon environment configuration:

```text
MYCELD_GRAPH_TRANSACTION_MAX_OPERATIONS=4096
MYCELD_GRAPH_TRANSACTION_MAX_COMMAND_BYTES=8388608
MYCELD_GRAPH_TRANSACTION_MAX_STAGED_BYTES=16777216
MYCELD_GRAPH_TRANSACTION_MAX_AGE=10m
```

Limits are checked while mutations are staged and again before Raft/WAL proposal. Rejections use gRPC `ResourceExhausted` with an `ErrorInfo` reason such as `GRAPH_TRANSACTION_COMMAND_BYTES`; rejected mutations do not enter the commit path. Clients that do not require one atomic commit should chunk work explicitly and commit each chunk separately.

## Search tuning

Hybrid search uses daemon-side weighted reciprocal-rank fusion. The RRF rank constant defaults to `60` and can be tuned system-wide:

```text
MYCELD_SEARCH_HYBRID_RRF_K=60
```

See [`docs/operations/cli/search.md`](docs/operations/cli/search.md).

## Authentication and sessions

Daemon Client and Admin APIs use short-lived bearer access tokens plus durable refresh sessions. SDK callers should use `mycel-go-sdk` or another SDK generated from `mycel-api` so access-token expiry, refresh-token rotation, and one retry on expired-token `Unauthenticated` are handled automatically.

Key daemon setting:

```text
MYCELD_ACCESS_TOKEN_TTL=15m
```

Keep access-token TTLs short; prefer refresh sessions over increasing the TTL for long-running services. Delegated principal session lifecycle, stale-session error reasons, and cache-eviction guidance are documented in [`docs/design/identity/delegated-principal-sessions.md`](docs/design/identity/delegated-principal-sessions.md). Admin auth details are documented in [`docs/design/admin/grpc-admin-auth.md`](docs/design/admin/grpc-admin-auth.md), with release notes in [`docs/design/identity/auth-refresh-release-notes.md`](docs/design/identity/auth-refresh-release-notes.md).

## Backups

Backups are owned by `myceld`, not by applications copying the data directory. Repository-backed backup sets are the only public durable daemon backup model. Operators manage policy, start operations, validate backup sets, and plan safe restores through the Admin Backup API or CLI:

```sh
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup policy get
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup policy set --default-repository-id repo-prod --capture-provider filesystem_immutable_copy --quiesce-read-profile control_only
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup start --repository-id repo-prod --reason 'before upgrade' --archive-format tar.zst --wait
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup validate --repository-id repo-prod --backup-set-id backup-set-...
```

Backups quiesce daemon work before snapshotting. New non-exempt RPCs during quiesce, including reads unless explicitly exempted/proven safe, may fail transiently with gRPC `codes.Unavailable`; applications should retry with bounded backoff. Users are not logged out by default.

Standalone deployments are represented as one-node repository backup sets. Clustered deployments include all authoritative members, convergence barriers, and per-node artifact evidence. Restore planning is safe/read-only; destructive restore execution remains explicit/offline or controller-helper driven.

See [`docs/operations/procedures/backup-restore.md`](docs/operations/procedures/backup-restore.md) and [`docs/operations/cli/admin.md`](docs/operations/cli/admin.md).

## CLI

The CLI connects to `myceld` over gRPC:

```sh
bin/mycel --daemon-addr 127.0.0.1:9091 --help
```

Admin APIs require an operator login. Client APIs require a standard-user login. Optional CLI configuration is loaded from `--config` or `MYCEL_CONFIG`; daemon connection defaults come from `MYCELD_GRPC_ADDR` and related `MYCELD_TLS*` settings.

TLS/mTLS daemon connection flags are available:

```sh
--daemon-tls
--daemon-tls-ca /path/to/ca.pem
--daemon-tls-client-cert /path/to/client.pem
--daemon-tls-client-key /path/to/client-key.pem
```

## Testing

- Daemon-only boundary check: `make check-daemon-only`
- Public Go surface check: `make check-public-surface`
- Run once: `make test` (includes daemon-only and public-surface checks)
- Verbose + coverage: `make test-verbose`
- Watch mode: `make test-watch` (requires `watchexec`)

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for contribution guidelines, repository boundaries, validation expectations, and AI-assisted contribution guidance. See [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community standards and [`CHANGELOG.md`](CHANGELOG.md) for release notes.

## Security

Please report suspected vulnerabilities privately through GitHub Security Advisories / private vulnerability reporting. See [`SECURITY.md`](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
