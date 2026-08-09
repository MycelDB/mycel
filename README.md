# mycel

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
- `internal/semantic/model/`, `internal/semantic/storage/`, and `internal/semantic/accounting/`: semantic/inference model, persistence, and usage accounting internals.
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

Stop it with:

```sh
make stop
```

## Authentication and sessions

Daemon Client and Admin APIs use short-lived bearer access tokens plus durable refresh sessions. SDK callers should use `mycel-go-sdk` or another SDK generated from `mycel-api` so access-token expiry, refresh-token rotation, and one retry on expired-token `Unauthenticated` are handled automatically.

Key daemon setting:

```text
MYCELD_ACCESS_TOKEN_TTL=15m
```

Keep access-token TTLs short; prefer refresh sessions over increasing the TTL for long-running services. Admin auth details are documented in [`docs/design/admin/grpc-admin-auth.md`](docs/design/admin/grpc-admin-auth.md), with release notes in [`docs/design/identity/auth-refresh-release-notes.md`](docs/design/identity/auth-refresh-release-notes.md).

## Backups

Backups are owned by `myceld`, not by applications copying the data directory. Operators manage policy and manual runs through the Admin Backup API or CLI:

```sh
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup policy get
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup policy set --enabled --schedule daily --time-of-day 22:00 --timezone UTC --archive-format tar.zst
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup trigger --reason 'before upgrade'
bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' admin backup status
```

Backups quiesce daemon work before snapshotting. New non-exempt RPCs during quiesce, including reads unless explicitly exempted/proven safe, may fail transiently with gRPC `codes.Unavailable`; applications should retry with bounded backoff. Users are not logged out by default.

Scheduled backups are disabled by default. Policies support interval, daily, and weekly schedules; daily/weekly schedules use wall-clock `time_of_day` plus an IANA timezone. Supported archive formats are `zip`, `tar`, `tar.gz`, and `tar.zst` through the `archive_format` policy/CLI flag. Key daemon settings include `MYCELD_BACKUP_ENABLED`, `MYCELD_BACKUP_DIR`, `MYCELD_BACKUP_INTERVAL`, `MYCELD_BACKUP_RETENTION_COUNT`, `MYCELD_BACKUP_INCLUDE_LOGS`, and the legacy archive-format seed `MYCELD_BACKUP_COMPRESSION`. The backup directory must be outside `MYCELD_DATA_DIR`. Restore is offline-only in the MVP: stop `myceld`, verify and extract the archive into an empty/restored data directory, then start `myceld` against that directory.

See [`docs/design/admin/backup.md`](docs/design/admin/backup.md) and [`docs/design/backup-restore/quiesce-and-backup.md`](docs/design/backup-restore/quiesce-and-backup.md).

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
