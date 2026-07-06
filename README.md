# MycelDB

Daemon and CLI for MycelDB graph spaces.

Module path: `github.com/myceldb/mycel`.

## Runtime model

MycelDB v2 is daemon-first and is being refactored to be daemon-only:

```text
myceld owns the data directory
mycel and applications talk to myceld over gRPC
```

Do not embed Mycel by opening an engine or file-backed sessions in an application process. Applications should use `mycel-go-sdk` and `mycel-api`; the `mycel` module contains daemon binaries and internal implementation packages only. The historical public `engine`, `session`, `domain`, `query`, and `store` implementation packages have been removed or internalized.

See [`docs/v2/design/daemon-only-boundary.md`](docs/v2/design/daemon-only-boundary.md) for the boundary and removal plan.

## Go package boundary

Applications should import:

```text
github.com/myceldb/mycel-api/gen/go/...
github.com/myceldb/mycel-go-sdk
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

- `github.com/myceldb/mycel-api/api/proto/`: protobuf service definitions.
- `github.com/myceldb/mycel-api/gen/go/`: generated Go gRPC/protobuf stubs.
- `github.com/myceldb/mycel-go-sdk`: Go daemon client helpers.

**Supported binaries in this module:**

- `cmd/myceld/`: daemon entrypoint and owner of local storage.
- `cmd/mycel/`: CLI client for daemon Admin and Client APIs.

**Daemon implementation internals:**

- `internal/daemon/`: runtime, modules, auth, and gRPC service adapters.
- `internal/graph/model/`: in-process graph records and template policies.
- `internal/graph/storage/`: graph persistence used by `myceld`.
- `internal/graph/template/storage/`: graph template catalog persistence.
- `internal/graph/change/`: neutral graph commit events/sinks used by semantic maintenance.
- `internal/blob/storage/`: blob persistence used by `myceld`.
- `internal/graph/filesession/`: file-backed graph session runtime used by `myceld`.
- `internal/graph/query/` and `internal/graph/metadataindex/`: graph query planning/evaluation and metadata indexing internals.
- `internal/session/api/`: internal daemon/session contract types shared by daemon modules and graph sessions.
- `internal/semantic/model/`, `internal/semantic/storage/`, and `internal/semantic/accounting/`: semantic/inference model, persistence, and usage accounting internals.
- `internal/space/model/`, `internal/space/access/`, and `internal/space/storage/`: space/access models and space/domain/ACL persistence.
- `internal/identity/model/`, `internal/identity/auth/`, and `internal/identity/storage/`: identity/auth models and user/session persistence.

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
