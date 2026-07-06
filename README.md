# MycelDB

Daemon and CLI for MycelDB graph spaces.

Module path: `github.com/myceldb/mycel`.

## Runtime model

MycelDB v2 is daemon-first and is being refactored to be daemon-only:

```text
myceld owns the data directory
mycel and applications talk to myceld over gRPC
```

Do not embed Mycel by opening an engine or file-backed sessions in an application process. The historical public `engine` and `session` runtime packages have been removed; remaining file-session code lives under `internal/` for daemon implementation and migration coverage only.

See [`docs/v2/design/daemon-only-boundary.md`](docs/v2/design/daemon-only-boundary.md) for the boundary and removal plan.

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
- `internal/graphstorage/`, `internal/blobstorage/`, `internal/session/filesession/`: local persistence used by `myceld`.
- `internal/session/`: internal session API/types and file-session implementation used by daemon modules.
- `domain/`, `query/`, and `store/`: transitional implementation packages from the embedded era. They are not supported application APIs and are scheduled to move under `internal/` after remaining consumers migrate.

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
