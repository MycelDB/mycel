# Makefile commands

Run these commands from the `mycel/` directory.

## Protobuf and boundary checks

- `make api-info` — print where protobuf definitions and generated daemon stubs live.
- `make generate-proto` — regenerate daemon Go protobuf stubs from the API definitions.
- `make check-daemon-only` — verify daemon-only package boundary rules.
- `make check-public-surface` — verify the public surface rules.

## Tests

- `make test` — regenerate protobuf stubs, run boundary checks, and run `go test ./...`.
- `make test-verbose` — regenerate protobuf stubs, run checks, run verbose tests with coverage, and print function coverage.
- `make test-watch` — rerun generation, checks, and verbose coverage tests when Go or shell files change. Requires `watchexec`.

## Coverage

- `make coverage` — run all Go tests with coverage and print function-level coverage. Writes `coverage/coverage.out` by default.
- `make coverage-html` — run full coverage and write `coverage/coverage.html`.
- `make daemon-coverage` — run coverage for daemon packages only: `./internal/daemon/...`. Writes `coverage/daemon-coverage.out` by default.
- `make daemon-coverage-html` — run daemon coverage and write `coverage/daemon-coverage.html`.
- `make coverage-clean` — remove the generated `coverage/` directory.

The `coverage/` directory is ignored by git. Coverage output paths can be overridden:

```bash
make coverage COVERAGE_DIR=/tmp/mycel-coverage
make coverage COVERAGE_OUT=/tmp/mycel-coverage.out
make daemon-coverage DAEMON_COVERAGE_OUT=/tmp/myceld-coverage.out
make coverage-html COVERAGE_HTML=/tmp/mycel-coverage.html
```

## Build

- `make build` — regenerate protobuf stubs, run checks, and build both CLI and daemon binaries into `bin/`.
- `make build-cli` — build `bin/mycel`.
- `make build-daemon` — build `bin/myceld`.

Binary names can be overridden:

```bash
make build CLI_BINARY=mycel-dev DAEMON_BINARY=myceld-dev
```

## Run locally

- `make run-cli` — regenerate protobuf stubs and run the CLI via `go run ./cmd/mycel`.
- `make run-daemon` — regenerate protobuf stubs and run the daemon via `go run ./cmd/myceld`.
- `make start` — build and start `myceld` in the background, writing a PID file and logs under the configured data directory.
- `make stop` — stop the background daemon started by `make start`.

Default local daemon settings:

```make
MYCELD_DATA_DIR = $(HOME)/mycel_data
MYCELD_GRPC_ADDR = 127.0.0.1:9091
MYCELD_PID_FILE = $(MYCELD_DATA_DIR)/myceld.pid
MYCELD_STDOUT_LOG = $(MYCELD_DATA_DIR)/log/myceld.stdout.log
```

Example override:

```bash
make start MYCELD_DATA_DIR=/tmp/mycel-data MYCELD_GRPC_ADDR=127.0.0.1:9093
make stop MYCELD_DATA_DIR=/tmp/mycel-data
```

## Raft clustering settings

Clustered deployments use the space-partitioned Raft runtime. Configure the fixed Raft node set with:

```bash
MYCELD_CLUSTER_RAFT_NODE_COUNT=3
MYCELD_CLUSTER_RAFT_PARTITION_COUNT=64
MYCELD_CLUSTER_RAFT_REPLICA_FACTOR=3
MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID=1
MYCELD_CLUSTER_RAFT_NODE_ADDRS=127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 # index = node_id - 1
```

The Raft sizing values and node address map are bootstrap-time cluster settings and should be treated as immutable after cluster bootstrap.
