# Makefile commands

Run these commands from the `mycel/` directory.

## Protobuf and boundary checks

- `make api-info` — print where protobuf definitions and generated daemon stubs live.
- `make generate-proto` — regenerate daemon Go protobuf stubs from the API definitions.
- `make check-daemon-only` — verify daemon-only package boundary rules.
- `make check-public-surface` — verify the public surface rules.

## Tests

For the full Raft-focused test matrix and destructive cluster gate details, see `raft-cluster-test-matrix.md`.

- `make test` — regenerate protobuf stubs, run boundary checks, and run `go test ./...`.
- `make test-cluster-identity` — run the fast in-process clustering/readiness/CLI regression suite used to guard authoritative Raft metadata behavior.
- `make test-phase-a` — run the fast Phase A release-gate suite covering readiness/admin fields, raft group/transport diagnostics, backend auth, and raft-mode graph fail-closed behavior.
- `make test-phase-d` — run the focused Phase D raft command coverage suite, including record classification guardrails, composite state-machine dispatch hardening, D5 fail-closed behavior, and multi-subsystem raft restart/convergence tests.
- `make test-phase-e` — run the focused Phase E routing suite covering session/transaction home-node routing, forwarded client requests, cross-node transaction-overlay workflows, home-node loss/session-lost behavior, backend auth rejection, and leader-change commit safety.
- `make test-phase-f` — run the focused Phase F read-consistency suite covering consensus read-index barriers, graph strong reads, query/metadata read inheritance, read metadata, stale-read rejection, and admin/CLI read diagnostics.
- `make test-phase-g` — run the focused Phase G diagnostics/forensics suite covering local graph checksums, local admin diagnostics, backend peer collection, consistency classification, forensic export/diff, CLI output, script syntax, and manual-repair planning guardrails.
- `make test-compose-cluster` — destructive local compose validation for fresh bootstrap, real pod-to-pod graph write/read/query/consistency, restart data-plane stability, and persisted identity stability. Requires the sibling `../../knot_pkm/knot_pkm_server` checkout and Docker. The target supplies a default compose-only `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` if the environment does not set one.
- `make test-k3s-cluster` — destructive local K3s/k3d validation for fresh bootstrap, real pod-to-pod graph write/read/query/consistency, rolling restart, and one-PVC replacement/rejoin with data-plane revalidation. Requires Docker, `kubectl`, and preferably `k3d`; creates/uses the `knotbase-dev` k3d cluster by default.
- `make test-k3s-system-backup-restore` — destructive local K3s/k3d validation for full-system per-pod daemon backup archives: creates graph/blob data, captures a system backup from each pod, wipes the namespace/PVCs, restores each ordinal's archive into fresh PVCs, restarts the StatefulSet, and verifies graph/blob data through every pod.
- `make test-cluster-release-gate` — full pre-release clustering gate: `make test`, `make test-phase-d`, `make test-phase-e`, `make test-phase-f`, `make test-phase-g`, destructive compose validation, destructive K3s validation, and destructive K3s system backup/restore validation.
- `make test-cluster-soak` — optional longer destructive Compose soak using repeated identity/data-plane validation and periodic `myceld` restarts. It supports `MYCEL_CLUSTER_SOAK_WRITES`; reserved forced snapshot/PVC replacement flags fail closed until a safe admin harness exists. It is not part of default CI or the release gate.
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
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=<shared-generated-secret>
```

The Raft sizing values and node address map are bootstrap-time cluster settings and should be treated as immutable after cluster bootstrap. Multi-node Raft clusters also require a non-empty shared backend auth token for internode RPCs.

Raft compaction remains off by default while subsystem snapshot recovery hardening continues:

```bash
MYCELD_CLUSTER_RAFT_COMPACTION_MODE=off
MYCELD_CLUSTER_RAFT_SNAPSHOT_ENTRIES=0
MYCELD_CLUSTER_RAFT_SNAPSHOT_INTERVAL=0s
MYCELD_CLUSTER_RAFT_SNAPSHOT_MAX_LOG_BYTES=0
MYCELD_CLUSTER_RAFT_SNAPSHOT_MIN_RETAIN_ENTRIES=0
```

The daemon parses and validates these knobs, but no automatic production compaction loop is enabled yet.
