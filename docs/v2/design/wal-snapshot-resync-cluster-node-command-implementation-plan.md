# WAL Snapshot Resync Cluster Node Command Implementation Plan

## Status

Implemented initial end-to-end operator path: public admin API, daemon coordinator, CLI command, SDK proto generation, and basic admin command/service bindings. The validation script currently performs command availability smoke validation; deterministic snapshot-required forcing remains follow-up.

## Objective

Expose the snapshot resync workflow through the authenticated public admin API and CLI command:

```bash
mycel cluster node resync NODE
```

`NODE` may be a node name or node ID. The command must run against the current primary and target an active follower.

## Phase 1: Public admin API

Add `ResyncClusterNode` to `mycel.admin.v1.AdminClusterService`.

Request:

```proto
message ResyncClusterNodeRequest {
  string target = 1;
}
```

Response:

```proto
message ResyncClusterNodeResponse {
  string operation_id = 1;
  string target_node_id = 2;
  string target_node_name = 3;
  uint64 snapshot_base_lsn = 4;
  uint64 total_bytes = 5;
  string checksum = 6;
}
```

Regenerate protos for daemon, Go SDK, Rust SDK, and admin bindings as needed.

## Phase 2: Primary resync coordinator

Add an internal coordinator that:

1. Requires local node to be cluster primary.
2. Resolves target using node name or node ID.
3. Requires target to be an active follower.
4. Requires target backend advertise address.
5. Creates a snapshot with `replication.SnapshotCreator`.
6. Streams the archive to follower with backend `InstallSnapshot`.
7. Returns operation ID, target, base LSN, byte count, and checksum.

## Phase 3: Daemon admin service handler

Implement `AdminClusterService.ResyncClusterNode` in:

```text
mycel/internal/daemon/api/admin/cluster_service.go
```

Requirements:

- authenticated admin-only endpoint
- reject standalone/unadmitted/follower local node
- return structured not-primary hints via existing authority error behavior
- surface target validation errors clearly
- no plaintext secrets in responses/logs

## Phase 4: CLI command

Add:

```bash
mycel cluster node resync NODE
```

Expected output:

```text
Resync completed
Target: node-b (node_...)
Snapshot base LSN: 123
Bytes transferred: 456789
Operation: resync-...
```

On follower/not-primary, reuse existing not-primary formatting and primary hint output.

## Phase 5: SDK support

Update generated SDK clients after proto regeneration.

Add convenience methods if the current SDK style has hand-written admin helpers:

- Go SDK cluster/admin method for resync
- Rust SDK cluster/admin method for resync

## Phase 6: mycel-admin UX

Add UI support after the API is available:

- show snapshot-required state on follower/member detail
- expose a “Resync node” action for active followers when connected to primary
- show progress/result/error state
- for non-primary errors, show existing primary hint guidance

This can be minimal initially: button + confirmation + result toast/panel.

## Phase 7: E2E validation script

Add:

```text
mycel/scripts/validateWALSnapshotResync.sh
```

Flow:

1. Start primary and follower cluster.
2. Create data on primary.
3. Force follower into snapshot-required state by WAL retention/checkpoint manipulation or controlled data-dir reset.
4. Verify follower replication status reports `snapshot_required`.
5. Run:

```bash
mycel cluster node resync node-b
```

6. Verify follower returns to `caught_up` or `streaming` with applied LSN at or beyond snapshot base.
7. Verify materialized data is readable on follower.

## Phase 8: Tests

Add focused tests for:

- coordinator rejects non-primary local node
- target not found
- target is primary
- target pending/inactive
- target has missing backend address
- successful coordinator path using fake snapshot creator/client
- admin service success/error mapping
- CLI command parsing/output

## Phase 9: Documentation updates

Update:

```text
mycel/docs/v2/design/wal-snapshot-resync.md
mycel/docs/v2/design/wal-snapshot-resync-implementation-plan.md
mycel/docs/v2/design/wal-snapshot-resync-materialized-install-implementation-plan.md
mycel/docs/v2/design/write-ahead-log-operational-guide.md
```

Document:

- operator command: `mycel cluster node resync NODE`
- primary-only requirement
- target must be active follower
- expected snapshot-required recovery flow
- known limitations

## Validation commands

```bash
cd mycel
./scripts/generate-proto.sh
go test ./internal/...

cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint

cd ../mycel-go-sdk
./scripts/generate-proto.sh
go test ./...

cd ../mycel-rust-sdk
cargo check -p mycel-proto
cargo check -p mycel-sdk

cd ../mycel-admin/src-tauri
cargo check

cd ../../mycel-admin
npm test -- --runInBand
npm run build
```

## Acceptance criteria

Complete when:

- `mycel cluster node resync NODE` exists
- command requires primary authority
- target must be active follower
- primary creates backup-based snapshot and streams it to follower
- follower installs materialized snapshot and resets replication progress
- command reports operation/base LSN/bytes/checksum
- SDKs build with updated API
- mycel-admin exposes a basic resync action or documented follow-up if deferred
- e2e validation script passes
