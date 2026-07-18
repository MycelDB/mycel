# Clustering Completion Implementation Plan

## Status

Forward implementation plan.

This plan captures the remaining work to turn the current static-primary clustering foundation into a more complete operational clustering system.

## Current foundation

Implemented:

- stable node/cluster identity
- membership/admission with one-time join tokens
- static primary authority with epochs
- primary-only durable write guardrails
- WAL propagation to followers
- snapshot-required detection
- snapshot resync command and UI action
- materialized snapshot install, live reload, rollback, cleanup
- safe planned switchover initial implementation
- SDK not-primary hint extraction and primary-follow helper layer

Major remaining gaps:

- full SDK auto-follow integration into high-level methods
- switchover intent/recovery hardening
- emergency manual failover
- explicit snapshot path policy hardening
- richer cluster health/observability
- authority propagation/reconciliation hardening
- production security for internode RPC
- long-term consensus/election architecture

## Phase 1: SDK primary-follow integration

### Objective

Applications should not need to manually parse not-primary errors, discover the new primary, rebuild clients, or reconnect.

### Work

- Integrate Go SDK `DoRead`/primary-follow into high-level read/list/get/query helpers.
- Add equivalent Rust high-level retry helpers.
- Add typed error for unsafe writes after primary changed:
  - Go: `PrimaryChangedRetryRequiredError`
  - Rust: exported structured error variant or helper type
- Invalidate transaction/session handles on detected primary change.
- Add docs/examples:

```go
client, _ := mycel.Dial(ctx, cfg)
// On switchover, read helpers follow primary automatically.
```

### Acceptance

- read/idempotent helpers reconnect and retry once on not-primary
- unsafe operations reconnect but return retry-required typed error
- tests cover hint extraction, reconnect, retry, and unsafe behavior

## Phase 2: Switchover intent and crash recovery

### Objective

Close the partial-failure window in planned switchover.

### Work

Add:

```text
<data_dir>/meta/clustering/switchover-intent.json
```

Intent phases:

- `started`
- `target_installing`
- `target_installed`
- `local_installed`
- `completed`
- `failed`

Coordinator writes intent before installing authority on target and updates it after each phase.

Startup behavior:

- if pending intent indicates target may have installed higher epoch, old primary must not accept primary writes until reconciled
- if enough information exists, complete local demotion automatically
- expose pending intent in cluster status

### Acceptance

- old primary crash after target install cannot restart as writable primary at old epoch
- tests simulate pending intent states
- planned switchover e2e still passes

## Phase 3: Authority propagation and reconciliation hardening

### Objective

All nodes converge reliably on the highest valid authority epoch.

### Work

- Propagate authority install to all active followers after switchover.
- Reject stale authority updates with older/equal epoch everywhere.
- Ensure registration/view refresh always carries authority.
- Add periodic authority reconciliation from known peers.
- Add status fields for authority source/epoch freshness.

### Acceptance

- stale authority cannot overwrite newer authority
- disconnected follower learns new authority on reconnect
- tests cover authority convergence after follower restart

## Phase 4: Emergency manual failover

### Objective

Allow operator-controlled promotion when old primary is dead/fenced.

### Proposed command

```bash
mycel cluster primary failover NODE --force
```

or, on target node:

```bash
mycel cluster primary promote --force
```

### Work

- require explicit `--force`
- require target is local admitted follower or reachable active follower
- require operator acknowledgement that old primary is fenced/dead
- bump authority epoch beyond last known epoch
- persist authority locally
- expose warning about possible data loss if target was behind
- update primary hints after promotion

### Acceptance

- no accidental failover without `--force`
- target becomes primary and accepts writes
- old primary, if later restarted, must not silently accept old-epoch writes

## Phase 5: Explicit snapshot archive path policy

### Objective

Avoid accidentally copying local-only or future unsafe files through snapshot resync.

### Work

Move from broad include-with-exclusions to explicit managed roots, for example:

```text
admins/
users/
meta/spaces.json
meta/domains.json
meta/acl*.json
templates/
graphs/
blobs/
blob_meta/
meta/semantic*
meta/accounting/
```

Always exclude/preserve:

```text
meta/clustering/**
wal/**
log/**
logs/**
```

### Acceptance

- tests prove managed roots included
- tests prove unknown root files excluded
- unsafe paths rejected
- resync e2e passes

## Phase 6: Cluster health and observability

### Objective

Make operators understand cluster state without reading logs/files.

### Work

Expose in admin API/CLI/UI:

- node role and authority epoch
- primary/follower health
- replication lag
- snapshot-required details
- resync history
- switchover history
- pending switchover intent
- peer reachability
- last authority update time

Add command examples:

```bash
mycel cluster health
mycel cluster primary status
mycel cluster node resync-history
mycel cluster primary switch-history
```

### Acceptance

- UI shows actionable degraded states
- CLI outputs stable JSON for automation
- e2e scripts assert health before/after resync/switchover

## Phase 7: Internode RPC security

### Objective

Prevent unauthorized nodes from calling backend cluster APIs.

### Work

- require mTLS or signed node identity for backend RPC
- verify node ID/admission against membership
- reject unadmitted backend calls except controlled join/register flow
- secure join token exchange
- document cert/key rotation story

### Acceptance

- unadmitted node cannot stream WAL, install authority, install snapshot, or query sensitive backend APIs
- tests cover rejected backend calls

## Phase 8: Session and transaction behavior under primary changes

### Objective

Make switchover/failover behavior predictable for connected clients.

### Work

- document node-local session limitations
- invalidate write transactions on primary change
- add SDK error: transaction/session must be reopened on new primary
- optionally replicate or recreate session metadata later
- add idempotency keys for mutation APIs before automatic write retry

### Acceptance

- open write transaction on old primary fails cleanly after switchover
- SDK points client to new primary and tells caller to reopen transaction
- docs include retry/reopen examples

## Phase 9: Membership lifecycle operations

### Objective

Complete day-2 node operations.

### Work

Commands/API/UI for:

```bash
mycel cluster node remove NODE
mycel cluster node drain NODE
mycel cluster node rejoin NODE
mycel cluster node rename NODE NEW_NAME
```

Rules:

- cannot remove current primary without switchover/failover
- removing follower revokes membership and backend access
- draining follower stops replication/resync target eligibility

### Acceptance

- membership state transitions are durable and visible
- removed node cannot call privileged internode APIs

## Phase 10: Retention and snapshot automation

### Objective

Reduce manual resync burden.

### Work

- expose WAL retention policy controls
- warn before primary retention would strand followers
- optionally auto-trigger snapshot-required alerts
- optionally allow operator-approved auto-resync
- cleanup old resync artifacts/history with policy

### Acceptance

- operators can see whether followers are at risk of retention gaps
- snapshot-required state has clear remediation path

## Phase 11: Backup/snapshot consistency hardening

### Objective

Strengthen snapshot install guarantees.

### Work

- atomic per-managed-root replacement instead of per-file rollback where possible
- fsync important directories/files
- manifest includes root policy version
- backup/restore verification commands
- checksums for all files and archive

### Acceptance

- failed install cannot leave mixed old/new managed roots
- recovery tests pass after process kill during install where feasible

## Phase 12: Long-term consensus/election architecture

### Objective

Prepare for automatic failover.

### Work

Create design and prototype for either:

- external lease/fencing-backed primary authority, or
- embedded Raft/consensus for authority and membership

Scope should include:

- authority log
- membership changes
- WAL relationship to consensus
- snapshots
- client routing
- operational migration from static-primary mode

### Acceptance

- documented migration path from manual authority to consensus-backed authority
- prototype or spike validates core assumptions

## Recommended order

1. SDK primary-follow integration
2. switchover intent/recovery
3. authority propagation/reconciliation
4. emergency manual failover
5. explicit snapshot path policy
6. observability/health UX
7. internode RPC security
8. session/transaction behavior
9. membership lifecycle
10. retention/snapshot automation
11. snapshot consistency hardening
12. consensus/election architecture

## Validation baseline

Run after each phase as applicable:

```bash
cd mycel
go test ./internal/...
./scripts/validateWALPropagation.sh
./scripts/validateWALSnapshotResync.sh
./scripts/validateClusterPrimarySwitchover.sh

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
cd ..
npm test -- --runInBand
npm run build
```
