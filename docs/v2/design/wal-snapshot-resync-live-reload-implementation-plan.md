# WAL Snapshot Resync Live Reload Implementation Plan

## Status

Implemented. Snapshot install now invokes a runtime reload callback before receive-log clear/progress reset. The space module reloads cached space/domain/template/access stores, and the e2e validation verifies follower reads resynced data immediately without restart.

## Objective

After `mycel cluster node resync NODE` installs a materialized snapshot on a follower, the follower should serve the resynced data immediately without requiring a daemon restart.

Previous behavior: snapshot files were durably installed, but some daemon modules kept in-memory state loaded from disk at startup, so the e2e script restarted the follower after resync. Current behavior: follower services reload after snapshot install and the follower can serve resynced data immediately.

## Phase 1: Inventory reload-sensitive stores

Audit daemon modules and stores to classify whether they:

- read from disk on every operation
- cache data in memory
- hold open file handles/indexes
- require explicit reload after files are replaced

Primary files/packages to inspect:

```text
mycel/internal/daemon/modules/admin
mycel/internal/daemon/modules/user
mycel/internal/daemon/modules/space
mycel/internal/daemon/modules/blob
mycel/internal/daemon/modules/graph
mycel/internal/daemon/modules/semantic
mycel/internal/embedding/store
mycel/internal/graph/storage
mycel/internal/graph/template/storage
mycel/internal/space/storage
mycel/internal/identity/storage
```

Deliverable: add notes to the design doc or implementation PR describing which modules need reload hooks.

## Phase 2: Define reload interface

Add a small runtime-level interface, likely in:

```text
mycel/internal/daemon/runtime/runtime.go
```

Suggested interface:

```go
type SnapshotReloadable interface {
    ReloadAfterSnapshot(ctx context.Context) error
}
```

Add runtime helper:

```go
func (r *Runtime) ReloadAfterSnapshot(ctx context.Context) error
```

Behavior:

- iterate registered services in startup order
- call `ReloadAfterSnapshot` for services that implement it
- aggregate/return first error
- log reload start/success/failure per service

## Phase 3: Implement module reload hooks

For each reload-sensitive module, implement:

```go
func (m *Module) ReloadAfterSnapshot(ctx context.Context) error
```

The simplest safe first implementation can re-open/reload backing stores from disk under the module lock.

Likely modules:

- admin module
- user module
- space module
- graph module
- semantic module
- embedding/provider metadata if cached
- backup policy/status only if snapshot install touches backup metadata, otherwise preserve/exclude

If a module is already disk-read-through and needs no reload, document that and omit the hook.

## Phase 4: Wire reload callback into SnapshotInstaller

Extend:

```text
mycel/internal/clustering/replication/snapshot_installer.go
```

Add field:

```go
ReloadAfterInstall func(ctx context.Context) error
```

Installer order should be:

1. validate descriptor
2. stage archive
3. verify size/checksum
4. extract and validate manifest
5. install materialized files
6. call `ReloadAfterInstall(ctx)`
7. clear receive log
8. reset replication progress to snapshot base LSN
9. return success

Important: if reload fails, do **not** clear receive log or reset progress. Return an error so the operator knows the follower is not fully usable.

## Phase 5: Wire runtime callback in daemon app

In:

```text
mycel/internal/daemon/app/app.go
```

When constructing `replication.SnapshotInstaller`, pass:

```go
ReloadAfterInstall: rt.ReloadAfterSnapshot,
```

## Phase 6: Tests

Add focused tests:

1. `SnapshotInstaller` calls reload after materialized install.
2. Reload failure prevents receive-log clear.
3. Reload failure prevents progress reset.
4. Reload success clears receive log and resets progress.
5. Runtime reload helper calls only services implementing `SnapshotReloadable`.
6. Runtime reload helper returns error if any service reload fails.

Suggested files:

```text
mycel/internal/clustering/replication/snapshot_installer_test.go
mycel/internal/daemon/runtime/runtime_test.go
```

## Phase 7: Update e2e validation script

Update:

```text
mycel/scripts/validateWALSnapshotResync.sh
```

Remove the post-resync follower restart.

Expected flow after resync:

```bash
mycel cluster node resync node-b
mycel --daemon-addr "$NODE_B_ADDR" -u "$OWNER_USERNAME" -p "$OWNER_PASSWORD" space list
```

The follower should show the resynced space immediately.

## Phase 8: Documentation updates

Update:

```text
mycel/docs/v2/design/wal-snapshot-resync.md
mycel/docs/v2/design/wal-snapshot-resync-materialized-install-implementation-plan.md
mycel/docs/v2/design/wal-snapshot-resync-cluster-node-command-implementation-plan.md
mycel/docs/v2/design/write-ahead-log-operational-guide.md
```

Document:

- snapshot resync now reloads follower services after install
- command completion means follower durable state is installed and live-readable
- reload failure behavior
- known limitations if any modules are still restart-required

## Validation commands

Focused:

```bash
cd mycel
go test ./internal/clustering/replication ./internal/daemon/runtime ./internal/daemon/app
./scripts/validateWALSnapshotResync.sh
```

Full:

```bash
cd mycel
go test ./internal/...
```

If admin/UI files change:

```bash
cd mycel-admin/src-tauri
cargo check
cd ..
npm test -- --runInBand
npm run build
```

## Acceptance criteria

Complete when:

- follower no longer needs restart after snapshot resync
- `SnapshotInstaller` invokes a runtime reload callback after materialized install
- reload failure prevents progress reset and returns an operator-visible error
- affected modules reload their in-memory state from installed snapshot files
- `validateWALSnapshotResync.sh` passes without restarting follower after resync
