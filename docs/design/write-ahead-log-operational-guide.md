# Write-Ahead Log Operational Guide

## Status

Implemented internal WAL foundation with daemon integration.

This guide documents the current operational and developer behavior for Mycel's WAL implementation.

## Blob payload replication

Logical WAL records for blobs contain metadata and a payload descriptor only. Followers fetch missing blob bytes from the primary before applying blob metadata.

Operationally:

- blob payload transfer failures appear as follower replication lag/errors;
- metadata for a new blob is not visible on a follower until bytes are locally installed and checksum-verified;
- snapshot/resync still copies existing blob payloads and remains the recommended repair for severe inconsistencies.

Validate with:

```bash
./scripts/validateBlobPayloadReplication.sh
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=test-token ./scripts/validateBlobPayloadReplication.sh
```

## Directory layout

Default paths under `MYCELD_DATA_DIR`:

```text
wal/
  0000000000000001.wal
  ...

meta/wal/
  progress.json
  checkpoint.json
```

- `wal/*.wal` are segmented WAL files.
- `meta/wal/progress.json` stores the highest locally applied LSN.
- `meta/wal/checkpoint.json` stores the latest checkpoint LSN used by backup/retention.

## Configuration

Environment variables:

```text
MYCELD_WAL_ENABLED=true
MYCELD_WAL_DIR=<data_dir>/wal
MYCELD_WAL_SEGMENT_BYTES=67108864
MYCELD_WAL_SYNC_POLICY=always
```

Current supported sync policy:

```text
always
```

This means WAL records are fsynced before their local durable-state applier is run.

## Write path

WAL-backed mutations follow this shape:

```text
validate command
resolve IDs/timestamps/defaults/secrets into durable representation
append WAL record
sync WAL
apply record to local durable state
persist applied_lsn
return success
```

The invariant is:

> For converted mutation paths, durable state is updated only after the corresponding WAL record is committed.

## Startup recovery

During daemon startup:

1. WAL manager opens and validates segments.
2. Services initialize and register WAL appliers.
3. Recovery reads `meta/wal/progress.json`.
4. WAL records after `applied_lsn` are replayed in order.
5. Startup fails if a committed record has no registered applier or cannot be applied.

A torn final frame is truncated. Mid-log corruption fails startup.

## Checkpoints and backup

Before manual or scheduled backup, the backup module:

1. Creates a checkpoint at the current applied LSN.
2. Writes `meta/wal/checkpoint.json`.
3. Retains WAL from `checkpoint_lsn + 1` onward.
4. Runs the backup snapshot/archive flow.

Backups therefore include checkpoint metadata and only require WAL after the checkpoint for future recovery/replica catch-up.

## Retention

`wal.Manager.RetainFrom(ctx, lsn)` removes old WAL segments whose entire contents are before the requested LSN. It never deletes the active/final segment.

Future replicas must not request records older than `OldestRetainedLSN`; they must re-bootstrap from a newer snapshot/checkpoint.

## Failure behavior

### WAL append/sync failure

The mutation fails before local durable state is updated.

### Local apply failure after WAL commit

The mutation returns an error. On restart, recovery will retry the committed WAL record. Appliers should therefore be deterministic and idempotent where possible.

### Progress update failure after apply

The mutation returns an error. On restart, the record may replay. Appliers for converted paths are designed to tolerate matching replays or report conflicts.

### Disk full

A disk-full error during WAL append/sync prevents mutation. Disk-full during local apply or progress update may require freeing disk and restarting recovery.

## Adding a WAL-backed mutation

1. Define a stable record type, e.g.:

```go
const recordTypeExample wal.RecordType = "example.thing.put.v1"
```

2. Define a deterministic payload struct.
3. Resolve all non-deterministic values before append:
   - UUIDs
   - timestamps
   - default values
   - hashes/encrypted values
4. Register an applier during module init:

```go
rt.WALRegistry.Register(recordTypeExample, wal.ApplierFunc(m.applyExample))
```

5. Mutation path should:

```go
payload := buildRecord(...)
lsn, err := rt.WAL.Append(ctx, wal.PendingRecord{...})
rt.WAL.Sync(ctx, lsn)
apply(payload)
rt.WALProgress.SetAppliedLSN(ctx, lsn)
rt.WALWaiter.SetApplied(lsn)
```

6. Add tests for:
   - WAL append before apply
   - recovery from committed/unapplied record
   - idempotent replay
   - invalid command rejected before WAL append

## Payload rules

Do not rely on replay-time behavior for:

- current time
- random IDs
- config-derived defaults
- external provider calls
- plaintext secrets

WAL payloads should contain the durable representation. For secrets, WAL stores encrypted/hash state only, never plaintext.

## Current WAL-backed areas

- space metadata
- space templates
- identity user metadata
- admin/operator metadata
- blob metadata
- graph commits
- semantic/inference metadata
- semantic maintenance/accounting
- embedding provider keys
- backup policy/delete metadata

## Known follow-ups

- auth refresh-session lifecycle WAL records
- semantic vector artifact WAL strategy
- public replication service
- public read `min_lsn` API
- automatic leader election / quorum commit
- stronger migration/downgrade tooling
