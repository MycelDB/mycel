# WAL Replication Readiness

## Status

Implemented internal foundation. No public replica protocol or read-replica runtime is exposed yet.

This document records the Phase 8 replication-ready WAL APIs and the intended future replica flow.

## Internal WAL APIs

The WAL manager now exposes these replication-oriented APIs:

```go
Append(ctx, record) (wal.LSN, error)
Sync(ctx, lsn) error
ReadFrom(ctx, lsn) (*wal.Iterator, error)
ReadNextBlocking(ctx, lsn) (wal.Record, bool, error)
WaitUntilCommitted(ctx, lsn) error
LastCommittedLSN() wal.LSN
OldestRetainedLSN() (wal.LSN, error)
Status() (wal.Status, error)
RetainFrom(ctx, lsn) error
```

`wal.Status` reports:

```go
type Status struct {
    LastCommittedLSN    LSN
    OldestRetainedLSN   LSN
    CurrentSegmentStart LSN
    CurrentSegmentBytes int64
}
```

## Future replica pull flow

A future replica can bootstrap from a checkpointed snapshot, then request records from the next LSN.

```text
1. Restore snapshot/checkpoint.
2. Read checkpoint_lsn from meta/wal/checkpoint.json.
3. Connect to leader.
4. Request WAL from checkpoint_lsn + 1.
5. For each record:
   a. append to local replica WAL
   b. sync according to replica durability policy
   c. apply through registered applier
   d. advance applied_lsn
6. Report lag = leader_last_committed_lsn - local_applied_lsn.
```

## Future leader streaming loop

A leader-side streaming loop can use `ReadNextBlocking`:

```go
next := requestedLSN
for {
    rec, ok, err := walManager.ReadNextBlocking(ctx, next)
    if err != nil { return err }
    if !ok { continue }
    send(rec)
    next = rec.LSN.Next()
}
```

`ReadNextBlocking` waits until the target LSN has been committed or the context is cancelled.

## Retention interaction

Replicas must not request an LSN older than `OldestRetainedLSN`.

If a replica asks for an older LSN, the future protocol should return a specific error instructing the replica to re-bootstrap from a newer snapshot.

```text
requested_lsn < oldest_retained_lsn => re-bootstrap required
```

## Read freshness

Writers already receive or internally track commit LSNs for converted paths. Future read APIs can accept `min_lsn` and use:

```go
walWaiter.WaitUntilApplied(ctx, minLSN)
```

If the local node cannot reach `min_lsn` before timeout, routing can fall back to the leader or return a retryable stale-replica error.

## Not implemented yet

- Network replication service.
- Replica membership configuration.
- Replica authentication/authorization.
- Leader election.
- Quorum commit.
- Public read `min_lsn` API fields.
