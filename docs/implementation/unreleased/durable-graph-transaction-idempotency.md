# Durable graph transaction idempotency

Issue: [mycel#220](https://github.com/MycelDB/mycel/issues/220)

## Public contract

`CommitTransactionRequest.idempotency_key` opts a non-empty read-write graph
transaction into durable commit idempotency. The key is at most 128 UTF-8 bytes
and is scoped to the authenticated principal plus the explicit `space_id` and
`domain_id`. The scope must match the transaction. A successful response
includes the original commit metadata and the record's expiration time.
`idempotent_replay` distinguishes a replay from the first commit.

A retry must stage the same canonical graph mutation. Reusing a retained key for
a different mutation returns `AlreadyExists`; the daemon does not execute the
new mutation. Empty transactions cannot reserve keys. `operation_id` remains
correlation metadata and has no deduplication semantics.

## Durable binding and result

The graph service canonicalizes the sorted transaction overlay, excluding
server-assigned node and edge timestamps, sorting and de-duplicating label sets,
and normalizing empty versus nil maps before hashing it with SHA-256. The
storage transaction atomically persists this digest and the replayable commit
metadata in the same transaction segment record as graph writes. The retained
result includes:

- commit, transaction, session, space, and domain IDs;
- base and committed graph revisions;
- operation count and operation ID;
- commit and expiration times; and
- the idempotency key.

A matching retry returns those original values without advancing the graph
revision. The same atomic commit record also retains the graph-change delivery
intent and a durable delivered marker. If a process stops after graph durability
but before delivery completes, lookup or retry repairs the pending delivery.
The built-in notification history de-duplicates the same domain revision, so a
repair cannot repeat lexical/semantic index work, automation triggers, or
blob-reference deltas. A custom sink must likewise de-duplicate by scope and
revision because a crash between its acceptance and the delivered marker can
repeat the sink call.

## Storage modes and recovery

Direct, WAL, and Raft modes all use the same storage-level compare-and-commit
primitive. Its idempotency lookup runs before optimistic base-revision conflict
checking so a retry can return an older successful result after unrelated later
commits. WAL replay and Raft state-machine application therefore converge on the
stored result rather than repeating graph effects.

Version 2 Raft snapshots include every live retained idempotency binding,
original replay result, expiration, delivery intent, and delivery state.
Snapshot restore rebuilds the bounded store index before compacted log entries
are discarded; restored matching retries return the original revision and
mismatched mutations still fail closed. Version 1 snapshots remain readable and
contain no idempotency state because they predate this contract.

Raft command IDs bind the principal scope, key, and mutation digest. The
idempotency record is part of the replicated graph commit, so a new leader can
answer a retry. Strong graph reads and the internal graph-read forwarder expose
stored results to a daemon that does not retain the original in-memory session;
this covers restart and lost-response recovery.

## Retention and capacity

Records expire 30 days after the first commit. Expired records are purged on
lookup and commit and ignored during store recovery. Each domain store retains
at most 100,000 live records. When that bound is full and no expired record can
be removed, a new idempotent commit fails closed with `ResourceExhausted`; graph
writes are not applied. Once a record expires, reuse is treated as a new commit,
so callers must not depend on deduplication beyond the returned expiration.

`graph/service.Module.IdempotencyMetrics` reports replay hits, misses,
mismatches, capacity rejections, expiry evictions, and active/capacity record
counts. `AdminClusterService.GetClusterRuntimeStatus` publishes these values in
`graph_idempotency` for operational alerting.

## SDK guidance

Stable caller-assigned node or edge IDs are normally sufficient for retrying a
single upsert when duplicate entity creation is the only concern. Use durable
transaction idempotency for grouped writes, non-upsert mutations, or an
ambiguous commit response that could otherwise repeat downstream effects. The
Go SDK exposes `CommitTransactionIdempotent`,
`CommitTransactionWithOptions`, and `CommitIdempotentTransactionWithRetry`.
The retry helper preserves the exact transaction ID, key, space, and domain and
retries only caller-approved ambiguous errors (transport unavailability,
deadline, unknown, or internal by default). Explicit conflicts, capacity
failures, and validation failures are not retried. Callers persist a key before
the first attempt and reuse the exact key, scope, and mutation on retry.
