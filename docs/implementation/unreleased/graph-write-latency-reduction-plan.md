# Graph write latency reduction implementation plan

## Status

Planned. This plan coordinates the follow-up work from the staging graph write latency investigation.

Tracking issues:

- Parent tracker: [#90](https://github.com/MycelDB/mycel/issues/90)
- Async semantic dirty markers: [#89](https://github.com/MycelDB/mycel/issues/89)
- Real Commonfolio slow update tracing: [#91](https://github.com/MycelDB/mycel/issues/91)
- Batch Commonfolio graph writes: [#92](https://github.com/MycelDB/mycel/issues/92)
- Server-side replace-references operation: [#93](https://github.com/MycelDB/mycel/issues/93)
- Async lexical indexing sink: [#94](https://github.com/MycelDB/mycel/issues/94)
- Raft storage append optimization: [#95](https://github.com/MycelDB/mycel/issues/95)
- Graph apply/storage profiling: [#96](https://github.com/MycelDB/mycel/issues/96)
- Sink audit for nested synchronous Raft proposals: [#97](https://github.com/MycelDB/mycel/issues/97)

## Background

Fresh staging benchmarks reproduced slow graph commit latency on clean PVCs. Relationship-aware benchmarks showed that the pre-commit graph operation is fast, while transaction commit is slow.

Representative staging run:

- artifact: `/tmp/mycel-graph-raft-semantic-final-staging-20260923T022838Z`
- image: `local/mycel:raft-semantic-final-5539e86-20260923T022701Z`
- workload: `commonfolio-journal-entry update-references`, 10k seed nodes, refs=2, 50 operations

Client p50:

- `graph_operation`: 8.589ms
- `transaction_commit`: 1062.852ms
- `transaction_total`: 1093.958ms

Internal commit p50:

- total: 1017.351ms
- graph Raft propose: 450.083ms
- graph Raft storage append: 164.612ms
- graph state-machine apply: 114.903ms
- change sinks total: 563.644ms
- semantic sink: 520.658ms
- lexical sink: 40.650ms

Semantic sink p50:

- dirty event marshal: 0.025ms
- dirty event Raft build: 0.021ms
- dirty event Raft propose: 520.491ms
- local open/write/sync: 0ms in the Raft path

The main conclusion is that the current write path pays for a graph Raft proposal and then a nested semantic-maintenance Raft proposal before returning success to the client. Real Commonfolio user-facing slow updates may also perform multiple commits per logical save, multiplying the fixed commit cost.

## Goals

- Make one logical Commonfolio save normally pay for one user-facing graph Raft commit.
- Remove avoidable secondary synchronous Raft proposals from the graph commit path.
- Preserve durable graph commit semantics.
- Move secondary indexes and maintenance work to durable/replayable asynchronous processing where consistency permits.
- Measure real Commonfolio slow updates without logging payloads, secrets, user content, or graph data.
- Identify remaining unavoidable Raft/storage/apply costs and optimize them with evidence.

## Non-goals

- Do not weaken graph commit durability.
- Do not make semantic or lexical dirty work in-memory-only fire-and-forget.
- Do not log graph payloads, node content, secrets, credentials, or user data.
- Do not require a large API break before obtaining diagnostic evidence.

## Design principles

### Authoritative writes are synchronous

A successful graph commit must still mean that the graph mutation is durably committed and applied according to graph read semantics.

### Secondary work is asynchronous by default

Semantic maintenance, lexical indexing, notifications, and other derived projections should not add another synchronous Raft round to the client-facing graph commit path unless strict semantics explicitly require it.

### Async work must be durable or replayable

Background processing must be based on a durable outbox, graph commit history, committed revisions, or another replayable source. In-memory goroutines alone are not sufficient.

### Idempotency and replay are required

Dirty marker and indexing workers must tolerate duplicate events, replays, leader changes, crash/restart, and partial progress. Use graph transaction IDs, command IDs, revisions, and stable work keys for idempotency.

## Phased plan

### Phase 0: trace real Commonfolio slow updates

Issue: [#91](https://github.com/MycelDB/mycel/issues/91)

Add safe request-level instrumentation around the real Commonfolio save/update path.

Record counts and durations only:

- request/correlation ID
- Mycel transaction count
- Mycel commit count
- Mycel RPC count by method
- node/edge create/update/delete counts
- retry count
- phase durations

Do not record payloads, node content, secrets, credentials, or user data.

Deliverables:

- Trace evidence for at least one real slow Commonfolio update.
- Answer whether a ~10s save is one slow commit, many commits, retries, or application-layer work.
- Feed findings into phases 2 and 3.

Acceptance:

- A real Commonfolio slow update can be explained in terms of commit count, RPC count, retries, and elapsed time by phase.

### Phase 1: async durable semantic dirty markers

Issue: [#89](https://github.com/MycelDB/mycel/issues/89)

Replace the synchronous semantic-maintenance Raft proposal inside the graph change sink with durable/replayable async dirty marker processing.

Preferred implementation shape:

```text
graph commit Raft proposal
  -> apply graph mutation
  -> durably record semantic dirty marker/outbox entry in the same authoritative graph apply path
  -> return success to client

background semantic maintenance worker
  -> reads durable dirty markers/outbox
  -> updates semantic maintenance/index state asynchronously
  -> checkpoints progress
```

Important requirements:

- The marker must be durable or replayable before the client observes graph commit success.
- Work must be idempotent by graph command ID, transaction ID, revision, or equivalent stable key.
- Per-space/node ordering semantics must be defined.
- Crash/restart after a successful graph commit must not lose semantic maintenance work.

Acceptance:

- Graph commit no longer blocks on `dirty_event_raft_propose_ms`.
- Semantic freshness is documented as eventual after graph writes.
- Crash/restart tests prove semantic dirty work is recovered after a successful graph commit.

### Phase 2: batch Commonfolio graph writes into one transaction

Issue: [#92](https://github.com/MycelDB/mycel/issues/92)

Update Commonfolio-style write flows so one logical save stages node updates, reference edge changes, and related metadata in one graph transaction, then commits once.

Target shape:

```text
begin transaction
  update node
  delete stale reference edges
  create new reference edges
  update related metadata if needed
commit once
```

Avoid committing each sub-operation independently.

Acceptance:

- Instrumentation shows one logical Commonfolio save normally maps to one Mycel commit.
- Node and reference updates are atomic.
- Retries do not create duplicate edges.

### Phase 3: add server-side replace-references helper

Issue: [#93](https://github.com/MycelDB/mycel/issues/93)

Add a server-side graph operation/helper that computes and stages reference edge diffs in the active transaction.

Desired capability:

```text
replace references for source node
  labels/reference types
  desired target node IDs
  mode: replace/add/remove
```

This reduces client-side RPCs, avoids repeated read/diff/write loops, and makes the efficient one-transaction path easier to use correctly.

Acceptance:

- Tests cover add/remove/no-op replacement.
- Tests cover atomic node update plus reference replacement in one transaction.
- Benchmarks show fewer RPCs and one commit for reference-heavy updates.

### Phase 4: async durable lexical indexing

Issue: [#94](https://github.com/MycelDB/mycel/issues/94)

Move lexical indexing sink work off the synchronous graph commit path using the same durable/replayable outbox or graph-change replay principles.

Acceptance:

- Graph commit no longer waits for lexical index writes by default.
- Lexical/search freshness is documented as eventual.
- Crash/restart tests prove committed graph changes eventually reach the lexical index.

### Phase 5: optimize graph Raft storage append latency

Issue: [#95](https://github.com/MycelDB/mycel/issues/95)

After avoidable nested proposals are removed, the graph Raft proposal remains the authoritative commit cost. Current `raft_storage_append_ms` p50 around 165ms is high and should be explained.

Investigate:

- PVC/storage class and disk latency
- fsync frequency and group commit opportunities
- Raft log write amplification
- payload size and encoding cost
- leader/follower placement and network latency
- partition contention

Acceptance:

- `raft_storage_append_ms` is broken down into actionable sub-timings.
- Staging results identify whether the bottleneck is infrastructure, fsync policy, implementation batching, or payload size.
- Low-risk improvements are implemented or tracked separately.

### Phase 6: profile graph state-machine apply/storage commit

Issue: [#96](https://github.com/MycelDB/mycel/issues/96)

Current graph state-machine apply p50 is roughly 115ms for the representative update. Add sub-timing around graph storage apply to identify the dominant costs.

Instrumentation added:

- write tracing now emits `graph_write_apply_timing` from `applyGraphCommitRecord` when `MYCELD_GRAPH_WRITE_TRACE` is enabled;
- the event breaks down record normalization, graph store open, storage transaction begin, expected revision setup, put/delete loops, storage commit, and the lower-level `graphstorage.CommitTiming` fields;
- payload/content is not logged; only IDs, counts, revision, and timing values are emitted.

Use this event with the relationship-heavy raft benchmark below to identify the dominant apply/storage sub-step before selecting storage optimizations.

#### Profiling result

Local single-node Raft profiling artifact:

```text
/tmp/mycel-issue96-apply-profile-20260929T111848Z
```

Configuration:

- `MYCELD_CLUSTER_RAFT_NODE_COUNT=1`
- `MYCELD_CLUSTER_RAFT_REPLICA_FACTOR=1`
- `MYCELD_GRAPH_WRITE_TRACE=1`
- workload: `commonfolio-journal-entry update-references`, 10k seed nodes, refs=2, 50 operations

Batch size 1 client summary:

- `transaction_commit` p50 70.408ms, p95 82.252ms
- `transaction_total` p50 98.074ms, p95 113.217ms

Batch size 1 measured `graph_write_apply_timing` records:

- apply total p50 21.283ms, p95 27.498ms
- storage commit p50 21.278ms, p95 27.494ms
- node segment sync p50 7.899ms
- edge segment sync p50 8.034ms
- txn segment sync p50 4.131ms
- in-memory apply p50 0.028ms
- append, validation, conflict check, and index maintenance work were sub-ms at p50

Batch size 10 kept per-transaction commit latency similar while amortizing the fixed durable sync bundle over 10 logical updates:

- `transaction_commit` p50 64.019ms, p95 74.997ms for 5 measured transactions
- apply total p50 19.082ms, p95 23.700ms

Conclusion: for this workload, graph state-machine apply is dominated by the graph storage durable commit protocol's sequential node, edge, and txn segment fsyncs, not CPU, in-memory apply, conflict checks, or index maintenance. The low-risk near-term mitigation is batching logical updates into fewer graph transactions. Larger storage improvements are tracked in [#116](https://github.com/MycelDB/mycel/issues/116).

#### #116 low-risk storage sync optimization

Selected low-risk design:

- keep the existing `.kseg` segment format;
- keep the crash-recovery invariant that node/edge data segment syncs complete before the transaction commit marker is appended and synced;
- skip node/edge segment syncs for untouched data segments;
- run touched node and edge segment syncs concurrently, then append and sync the txn commit marker only after both data syncs have succeeded.

This does not weaken successful graph commit durability: a committed transaction is still advertised by a durable txn commit record written after the corresponding data segment records were durably synced. A crash before the txn commit marker remains an uncommitted transaction and is ignored by replay.

Post-change local single-node Raft benchmark artifact:

```text
/tmp/mycel-issue116-fsync-profile-20260929T120904Z
```

Same relationship-heavy workload, batch size 1:

- `transaction_commit` p50 65.140ms, p95 80.740ms
- `transaction_total` p50 90.703ms, p95 108.409ms
- apply total p50 20.306ms, p95 23.222ms
- storage commit p50 20.301ms, p95 23.217ms

Compared with the pre-change #96 profile, apply/storage p50 moved from 21.283ms to 20.306ms and p95 moved from 27.498ms to 23.222ms. The remaining cost is still durable filesystem sync time, especially the txn segment sync plus the slower of node/edge data syncs.

Investigate:

- graph storage transaction begin/commit
- node/edge put/delete loops
- storage fsync/write amplification
- lock contention
- index maintenance
- record decode/materialization
- revision bookkeeping

Acceptance:

- `applyGraphCommitRecord` and storage transaction commit are broken down into actionable sub-timings.
- Dominant apply/storage costs are identified and either optimized or tracked.

### Phase 7: audit graph commit sinks for nested synchronous Raft proposals

Issue: [#97](https://github.com/MycelDB/mycel/issues/97)

Inventory graph commit sinks and maintenance hooks. Define a policy that prevents secondary sinks from adding hidden synchronous Raft proposals to user-facing graph commits.

#### Sink inventory and policy

| Sink or hook | Standalone behavior | Clustered/Raft behavior | Durability model | User-facing commit policy |
| --- | --- | --- | --- | --- |
| Authoritative graph storage commit | Synchronous local store/WAL apply | Synchronous graph Raft proposal and state-machine apply | authoritative graph data | Required synchronous work. |
| Graph-change notification history | Synchronous commit sink | Synchronous raft-apply sink only, after graph command applies | durable graph-change history/outbox | Allowed synchronous sink because async semantic/lexical consumers replay from it. It must not call secondary Raft groups. |
| Semantic dirty marker creation | Legacy synchronous standalone sink | Async graph-change consumer reading notification history | durable/replayable dirty work from graph-change history | Must not perform nested synchronous semantic Raft proposal on clustered user-facing commits. |
| Lexical indexing | Async graph-change consumer | Async graph-change consumer | derived index from durable graph-change history and graph state | Must not block user-facing graph commits. |
| Automation graph triggers | Async/replay path from graph-change history in raft mode; local durable runtime writes enter automation gates | async/replay by execution leader | durable invocation/runtime state | Must not be added to synchronous graph commit fanout. |
| Future maintenance/index sinks | Not allowed by default | Not allowed by default | must use durable outbox, graph-change history, or checkpointed replay | Require explicit justification and tests before adding synchronous behavior. |

Clustered sink policy is encoded by `graphCommitSinkPolicyForConfig` and guarded by tests:

- standalone keeps the legacy synchronous `graph_change_notification` + `semantic` fanout;
- clustered graph commits have no generic synchronous `SetChangeSink` fanout;
- clustered raft apply synchronously records only `graph_change_notification`;
- clustered lexical and semantic processing are async graph-change consumers.

Acceptance:

- Sink inventory documents sync/async behavior and durability model.
- Any synchronous nested Raft proposals are removed, justified, or tracked.
- New sink guidance is documented for future graph-change consumers.

## Validation strategy

Use the relationship-heavy benchmark workload from the investigation as the baseline:

```text
seed template: commonfolio-journal
operation template: commonfolio-journal-entry
operation: update-references
seed nodes: 10000
references per node: 2
operations: 50+
batch size: compare 1 vs larger batches
```

For each phase, capture:

- client `graph_operation`, `transaction_commit`, and `transaction_total`
- daemon `graph_write_commit_timing`
- sink timing records
- commit/RPC count for real Commonfolio flows
- crash/restart recovery evidence for async sinks

## Expected outcomes

Near-term expected improvement:

- Removing the nested semantic-maintenance Raft proposal should eliminate roughly one additional synchronous Raft wait from graph commit latency.
- Batching Commonfolio writes should prevent multi-second saves caused by repeated commits.
- Async lexical indexing removes a smaller but measurable commit-time sink.

Longer-term improvement:

- Raft storage append and graph apply profiling should identify the remaining authoritative commit cost that cannot be avoided through batching or async secondary work.

## Rollout notes

- Gate risky async sink behavior behind configuration if needed during rollout.
- Document semantic and lexical freshness as eventual after graph writes.
- Keep graph commit trace logging payload-free.
- Prefer staged deployment to staging with fresh benchmark artifacts before merging to `develop`.
