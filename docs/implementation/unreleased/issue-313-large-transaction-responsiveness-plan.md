# Issue #313: Large-transaction responsiveness implementation plan

## Status

Investigation and implementation plan for [MycelDB/mycel#313](https://github.com/MycelDB/mycel/issues/313).

Working branch: `investigate/313-large-transaction-responsiveness`

Phase 0 measurement and immutable Compose comparison are complete. Phase 1 admission limits are now implemented in the investigation branch; queue fairness, cancellation-boundary coverage, and readiness semantics remain follow-up work.

This plan starts with measurement. The reported failure does not yet establish whether the dominant cause is Raft proposal pressure, state-machine apply time, storage locking/fsync, derived-work execution, or a combination of these.

## Problem statement

A client submits a large atomic graph transaction to a three-node Raft cluster. The request is close to the transport request limit but expands into many graph mutations and checkpoint metadata writes. The request returns HTTP 503 after roughly 25 seconds. Subsequent graph-backed login requests return HTTP 504, while process and TCP health checks continue to report healthy. Restarting the local cluster is currently used as recovery.

The server must prevent one transaction from making an affected partition or leader effectively unavailable, while preserving atomicity and clear commit semantics.

## Goals

- Identify the actual latency and contention boundary for large graph commits.
- Reject work that exceeds safe server budgets before it harms cluster responsiveness.
- Bound proposal, apply, storage-lock, and post-commit work.
- Preserve atomic transaction semantics; do not silently split one transaction into multiple commits.
- Define behavior when the client context expires before or after Raft accepts the command.
- Expose actionable, non-sensitive diagnostics for operators and clients.
- Provide deterministic tests and release-gate evidence for responsiveness and atomicity.

## Non-goals

- Changing Commonfolio batching or timeout behavior; those remain client-side mitigations.
- Silently converting an atomic transaction into independently committed chunks.
- Increasing request or RPC timeouts as the primary fix.
- Introducing a new asynchronous import/job API in this tranche.
- Making derived indexes or notifications authoritative for graph durability.

## Current implementation hotspots

The initial code review identifies these paths for measurement and likely hardening:

- `internal/graph/service/module.go:CommitTransactionGraph` builds one `graphCommitRecord`, serializes it, and proposes one Raft command.
- `internal/graph/service/raft.go` applies the complete command through the graph state machine.
- `internal/graph/storage/txn.go:CommitWithInfo` holds the `LocalStore` write lock through conflict checks, index validation, record appends, segment syncing, and in-memory index updates.
- `internal/graph/service/write_trace.go` already records graph-commit and storage timing fields that can be extended without logging payload contents.
- `internal/daemon/api/client/graph_service.go` maps graph commit failures to public gRPC errors and is the boundary for actionable admission errors.
- `internal/clustering/consensus` exposes Raft proposal timing, apply lag, and group health data that should be correlated with transaction traces.

These are investigation targets, not yet confirmed root causes.

## Phase 0 initial result

An opt-in three-node in-process baseline and bounded JSON timing capture are now implemented in `internal/daemon/app/issue313_baseline_test.go`. Run it with:

```sh
MYCELD_RUN_ISSUE313_BASELINE=1 go test -run '^TestIssue313LargeTransactionBaseline$' -v ./internal/daemon/app
```

One local run measured approximately:

| Nodes in one transaction | Encoded Raft command | Commit wall time |
| ---: | ---: | ---: |
| 16 | 17 KiB | 36 ms |
| 64 | 68 KiB | 42 ms |
| 256 | 272 KiB | 54 ms |
| 512 | 543 KiB | 57 ms |
| 1024 | 1.06 MiB | 65 ms |

The largest transaction showed roughly 22–34 ms state-machine storage apply time per replica and roughly 58 ms leader-side proposal time. A concurrent local graph read completed in roughly 0.01 ms in this run. This did **not** reproduce the reported 503/504 behavior.

These measurements are local in-process evidence only: they do not represent production limits, external Compose networking, HTTP/gRPC decoding, authentication, or the reported v0.21.2 environment. Immutable-image Compose runs against v0.21.2 and the current investigation candidate both completed the deterministic workload without reproducing the reported 503/504 sequence; the workload therefore classifies the failure as not reproduced rather than identifying a production bottleneck.

The external comparison used `docker.io/myceldb/mycel@sha256:98bf37f2bcfe5ad4fc5e48bbdd8b64caeb3ae63a914fc6cd3b19f877eefc84bb` for v0.21.2 and `local/mycel@sha256:af3a35be956d7071faf160cdb5aa4cfea0e7ff59fe719fc75444023b6e76483c` for revision `c306993`. Each run acknowledged 5/5 deterministic 1024-node transactions; v0.21.2 recorded 167/167 successful concurrent reads and the current candidate recorded 170/170. Both runs passed all phases, redaction scans, and cleanup absence checks. The transaction acknowledgements remained near the configured 10-second cadence throughout the 35-second stress phase, so the reported 25-second stall was not reproduced by this workload.

## Target behavior

For every graph transaction:

1. Admission checks estimate operation count, encoded size, property/payload bytes, and expected work.
2. Work over a configured safe budget is rejected promptly with `ResourceExhausted` and a stable diagnostic reason.
3. Accepted work has bounded per-partition queueing and cannot create an unbounded apply backlog.
4. Durable graph commit remains Raft-owned in clustered mode.
5. Derived notifications and rebuildable index work cannot delay the durable commit response indefinitely.
6. Client cancellation before proposal prevents acceptance; cancellation after acceptance does not undo a durable commit.
7. An ambiguous client result can be resolved using transaction identity and the existing idempotency/status mechanisms.
8. Readiness and health diagnostics distinguish process liveness from client/read/write readiness and partition overload.

## Implementation phases

### Phase 0: Reproduce and establish a baseline

1. Create a deterministic workload that increases node, edge, property, and total encoded transaction size independently.
2. Run it against a three-node Compose cluster while issuing graph-backed login, read, and small-write probes.
3. Capture bounded metadata only: operation counts, encoded sizes, partition/group IDs, queue/proposal/apply/storage timings, status codes, and readiness dimensions. Do not persist graph payloads or credentials.
4. Extend `graphCommitTiming` and `graphstorage.CommitTiming` only where needed to identify:
   - admission and queue wait;
   - Raft proposal and apply duration;
   - storage lock wait;
   - append and sync duration;
   - index/update duration;
   - change-sink duration.
5. Classify the failure as transport rejection, admission starvation, Raft/apply serialization, storage lock contention, sink backpressure, or a compound failure.

**Exit condition:** a repeatable baseline identifies the first responsiveness boundary and produces a sanitized timing record.

### Phase 1: Add transaction budgets and admission control

1. Define a versioned internal `GraphTransactionLimits` policy covering:
   - maximum encoded graph-commit command bytes;
   - maximum node puts/deletes and edge puts/deletes;
   - maximum aggregate property and payload bytes;
   - maximum active transaction age and staged-memory bytes;
   - maximum pending graph commits per partition.
2. Enforce cheap incremental limits while mutation operations are staged, then recheck the final encoded command before proposal.
3. Return `codes.ResourceExhausted` for budget or queue rejection, with stable machine-readable reason metadata and bounded human-readable details.
4. Keep safe defaults conservative and explicit; allow operator configuration only through existing daemon configuration mechanisms.
5. Ensure rejected transactions release overlays, mutation locks, queue reservations, and idempotency state.
6. Document the limits and explain that client-side chunking is required for workloads that do not require one atomic commit.

**Exit condition:** oversized or saturated work fails fast without entering the expensive Raft/storage path, and no partial graph state is visible.

### Phase 1 implementation status

Implemented in the investigation branch:

- `GraphTransactionLimits` bounds operation count, staged mutation bytes, encoded command bytes, and transaction age.
- Limits are checked incrementally before overlay mutation and again before Raft/WAL proposal.
- Rejections use `ResourceExhausted` with stable `ErrorInfo` reasons such as `GRAPH_TRANSACTION_COMMAND_BYTES`.
- Rejected statements restore their mutation scope; rejected commits restore the transaction overlay through the existing commit rollback path.
- Atomicity is preserved; no transaction is split.

The remaining Phase 1 work is pending: distinct node/edge and payload counters, queue-pressure admission, cancellation-boundary tests, and external validation of rejection behavior.

### Phase 2: Bound commit execution and preserve fairness

1. Ensure serialization, validation, and immutable record preparation occur before acquiring the longest-lived storage write lock where correctness permits.
2. Reduce the `LocalStore` critical section around append/sync/index updates, or introduce a safe per-domain/per-partition locking strategy after profiling proves the benefit.
3. Bound and instrument the per-partition proposal/apply queue. Reject rather than allow unbounded backlog.
4. Keep one atomic transaction as one Raft command; do not interleave or partially apply its mutations.
5. Audit graph-change notification and derived-index sinks. After durable commit, deliver them through bounded ordered workers that cannot hold the graph commit path indefinitely. Preserve ordering and retry/error diagnostics.
6. Ensure Raft apply uses an apply-safe context rather than abandoning a committed state-machine command merely because the originating client context expired. Apply work must still have server-side admission bounds.

**Exit condition:** accepted transactions remain atomic, and concurrent small reads/writes and graph-backed login probes remain within the declared responsiveness budget.

### Phase 3: Define ambiguous commit and readiness semantics

1. Specify and test the commit state machine:
   - canceled before proposal: not committed;
   - proposal accepted or committed: may complete despite client cancellation;
   - response lost or deadline exceeded after acceptance: status is queryable and idempotent retry returns the original result.
2. Reuse existing transaction IDs and graph idempotency records where possible instead of adding a second commit identity system.
3. Map admission, overload, deadline, conflict, and Raft-unavailable failures to distinct stable gRPC categories.
4. Extend admin diagnostics with bounded transaction pressure and partition apply/queue indicators where existing readiness/status APIs can carry them.
5. Keep TCP/process liveness separate from client-ready, read-ready, and write-ready state. Add an overload blocker only when the server cannot safely accept work, not merely because a single transaction is slow.

**Exit condition:** clients can distinguish rejected, canceled-before-acceptance, committed, and unknown-but-queryable outcomes without restarting the cluster.

### Phase 4: Qualification and documentation

1. Add unit coverage for limit accounting, cleanup on rejection, cancellation boundaries, idempotent retry, and no-partial-commit behavior.
2. Add storage benchmarks/tests for lock wait, append/sync, and index-update phases at representative transaction sizes.
3. Add a three-node Compose integration test with progressively larger transactions and concurrent login/read/small-write probes.
4. Add leader-change and response-loss cases around the commit boundary.
5. Add a Mycel Lab scenario after the contract is stable. It should remain credential-free, use bounded deterministic fixtures, and record no raw graph payloads.
6. Update transaction, configuration, health, and operations documentation with safe limits, rejection behavior, cancellation semantics, and client chunking guidance.
7. Run normal unit/race/vet checks and the relevant clustering phase tests. Destructive Compose/Lab validation requires explicit approval and an immutable candidate image.

## Observability requirements

All transaction diagnostics must be bounded and redacted. Recommended fields are:

- transaction and idempotency identifiers, hashed or scoped as appropriate;
- space/domain and partition identifiers;
- operation counts and encoded/estimated byte sizes;
- admission result and rejection reason;
- queue wait, proposal, apply, lock, append, sync, index, and sink durations;
- client deadline/cancellation state;
- final commit state and readiness impact.

Never log graph payloads, credentials, or raw request bodies.

## Acceptance criteria

The work is complete when:

- the original responsiveness failure is reproducible or explained with evidence;
- transactions over safe budgets are rejected before Raft proposal;
- no oversized transaction causes unbounded queueing or blocks unrelated requests beyond the declared budget;
- accepted transactions remain atomic across success, retry, cancellation, and leader change;
- a lost/deadline-exceeded response can be resolved without cluster restart;
- process, client, read, and write readiness are reported distinctly;
- tests cover both the safe-success path and prompt-rejection path;
- documentation gives clients actionable chunking and timeout guidance; and
- sanitized validation evidence passes cleanup and redaction checks.

## Open decisions

- Initial default limits and whether they should be static or dynamically derived from storage/partition capacity.
- Whether the first implementation should only reject by size/count or also enforce an estimated execution-cost budget.
- Whether post-commit sink decoupling belongs in this issue or a follow-up after Phase 0 identifies sink latency.
- Which existing admin status surface best exposes transaction pressure without requiring a public protobuf change.
