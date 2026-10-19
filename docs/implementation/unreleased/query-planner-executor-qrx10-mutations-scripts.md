# QRX10: native GQL mutations and script orchestration

## Status

Merged into `improved_querying` through PR #227 for [mycel#213](https://github.com/MycelDB/mycel/issues/213).

## Scope

QRX10 moves supported GQL writes and sequential script execution behind the transport-neutral query boundary. The daemon API adapter retains principal, transaction, domain, protobuf, and gRPC mapping responsibilities, but it no longer executes mutation plans or owns the script statement loop.

## Native mutation plans

The physical planner emits `FamilyMutation` with `AccessMutation` and one explicit command operator:

- `InsertNode`;
- `MergeNode`;
- `MatchSet`;
- `MatchDelete`;
- `CreateRelationship`;
- `MergeRelationship`.

Mutation plans require the consumer-owned `engine.MutationWriter` capability. They never use `OperatorLegacyBridge` and Explain remains side-effect-free.

Matched mutations prepare native read subplans and reuse finite node/index/adjacency sources, residual predicate evaluation, transaction snapshot/overlay propagation, cancellation checks, and shared resource budgets. Node deletes perform bounded incoming and outgoing adjacency validation before staging any selected edge deletion, preserving the existing incident-edge rule without leaving a partially changed overlay on validation failure.

All graph calls carry the complete `sessionmodel.GraphTransaction`. Before a statement writes, the engine enters an opaque graph-owned mutation scope. A per-transaction lease serializes the statement with primitive writes, compound writes, commit, and discard. After acquiring the lease, production composition revalidates the transaction against the authoritative session manager. Commit and discard record terminal graph state before releasing the lease, and rollback/close discard graph state before transitioning session state, so a queued stale request cannot stage or acknowledge writes after finalization. After the authoritative session transition succeeds, daemon and automation callers acknowledge graph finalization under the same lease; the graph then removes the short-lived terminal record, keeping lifecycle state bounded while authoritative validation continues to reject stale writes.

The graph records a bounded undo journal only for pre-existing node and edge entries first touched by the statement; it does not clone the accumulated transaction overlay. Any writer, cancellation, shaping, or resource-budget failure restores that journal before releasing the lease, so a failed statement cannot leave a partial overlay or erase a concurrent acknowledged write. Successful statements retain their staged writes; the engine never commits or rolls back the enclosing transaction. Matched rows and mutable `SET` workspaces remain charged through result shaping. `SET` coalesces assignments by entity ID, writes each entity once, and propagates the resulting value to every alias bound to that entity.

## Result compatibility

Mutation execution preserves:

- insert, update, delete, and relationship counters;
- returned rows and graph projection;
- parameter resolution;
- mutation `LIMIT` behavior;
- response page size as a statement-scope safety bound: mutations do not accept or emit continuations, and an oversized returned result rolls the statement scope back with `ResourceExhausted`;
- node/relationship merge identity rules;
- read-write access enforcement.

A mutating statement is prepared once and executed once. There is no shadow or fallback execution after native execution starts.

## Script application boundary

`internal/query/application.ExecuteScript` now owns transport-neutral script orchestration:

1. compile the complete script with schema and parameters;
2. reject read-write scripts before statement execution when the transaction is read-only;
3. normalize, prepare, and execute statements sequentially;
4. preserve source indexes and statement text;
5. stop after the first failure only when `stop_on_error` is set;
6. aggregate successful counters and result graphs;
7. invoke an optional neutral completion observer so the daemon can snapshot per-statement read metadata;
8. never commit, roll back, retry, or execute a statement twice.

The daemon converts neutral statement outcomes to protobuf responses and maps query errors to gRPC status codes.

## Compatibility and cleanup boundary

QRX11 removed the daemon-local read helpers, GQL-to-structured execution bridge, and temporary physical/engine ownership bridge. `internal/query/gql/execution` remains only for non-QueryService consumers that evaluate existing GQL execution-model contexts; supported QueryService reads, mutations, and scripts do not invoke it.

## Validation

Coverage includes:

- native physical mutation plan and Explain assertions;
- existing daemon GQL insert, set, delete, node merge, relationship create/merge, parameters, counters, rows, graph, and overlay tests;
- incident-edge validation before mutation writes;
- mutation continuation rejection plus rollback of oversized `SET` and `DELETE` result statements;
- overlay restoration after shaping-budget exhaustion, post-write cancellation, and a later writer failure;
- per-transaction write serialization across rollback and concurrent primitive writes;
- both mutation-before-finalization and finalization-before-mutation orderings for commit and discard, stale primitive-write rejection after commit, rollback, and close, and reclamation of acknowledged terminal records across repeated lifecycle operations;
- authoritative active-state revalidation after a queued statement acquires its lease;
- undo-journal growth based on statement-touched entries rather than accumulated overlay size;
- repeated node/edge binding coalescing for `SET`;
- matched-row clone accounting under a tight memory budget before writer invocation;
- sequential script aggregation and graph compatibility;
- stop-on-error and continue-on-error exact-once application tests;
- read-only script rejection before engine work;
- query-core transport/import boundary checks.
