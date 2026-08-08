# Graph-change Notification Implementation Plan

## Status

Draft implementation plan for the `add_callbacks` branch. This plan implements
the internal graph-change notification subsystem described in
[`docs/design/graph/graph-change-notification.md`](../../design/graph/graph-change-notification.md).

Status: implemented through GCN10 for the internal notification subsystem. The `add_callbacks` branch also adds the lower-level public `GraphChangeService.WatchGraphChanges` API backed by the internal notification subsystem. GraphQL subscription work remains a follow-up plan.

This plan intentionally excludes GraphQL subscription work. That surface should be designed and implemented after the internal committed change model and public graph-change watch path are stable.

## Goals

- Establish `internal/graph/change` as the canonical internal graph-change event
  model.
- Consolidate duplicate graph-change structures used by graph service commit
  results and change streams.
- Add a process-local graph-change notification subsystem for internal consumers.
- Support scope, filter, projection, origin metadata, checkpoints, resumability,
  and explicit gap handling.
- Persist a local bounded replay log for graph-change events.
- Preserve raft-mode fail-closed and leader-owned behavior.
- Keep graph commit paths functional after each tranche.

## Non-goals

- No public GraphQL subscription API.
- No new public webhook/callback API.
- No automatic cache repair, merge, conflict resolution, graph repair, rebalance,
  or authoritative-node selection.
- No separate raft-owned event-history records in V1.
- No follower-served stale notification delivery in raft mode.
- No generated public SDK/API code unless explicitly approved.

## Design decisions already made

- Lower-level internal notification subsystem first; GraphQL interface later.
- Projection is part of the V1 model.
- Resume uses `AfterRevision` and explicit gap reporting.
- Delivery is leader-routed in raft mode.
- Session and transaction creation both accept origin metadata in V1.
- V1 registrations are process-local.
- Event history is a local persistent bounded derived log, not an audit log.
- Default retention is the smaller of:
  - 10,000 committed events per space/domain/partition;
  - 24 hours.
- Projection can include both old and new node/edge snapshots, opt-in.
- `AffectedNodeIDs` includes edge endpoints for every edge create/update/delete.

## Existing code to reuse

Canonical starting point:

```text
internal/graph/change/graphchange.go
```

Existing graph-change structures:

```text
internal/graph/change.CommittedEvent
internal/graph/service.GraphChange // alias of canonical graphchange.Change
```

Existing graph commit notification points:

```text
internal/graph/service/module.go           // notifyGraphChangeSink, graphChangeEvent
internal/graph/filesession/file_session.go // graph-change sink path for file sessions
```

Legacy public change stream implementation replaced by `GraphChangeService.WatchGraphChanges`:

```text
internal/graph/notification/
internal/daemon/api/client/change_stream_service.go
internal/cli/cmd/change_stream.go
```

The old internal `internal/changestream/service/` module was removed on `add_callbacks`; automation now consumes the graph-change notification subsystem directly.

## Phase GCN0 — Inventory and compatibility tests

Status: implemented.

Goal: lock in current behavior before consolidation.

Tasks:

1. Add focused tests that document current graph-change sink behavior for:
   - graph service commits;
   - file-session commits;
   - no-op/read-only/rollback paths;
   - sink failure not failing the commit.
2. Add tests documenting current change-stream replay and `OUT_OF_RANGE` behavior
   if coverage is missing.
3. Record exact existing event payload expectations before changing types.

Expected validation:

```sh
go test ./internal/graph/change ./internal/graph/service ./internal/graph/filesession ./internal/graph/notification ./internal/daemon/api/client ./internal/cli/cmd
```

Exit criteria:

- Existing behavior is covered enough to refactor without losing events.
- No public API behavior changes.

## Phase GCN1 — Canonical graph-change model

Status: implemented.

Goal: evolve `internal/graph/change` into the canonical model.

Tasks:

1. Add canonical types to `internal/graph/change`:
   - `Change`;
   - `ChangeType`;
   - `OriginMetadata`;
   - `Projection`;
   - `Filter`;
   - `Scope`;
   - `Gap`.
2. Extend `CommittedEvent` to include:
   - canonical `Changes []Change`;
   - `CommitID` / transaction identifiers as needed;
   - `Revision` naming compatible with existing `GraphRevision`;
   - `Origin`;
   - `AffectedNodeIDs` and `AffectedEdgeIDs`.
3. Preserve existing fields temporarily where consumers depend on them:
   - created/updated/deleted node ID lists;
   - changed edge summaries;
   - parent/domain movement maps.
4. Add helper methods:
   - `Empty()`;
   - `AffectedNodes()`;
   - `ApplyProjection()`;
   - filter matching helpers if they are pure and lightweight.

Implementation notes:

- Keep the package dependency-light.
- Do not import daemon API, changestream, automation, semantic, or graph service
  packages into `internal/graph/change`.

Expected validation:

```sh
go test ./internal/graph/change ./internal/graph/service ./internal/graph/filesession
```

Exit criteria:

- Canonical model compiles and existing graph event tests continue passing.
- No duplicate packages are removed yet.

## Phase GCN2 — Graph service produces canonical changes

Status: implemented.

Goal: make the graph service produce canonical per-object changes.

Tasks:

1. Change `internal/graph/service` commit assembly to build
   `graphchange.Change` values.
2. Update `CommitResult.Changes` to either:
   - use `[]graphchange.Change`; or
   - temporarily alias `type GraphChange = graphchange.Change`.
3. Ensure old and new snapshots are populated where available for projection.
4. Ensure edge changes populate affected endpoints in `AffectedNodeIDs`.
5. Ensure committed event envelopes include canonical `Changes` and aggregate
   affected IDs.
6. Preserve existing sink behavior: sink errors are recorded but do not fail
   successful commits.

Expected validation:

```sh
go test ./internal/graph/service ./internal/daemon/api/client ./internal/session/service
```

Exit criteria:

- Graph commits return canonical changes.
- Existing client graph/query/session tests pass.
- No public API/protobuf changes yet.

## Phase GCN3 — Remove legacy change-stream duplication

Status: implemented.

Goal: remove duplicate change-stream graph-change structures and route public/internal consumers through the canonical graph-change model.

Tasks:

1. Replace legacy duplicate graph-change structures with `internal/graph/change` canonical types.
2. Delete the old public `WatchDomainChanges` surface in favor of `GraphChangeService.WatchGraphChanges`.
3. Move automation from the old internal change-stream observer onto the graph-change notification consumer API.
4. Remove the old internal `internal/changestream/service` module and transaction `PublishCommit` plumbing.
5. Keep CLI compatibility through `change-stream`/`changes` aliases backed by the new graph-change watch RPC.

Expected validation:

```sh
go test ./internal/graph/notification ./internal/daemon/api/client ./internal/cli/cmd
```

Exit criteria:

- Public graph-change watch behavior is covered by tests.
- Automation consumes graph-change notifications directly.
- Legacy changestream package and publisher wiring are removed.

## Phase GCN4 — Notification subsystem skeleton

Status: implemented.

Goal: add process-local registration and async fanout without persistence first.

Proposed package:

```text
internal/graph/notification
```

Tasks:

1. Add subsystem types:
   - `Registrar`;
   - `Registration`;
   - `Consumer`;
   - `ConsumerSpec`;
   - delivery diagnostics.
2. Implement process-local registration table.
3. Implement event publish/enqueue path.
4. Implement scope/filter matching.
5. Implement projection before handler invocation.
6. Invoke consumer handlers asynchronously outside graph commit path.
7. Recover panics from handlers and record failures.
8. Add deterministic tests for:
   - registration lifecycle;
   - matching and non-matching events;
   - projection behavior;
   - handler errors;
   - handler panic recovery;
   - close/unregister behavior.

Expected validation:

```sh
go test ./internal/graph/notification ./internal/graph/change
```

Exit criteria:

- Internal consumers can register and receive matching projected committed events
  in-process.
- No daemon wiring yet required.

## Phase GCN5 — Local persistent bounded replay log

Status: implemented.

Goal: add resumability and explicit gap behavior.

Tasks:

1. Add a local persistent event-log store for graph-change events.
2. Key history by space/domain/partition as appropriate for the graph storage and
   raft partition model.
3. Implement default retention:
   - 10,000 events;
   - 24 hours.
4. Implement replay after `AfterRevision`.
5. Implement explicit gap reporting when requested history is unavailable.
6. Persist/reload history across daemon restarts.
7. Ensure the replay log is treated as derived state, not authoritative graph
   data.
8. Add corruption/partial-write handling that fails closed for replay and reports
   gaps rather than emitting malformed events.

Expected validation:

```sh
go test ./internal/graph/notification ./internal/graph/change
```

Exit criteria:

- Consumers can resume from retained history.
- Out-of-range resumes produce gaps.
- Restart tests prove retained history survives daemon restart within policy.

## Phase GCN6 — Origin metadata plumbing

Status: implemented for internal session/transaction models and committed graph
events. Public API support is implemented as minimal
`BeginTransactionRequest.operation_id`, returned on `GraphTransaction` and
`TransactionCommit`; broader public origin fields remain future work.

Goal: attach origin metadata to committed graph events.

Tasks:

1. Add origin metadata fields to internal session and transaction models.
2. Add protobuf fields only when explicitly approved for API changes:
   - `OpenSessionRequest.origin`;
   - `BeginTransactionRequest.origin`.
3. Propagate session origin into transaction origin defaults.
4. Let transaction origin override/supplement session origin.
5. Server-populate trusted fields:
   - user ID;
   - session ID;
   - transaction ID.
6. Echo resolved origin metadata in committed graph events.
7. Add tests for:
   - session-only origin;
   - transaction-only origin;
   - transaction overriding session fields;
   - server-populated fields not spoofed by clients.

Expected validation:

```sh
go test ./internal/session/service ./internal/daemon/api/client ./internal/graph/service
```

Exit criteria:

- Committed graph events include origin metadata when supplied.
- Existing clients continue working when origin is omitted.

## Phase GCN7 — Wire graph service to notification subsystem

Status: implemented.

Goal: use the notification subsystem as the graph service event sink.

Tasks:

1. Instantiate the graph-change notification subsystem during daemon/runtime
   initialization.
2. Wire graph service committed events into the subsystem through the existing
   `graphchange.Sink` boundary or a small adapter.
3. Keep existing semantic/automation/change-stream sinks working through
   `graphchange.MultiSink` or notification consumers.
4. Ensure graph commit success does not depend on consumer handler success.
5. Expose internal diagnostics enough for tests and admin follow-up work.

Expected validation:

```sh
go test ./internal/daemon/app ./internal/daemon/runtime ./internal/graph/service ./internal/automation/service ./internal/graph/notification
```

Exit criteria:

- The graph service publishes committed events through the notification
  subsystem.
- Existing semantic, automation, and change-stream behavior remains functional.

## Phase GCN8 — Raft-mode leader routing and fail-closed semantics

Status: implemented for internal notification delivery. Public stream reconnect semantics remain future work.

Goal: make delivery safe in raft mode.

Tasks:

1. Ensure committed events are emitted only from committed/applied raft graph
   commands in raft mode.
2. Ensure delivery for a partition is owned by the partition leader.
3. On leadership loss, stop active delivery and require consumers to resume from
   checkpoint after reconnect/re-registration.
4. Ensure follower-local stale notification delivery is disallowed by default.
5. Preserve local persistent event history as derived per-replica replay buffer.
6. Report gaps if the new leader cannot replay from a requested revision.
7. Add focused raft-mode tests for:
   - leader-owned delivery;
   - no follower stale delivery;
   - leadership change plus resume;
   - gap on unavailable history.

Expected validation:

```sh
go test ./internal/graph/notification ./internal/graph/service ./internal/clustering/consensus ./internal/daemon/api/client
```

Exit criteria:

- Raft-mode notifications are leader-routed and fail closed.
- No separate raft-owned event-history records are introduced.

## Phase GCN9 — Cache consumer pilot

Status: implemented with a test cache consumer that invalidates affected nodes and domains on gaps.

Goal: prove the internal registration API with a simple internal cache-style
consumer.

Tasks:

1. Add a narrow internal test consumer that registers for node and edge changes.
2. Persist a test checkpoint.
3. Validate incremental invalidation from affected node IDs.
4. Validate broad invalidation on gap.
5. Confirm edge changes include both endpoints in affected node IDs.

This phase can be implemented as tests only if no production cache consumer is
ready.

Expected validation:

```sh
go test ./internal/graph/notification ./internal/graph/service
```

Exit criteria:

- The API is proven usable by cache-style consumers before public streaming or
  GraphQL subscription work begins.

## Phase GCN10 — Documentation and release gate updates

Status: implemented for design/implementation docs and validation notes.

Goal: document the internal subsystem and add validation targets.

Tasks:

1. Update design docs if implementation refines names or boundaries.
2. Update `docs/design/api/change-stream.md` if current change stream internals
   move behind the notification subsystem.
3. Add or update Makefile targets if focused graph-change notification tests are
   useful.
4. Add release-gate notes for raft-mode notification behavior once implemented.

Expected validation:

```sh
make docs-check
git diff --check
go test ./...
```

Exit criteria:

- Docs match implemented behavior.
- Full Go test suite passes.

## Validation strategy

During implementation, each tranche should leave mycel functional. Suggested
validation grows from focused to full:

```sh
go test ./internal/graph/change ./internal/graph/notification
go test ./internal/graph/service ./internal/graph/notification
go test ./internal/daemon/api/client ./internal/cli/cmd
go test ./...
make docs-check
git diff --check
```

Raft-specific work should also run the existing raft and cluster focused suites
where practical:

```sh
make test-phase-f
make test-phase-g
make test-cluster-soak
```

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Duplicate event structures drift during migration | Introduce canonical aliases before removing old structures. |
| Slow consumers affect commit latency | Publish/enqueue in commit path; invoke handlers asynchronously. |
| Event log is mistaken for audit log | Document and enforce bounded retention/gap behavior. |
| Raft followers emit stale events | Leader-owned delivery and fail-closed follower behavior. |
| Large old/new snapshots increase memory or disk use | Make snapshots projection opt-in and default to affected IDs. |
| Resume from compacted history is ambiguous | Return explicit gap with oldest/current revision evidence. |
| Origin metadata spoofing | Server-populate trusted fields and treat client fields as advisory. |

## Follow-up work after this plan

After the internal notification subsystem is implemented and validated, design
separate plans for:

- public gRPC streaming over the notification subsystem;
- standard GraphQL subscription syntax and execution;
- persistent external callback/webhook registration if needed;
- operator diagnostics for notification lag, gaps, and consumer failures;
- configurable retention policy surfaces.
