# Phase F — Read Consistency Model Implementation Plan

## Status

Complete for the current V1 scope. Phase E V1 routes session- and transaction-scoped unary requests to the correct home node or fails closed, and graph read paths can route committed/read-only reads to the current partition leader. Phase F defines and enforces the actual read guarantees behind those routes. F0 is complete: the read-consistency contract and read-path inventory are documented in `phase-f-read-consistency-inventory.md`, with matching code comments in the raft graph read path. F1 is complete: `consensus.Group` exposes a leader-only `LinearizableRead`/`WaitApplied` barrier backed by raft `ReadIndex`, plus local read-index diagnostics and focused tests. F2 is complete: graph committed/read-only reads and transaction base-revision lookup now use the consensus read barrier on the local partition leader, while read-write overlay reads remain home/leader local. F3 is complete: V1 read-only transactions are explicitly linearizable current-read contexts, not historical repeatable snapshots. F4 is complete: query and metadata catalog reads are verified to use routed graph manager reads, and read-write GQL reads preserve overlay read-your-writes. F5 is complete: read metadata is exposed on graph/query/metadata responses and threaded through internal graph read recording plus forwarded raft read envelopes. F6 is complete: `ReadOptions.allow_stale` is additive on graph/query/metadata read requests, but stale reads are rejected by default because no daemon stale-read config/implementation is enabled. F7 is complete: raft group read-index/apply-wait diagnostics are exposed through admin status/CLI output and structured logs record read barrier failures and slow apply waits without request payloads. F8 is complete: focused Phase F tests are bundled under `make test-phase-f` and included in the cluster release gate.

## Goal

In raft mode, a client must not observe committed graph state through one pod that another pod cannot safely validate for mutation. Reads must be either:

1. **strong/linearizable** by default, using raft leader/read-index semantics and waiting for local apply before serving; or
2. **explicitly stale/local** only when an API/config opt-in is added and the response is labeled as such.

Phase F should make the default client behavior safe for arbitrary ready-pod ingress while preserving Phase E's home-node routing and fail-closed behavior.

## Non-goals

- Do not implement Phase G divergence detection/repair tooling here.
- Do not make in-flight transaction overlays replicated or failover-transparent; Phase E V1 home-node semantics remain.
- Do not introduce generated public SDK/API code unless Reacticity explicitly approves the public proto change and generated-code update.
- Do not use Kubernetes service affinity as a consistency mechanism.
- Do not weaken Phase D ownership/fail-closed rules for non-session subsystem writes.

## Definitions

- **Committed read**: a read outside a read-write transaction overlay, or a read-only transaction read, that observes committed graph state.
- **Strong read / linearizable read**: a read served only after the owning raft group has confirmed a current read index and the serving state machine has applied at least that index.
- **Read index**: raft-provided committed index safe point returned by `raft.Node.ReadIndex`.
- **Applied index**: highest committed raft index locally applied to the graph state machine.
- **Observed graph revision**: Mycel graph/domain revision visible to the read after the read-index barrier.
- **Stale read**: a follower/local read that may lag the current leader and must be explicitly requested/labeled.

## V1 outcomes

- `internal/clustering/consensus.Group` tracks leader, commit index, and applied index, and exposes a leader-only `LinearizableRead`/`WaitApplied` read-index barrier API.
- `internal/graph/service/raft_read.go` forwards committed/read-only reads to the partition leader and the leader serves committed state only after the raft read-index/apply barrier succeeds.
- Read-only transactions carry `base_revision`, but the graph store remains latest-state oriented rather than a full historical MVCC snapshot store; V1 therefore documents read-only transactions as linearizable current-read contexts.
- Public graph/query/metadata responses expose additive read consistency metadata such as observed revision, read index, leader node, and strong/overlay mode where a proof/context exists.
- Diagnostics expose read-index attempts/failures/timeouts and applied-index wait behavior through admin status, CLI output, and structured logs.

## Key decisions for V1

1. **Strong reads are the default in raft mode.**
   - Committed graph reads must route to the partition leader and pass a read-index barrier.
   - If the leader is unknown, unreachable, not current, or cannot apply through the read index before deadline, the read fails closed with `Unavailable` or `DeadlineExceeded`.

2. **Follower stale reads are not enabled by default.**
   - Any stale/local read mode must be explicit and labeled. If public API changes are not approved in this phase, keep stale reads internal/disabled.

3. **Read-write transaction read-your-writes remains home-node/overlay local.**
   - Phase E already routes read-write transaction operations to the home node.
   - Phase F must keep enforcing that the home node is the partition leader before serving read-write overlay reads, so reads include staged overlay changes and do not forward away from the overlay.

4. **Read-only transaction snapshot semantics must be explicit.**
   - Minimum V1: `BeginTransaction(read-only)` records a strong observed revision after a read-index barrier, and all committed reads in that transaction must be at least that safe point.
   - If exact historical snapshot reads are required by public semantics, add MVCC/snapshot support before claiming repeatable-read snapshot isolation. Otherwise update API docs to state read-only transactions are linearizable/current-read contexts, not historical repeatable-read snapshots.

5. **Revision/read metadata is additive.**
   - Prefer internal metadata first.
   - Public proto additions, if approved, should use additive fields such as `ReadMetadata read_metadata = N` on graph/query responses and transaction objects. Do not commit generated SDK/API code unless explicitly approved.

## Phase F0 — Consistency contract and inventory

### Status

Implemented. See `phase-f-read-consistency-inventory.md` for the contract, terminology, current architecture gaps, graph/query/backend read-path classifications, out-of-scope reads, guardrails, and F3 open decisions.

### Tasks

- Write the exact raft-mode read contract in code comments and docs:
  - strong read default;
  - fail-closed behavior;
  - stale read opt-in requirements;
  - read-write transaction read-your-writes guarantee;
  - read-only transaction semantics and limitations.
- Inventory every graph/read path:
  - GraphService unary reads;
  - QueryService structured/GQL reads;
  - MetadataCatalog reads over graph metadata;
  - import/export reads;
  - blob reference count reads;
  - internal backend graph reads;
  - transaction base revision lookup.
- Classify each path as:
  - strong committed read;
  - read-write transaction overlay read;
  - explicitly stale/derived read;
  - non-graph subsystem read deferred to a later phase.

### Acceptance

- There is a checked-in inventory table and no unclassified graph/query read path in raft mode.
- The terms `strong`, `stale`, `read-index`, `observed_revision`, and `read-write overlay` are defined once and reused.

## Phase F1 — Consensus read-index barrier

### Status

Implemented. `internal/clustering/consensus.Group` now has `LinearizableRead`, `WaitApplied`, `ReadBarrierResult`, and local `ReadDiagnostics`. The implementation registers unique read contexts, calls `raft.Node.ReadIndex`, completes waiters from `Ready.ReadStates`, waits for `appliedIndex >= read_index`, rejects no-leader/non-leader local reads, cleans waiters on cancellation/leadership loss, and records attempts/success/failure/timeout/no-leader/not-leader/apply-wait diagnostics.

### Tasks

- Add a read barrier API to `internal/clustering/consensus.Group`, for example:

  ```go
  type ReadBarrierResult struct {
      Index uint64
      Term  uint64
  }

  func (g *Group) LinearizableRead(ctx context.Context) (ReadBarrierResult, error)
  func (g *Group) WaitApplied(ctx context.Context, index uint64) error
  ```

- Implement using `raft.Node.ReadIndex` and `Ready.ReadStates`:
  - generate unique read contexts;
  - track read-index waiters;
  - complete waiters when matching `ReadState` arrives;
  - wait until `appliedIndex >= readState.Index` before returning;
  - remove waiters on context cancellation, leadership loss, group stop, or timeout.
- Ensure the barrier is only considered valid for the current leader term/leader view.
- Add counters/diagnostics:
  - read-index attempts;
  - successes;
  - timeouts/cancellations;
  - no-leader failures;
  - apply-wait timeouts;
  - last sanitized failure.

### Acceptance

- Unit tests prove:
  - leader read-index succeeds after quorum;
  - follower/local non-leader use fails or routes rather than serving directly;
  - no-leader read-index fails closed;
  - context cancellation cleans waiters;
  - reads wait for applied index before serving.

## Phase F2 — Graph strong-read integration

### Status

Implemented. `internal/graph/service` now has `StrongReadContext` and calls the F1 consensus read barrier for local-leader committed/read-only graph reads before reading local graph storage. This covers `CurrentRevision`, `GetNode`, `ListNodes`, `GetEdge`, `ListEdges`, `BlobRefCount`, and backend `ExecuteLocalRaftGraphRead` dispatch. Hierarchy reads (`ListChildren`, `GetParent`) inherit the barrier through `ListEdges`. Remote raft graph read envelopes now carry internal strong-read metadata in JSON responses. Read-write transaction reads still bypass the read-index barrier intentionally and require the transaction home node to remain the local partition leader so overlays are never bypassed.

### Tasks

- Add a graph read barrier helper in `internal/graph/service`, for example:

  ```go
  type StrongReadContext struct {
      GroupID          consensus.GroupID
      PartitionID      uint32
      LeaderNodeID     consensus.NodeID
      ReadIndex        uint64
      AppliedIndex     uint64
      ObservedRevision int64
      Strong           bool
  }
  ```

- Update committed/read-only graph read paths to:
  1. resolve partition group;
  2. route to leader if not local leader;
  3. on leader, call `LinearizableRead`;
  4. wait for apply through returned read index;
  5. read from graph store;
  6. return internal read metadata.
- Apply to:
  - `CurrentRevision` / transaction base revision lookup;
  - `GetNode`, `ListNodes`;
  - `GetEdge`, `ListEdges`;
  - hierarchy reads (`ListChildren`, `GetParent`);
  - `BlobRefCount`;
  - backend `ExecuteRaftGraphRead` envelope.
- Ensure remote graph read responses carry internal read metadata back to the ingress node.
- Preserve Phase E behavior:
  - read-write transactions do not forward away from the home/leader;
  - read-write overlay reads include staged changes;
  - leadership changes fail closed.

### Acceptance

- A committed graph read cannot be served from a raft-mode leader until a read-index barrier has completed and local apply has reached the returned index.
- A non-leader never serves an authoritative committed graph read directly.
- Transaction base revisions are taken after a strong read barrier.

## Phase F3 — Read-only transaction revision semantics

### Status

Implemented. V1 chooses **Option A — Linearizable current-read transaction**. `BeginTransaction(read-only)` records the current graph revision returned by `CurrentRevision`, which in raft mode is a strong read-index/apply-barrier read after F2. Each read-only graph read performs a fresh strong read barrier and reads latest committed graph state, so it may observe revisions newer than `base_revision`. The graph module now fails closed if a read-only transaction's `base_revision` is ahead of the observed local graph revision. The current store is latest-state only; Mycel does not claim repeatable historical snapshot isolation until a future MVCC/snapshot store exists.

### Tasks

- Decide and implement one of the following explicit V1 models:

  **Option A — Linearizable current-read transaction (smaller V1): implemented.**
  - `BeginTransaction(read-only)` records the strong observed revision at creation.
  - Each read-only transaction read performs/uses a strong read barrier and may observe newer committed revisions than `base_revision`.
  - Public docs must stop calling this a repeatable historical snapshot.

  **Option B — Repeatable snapshot transaction (larger V1):**
  - Add graph-store snapshot/MVCC support keyed by domain/space revision.
  - `BeginTransaction(read-only)` pins `base_revision` after a strong read barrier.
  - All reads in the transaction use the pinned revision and fail if that revision is compacted/unavailable.

- Recommended sequence:
  1. implement Option A first if current product callers only require freshness/safety;
  2. add Option B only if repeatable-read snapshots are required by API contract or SDK callers.
- Add tests that prove the chosen semantics and prevent accidental unstated behavior.

### Acceptance

Implemented.

- Read-only transaction behavior is explicit, tested, and documented.
- Snapshot isolation is not implemented; docs and API comments now describe read-only transactions as linearizable current-read contexts rather than repeatable historical snapshots.

## Phase F4 — Query and metadata read consistency

### Status

Implemented. `QueryService` structured queries and GQL execution read through the graph manager (`ListNodes`/`ListEdges`) rather than directly opening stores, so raft-mode read-only/committed query paths inherit F2 leader routing and read-index/apply barriers. `MetadataCatalogService` uses the same graph-manager enumeration path through `allExportNodes`, preserving strong committed reads for read-only transactions and staged-overlay visibility for read-write transactions. Read-write GQL still requires a read-write transaction for mutating plans and reads staged overlay writes through the transaction home. Focused tests cover non-home ingress query forwarding to a partition leader, metadata catalog barrier inheritance, read-write GQL overlay read-your-writes, and no-leader query fail-closed behavior.

### Tasks

- Ensure QueryService paths inherit graph strong-read behavior through the graph manager:
  - structured `ExecuteQuery`;
  - `ExecuteGQL` read-only statements;
  - `ExecuteGQLScript` read portions.
- Ensure read-write GQL continues to run only in read-write transactions on the transaction home/leader.
- Ensure MetadataCatalog reads that inspect graph nodes/edges use the transaction's routed graph read path and receive the same strong/overlay behavior.
- Add focused tests where:
  - query through non-home/non-leader ingress routes to the home/leader and sees a just-committed node;
  - read-write GQL read after write sees staged overlay;
  - committed query during no-leader fails closed.

### Acceptance

Implemented.

- Query/metadata reads cannot bypass graph read barriers in raft mode.
- Read-write transaction query paths provide read-your-writes.

## Phase F5 — Read metadata exposure

### Status

Implemented. Added internal `graph/service.ReadMetadata` with consistency mode, raft group/leader/read-index/applied-index/observed-revision fields, stale flags, and a context-scoped recorder used by graph, query, and metadata services. Raft strong reads record metadata only after a read-index/apply proof exists; read-write transaction reads record `overlay`; stale remains unavailable. Public additive proto fields were added to graph/query/metadata responses in `mycel-api`, and daemon internal stubs were regenerated locally under `mycel/internal/gen`. Forwarded raft graph read envelopes already carry strong-read context and now record that metadata on the ingress response path.

### Tasks

- Define internal read metadata structs first:
  - consistency mode: `strong`, `stale`, `overlay`;
  - raft group ID;
  - leader node ID;
  - read index;
  - applied index;
  - observed revision;
  - stale flag/reason when applicable.
- Thread metadata through internal graph/query results and forwarded read envelopes.
- If public exposure is approved, add additive proto fields in `mycel-api`, for example:

  ```proto
  message ReadMetadata {
    string consistency = 1;
    string raft_group_id = 2;
    uint64 leader_node_id = 3;
    uint64 read_index = 4;
    uint64 applied_index = 5;
    int64 observed_revision = 6;
    bool stale = 7;
    string stale_reason = 8;
  }
  ```

  Then add `ReadMetadata read_metadata = N` to graph/query response messages where useful.
- Regenerate daemon stubs locally in `mycel` only after public proto approval.

### Acceptance

Implemented.

- Operators/developers can identify whether a read was strong, overlay, or explicitly stale.
- No response claims strong consistency without a read-index/apply proof in raft mode.

## Phase F6 — Optional stale-read opt-in

### Status

Implemented as a disabled-by-default guardrail. Public read requests now have additive `ReadOptions.allow_stale` where stale reads could be requested in the future. The daemon rejects `allow_stale=true` with `FailedPrecondition` on graph/query/metadata read APIs because there is no enabled daemon stale-read config or stale-read implementation. Default reads continue to use strong/overlay paths and never emit stale metadata. Future work can add a daemon config flag and actual local/follower read implementation, but it must require both config and request opt-in and must label responses as stale.

### Tasks

- Keep stale reads disabled unless explicitly enabled by config/API.
- If implemented, require both:
  - daemon config allowing stale reads; and
  - request-level opt-in or dedicated API path.
- Label stale responses with metadata including applied index, leader known/unknown, and staleness reason.
- Never use stale reads for:
  - edge endpoint validation;
  - read-write transaction overlay reads;
  - transaction base revision lookup;
  - schema validation used by writes;
  - authorization checks.

### Acceptance

Implemented for the default-disabled V1 scope.

- Tests prove stale reads are impossible by default.
- Stale response labeling remains reserved because no stale-read execution path exists yet; write validation paths do not accept `ReadOptions` and continue using strong/overlay behavior.

## Phase F7 — Diagnostics and admin visibility

### Status

Implemented. `consensus.Group` read diagnostics now include attempts, successes, failures, timeout/no-leader/not-leader/apply-wait counters, last failure timestamp/reason, last successful read index, last failed/successful apply-wait index, and last apply-wait duration. `ListRaftGroups` exposes these as `RaftReadDiagnostics`, and the `mycel cluster raft-groups` CLI includes the counters in JSON and concise text output. Read-index failures and apply-wait failures are logged with sanitized group/node/reason context, and slow successful apply waits log duration without request payloads or tokens. The operations runbook documents how to interpret the fields.

### Tasks

- Extend local/admin diagnostics with read-index state:
  - attempts/success/failure counts by raft group;
  - timeout counts;
  - last read failure reason;
  - last read-index and applied-index wait;
  - current applied/commit lag already available from raft group diagnostics.
- Add structured logs for read-index failures and slow apply waits without payload/token leakage.
- Document runbook interpretation in `docs/operations/raft-cluster-operations.md`.

### Acceptance

Implemented.

- Operators can distinguish:
  - no leader;
  - backend route failure;
  - read-index quorum timeout;
  - local apply lag;
  - stale-read disallowed;
  - transaction overlay/home-node loss.

## Phase F8 — Tests and release gates

### Status

Implemented. Added `make test-phase-f` to run the focused Phase F read-consistency packages with fresh test results: consensus, backend forwarding, graph service, client API, admin API, and CLI coverage. The target includes read-index barrier tests, graph strong-read tests, read-only current-read semantics, query/metadata consistency tests, read metadata assertions, stale-read rejection tests, and admin/CLI read diagnostics. `test-cluster-release-gate` now includes `test-phase-f` before destructive compose/K3s validations.

### Unit/component tests

- consensus read-index success/failure/cancellation/apply-wait behavior;
- graph committed reads wait for read index before serving;
- non-leader committed reads route/fail closed;
- no-leader committed reads fail closed;
- read-write transaction read-your-writes after staged mutations;
- read-only transaction semantics chosen in F3;
- query/metadata paths use the graph read barrier;
- optional stale reads disabled by default.

### In-process multi-node tests

- Commit on node A; read through node B/C immediately; assert read routes/barriers and returns committed state only after safe apply.
- Read through non-leader while leader is available; assert no local stale fallback.
- Leader change during committed reads; assert fail-closed or successful new-leader read-index, never stale local fallback.
- Read-write transaction: create/update through one ingress, read through another ingress, assert overlay is visible through home routing.
- Read-only transaction/query after concurrent commits; assert the documented F3 semantics.

### Release gates

Implemented for focused local gates:

- `make test-phase-f` runs the focused Phase F package suite.
- `test-phase-f` is included in `test-cluster-release-gate` before destructive compose/K3s validations.
- Destructive compose/K3s validations remain manual/pre-release checks before publishing a clustering-capable image.

### Acceptance

Implemented.

- Focused tests prove clients cannot observe a committed node through one pod that another authoritative write path cannot validate.
- `go test ./...` and `git diff --check` pass.

## Suggested implementation order

1. F0 contract/inventory.
2. F1 consensus read-index barrier.
3. F2 graph strong-read integration for committed reads and base revision lookup.
4. F3 read-only transaction semantics decision and enforcement.
5. F4 query/metadata consistency coverage.
6. F5 internal/public read metadata exposure as approved.
7. F6 optional stale-read opt-in only if needed.
8. F7 diagnostics/admin docs.
9. F8 focused tests and release-gate wiring.

## Definition of done

Phase F is complete when:

- raft-mode committed graph/query reads are strong by default;
- read-write transaction reads provide read-your-writes and never bypass overlays;
- read-only transaction semantics are explicit, implemented, and tested;
- stale reads are impossible unless explicitly enabled and labeled;
- read-index/apply-lag diagnostics are available;
- focused Phase F tests and docs exist;
- the original acceptance condition is satisfied: clients cannot observe committed graph state on one pod that another pod cannot safely validate for mutation.

## Residual risks after Phase F

- Phase G is still required for divergence detection and repair tooling.
- If F3 chooses linearizable current-read transactions rather than MVCC snapshots, repeatable historical snapshot isolation remains a future enhancement.
- Strong reads depend on healthy quorum and may increase read latency during leader changes or apply lag.
