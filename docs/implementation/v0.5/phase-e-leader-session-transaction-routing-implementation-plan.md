# Phase E — Leader, Session, and Transaction Routing Implementation Plan

## Status

Complete for V1. Phase D is complete for the initial raft command ownership scope. Phase E now makes session- and transaction-scoped client workflows safe when requests enter through different ready pods: requests route to the correct home/leader where implemented or fail closed with documented route/session errors. Phase F read-index semantics are complete for V1; Phase G divergence detection/repair remains future work.

## Goal

In raft mode, clients may connect to any ready pod without silently violating correctness. A pod that receives a request must either:

1. serve it locally because it is the correct owner/leader/home node;
2. forward it to the correct owner/leader/home node and return the result; or
3. fail closed with a documented retryable route/session error.

The main Phase E gap is daemon-owned graph sessions and transactions. Today in-flight graph session/transaction state is process-local. A load-balanced request path can therefore hit a different pod than the one that opened the session or transaction.

## Non-goals

- Do not implement the full Phase F read consistency model here. Phase E may add leader routing for reads, but read-index/linearizable-read semantics remain Phase F unless needed to avoid stale local fallback.
- Do not make in-flight read-write transaction overlays durable or replicated in V1. V1 may keep them home-node local and fail them on home-node loss.
- Do not implement divergence detection/repair tooling; that is Phase G.
- Do not move daemon API adapters out of `internal/daemon/api`.
- Do not commit generated API/SDK code unless explicitly approved.

## Key decisions for V1

1. **Session and transaction home-node routing, not replicated overlays.**
   - Graph session and in-flight transaction lifecycle state remains local to a home node for V1.
   - The session home node is the node that creates the session.
   - Transactions inherit the session home node.
   - Any node receiving a session/transaction/graph/query/import-export request routes it to the home node.

2. **Committed durable state remains raft-owned.**
   - Phase E routes in-flight workflow state; it does not change Phase D ownership of committed records.
   - `CommitTransaction` still converts the overlay into graph raft commands and succeeds only after the owning raft group commits/applies.

3. **Home-node loss semantics are explicit.**
   - If the home node is unavailable, active sessions/transactions return a retryable `Unavailable` or documented session-lost `FailedPrecondition`.
   - Committed state remains safe and can be read/opened in a new session after recovery.

4. **Routing is daemon-internal.**
   - Existing public client API messages can remain additive-only if route metadata is exposed later.
   - Initial routing can use server-side registries and backend RPCs.

5. **No stale local fallback.**
   - If routing metadata is missing, conflicting, stale, or points to an unreachable node, raft mode must fail closed rather than using local session/transaction maps.

## Current architecture summary

- `internal/session/service.Module` owns in-memory maps:
  - `sessions map[string]GraphSession`
  - `transactions map[string]GraphTransaction`
  - local revision tracking and commit records.
- `internal/daemon/api/client.SessionService` directly calls the local session subsystem.
- `internal/daemon/api/client.TransactionService` directly calls the local session subsystem and graph subsystem.
- `internal/daemon/api/client.GraphService`, query, import/export, and metadata catalog resolve `transaction_id` locally via the session subsystem before calling graph/query logic.
- Raft-mode graph writes already fail closed unless executed on the owning partition leader; Phase E must combine that with session/transaction home routing.
- Backend internode RPCs already exist for raft messages, blob payloads, space reads, graph reads, and semantic reads; Phase E should extend the backend service with session/transaction forwarding RPCs.

## Phase E0 — Routing model and invariants

### Status

Implemented. `internal/clustering/routing` now defines route decisions, canonical route errors with gRPC status mapping, forwarding-loop metadata guardrails, and tests. `internal/runtime` exposes `LocalRouteIdentityProvider`, implemented by daemon runtime for API adapters and subsystems that need local route identity.

### Tasks

- Define a small routing model under clustering/session runtime code:
  - `HomeNodeID` / raft node ID;
  - backend address;
  - route generation/epoch if needed;
  - reason codes for `local`, `forwarded`, `unavailable`, `unknown_home`, `home_mismatch`.
- Add a daemon-level local node identity accessor usable by API adapters and subsystems.
- Define canonical errors:
  - `ErrRouteUnavailable` -> gRPC `Unavailable`;
  - `ErrUnknownSessionHome` -> `NotFound` or `Unavailable` depending leakage policy;
  - `ErrSessionHomeMismatch` -> `FailedPrecondition`;
  - `ErrForwardingLoop` -> `Internal`/`FailedPrecondition`.
- Add guardrails preventing forwarding loops, e.g. backend forwarded requests carry `x-mycel-forwarded-from` / `x-mycel-route-depth` metadata.

### Acceptance

- Routing invariants are documented in code and tests.
- No Phase E path can recursively forward indefinitely.

## Phase E1 — Session home-node registry

### Status

Implemented. The session subsystem now records local session and transaction route records, exposes route diagnostics, encodes raft-mode home node IDs into new session/transaction IDs (`s.<node>.<uuid>` / `tx.<node>.<uuid>`), preserves UUID-compatible IDs outside raft mode, updates route state on heartbeat/close/commit/rollback/expiry, and rejects remote/legacy IDs in raft mode instead of silently using local maps.

### Tasks

- Add a `SessionRouteRegistry` owned by the session subsystem or daemon runtime.
- On `OpenSession`, assign and record `session_id -> home_node_id`.
- On `BeginTransaction`, record `transaction_id -> session_id/home_node_id`.
- On `CloseSession`, rollback/close active transactions and remove/mark route records.
- On transaction terminal states, remove/mark `transaction_id` route record.
- Make route records observable in local diagnostics without exposing secrets.

### Storage options

V1 preferred:

- session/transaction IDs include an encoded home-node prefix, e.g. `s.<node>.<uuid>` / `tx.<node>.<uuid>`, while retaining UUID compatibility only if API constraints require it;
- local registry confirms decoded home route and current state.

Alternative:

- system-raft lightweight route directory for session home mappings. This improves arbitrary-node lookup but adds write traffic for ephemeral state. Only use if encoded IDs are not viable.

### Acceptance

- A node can determine a home-node candidate from `session_id` or `transaction_id` without a local map.
- Missing local state on the home node does not silently recreate or accept a session.

## Phase E2 — Backend forwarding RPCs

### Status

Implemented substrate. The internal cluster backend protocol now has `ForwardClientRequest`, a generic daemon-internal forwarding envelope for session/transaction/graph/query/import-export/metadata operations. `internal/clustering/backend` includes client/server helpers, principal metadata envelopes, payload-type/request-id preservation, route-depth metadata enforcement, and handler registration via `WithClientRequestForwarder`. Public API adapter routing remains Phase E3.

### Tasks

Add cluster backend RPC handling for internal forwarding. Keep this in `internal/clustering/backend` and daemon backend service wiring; public daemon API adapters remain in `internal/daemon/api`.

Minimum forwarded operations:

- Session:
  - open session (optional; can always create locally on ingress);
  - get session;
  - heartbeat session;
  - close session.
- Transaction:
  - begin transaction;
  - get transaction;
  - commit transaction;
  - rollback transaction;
  - close transaction.
- Graph/query by transaction:
  - graph node/edge mutations and reads that require transaction overlay;
  - query execution by `transaction_id`;
  - import/export/metadata catalog methods that depend on transaction-local state.
- Streaming operations:
  - `CreateBlobNode` either routes the full stream to the home node or fails closed when not on home node in V1.

### Implementation guidance

- Prefer forwarding the original protobuf payload to the home node backend service to avoid duplicating API mapping logic.
- Backend forwarded handlers should call existing `internal/daemon/api/client` service methods with authenticated principal context reconstructed from verified internal request metadata.
- Backend auth token remains mandatory in multi-node raft mode.
- Include request IDs/idempotency keys where available.

### Acceptance

- Backend forwarding preserves authorization principal, request metadata needed for idempotency, and deadlines/cancellation.
- Forwarded requests are rejected if backend auth or cluster ID does not match.

## Phase E3 — API adapter routing wrappers

### Status

Implemented for unary session/transaction/graph/query/metadata-catalog paths. Client API services now accept a `ClientRequestRouter`, forward remote-home session/transaction requests through the Phase E2 backend envelope, and otherwise execute locally. Streaming `CreateBlobNode` and import/export currently fail closed for remote-home transaction IDs instead of buffering/forwarding streams. Server composition wires a shared backend router and backend forwarded-request handler without moving API adapters out of `internal/daemon/api`.

### Tasks

- Add routing wrappers around client API services in `internal/daemon/api/client`:
  - `SessionService` routes by `session_id` for get/heartbeat/close.
  - `TransactionService` routes by `session_id` for begin and by `transaction_id` for get/commit/rollback/close.
  - `GraphService` routes every method by `transaction_id` before resolving the transaction locally.
  - `QueryService`, import/export, and metadata catalog route by `transaction_id` where applicable.
- Keep direct local behavior in standalone mode.
- In raft mode, if route cannot be established, fail closed.

### Acceptance

- `OpenSession` on pod A and `BeginTransaction` on pod B route safely to A or create with an explicit home model.
- Graph/query methods with a transaction opened elsewhere no longer return local `transaction not found` unless the routed home node says so.

## Phase E4 — Partition leader write routing

### Status

Implemented V1 guardrail. Read-write graph transactions now require the session home node to also be the local graph partition leader before `BeginTransaction` succeeds. If the session home is not the partition leader, the request fails closed with `Unavailable` instead of allowing staging/validation on an unsafe node and failing later. Graph commit now rechecks local partition leadership before cloning/deleting overlays or proposing raft commands, so leader changes during active transactions fail safely and preserve the overlay for explicit retry/rollback handling. Read-only transactions do not require write-leader ownership. Non-session subsystem writes remain Phase D fail-closed at their subsystem raft proposal paths rather than generic-forwarded in E4 V1.

### Tasks

- Audit all write paths that currently require the local node to be the partition leader:
  - graph transaction commit;
  - graph mutation staging if it validates against committed state;
  - blob metadata operations;
  - schema writes;
  - space/domain writes;
  - semantic partition writes.
- Decide whether Phase E implements generic forward-to-partition-leader for each, or leaves non-session subsystem writes as Phase D fail-closed with documented retry.
- For graph transaction V1, choose one of:
  1. session home node must also be partition leader for write transactions; otherwise `BeginTransaction(read-write)` routes/fails; or
  2. home node forwards commit to partition leader with overlay payload; or
  3. ingress routes write session creation to current partition leader.

Recommended V1:

- Read-write transaction home should be the current partition leader for the transaction's space.
- `OpenSession` may occur anywhere, but `BeginTransaction(read-write)` routes to the partition leader and pins the transaction home there.
- Read-only transactions can be home-node routed initially and remain subject to Phase F read guarantees.

### Acceptance

- A read-write transaction cannot stage/validate graph mutations on a node that cannot safely commit them.
- Leader changes during an active read-write transaction produce a clear retry/session-lost error rather than local fallback.

## Phase E5 — Read/query routing boundary

### Status

Implemented V1 boundary. Transaction-scoped graph/query/metadata reads route to the transaction home through E3 wrappers. Read-only/committed graph reads on non-leaders route to the partition leader via raft graph read forwarding, while read-write transaction reads require the local node to remain the graph partition leader so staged overlays are never bypassed by forwarding to another node. Committed blob reference counts now use the same leader-read forwarding path. Phase F now layers formal read-index/linearizable read semantics onto these routes.

### Tasks

- For transaction-overlay reads, route to transaction home.
- For committed graph reads outside write overlays, route to partition leader or fail closed.
- Ensure query execution by transaction ID runs where the transaction overlay exists.
- Document that linearizable/read-index details are owned by Phase F and now complete for V1.

### Acceptance

- Reads inside a transaction see that transaction's staged overlay when routed through any ingress pod.
- No graph/query read validates against stale local state when a safer route is required.

## Phase E6 — Observability and admin diagnostics

### Status

Implemented local diagnostics. Session route diagnostics now split local/remote route counts and active local/remote sessions/transactions. The client request router tracks forwarding attempts, successes, failures, local decisions, unknown-home/home-mismatch/unavailable/loop failures, and sanitized last-failure context. The backend forwarded-request handler tracks received/dispatched/failure counts, cluster rejections, route-loop rejections, and sanitized last-failure context. Public admin proto exposure is deferred until the next additive API tranche so generated public API code remains unchanged.

### Tasks

- Add local diagnostics:
  - active sessions by home/local count;
  - active transactions by home/local count;
  - forwarding attempts/success/failure counters;
  - last route failure with sanitized context;
  - route depth/loop rejections.
- Extend admin cluster/raft diagnostics if appropriate.
- Add structured logs for route decisions and failures without leaking tokens or payloads.

### Acceptance

- Operators can distinguish no leader, unknown home node, home node unreachable, backend auth failure, and transaction lost.

## Phase E7 — Tests

### Status

Implemented focused E7 coverage. Added in-process multi-node client-routing tests for cross-node session lifecycle, cross-node transaction/graph overlay workflow, home-node unreachable fail-closed behavior with new local session creation elsewhere, backend auth mismatch rejection, and leader-change/active read-write transaction commit safety. Added `make test-phase-e` as the focused Phase E test target. Destructive compose/K3s gates remain deferred until E8/release-gate hardening.

### Unit tests

- session ID / transaction ID home-node encoding/decoding;
- route registry lifecycle;
- route-loop rejection;
- route error mapping;
- standalone mode remains local.

### In-process multi-node tests

- `OpenSession` on node A; `GetSession`, heartbeat, and close via node B/C.
- `OpenSession` on A; `BeginTransaction` on B; graph create/update via C; commit via A or C.
- Transaction-overlay read via non-home node returns staged data by forwarding.
- Home node stopped/unreachable: active transaction fails with documented error; new session can be opened elsewhere.
- Leader change during read-write transaction: commit fails safely or routes according to V1 semantics.
- Backend auth mismatch rejects forwarding.

### Destructive/integration gates

- Focused in-process tests are stable and covered by `make test-phase-e`.
- `test-phase-e` is included in `test-cluster-release-gate`.
- Compose/K3s destructive validations should still be rerun before publishing a clustering-capable image; broader load-balanced workflow validation remains part of release hardening.

### Acceptance

- Cross-pod session/transaction workflows either succeed through routing or fail with expected route/session errors.
- No test path observes local `transaction not found` for a valid remote-home transaction unless the home node says it is not found.

## Phase E8 — Documentation and release gates

### Status

Implemented. Updated the reliability design and raft operations guide with Phase E V1 routing behavior, retryable errors, home-node loss semantics, the handoff to Phase F read consistency, and the focused `test-phase-e` release gate. Public API/proto documentation did not require updates because Phase E changes daemon-internal routing behavior and uses existing public session/transaction IDs and RPCs.

### Tasks

- Update `docs/design/clustering-replication-reliability.md` Phase E status.
- Update `docs/operations/raft-cluster-operations.md` with:
  - routing behavior;
  - retryable errors;
  - home-node loss semantics;
  - handoff to Phase F read consistency.
- Update API docs for session/transaction behavior if public semantics change.
- Add `docs/implementation/phase-e-*` progress notes as tranches complete.

### Acceptance

- Operators and SDK users know whether arbitrary ready-pod routing is supported, partially supported, or still constrained.

## Suggested implementation order

1. E0 routing invariants/errors.
2. E1 session/transaction home registry and ID/home-node strategy.
3. E2 backend forwarding RPC substrate.
4. E3 API adapter routing for session/transaction lifecycle.
5. E4 graph write transaction routing/pinning.
6. E5 query/graph overlay read routing.
7. E6 diagnostics.
8. E7 tests and `make test-phase-e`.
9. E8 docs/release gates.

## Definition of done

Phase E is complete when:

- session and transaction operations are safe through any ready pod in raft mode;
- graph/query operations using `transaction_id` route to the transaction home node or fail closed;
- read-write transactions cannot stage/validate/commit on an unsafe node;
- home-node loss behavior is explicit and tested;
- route failures are diagnosable;
- focused Phase E tests and docs exist.

## Residual risks after Phase E

- Phase F is complete for the V1 formal read-index/linearizable read consistency model.
- Phase G is still required for divergence detection and repair tooling.
- V1 home-node local transaction overlays mean active transactions are not failover-transparent.
