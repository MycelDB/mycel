# Atomic inference budget enforcement

Tracking: [mycel#239](https://github.com/MycelDB/mycel/issues/239), historical [mycel-api#37](https://github.com/MycelDB/mycel-api/issues/37) planning, and Mycel Lab SGT1b.

> **Supersession note:** the client-managed public budget-scope API and its former
> protobuf/SDK delivery steps were abandoned. The internal reservation,
> accounting, replay, and ambiguity machinery remains reusable. The replacement
> operator-facing architecture is defined by
> [Procedure inference observability and control](../../design/automation/procedure-inference-observability-and-control.md)
> and its [implementation plan](procedure-inference-observability-and-control-implementation-plan.md).

## Purpose

Hosted-provider calls can outlive the client request that caused them and can be retried by background semantic maintenance. Harness-side counters therefore cannot provide hard request, retry, concurrency, token, duration, or cost limits. The daemon must durably reserve capacity before each provider side effect and reconcile it afterward.

This plan defines the reusable daemon-owned accounting semantics. It no longer proposes a public client-created budget scope. Future public contracts expose daemon-owned procedure execution evidence and controls instead.

## Safety invariants

1. A provider connector is never called for a budgeted invocation unless its attempt reservation is durably committed first.
2. Request, attempt, retry, concurrency, batch, token, duration, deadline, and cost checks happen in one serialized decision against the latest scope state.
3. Attempt identity is idempotent. Reusing an attempt identity never causes another provider side effect.
4. A crash after reservation but before completion remains charged conservatively. Replay cannot refund or duplicate the attempt.
5. Completion can reduce a conservative reservation only after a known provider outcome is durably recorded.
6. Unknown routes, missing pricing required by a cost limit, unsupported batching, expired scopes, and exhausted limits fail before connector access.
7. Provider work never runs while a graph or Raft write lock is held. Only short local accounting mutations surround the provider call.
8. Persisted records contain identifiers, bounded counters, durations, price metadata, and allowlisted denial categories only—never credentials, authorization metadata, prompts, projected text, vectors, or provider response bodies.
9. Scope, attempt, and retention cardinalities have hard implementation maxima.

## Scope model

A budget scope has a bounded operator/run identity, lifecycle timestamps, optional pinned route identity, immutable limits, accumulated usage, and a bounded set of attempt reservations.

Limits include:

- distinct requests;
- provider attempts and retry attempts;
- concurrent active provider requests;
- `single` or `batch` mode and maximum batch items;
- per-attempt and aggregate input/output token reservations;
- per-attempt and aggregate provider duration;
- absolute deadline;
- optional currency, price-definition version, route prices, and maximum estimated cost.

Zero is not silently interpreted as an enforced zero. Internal capability/status values distinguish configured limits from unavailable enforcement; any future operator-facing status must preserve that distinction.

## Attempt lifecycle

1. Resolve the inference route and policy.
2. Validate connector batching capability and construct a bounded attempt descriptor.
3. Reserve the configured per-request input-token ceiling (never a heuristic estimate), conservative output tokens, provider duration, and cost.
4. Under the budget mutation lock, reject or durably append the complete next scope state.
5. Release the lock, resolve credentials, and call the provider with a deadline no later than the reserved duration, scope deadline, or caller deadline. For generative calls, pass the exact reserved output-token ceiling to the connector.
6. Under the budget mutation lock, durably complete the reservation with allowlisted outcome, actual usage, and measured provider duration.
7. Persist ordinary usage evidence separately.

An active reservation whose connector outcome is unknown remains charged and continues to consume concurrency for the scope. An elapsed reservation deadline marks the attempt ambiguous but does not free its slot; only a durable completion does. A duplicate attempt identity is terminal and cannot re-enter the connector. Provider-reported token usage refunds token and cost reservations only for provider-reported usage or the in-process deterministic fake connector's authoritative accounting; hosted `estimated` and `unavailable` usage retain the conservative charge.

## Persistence and replay

Budget state uses the inference accounting store and a dedicated encrypted file. Mutations are full-state, idempotent records under the dedicated versioned `inference.budget.mutation.v1` WAL type. Unsupported record versions or mutation kinds fail closed. The module serializes canonical read/decide/append/apply operations so concurrent attempts cannot oversubscribe.

WAL replay upserts the same scope state. A crash after WAL sync but before local apply can consume capacity without making a provider call, which is conservative and safe. If local apply fails after a possible WAL commit, the live module poisons further budget mutations until recovery so a later full-state record cannot erase ambiguous consumption. A crash after the provider side effect but before completion leaves the attempt active/ambiguous and conservatively charged.

Budget mutations use the system Raft group when clustering is configured. Scope creation, reservation, terminal completion, lifecycle transitions, and pruning are proposed as versioned `inference.budget.mutation.v1` system commands and applied to every replica; the system snapshot includes complete scope state and the scope-sequence high-water mark. A missing system group or leader fails closed. Standalone deployments retain the local WAL path, and provider work begins only after the replicated reservation has applied.

## Batching

Current connectors accept one logical item. They advertise `single` and enforce exactly one item before request construction. A future connector may advertise `batch` with a finite maximum, but configuration cannot claim batching that the connector implementation does not support.

## Cost

Cost is integer micro-units in one explicit ISO-style currency. A versioned price definition provides input/output micro-units per one million tokens and is pinned to the resolved endpoint/model/capability/operation route. Reservation rounds upward. A required cost limit without a positive authoritative price for every token component the operation can consume fails closed; omitted prices are never interpreted as a free route.

## Retention

Scopes have explicit expiry and terminal state. The implementation caps attempts per scope and total retained scopes. Retention must extend through the absolute provider deadline. Cleanup may remove only terminal/expired scopes after retention and after every active reservation lease has elapsed; it cannot refund active/ambiguous work.

## Revised delivery relationship

1. Retain the implemented domain model, validation, atomic accounting store, WAL replay, connector integration, and ambiguity protections as internal daemon machinery.
2. Adapt the machinery to daemon-created procedure inference attempts where its invariants match; do not accept caller-created enforcement scopes from ordinary graph clients.
3. Expose typed procedure attempt history, durable cursor polling, procedure controls, and procedure-scoped endpoint authorization through the superseding design.
4. Regenerate SDK bindings only from the approved replacement source protobuf contract.
5. Reframe Mycel Lab SGT1b and later live-provider coverage around daemon-owned execution attempts and controls while preserving the Lab's independent orchestration budget.

## Acceptance tests

- Concurrent reservations cannot exceed any hard limit.
- Duplicate attempt identities never call a connector twice.
- Every retry consumes exactly one attempt and retry unit.
- Single-item connectors reject a batch before request construction.
- Deadline, token, duration, and cost exhaustion deny the next attempt before provider access.
- Missing route pricing fails closed when cost enforcement is required.
- Cancellation completes accounting with a bounded category.
- Restart and WAL replay preserve consumption and active ambiguity.
- Retention and scope/attempt capacity are bounded.
- Persisted and emitted diagnostics contain no request content, credentials, vectors, provider bodies, or raw errors.
