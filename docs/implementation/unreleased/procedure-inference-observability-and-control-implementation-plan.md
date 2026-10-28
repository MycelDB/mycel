# Procedure inference observability and control implementation plan

## Status

Core API/SDK/daemon implementation merged to `develop`; unreleased. PIO7
destructive/live acceptance remains pending explicit approval. See
[release readiness](procedure-inference-observability-control-release-readiness.md).

Design authority:
[Procedure inference observability and control](../../design/automation/procedure-inference-observability-and-control.md).

This plan replaces the proposed delivery of a client-managed public inference
budget service. The merged daemon inference-budget implementation remains
internal reusable machinery; graph clients do not create scopes for inference
hidden inside procedures.

## Outcome

Deliver an authoritative, sanitized procedure inference-attempt ledger with:

- one durable attempt identity before each possible provider side effect;
- procedure, binding, run, trigger, principal, route, authorization, usage, and
  outcome correlation;
- current-state and incremental cursor-based queries;
- explicit procedure enable/disable semantics;
- procedure-scoped endpoint denial;
- standalone and clustered recovery correctness; and
- a versioned transport-neutral change envelope delivered by polling now and
  reusable by a separately designed future message-queue adapter.

## Constraints

- Public source protobuf changes belong in `mycel-api`; generated daemon and SDK
  bindings are regenerated, never hand-edited.
- Provider work stays outside graph transactions, Raft apply, and publication
  critical sections.
- No provider call occurs unless the required pre-attempt state is durably
  committed.
- Clustered user-visible writes are Raft-owned, derived/rebuildable, or rejected.
- Unknown/stale authorization or control revisions fail closed.
- Cancellation and deadlines preserve their identity.
- Unknown provider outcomes remain ambiguous.
- Persisted and emitted evidence excludes prompts, outputs, graph values,
  vectors, credentials, authorization secrets/headers/values, free-form
  authorization metadata, provider request IDs/bodies, raw errors, and arbitrary
  metadata. Bounded policy-decision IDs, credential-grant IDs, and committed
  authorization revisions are classified correlation fields and remain allowed.
- Pagination is bounded, opaque, filter-bound, and based on examined sequence.
- `not-run`, missing history, and unsupported capability are never success.
- No Kafka, RabbitMQ, or other message-queue client, relay, configuration, or
  broker validation is implemented in this plan.
- Existing automation RPC shapes remain compatible while typed consumers
  migrate; unsafe legacy `run_json` contents are sanitized and newly gated rather
  than preserved as a security bypass.

## Current-state inventory

### Reusable implementation

- `internal/automation/model` already contains procedure, binding, invocation,
  run, workflow-step, actor, owner, on-behalf-of, event-origin, route, policy,
  token, output, and mutation identities.
- `internal/automation/service` already owns deterministic graph-triggered
  invocation IDs, Raft claims, retries, run persistence, and output fencing.
- `automation.mutation.v1` already carries procedure/binding and execution
  mutations through space-partition Raft paths.
- `internal/inference/service` already resolves profiles, endpoint/model/
  capability routes, credential grants, policies, principal context, and usage.
- Internal budget reservation code already provides attempt identity,
  conservative ambiguity, durable pre-provider accounting, replay, bounded
  outcomes, and route checks.
- Client and admin automation services already expose invocation listing, run
  retrieval, retry/cancel, binding enable/disable, and procedure CRUD.

### Required corrections

- Current automation execution opens a graph transaction before inference and
  can retain it across provider access. Refactor the read/input phase so no graph
  transaction remains open during the provider call; reopen/revalidate for
  publication.
- Procedure and binding definitions are overwritten by ID today; updates do not
  enforce monotonic immutable versions, bindings do not prove a procedure-version
  match, and queued execution resolves latest-by-ID procedure/binding content
  instead of its pinned execution configuration.
- Run IDs are currently allocated randomly inside execution and run state is
  persisted afterward, so reclaim after a crash can allocate a new run/attempt
  identity.
- Procedure status lacks explicit enable/disable RPCs, cross-partition control
  fencing, and publication fencing.
- Inference resolve context lacks typed procedure/binding/run identity.
- Policy/grant scopes cannot select procedures or endpoint-deny dimensions.
- Current usage telemetry is node-local in clustered mode and cannot prove a
  complete procedure history.
- Invocation listing and inference usage use limit/offset behavior unsuitable
  for incremental collection.
- Raw run JSON and free-form inference metadata are not a safe stable public
  collection contract.

## Delivery strategy

Use small functional tranches. Each tranche must leave existing automation
execution operational and must not expose a public claim that the daemon cannot
prove.

## PIO0 — Freeze contracts and failure semantics

### Work

1. Finalize attempt states and allowed transitions.
2. Define deterministic run, attempt, and change-event identity plus recovery
   reuse rules.
3. Define one-space sequence scope, mandatory-space cursor binding, high-water,
   earliest-retained, and gap behavior.
4. Freeze the cross-partition procedure-control epoch/barrier protocol and the
   endpoint-revoke linearization point.
5. Define the safe public field allowlist, legacy run migration, and
   prohibited-field tests.
6. Decide seven-day defaults plus separate hard age/count/byte bounds for
   materialized run/attempt state and change history.
7. Define capability names for attempt read and control operations.
8. Amend Mycel Lab planning so canceled client-managed budget API prerequisites
   are superseded without rewriting frozen historical evidence.

### Acceptance

- Design review has no unresolved P0/P1 questions about authority, crash
  ambiguity, pagination, revocation, disable, or redaction.
- No source protobuf or daemon behavior is changed in this tranche.

## PIO1 — Automation attempt domain and persistence

### Work

1. Add immutable procedure definitions keyed by `(procedure_id, version)` or
   canonical definition hash and immutable binding execution configurations
   keyed by `(binding_id, version)` or canonical snapshots. Enforce monotonic
   updates, exact procedure/binding-version matching, and invocation pinning and
   resolution against both definitions rather than latest-by-ID content. Retain
   pinned deleted versions through execution retention or fail queued work with
   a fixed unavailable-definition category.
2. Add `ProcedureInferenceAttempt` and `ProcedureInferenceAttemptChange` to
   `internal/automation/model`.
3. Add a closed transition validator and allowlisted outcome/denial categories.
4. Commit a deterministic run ID derived from invocation and logical run-attempt
   number, or an equivalent durable allocation, before execution. Reclaim must
   reuse this allocation.
5. Extend automation storage with get/list/upsert attempt and append/list change
   operations.
6. Add per-space monotonic sequence state plus separate change-history and
   materialized-state retention watermarks.
7. Extend `automation.mutation.v1` canonical mutation kinds and apply paths for
   run allocation, attempt creation/transition, and referentially safe retention
   pruning.
8. Include immutable definitions, run allocations, attempts, changes, sequence
   state, and retention watermarks in automation snapshot/restore.
9. Add record-coverage classification and backup/quiesce writer inventory.
10. Keep standalone writes under the existing quiesce/local-write gates.

### Tests

- valid and invalid state transitions;
- idempotent duplicate transition replay;
- monotonic immutable procedure and binding versions plus exact invocation
  pinning;
- update/delete/restart never substitutes latest-by-ID procedure or binding
  content for a queued invocation;
- deterministic run/attempt/change IDs and reclaim reuse;
- monotonic sequence under concurrent transitions;
- restart/WAL replay;
- Raft apply and snapshot restore;
- bounded count/byte/age retention for both state and changes;
- referential pruning order and separate earliest-retained watermarks;
- capacity exhaustion rejects new provider work before connector access;
- serialization contains none of the prohibited fields.

### Acceptance

Attempt state is authoritative and restart-safe without any provider integration
or public API claim.

## PIO2 — Provider execution integration

### Work

1. Resolve the immutable procedure definition, pinned binding execution
   configuration, and precommitted/recovered run allocation before execution,
   then carry typed procedure, binding, invocation,
   run, workflow-step, and principal identity through the automation-to-inference
   port.
2. Split or wrap inference execution so route/policy preparation can complete
   before connector access while secret values remain inside inference.
3. Commit requested attempt state before resolution whose failure must be
   observable.
4. Commit authorized/started state with configured route, policy decision,
   grant, and authorization revision before credential resolution or connector
   access.
5. Invoke the provider outside graph and Raft critical sections.
6. Commit terminal or ambiguous state before output publication.
7. Reuse internal budget attempt/accounting primitives where their invariants
   match, but derive identities and scopes inside the daemon rather than from a
   public caller.
8. Attach attempt IDs to the already durable run state. Reclaim looks up the
   existing run/step/provider ordinal; a `started` attempt becomes ambiguous and
   is never replaced by a new run or provider-attempt identity.
9. Map all persisted outcomes to closed sanitized categories.
10. Refactor graph input/output execution so no graph transaction remains open
    during provider access. Re-read and revalidate required graph state before
    publication.

### Tests

- no connector hit when requested/authorized transition cannot commit;
- credential resolution occurs only after the durable authorization transition;
- one durable attempt per provider retry;
- duplicate attempt ID never calls the connector twice;
- cancellation and deadline identity;
- crash after requested, authorized, and possible provider side effect;
- crash after provider access but before final run persistence, followed by
  reclaim, does not allocate a new run/attempt or repeat the side effect;
- unknown outcome becomes ambiguous;
- terminal evidence failure blocks publication;
- no graph or Raft lock held during provider work;
- deterministic fake usage is authoritative while hosted missing usage remains
  unavailable/ambiguous as appropriate;
- no prompt, output, vector, credential, provider ID/body, or raw error in
  persisted records.

### Acceptance

Every automation provider side effect has authoritative attempt evidence, or the
provider is not called.

## PIO3 — Procedure and binding status fences

### Work

1. Add manager operations for idempotent procedure enable/disable with controlling
   principal, monotonically increasing control epoch, affected execution
   partition set, acknowledgement state, and barrier completion revision.
2. Add explicit client/admin `EnableGraphProcedure` and
   `DisableGraphProcedure` source protobuf RPCs in `mycel-api`.
3. After binding regeneration, implement the client/admin daemon handlers with
   scoped authorization, leader routing, idempotency, and barrier-result mapping.
4. Propagate the procedure-control epoch from the domain configuration authority
   into every affected space execution partition. Resume incomplete fanout after
   restart and fail closed when affected partitions cannot be enumerated.
5. Define barrier completion, not the initial domain commit, as the externally
   visible disable linearization point. Serialize binding creation/update with
   procedure control state.
6. Reject attempt-start and output-publication mutations whose control epoch is
   older than the local applied fence.
7. Treat a committed `started` transition as an in-flight call that may enter or
   finish the provider after disable begins; no later `started` transition may
   commit after barrier completion.
8. Prevent new invocations after disable and convert pending/retryable
   invocations to a fixed disabled disposition.
9. Record disable/enable control changes in the attempt/execution change feed or
   a compatible safe control envelope.
10. Preserve existing binding enable/disable behavior as the narrower control.

### Tests

- client/admin handlers enforce scoped authorization and map barrier state;
- repeated enable/disable is idempotent;
- cluster follower entry routes or fails closed correctly;
- pending and retryable work cannot commit `started` after barrier completion;
- races where disable commits between status read, attempt-start commit,
  connector entry, and publication;
- multi-partition propagation, partial acknowledgement, restart, and resume;
- concurrent binding creation cannot escape the barrier;
- already-started calls remain accounted and may finish;
- unpublished output is suppressed after disable;
- re-enable does not resurrect canceled work automatically;
- control actor and committed revision are visible without leaking metadata.

### Acceptance

Operators have an explicit, Raft-owned procedure kill switch with documented
in-flight semantics.

## PIO4 — Procedure-scoped Intelligence Access control

### Work

1. Add procedure ID/version and binding ID/version to inference scope, resolve
   requests, policy decisions, grants, usage, and projection mappings.
2. Add endpoint/model/capability selectors to access policies.
3. Update policy matching to evaluate the selected route, not only profile,
   operation, privacy, and content scope.
4. Require positive procedure authorization to bind to exact version or
   immutable definition hash where configured.
5. Make logical-procedure endpoint denies apply across versions by default.
6. Add an optional convenience revoke/restore RPC only if it materializes the
   same access-policy authority.
7. Require a committed policy read or proven projection watermark before each
   attempt and retry.
8. Carry authorization revision into attempt evidence and publication fences.
9. Emit bounded fixed denial categories.

### Tests

- procedure P denied endpoint E while procedure Q may still use E;
- denial of E allows another route only when profile and policies authorize it;
- expiring a credential grant is not treated as endpoint denial;
- publishing a new procedure version cannot bypass a logical-procedure deny;
- exact-version allow does not authorize changed code;
- every retry reauthorizes;
- stale projection fails closed;
- revoke race honors the defined authorization linearization point;
- publication after revoke is fenced as specified;
- snapshot/restart/failover preserve policy behavior.

### Acceptance

Endpoint use can be revoked for one procedure without globally disabling the
endpoint or relying on credential-selection accidents.

## PIO5 — Public query contract

### Source API work

In `mycel-api`, add additive typed messages/RPCs to client and admin automation
services:

- `GetProcedureInferenceAttempt`;
- `ListProcedureInferenceAttempts`;
- `ListProcedureInferenceAttemptChanges`;
- typed automation run detail and bounded attempt references;
- optionally `ListAutomationRuns` if required for current-state exploration.

The current-state list supports filters for procedure/binding/run/route/state/
principal/time. The initial change-feed request requires exactly one `space_id`;
omitted-space and cross-space requests are rejected, including for admins. It
returns ascending immutable changes, `next_cursor`, high-water sequence,
earliest retained sequence, and explicit gap state for that space.

### Daemon adapter work

1. Require scoped `automation.read` and `inference.audit.read`.
2. Hide unauthorized identities and counts.
3. Encode opaque authenticated/request-bound cursors.
4. Advance cursors by examined sequence even when no event matches filters.
5. Return deterministic snapshot-bound state pages.
6. Map only the safe field allowlist.
7. Keep the `run_json` field during compatibility migration but generate it from
   a sanitized projection, require the new audit authorization, and never
   serialize credential IDs, provider request IDs, raw errors, or arbitrary
   metadata. Sanitize historical records on read and rewrite or expire them
   through an explicit migration.
8. Ensure typed attempt references cannot make legacy run retrieval an
   authorization bypass.
9. Populate or deprecate the existing unused `inference_usage_event_ids` field
   deliberately rather than leaving ambiguous behavior.

### SDK/CLI work

1. Regenerate daemon, Go SDK, and Rust SDK bindings from released source protos.
2. Add CLI commands for attempt get/list/change-tail and procedure
   enable/disable.
3. Keep collection logic in applications or dedicated consumers, not SDK hidden
   background workers.

### Tests

- exact filters and combinations;
- page bounds and invalid tokens;
- mandatory single-space feed scope and omitted/cross-space rejection;
- filter-bound cursor rejection;
- no skip/duplicate under concurrent appends;
- empty matching page advances examined cursor;
- snapshot consistency for current-state listing;
- retention gap fails closed;
- authorization isolation;
- safe mapping/redaction negative tests, including historical raw runs;
- old clients continue to compile and existing RPCs retain behavior.

### Acceptance

A Commonfolio service principal can reliably poll procedure inference changes
without reading provider payloads or silently missing retained history.

## PIO6 — Retention, reconciliation, and operations

### Work

1. Configure separate Raft-owned age/count/byte retention for materialized
   runs/attempts and change history with a minimum seven-day default target.
2. Never prune active attempts. Prune ambiguous attempts only after provider
   deadline, reservation lease, and required audit retention have elapsed.
3. Prune changes before state and delete invocation/run/attempt references in a
   referentially safe mutation. Fail new procedure attempts before provider
   access when state capacity cannot be made available safely.
4. Add operator-visible change and state high-water/earliest-retained values,
   retained count/bytes, and gap-risk diagnostics.
5. Add a reconciliation command/API that compares materialized attempts, change
   records, runs, and invocation references without exposing unsafe data and
   reports when state retention makes reconciliation impossible.
6. Add backup/restore coverage for attempt state and sequence watermarks.
7. Document Commonfolio polling behavior: durable cursor persistence,
   idempotent event insert, retry, gap handling, and reconciliation.
8. Add metrics for requested/authorized/started/terminal/ambiguous attempts,
   feed lag, retention pressure, and projection revision.

### Tests

- change-history and materialized-state pruning at every bound;
- active/ambiguous retention and referential deletion order;
- full capacity with no safely prunable state rejects before provider access;
- collector offline within retention catches up;
- collector beyond retention receives explicit gap;
- backup/restore preserves sequence and prevents cursor rollback;
- reconciliation detects orphan/missing attempt/run/change references;
- diagnostics remain bounded and sanitized.

### Acceptance

The polling contract has operationally enforceable completeness limits and
recovery procedures.

## Deferred message-queue adapters

No message-queue adapter is an implementation tranche in this plan. The current
work stops at the authoritative change log and cursor-based polling API. Its
immutable event ID, schema version, mandatory space scope, monotonic sequence,
sanitized payload, at-least-once consumer model, and explicit retention gaps are
the compatibility boundary for a future external relay or transactional-outbox
adapter.

Any Kafka, RabbitMQ, or other transport requires a separate approved design,
implementation plan, dependencies, configuration, checkpoint authority,
security review, and broker failure tests. That future work must not change the
attempt ledger into a transport-owned authority or remove polling-based replay.

## PIO7 — Mycel Lab acceptance

### Standalone scenarios

- one committed graph change triggers one procedure attempt lifecycle;
- actor/on-behalf/event-origin attribution;
- success, denial, cancellation, timeout, retry, and ambiguity;
- procedure disable before enqueue, before provider, during provider, and before
  publication;
- endpoint revoke for one procedure with another procedure unaffected;
- polling resume and deduplication;
- retention gap negative control;
- artifact redaction.

### Three-node scenarios

- leader failover before provider access;
- failover after possible provider side effect;
- no duplicate attempt/provider execution for one durable identity;
- committed procedure disable and endpoint revoke survive restart/snapshot;
- follower reads route to authoritative state;
- polling high-water and cursor continuity across failover;
- backup/restore preserves attempts and sequence watermarks.

### Evidence policy

Artifacts contain only classified synthetic IDs, states, counters, revisions,
sequence/cursor diagnostics, and sanitized categories. Bounded synthetic
policy-decision IDs, credential-grant IDs, and authorization revisions may be
retained for correlation. Artifacts never retain prompts, outputs, graph values,
vectors, credentials, authorization secrets/headers/values, free-form
authorization metadata, provider request IDs/bodies, or raw errors.

### Acceptance

Required unsupported capabilities fail. `not-run` is not success. Destructive
runs require separate explicit approval and preserve failed evidence.

## Cross-repository order

1. Approve daemon design and domain semantics.
2. Implement PIO1–PIO4 daemon foundations behind internal ports.
3. Add source protobufs in `mycel-api`.
4. Regenerate Go/Rust SDK and daemon bindings.
5. Implement the PIO3 procedure-control client/admin daemon handlers and the
   PIO5 query adapters and CLI.
6. Implement PIO6 operations and retention evidence.
7. Update Mycel Lab contracts and execute approved PIO7 acceptance scenarios.
8. Integrate Commonfolio polling only after a released API/SDK exists and under
   its independently managed change process.

## Validation commands

Documentation tranche:

```sh
make docs-check
git diff --check
```

Daemon implementation tranches:

```sh
go test -p 1 ./...
go test -race ./internal/automation/... ./internal/inference/...
go vet ./internal/automation/... ./internal/inference/...
scripts/check-daemon-only.sh
scripts/check-public-surface.sh
python3 scripts/checkDocs.py
git diff --check
```

Public API and SDK repositories use their own lint, breaking-change, generation,
and exact-head review gates. Cluster and destructive Lab suites run only with
fresh explicit approval.

## Completion criteria

- Every possible procedure provider side effect has a durable attempt identity
  committed first.
- Current-state and change-feed queries are typed, bounded, authorized, and
  fail closed on history gaps.
- Procedure disable and endpoint revoke have tested cluster linearization and
  publication-fence semantics.
- No new or migrated active attempt/run state or emitted API evidence contains
  prohibited data; the legacy `run_json` path is sanitized and gated.
- Restart, replay, snapshot, failover, and backup/restore preserve attempt and
  sequence authority.
- Polling exposes a versioned, transport-neutral, at-least-once change envelope
  without adding a message-queue dependency.
- Existing graph clients remain unaware of hidden procedure inference.
