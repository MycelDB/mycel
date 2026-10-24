# Procedure inference observability and control

## Status

Proposed design. This document defines the target architecture for recording,
querying, and controlling inference initiated implicitly by graph procedures.
It supersedes the proposed client-managed public inference-budget-scope model.
Graph clients do not need to know that a committed mutation may trigger
inference, and they do not provision enforcement state for that hidden work.

The daemon remains responsible for execution, authorization, durable attempt
identity, accounting, and publication fencing. Applications such as
Commonfolio may read safe execution evidence and operators may disable a
procedure or revoke its access to a configured inference endpoint.

## Problem

A graph procedure can invoke an LLM after a graph change has committed. The
client that submitted the graph change may not know that inference is part of
the procedure. Requiring that client to preflight an endpoint or create a
budget scope would:

- leak procedure implementation details into application transactions;
- let callers omit or weaken controls that the daemon must own;
- couple graph writes to provider topology;
- fail for scheduled, replayed, retried, or recovery-driven executions; and
- provide no authoritative answer after leader failover about what the daemon
  actually attempted.

Operators and applications instead need answers to these questions:

- Which procedure and immutable version ran?
- Which binding and graph event caused it to run?
- Who initiated the graph change, which principal executed the procedure, and
  on whose behalf did it run?
- Which configured endpoint, model, and capability did the daemon select?
- Was inference authorized, denied, attempted, completed, canceled, or left
  ambiguous by a crash?
- How many tokens and how much provider time were reported?
- Can an operator stop a badly behaved procedure?
- Can an operator prevent one procedure from using one endpoint without
  disabling that endpoint for every workload?
- Can a collector consume the history incrementally without silent gaps?

Current automation invocations and runs contain much of this information, but
there is no typed, authoritative, multi-attempt inference record or reliable
incremental collection contract. Existing inference usage telemetry is a local
runtime projection in clustered mode and uses mutable offset pagination.

## Goals

- Keep graph clients unaware of hidden inference implementation details.
- Record one durable, bounded procedure inference-attempt identity before every
  possible provider side effect.
- Make procedure inference evidence authoritative in standalone and clustered
  deployments.
- Correlate attempts with procedure, binding, invocation, run, workflow step,
  graph trigger, route, authorization decision, and principal identities.
- Provide typed current-state queries and an append-only incremental change
  feed with explicit retention-gap semantics.
- Add explicit procedure enable/disable behavior.
- Support procedure-scoped endpoint denial and restoration through the existing
  Intelligence Access policy authority.
- Keep provider work outside graph transactions, Raft apply, and publication
  critical sections.
- Define a versioned, sanitized, transport-neutral change envelope that polling
  uses now and that a future message-queue adapter can relay without changing
  execution authority or consumer semantics.
- Preserve existing automation and inference APIs additively while typed clients
  migrate away from raw run JSON and unsafe free-form telemetry.

## Non-goals

- Do not store prompts, completions, graph values, vectors, credentials,
  authorization headers, provider bodies, or raw provider errors in execution
  evidence.
- Do not make Activity Events authoritative for procedure execution.
- Do not implement or require Kafka, RabbitMQ, or any other message-queue
  dependency in the current delivery.
- Do not promise exactly-once provider side effects. Crash ambiguity remains
  explicit and conservatively recorded.
- Do not synchronously roll back the source graph transaction when a later
  procedure invocation fails or is denied.
- Do not expose a public reserve/complete protocol that application clients can
  manipulate.
- Do not alter semantic generation accounting in the first tranche; the event
  envelope may support additional workload kinds later through a new schema
  version.

## Terminology

### Procedure inference attempt

One authorization and provider-attempt lifecycle within one automation run. A
retry is a new attempt with its own identity. A workflow step may create zero or
more attempts.

### Attempt state

The authoritative materialized state of an attempt. Initial states prove that
the daemon intended and was authorized to perform work; terminal states record
known completion, denial, cancellation, failure, or crash ambiguity.

### Attempt change

An immutable transition in the attempt lifecycle. Changes are assigned a
monotonic sequence in the owning execution scope and are the source for polling
and asynchronous delivery.

### Configured route identity

The endpoint, model, capability, and operation selected from daemon
configuration. It must never be presented as provider-observed identity.

### Initiating and executing identities

The event-origin principal initiated the triggering graph change when known.
The actor principal executes the procedure. The on-behalf-of principal is the
identity represented to Intelligence Access. The binding owner is a separate
configuration identity. These fields are never inferred from one another.

## Existing foundation

The current implementation already provides:

- first-class procedure and binding ID/version fields, although current storage
  overwrites definitions by ID and does not yet make those versions immutable;
- binding-scoped actor, owner, on-behalf-of, profile, space, and domain context;
- deterministic invocation IDs for graph-triggered work;
- Raft-owned invocation, run, claim, retry, and output-fence state for supported
  clustered execution;
- run records containing route, policy, grant, token, mutation, and principal
  provenance for the current single-inference path;
- usage events correlated by automation/binding ID and run ID;
- binding enable/disable and pending invocation cancellation;
- Raft-owned Intelligence Access profiles, credential grants, and policies with
  derived inference runtime projections; and
- internal durable inference attempt reservation/accounting primitives from the
  atomic inference budget work.

The design extends these foundations rather than creating an unrelated audit
subsystem.

## Authority model

### Procedure configuration and immutable definitions

Procedure and binding definitions remain automation configuration. Their current
Raft ownership and partition routing remain authoritative in clustered mode.

Before version-bound evidence or authorization is enabled, procedure definitions
must become immutable by `(procedure_id, version)` or by an equivalent canonical
definition hash and snapshot. Binding execution configuration must likewise be
immutable by `(binding_id, version)` or captured as a canonical execution
snapshot. Updates create monotonically newer definitions; they do not overwrite
the procedure or binding content pinned by queued work.

Bindings reference an exact procedure definition, validation rejects a version
mismatch, and invocation creation pins both the procedure definition and binding
execution configuration needed after restart. A binding update can change the
inference profile, actor, owner, on-behalf-of identity, scope, condition, or
input mapping, so latest-by-ID binding resolution is forbidden for queued work.
Deletion retains pinned versions through execution retention, or queued work
fails with a fixed unavailable-definition category; it never substitutes newer
content. Authorization and evidence use the pinned values.

### Execution and attempts

A procedure inference attempt belongs to the same space-partition execution
authority as its invocation and run. In standalone mode it is stored through the
local durable automation path. In clustered mode creation and transitions are
committed through the owning space partition before they are visible or used as
proof.

Domain-wide scheduled bindings without an owning space remain fail-closed in
clustered mode, matching existing scheduled-execution semantics.

### Intelligence Access controls

Procedure-scoped endpoint controls belong to the existing space-owned
Intelligence Access authority. Runtime authorization must use either a committed
read or a derived projection with a proven applied revision. A stale or
unprovable projection cannot authorize a provider attempt.

### Activity Events

Activity Events may summarize notable control or failure events for operators,
but they remain non-authoritative. They must reference execution/control IDs
rather than duplicate inference payloads.

## Data model

### ProcedureInferenceAttempt

The authoritative state should contain only bounded, safe fields:

- `attempt_id`;
- `record_version`;
- `space_id` and `domain_id`;
- `procedure_id` and exact `procedure_version`;
- `binding_id` and exact `binding_version`;
- `invocation_id`, `run_id`, and `attempt_number`;
- optional `workflow_step_id` and step-attempt number;
- source graph event ID and, when available, source transaction ID and graph
  revision;
- event-origin, actor, owner, and on-behalf-of principal IDs;
- operation and configured profile ID;
- configured endpoint, model, and capability IDs after resolution;
- policy-decision ID and credential-grant ID;
- authorization/configuration revision used by the attempt;
- state and allowlisted outcome/denial category;
- token counts plus token-count authority;
- provider duration when known;
- requested, authorized, started, and completed timestamps; and
- ambiguity and publication-disposition flags.

Credential IDs, secret IDs, provider request IDs, arbitrary metadata, and raw
errors are deliberately omitted from this public-safe record.

### Run and attempt identity

The run identity and logical run-attempt number must be committed before
execution starts. Recovery reuses that allocation instead of generating a new
run ID. A deterministic run ID derived from invocation and logical run-attempt
number is preferred; an equivalently durable allocation is acceptable.

The provider-attempt ID must then be deterministic or durably allocated before
connector access. Its identity includes the pinned run, workflow step, and
provider-attempt ordinal. Recovery looks up this tuple before allocating work.
A `started` attempt is treated as possibly executed and cannot be automatically
replaced by another attempt or retried under a new run identity. Reusing an
attempt identity must never create a second provider side effect.

### Attempt states

The initial closed state set is:

- `requested` — durable intent exists; no authorization claim;
- `authorized` — route and policy decision are fixed for this attempt;
- `started` — provider access may have occurred;
- `succeeded`;
- `failed`;
- `denied`;
- `canceled`; and
- `ambiguous` — provider outcome cannot be proven.

`requested`, `authorized`, and `started` are non-terminal. A restart must not
silently translate them to success or erase them. Recovery may mark an elapsed
`started` attempt ambiguous, but it cannot claim the provider did not execute.

### ProcedureInferenceAttemptChange

Each committed lifecycle transition appends a bounded change containing:

- stable event ID;
- monotonic sequence;
- schema version;
- change kind;
- attempt ID and record version;
- a sanitized attempt snapshot; and
- occurred and committed timestamps.

The state machine assigns the sequence deterministically. A practical initial
scope is one monotonic sequence per space, persisted in automation snapshots.
Cursors bind to that space and the complete query filter.

## Provider execution flow

A procedure provider call follows this order:

1. The automation leader resolves the pinned immutable procedure definition and
   binding execution configuration, claim, and principal context.
2. The daemon commits or reuses the invocation's deterministic/durable run and
   logical run-attempt allocation.
3. The daemon durably creates or recovers the requested provider attempt through
   the automation execution authority.
4. Intelligence Access resolves the profile, route, credential grant, and
   policy decision without exposing secret material to the automation record.
5. The daemon revalidates current procedure/binding status and the committed
   access-control revision.
6. The automation authority commits the authorized/started attempt state.
7. The daemon releases all Raft/state-machine and graph publication critical
   sections, resolves the secret, and invokes the connector.
8. The daemon commits one terminal or ambiguous attempt transition with bounded
   usage and outcome fields.
9. Procedure output parsing and graph mutation preparation proceed.
10. Before publishing graph output, the daemon revalidates claim ownership,
    procedure/binding status, and the required access-control revision.
11. The graph commit atomically records a graph-owned idempotency receipt keyed
    by the invocation-stable output key. Automation receipts remain protected
    from generic idempotency expiry until terminal publication disposition is
    durable; a replicated acknowledgement then releases them to normal expiry.
12. The already durable run records the attempt IDs and publication disposition.

All stages after durable provider success share one cancellation-detached,
bounded publication context. If the publisher is interrupted, the execution
leader probes the graph-owned receipt and finalizes the attempt, run, and
invocation without another provider request. Receipt lookup does not depend on
mutable entity metadata. An unexpired claim is reclaimed only when its persisted
process incarnation differs from the current daemon; an active publisher in the
same incarnation retains its lease.

Failure to commit the required pre-provider transition means the connector is
not called. Failure to commit terminal evidence after a possible provider side
effect leaves the attempt ambiguous and blocks false-success publication.

Provider work must never execute during Raft apply or while a graph transaction
or publication lock is held. Existing automation execution that keeps a graph
transaction open across provider access must be refactored before this flow is
considered complete.

## Query model

The Automation client and admin services expose three complementary views.
Source protobuf changes remain owned by `mycel-api`.

### GetProcedureInferenceAttempt

Returns one current authoritative attempt by ID after scoped authorization.
Unknown or unauthorized identities do not leak cross-space existence.

### ListProcedureInferenceAttempts

Returns current attempt states with filters for:

- space/domain;
- procedure ID/version;
- binding ID/version;
- invocation/run/workflow step;
- endpoint/model/capability;
- operation and state;
- event-origin, actor, owner, or on-behalf-of principal; and
- requested/completed time.

Results use deterministic ordering and opaque snapshot-bound pagination. This
view answers current and historical state questions but is not the incremental
collector feed.

### ListProcedureInferenceAttemptChanges

Returns immutable changes in ascending sequence order. The initial contract
requires exactly one `space_id`; an omitted space or cross-space request is
rejected. Admin clients enumerate authorized spaces rather than receiving an
unrepresentable scalar cross-space watermark. The RPC accepts a bounded page
size, remaining filters, and an opaque `after_cursor` bound to the mandatory
space. The response contains:

- matching changes;
- `next_cursor` representing the last examined sequence, even when no records
  matched;
- current high-water sequence;
- earliest retained sequence; and
- explicit history-gap state.

A stale cursor never silently restarts at the earliest retained event. Required
collectors treat a history gap as an operational failure requiring reconciliation
from current attempt/run state.

Commonfolio persists the cursor only after durably storing all returned changes
and deduplicates by event ID. At-least-once delivery is the supported contract.

### Automation run integration

`GetAutomationRun` gains a typed, sanitized run representation and bounded
attempt references. Large attempt collections are retrieved through the attempt
list API. During compatibility migration the `run_json` field may remain, but
its value must be generated from a sanitized projection and guarded by the same
automation plus inference-audit authorization. It must not serialize the current
raw stored run structure, which can contain credential IDs, provider request
IDs, and raw errors. Historical active records are sanitized on read and
rewritten or expired under an explicit migration; new attempt references cannot
create a bypass around the typed audit path.

A typed `ListAutomationRuns` may be added for direct run-state exploration. The
incremental attempt-change feed remains the usage collection source.

## Authorization and visibility

Safe attempt reads require both automation visibility and inference-audit
visibility for the requested scope. A Commonfolio service principal should
receive narrowly scoped `automation.read` and `inference.audit.read`
capabilities for its space/domain.

Responses never use unauthorized counts, identities, or pagination offsets.
Opaque cursors are request-bound and cannot be replayed with broader filters.

## Procedure disable semantics

Add explicit `EnableGraphProcedure` and `DisableGraphProcedure` operations rather
than requiring a full JSON replacement to change status.

Procedure configuration and space execution can belong to different Raft
partitions, so the domain configuration commit alone is not the externally
visible disable linearization point. A disable creates a monotonically increasing
procedure-control epoch and propagates a fence to every execution partition that
owns a binding for the procedure. Binding creation/update is serialized with the
domain control state. The disable operation completes only after the required
execution partitions acknowledge the fence; an interrupted operation remains
pending and resumable. Unsupported or unenumerable target scope fails closed.

Each execution partition rejects attempt-start and output-publication mutations
whose control epoch is older than its applied fence. A committed `started`
transition is the provider-attempt linearization point: it may enter or finish
the provider after disable begins. The completed disable barrier guarantees that
no later `started` transition can commit, rather than claiming that an already
started call can be retracted.

After the barrier completes, disable:

- prevents new matching invocations;
- causes pending and retryable invocations to terminate with a fixed disabled
  category;
- prevents new provider attempts and retries;
- records any already-started call and its known/ambiguous outcome;
- suppresses publication of output that was not committed before the local
  control fence; and
- preserves all retained execution evidence.

Binding disable remains a narrower control affecting one deployment of a shared
procedure. Enable/disable operations are idempotent and record the controlling
principal, control epoch, affected partition set, acknowledgements, and barrier
completion revision.

## Procedure-scoped endpoint control

Extend Intelligence Access scope and runtime resolve context with typed:

- procedure ID/version;
- binding ID/version; and
- invocation/run identity for evidence correlation.

Extend access policies with endpoint, model, and capability selectors.
Credential grants may use procedure/binding scope as an additional restriction,
but expiring a credential grant is not endpoint revocation because another
credential or endpoint may be selected.

Positive authorization binds to an exact procedure version or immutable
definition hash so changed code requires reauthorization. Endpoint denial binds
to the logical procedure ID across versions by default so publishing a new
version cannot bypass a revocation. An optional exact-version deny may support a
narrower emergency or migration case.

A procedure/endpoint revoke operation may be exposed as convenience API, but it
must materialize the same authoritative deny-policy state rather than introduce
a second authorization system.

Every retry performs a new authorization check. Provider calls authorized before
the revocation linearization point may finish; later attempts fail with a fixed
machine-readable category. Publication revalidates relevant procedure and access
control revisions.

## Retention and completeness

Both materialized runs/attempts and attempt changes use Raft-owned bounded
retention by age, count, and serialized bytes. The initial release uses hard
per-space limits of 10,000 attempts, 50,000 changes, 64 MiB of materialized
state, and 64 MiB of change history. Retention targets 8,000 attempts, 20,000
changes, 30 days, 48 MiB of state, and 48 MiB of change history. Attempt records
are limited to 64 KiB encoded and persisted evidence strings to 1,024 bytes. An
active attempt is never pruned. The initial release also retains ambiguous
attempts indefinitely; operators must resolve capacity pressure rather than lose
uncertain provider-side-effect evidence.

Pruning removes change history first, then run/attempt state in one referentially
safe mutation after no retained invocation or run references it. Separate
change-feed and materialized-state earliest-retained watermarks make expiration
explicit. Reconciliation can recover from retained current state only while that
state remains available; afterward it reports an unrecoverable retention gap.

Hard state capacity is enforced before a new provider attempt. If terminal state
cannot be safely pruned and a new durable attempt cannot be retained, the daemon
fails that procedure attempt before provider access rather than growing without
bound. No API or future delivery consumer may interpret an expired range as an
empty successful interval.

## Future message-queue compatibility

No Kafka, RabbitMQ, or other message-queue client, configuration, relay, topic,
or broker test is part of this design's current implementation scope. Polling is
the only delivery mechanism being built now.

The committed attempt-change record nevertheless provides a transport-neutral
future integration boundary:

- each change has an immutable event ID and schema version;
- sequence and ordering are defined per mandatory `space_id`;
- the sanitized envelope contains no transport-specific fields;
- the change is committed atomically with its authoritative attempt transition;
- consumers are designed for at-least-once delivery and event-ID deduplication;
- cursors advance only across contiguous examined sequences; and
- retention gaps are explicit.

A future adapter may poll this feed externally or use the committed change log
as a transactional-outbox source. It must remain a delivery mechanism rather
than a second authority, preserve per-space order, checkpoint only contiguous
acknowledged events, and leave polling available for replay and repair. Adding
such an adapter requires a separate design and implementation plan for its
chosen transport; no broker-specific decision is frozen here.

## Security and redaction

Persisted and emitted safe evidence must not include:

- prompts, inputs, outputs, projected text, or graph properties;
- vectors;
- credentials, secret references, TLS material, or authorization values;
- provider request/resource IDs, headers, bodies, or observed route claims;
- raw error text or arbitrary connector codes; or
- arbitrary caller/provider metadata.

Bounded policy-decision IDs, credential-grant IDs, and committed authorization
revisions are classified correlation fields, not authorization secrets, and may
be retained. Outcome and denial fields use closed allowlists. Configured route
IDs are labeled as configured daemon identities. Token-count authority
distinguishes provider-reported, estimated, and unavailable counts.

## Failure semantics

- No partition leader or failed committed pre-attempt write: no provider call.
- Authorization projection without a proven revision: fail closed.
- Crash before provider access: attempt remains requested/authorized and may be
  recovered without claiming provider execution.
- Crash after possible provider access: attempt becomes ambiguous unless a
  terminal outcome can be proven.
- Failed terminal evidence write: do not publish output as success.
- Procedure disabled while queued: skip without provider access.
- Procedure disabled or endpoint revoked after authorization: an in-flight call
  may finish, but unpublished output is fenced and evidence remains visible.
- Collector cursor behind retention: explicit history gap.

## Compatibility and migration

All public protobuf changes are additive. Existing automation invocation and
`run_json` fields remain during migration, but run JSON becomes a sanitized,
authorization-gated projection rather than serialization of unsafe stored
fields. Existing inference usage APIs remain neutral telemetry and are not
upgraded implicitly into authoritative procedure execution evidence.

Legacy combined automations are projected into procedure/binding identity using
the existing compatibility expansion. Their attempt evidence carries the derived
procedure and binding IDs so consumers do not need separate schemas.

The daemon's internal atomic budget structures remain available as reusable
attempt reservation/accounting machinery. The canceled public client-managed
budget-scope service is not revived by this design.

## Open decisions

- Whether source transaction ID/revision is mandatory for all trigger types or
  only graph-triggered attempts.
