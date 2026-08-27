# Clustered automation execution

## Status

Implemented for graph-triggered automations and space-scoped scheduled workflow
automations. This document describes the clustered execution model after graph
procedure and automation binding configuration became Raft-owned.

## Problem

Graph automation configuration and graph automation execution have different
safety requirements.

Procedure and binding definitions are durable operator-authored configuration.
In clustered/Raft mode they must be replicated through Raft so every node sees
the same automation configuration.

Execution creates user-visible runtime state and side effects:

- automation invocations;
- automation runs;
- schedule checkpoints;
- debounce and idempotency records;
- inference usage and policy decisions;
- graph writes produced by automation output actions.

If every pod independently evaluates and runs a binding, a single graph event can
produce duplicate inference calls and duplicate graph mutations. If only one pod
runs the automation but its execution state is purely local, failover can lose or
strand pending/running work and retry/cancel/list APIs can disagree across pods.

The clustered model must therefore define a single execution owner and recovery
semantics.

## Ownership rule

For graph-triggered automations:

> The Raft leader for the affected space partition is the only pod that may
> enqueue and execute automations for graph events in that space.

This is the natural authority boundary because graph writes are already owned by
the space partition. The same partition leader that commits or applies the graph
event is the only node allowed to turn that committed event into automation
execution work.

A graph-triggered execution flow should be:

1. A graph transaction for space `S` is committed through the Raft partition for
   `S`.
2. The partition leader publishes or handles the committed graph event.
3. The leader evaluates Raft-replicated automation bindings for the event's
   space/domain.
4. The leader creates one deterministic invocation per matched binding/target.
5. The leader claims and runs that invocation.
6. Output graph mutations are submitted through the normal Raft graph write path.

Followers may maintain local read projections of procedures and bindings, but
followers must not independently enqueue or run graph-triggered automations.

## State authority

### Raft-owned configuration

The following state is durable automation configuration and must be Raft-owned:

- graph procedures;
- graph automation bindings.

This state is partitioned by the domain UUID, because graph procedures and
bindings are domain-scoped. Bindings may additionally carry a space scope, but
their canonical storage and Raft command routing are domain-based.

### Execution state

The following state is execution/runtime state:

- invocations;
- runs;
- retry/cancel status;
- debounce records;
- successful-input idempotency records;
- schedule checkpoints;
- workflow instances and step runs.

This state must not be written independently by every pod. For graph-triggered
clustered execution, invocations, runs, successful-input idempotency records,
claims, and graph replay cursors are Raft-owned using the same space-partition
authority as the triggering graph event. Retained graph notification history is
recorded on Raft apply and is used as replay evidence, while the Raft-owned
cursor remains the automation checkpoint.

Space-scoped scheduled workflow execution uses the same partition authority:
only the leader for the binding's scoped space may enqueue due windows, and the
invocation, run, schedule checkpoint, workflow instance, and workflow step-run
state are Raft-owned. Domain-wide scheduled bindings without a space scope do
not identify an owning partition and remain fail-closed/no-op in Raft mode.

## Deterministic invocation identity

Invocation creation must be idempotent. For graph-triggered automations, the
invocation key should be derived from immutable trigger inputs, for example:

```text
space_id + domain_id + graph_event_id + binding_id + target_node_id
```

If the same event is replayed or a new leader reprocesses committed history, the
same invocation ID is produced. Existing invocation handling can then decide:

- already completed: do not rerun;
- pending: keep or claim;
- running with unexpired lease: do not steal;
- running with expired lease: retry or mark abandoned according to policy;
- failed retryable: retry if retry budget allows;
- failed permanent/cancelled: do not rerun unless explicitly retried.

Graph output writes carry idempotency and fence metadata so a retried
invocation cannot apply duplicate output mutations, and a stale worker cannot
commit output after leadership or claim loss.

## Claiming and leases

If execution state is not fully Raft-owned, claims must still be authoritative.
A claim should include:

- invocation ID;
- owner node ID;
- lease expiration;
- attempt number;
- fencing token or monotonically increasing claim version.

Only the current partition leader may create or renew claims. A worker must stop
processing when it loses leadership or its claim expires. Graph output commits include the invocation ID, owner node ID, claim version,
claim token, and output idempotency key. In Raft mode, the graph state machine
fails closed before applying automation-tagged output unless the automation
subsystem validates that the metadata matches the current Raft-owned running
invocation claim. Apply-time validation intentionally avoids wall-clock lease
checks so replicas remain deterministic; workers check and renew lease expiry
before proposing the graph output commit.

## Failover behavior

The local leader for a space partition reconciles graph-triggered automation
work conservatively:

1. Read retained committed graph notification history since the Raft-owned
   automation cursor for the space/domain.
2. Recompute deterministic invocation IDs for matching bindings.
3. Create missing invocations through the Raft-owned invocation path.
4. Reclaim expired running invocations through the Raft-owned claim path.
5. Continue pending/retryable work through the leader-gated worker.

Retained-history gaps fail closed and do not advance the cursor. The
reconciliation process prefers at-most-once side effects where possible and uses
deterministic idempotency keys to make necessary retries safe.

Automation metrics expose replay/reclaim diagnostics for operator visibility:
scopes scanned, follower skips, replayed events, skipped replay events, created
or already-existing invocations, cursor advances, lossless replay gaps, expired
claim reclaims, and unreclaimable/stranded running claims.

## Scheduled automations

Scheduled automations need a separate ownership rule because they are not driven
by a graph commit event.

For space-scoped scheduled bindings, the implemented model is:

- partition scheduled bindings by their scoped space;
- only the corresponding partition leader evaluates due schedules;
- schedule checkpoints are Raft-owned;
- schedule invocation IDs are deterministic by space, domain, binding, and
  scheduled window;
- missed schedules after failover are reconciled from durable checkpoint state,
  one due window per scheduler pass.

Domain-wide scheduled bindings without a space scope remain fail-closed/no-op in
Raft mode until they have an explicit partition ownership model.

## Legacy definitions

Legacy single-file automation definitions are not Raft-owned configuration. In
clustered/Raft mode, runtime execution must ignore legacy definitions unless they
have been migrated into Raft-owned procedure/binding configuration.

This prevents stale node-local definitions from producing automation side effects
outside the clustered authority model.

## Operator/API behavior

Clustered admin APIs should allow safe configuration operations:

- create/list/get/update graph procedures;
- create/list/get/update graph automation bindings;
- enable/disable bindings only through Raft-owned configuration updates.

Runtime read APIs (`ListAutomationInvocations` and `GetAutomationRun`) forward
or aggregate through partition leaders when backend routing is configured, and
leaders use Raft ReadIndex before reading local projections. Runtime APIs that
mutate execution state, such as retry/cancel or schedule control, must either
use the authoritative execution-state path or fail closed in clustered mode.

## Open implementation questions

- What is the retention policy for graph event history required for automation
  recovery?
- Should automation replay/fencing diagnostics be surfaced through a dedicated
  admin API in addition to internal service metrics?
- Should policy decisions and inference usage remain local audit projections, or
  become Raft-owned execution evidence for automation runs?
