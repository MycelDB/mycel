# Clustered automation execution implementation plan

## Status

In progress. CAE-1 through CAE-8 are complete for graph-triggered and
scheduled automation execution. CAE-5 is complete with invocation-scoped output
idempotency, claim renewal, and deterministic graph-state-machine claim/fencing
validation. CAE-6 is complete with retained graph-event replay, Raft-owned
replay cursors, live failover coverage, and replay/reclaim diagnostics. CAE-7 is
complete with partition-leader runtime read forwarding and fail-closed follower
mutations. CAE-8
is complete with leader-only scheduled enqueue, deterministic schedule
invocation IDs, Raft-owned workflow runtime state, and Raft-owned schedule
checkpoints. CAE-9 automated cluster validation has passed for the local
Compose stack, K3s cluster validation, and K3s Raft disruption smoke/edges;
manual Knot PKM onboarding/page-summary evidence remains to be captured. This
plan implements
[Clustered automation execution](../../design/automation/clustered-automation-execution.md).

Graph procedure and automation binding configuration can be made Raft-owned
independently from execution. This plan covers the remaining clustered execution
work: ensuring each automation invocation is created and run by exactly one pod,
with safe failover and no duplicate side effects.

## Problem statement

In a three-node Raft dev stack, graph automation configuration may be replicated,
but execution state is still local runtime state. Running automation workers on
multiple pods without an ownership model can cause duplicate inference calls and
duplicate graph writes. Running on only one pod without durable execution state
can lose or strand pending/running work after failover.

The intended clustered behavior is:

- only the leader for the affected space/partition enqueues graph-triggered
  invocations;
- each invocation has a deterministic identity;
- exactly one worker owns an invocation attempt at a time;
- output graph writes are idempotent/fenced;
- a new leader can reconcile after failover without rerunning completed work;
- scheduled automations run only through equivalent leader/checkpoint semantics.

## Non-goals

- Do not make every pod execute the same automation.
- Do not add automatic split-brain repair or cross-node local-state merge.
- Do not rely on local files as authoritative execution state in Raft mode.
- Do not run scheduled automations from divergent node-local checkpoints or
  workflow state.
- Do not change provider credential/profile authority; Intelligence Access
  remains semantic/Raft-authoritative with standalone inference as runtime
  projection.

## Safety rules

- Durable user-visible automation execution state in Raft mode must be
  Raft-owned or explicitly rebuildable from authoritative data.
- Graph-triggered automation enqueue must be single-owner and deterministic.
- Retried/replayed graph events must not create duplicate invocations.
- Retried/replayed invocations must not create duplicate graph output mutations.
- Runtime writes must fail closed when ownership or fencing cannot be proven.
- Execution metadata must not include secrets, tokens, private keys, or raw
  provider responses.

## Architecture summary

### Configuration authority

Graph procedures and automation bindings are Raft-owned configuration,
partitioned by domain UUID. They are replicated to every node and used as the
source of truth for runtime matching.

### Execution authority

Graph-triggered execution is owned by the Raft leader for the affected space
partition. Followers may read replicated configuration but must not enqueue or
run graph-triggered invocations for that space.

### Invocation identity

For graph-triggered automations, invocation IDs are deterministic:

```text
hash(space_id, domain_id, graph_event_id, binding_id, target_node_id)
```

The deterministic ID makes graph-event replay, leader failover, and retry safe:
reprocessing the same event/binding/target resolves to the same invocation.

### Execution state

The implementation uses Raft-owned execution state for graph-triggered and
space-scoped scheduled invocations, claims, runs, workflow instances,
workflow-step runs, schedule checkpoints, and successful-output idempotency
records. Runtime state is partitioned by the same space partition that owns the
triggering graph event or scheduled binding scope.

## Current implementation progress

Implemented in the current tranche:

- deterministic graph-triggered invocation IDs;
- Raft-owned graph-triggered invocation records;
- Raft-owned run records;
- Raft-owned successful-input idempotency records;
- Raft-owned schedule checkpoint record support in snapshots/apply paths with
  explicit `space_id` partition ownership metadata;
- leader-only graph-triggered enqueue/process based on the affected space
  partition leader;
- per-attempt claim owner/version/token/lease fields;
- expired running claim reclaim;
- same-version conflicting claim rejection;
- cancel fencing for pending/retryable invocations;
- clustered cancellation of already-running invocations fails closed unless and
  until explicit safe running-cancel semantics are designed;
- invocation-scoped output idempotency metadata on graph output writes;
- pre-inference output-idempotency detection so retried invocations whose output
  already exists do not call the provider again;
- duplicate-output retries repair successful-input idempotency indexes;
- deterministic graph-state-machine validation of automation output fences;
- retained graph-event replay from Raft-apply notification history;
- Raft-owned graph replay cursors;
- replay/reclaim diagnostics for scopes, follower skips, replayed/skipped
  events, created/existing invocations, cursor advances, gap failures, expired
  claim reclaims, and unreclaimable/stranded claims;
- inference runtime evidence writes required by Resolve/Invoke, specifically
  policy decisions and usage events, explicitly bypass the clustered local-write
  authority gate because Intelligence Access configuration remains
  semantic/Raft-authoritative and these records are runtime evidence;
- partition-leader runtime read forwarding for invocation list and run get APIs,
  with leader-side ReadIndex barriers;
- fail-closed retry/cancel behavior on non-leader nodes;
- leader-only scheduled enqueue for space-scoped scheduled bindings;
- deterministic schedule invocation IDs keyed by binding and scheduled window;
- Raft-owned workflow instance and step-run creation for scheduled workflows.

Deferred from this tranche:

- CAE-9 live three-node/Knot PKM validation.

## Phases

## Phase CAE-1: Audit and classify current automation state

Status: complete.

Goal: make an explicit state inventory before changing runtime behavior.

### CAE-1 audit findings

Current automation persistence is implemented by
`internal/automation/storage.FileStore` under the automation module data
directory. The store has no hidden database or remote authority; every durable
record below maps to JSON files written by `internal/automation/storage/file.go`.

| Path | Store methods | Classification | Clustered/Raft status |
| --- | --- | --- | --- |
| `procedures/<domain_id>/<procedure_id>.json` | `PutProcedure`, `DeleteProcedure`, `ListProcedures` | durable configuration | In scope for the current configuration-Raft tranche; create/update/delete must use automation Raft mutations. |
| `bindings/<domain_id>/<binding_id>.json` | `PutBinding`, `DeleteBinding`, `ListBindings` | durable configuration | In scope for the current configuration-Raft tranche; create/update/delete/status changes must use automation Raft mutations. |
| `definitions/<domain_id>/<automation_id>.json` | `PutDefinition`, `DeleteDefinition`, `ListDefinitions` | legacy combined automation configuration | Out of clustered execution scope; local writes remain gated and runtime selection ignores legacy definitions in Raft mode. |
| `invocations/<domain_id>/<day>/<invocation_id>.json` | `PutInvocation`, `GetInvocation`, `ListInvocations` | graph-triggered and scheduled execution state | Raft-owned for graph-triggered and space-scoped scheduled execution in clustered mode. |
| `runs/<domain_id>/<day>/<run_id>.json` | `PutRun`, `GetRun` | execution evidence/runtime state | Raft-owned for clustered graph-triggered and space-scoped scheduled execution. |
| `indexes/successful-input/<domain_id>/<automation_id>/v<version>/<element_id>/<input_hash>.json` | `PutSuccessfulInputIndex`, `GetSuccessfulInputIndex` | output/idempotency runtime state | Raft-owned for clustered graph-triggered and space-scoped scheduled execution. |
| `schedule-checkpoints/<domain_id>/<automation_id>.json` | `PutScheduleCheckpoint`, `GetScheduleCheckpoint` | scheduled execution cursor/checkpoint with `space_id` ownership metadata | Raft-owned for space-scoped scheduled execution; domain-wide schedules without space scope remain no-op/fail-closed in Raft mode. |
| `workflow-instances/<domain_id>/<day>/<instance_id>.json` | `PutWorkflowInstance`, `GetWorkflowInstance`, `ListWorkflowInstances` | workflow runtime state | Raft-owned when created from clustered scheduled invocations. |
| `workflow-steps/<domain_id>/<day>/<step_run_id>.json` | `PutWorkflowStepRun`, `ListWorkflowStepRuns` | workflow runtime state | Raft-owned when created from clustered scheduled invocations. |
| `proposals/<domain_id>/<day>/<proposal_id>.json` | `PutProposal`, `GetProposal`, `ListProposals` | human-approval/proposal runtime state | Out of clustered execution scope for now; local writes must remain gated. |
| `policies/<domain_id>.json` | `PutPolicy`, `GetPolicy` | automation service policy/configuration | Not part of graph procedure/binding config Raft tranche; local writes remain gated until a separate authority decision is made. |

Graph-change notification persistence is separate from automation storage:

| Path | Owner | Classification | Clustered/Raft status |
| --- | --- | --- | --- |
| `graph-change-notification/<space_id>/<domain_id>.jsonl` | `internal/graph/notification.Module` | retained committed graph-event replay history | Local retained notification history, not automation execution authority. Useful for future CAE-6 reconciliation only if retention/authority constraints are satisfied. |
| `graph-change-notification/<space_id>/<domain_id>.state.json` | `internal/graph/notification.Module` | current notification revision marker | Local notification cursor, not an automation execution checkpoint. |

### Current write-path classification

Configuration writes:

- legacy combined automations:
  - `CreateAutomationAs`, `UpdateAutomationAs`, `DeleteAutomation`,
    `SetAutomationStatusAs`;
  - local-write-gated and not Raft-enabled for clustered execution.
- graph procedures:
  - `CreateProcedureAs`, `UpdateProcedureAs`, `DeleteProcedure`;
  - in the configuration-Raft tranche via `automation.mutation.v1`.
- graph automation bindings:
  - `CreateBindingAs`, `UpdateBindingAs`, `DeleteBinding`,
    `SetBindingStatusAs`;
  - in the configuration-Raft tranche via `automation.mutation.v1`.

Runtime/execution writes:

- `HandleGraphChange` writes invocations from committed graph events.
- `ProcessScheduled` writes invocations and schedule checkpoints.
- `ProcessPending` writes invocation status, runs, workflow instances, workflow
  step runs, and successful-input idempotency records.
- `RetryInvocation` and `CancelInvocation` mutate invocation state.
- `PutPolicy` mutates automation policy.
- `PutProposal`/proposal helpers mutate proposal records.

These runtime write paths now use deterministic IDs, Raft-owned execution state,
leader-only ownership, committed runtime reads, and output fencing for
clustered graph-triggered and space-scoped scheduled execution. Surfaces without
an ownership model, such as domain-wide scheduled bindings and running
cancellation, fail closed rather than mutating divergent node-local state.

### Graph notification leader-gate confirmation

`internal/graph/notification.Module` supports a `WithLeaderGate` hook. The gate
runs inside `OnGraphCommitted` before the event is persisted to local
notification history or delivered to registered consumers. If the gate returns an
error, the notification module records the failure and does not deliver the
event.

This means current notification delivery can be prevented on nodes that are not
local graph leaders. It does not by itself make automation execution safe,
because automation invocation/run/checkpoint/idempotency records are still local
files. CAE-4 must add automation-specific leader ownership and claiming before
clustered execution can be enabled.

### Admin/API mutation audit

Admin automation APIs that mutate configuration:

- `CreateAutomation`, `UpdateAutomation`, `DeleteAutomation`,
  `EnableAutomation`, `DisableAutomation`;
- `CreateGraphProcedure`, `UpdateGraphProcedure`, `DeleteGraphProcedure`;
- `CreateGraphAutomationBinding`, `UpdateGraphAutomationBinding`,
  `DeleteGraphAutomationBinding`, `EnableGraphAutomationBinding`,
  `DisableGraphAutomationBinding`.

Admin automation APIs that mutate runtime execution state:

- `RetryAutomationInvocation`;
- `CancelAutomationInvocation`.

Read-only runtime APIs:

- `ListAutomationInvocations`;
- `GetAutomationRun`.

CLI commands mirror these API surfaces, including legacy automation
create/update/delete/enable/disable, graph procedure/binding provisioning,
invocation retry/cancel, invocation listing, and run retrieval.

### CAE-1 acceptance evidence

- State inventory is documented above.
- Runtime write paths are classified as unsafe to enable in clustered mode until
  later phases.
- Current graph notification leader-gate behavior is identified and scoped: it is
  useful input for leader delivery but not sufficient execution authority.
- Admin and CLI runtime mutation surfaces are identified for CAE-7 routing and
  fail-closed behavior.

Validation:

```sh
go test ./internal/automation/service ./internal/graph/notification
```

## Phase CAE-2: Deterministic graph-triggered invocation IDs

Status: complete.

Goal: make graph-event replay idempotent before enabling clustered execution.

### CAE-2 implementation

Graph-triggered invocation enqueue now uses deterministic invocation IDs instead
of random UUIDs. The ID helper is:

```text
graphTriggeredInvocationID(space_id, domain_id, graph_event_id, binding_id, target_node_id)
```

The helper returns a stable UUID string derived from:

- space ID;
- domain ID;
- graph event/commit ID;
- binding ID;
- changed/target node ID.

`HandleGraphChange` now creates graph-triggered invocations through an
idempotent helper. When the deterministic ID already exists:

- if the existing invocation has the same trigger/config/runtime metadata,
  enqueue is a no-op, regardless of current status;
- if the same ID exists with different trigger metadata, enqueue fails closed
  with an explicit conflict error;
- store read/write errors are propagated instead of being ignored.

This makes repeated delivery or replay of the same committed graph event safe for
pending, running, completed, retryable, failed, skipped, or cancelled invocation
records. Later phases still need authoritative claim/run state before clustered
execution is enabled.

### CAE-2 changed files

- `internal/automation/service/manager.go`
- `internal/automation/service/graph_invocation_id_test.go`

### CAE-2 acceptance evidence

- Deterministic ID stability and UUID formatting are covered by
  `TestGraphTriggeredInvocationIDIsStableAndScoped`.
- Binding and target-node scoping are covered by
  `TestGraphTriggeredInvocationIDIsStableAndScoped`.
- Replay/idempotent enqueue is covered by
  `TestHandleGraphChangeReplayUsesSameInvocation`.
- Completed invocation replay no-op behavior is covered by
  `TestHandleGraphChangeReplayDoesNotOverwriteCompletedInvocation`.
- Conflicting same-ID metadata fails closed without overwrite in
  `TestHandleGraphChangeRejectsDeterministicInvocationConflict`.

Validation:

```sh
go test ./internal/automation/service
```

## Phase CAE-3: Raft-owned invocation/claim state

Status: complete for graph-triggered invocation, claim, run, and successful-input
idempotency state. Workflow instance/proposal state remains out of scope.

Goal: make pending/running ownership durable and single-owner.

Tasks:

1. Add automation execution Raft mutation records, partitioned by the triggering
   space ID:
   - invocation upsert/create;
   - invocation status update;
   - claim acquire/renew/release;
   - run append/upsert;
   - successful-output idempotency record.
2. Add apply handlers that are idempotent for replay and reject conflicting
   mutations.
3. Add snapshot/restore coverage for graph-triggered execution state.
4. Validate snapshot payloads before destructive restore.
5. Preserve schedule/workflow/proposal state as fail-closed unless explicitly
   moved into this authority model.

Acceptance criteria:

- Pending invocation state survives node restart and Raft snapshot install.
- A committed graph event can be replayed without duplicating invocations.
- Conflicting create/update records fail closed.
- Snapshot restore cannot delete current execution state before validating the
  incoming snapshot.

Tests:

- Raft apply/replay idempotency tests.
- Snapshot/restore validation-before-delete tests.
- Multi-node simulated Raft test showing invocation state replicated to all
  nodes.

Validation:

```sh
go test ./internal/automation/service ./internal/daemon/app ./internal/clustering/consensus
make test-phase-d
```

## Phase CAE-4: Leader-only enqueue and worker ownership

Status: complete for graph-triggered automations delivered to the current local
space-partition leader.

Goal: ensure exactly one pod enqueues and processes graph-triggered automation
work.

Tasks:

1. Add an automation execution ownership check that requires the current node to
   be leader for the triggering space partition before enqueueing.
2. Make `HandleGraphChange` fail closed or no-op on followers.
3. Add claim acquisition before processing an invocation.
4. Include owner node ID, claim version/fencing token, and lease expiration in
   execution state.
5. Stop or abort processing when leadership is lost or the claim expires.
6. Ensure workers only scan/process partitions they currently lead.

Acceptance criteria:

- In a three-node cluster, exactly one node creates the invocation for a graph
  event.
- Followers do not enqueue or process invocations for partitions they do not
  lead.
- Claim conflicts are resolved deterministically through Raft.

Tests:

- Multi-node simulated Raft test delivering the same graph event to all nodes;
  only one invocation is created.
- Leader-loss test where an old worker cannot complete after losing leadership.
- Claim conflict test where only one claim wins.

Validation:

```sh
go test ./internal/automation/service ./internal/graph/notification ./internal/daemon/app
make test-phase-d
```

## Phase CAE-5: Idempotent/fenced graph output commits

Status: complete for graph-triggered automations. Invocation-scoped output
idempotency metadata, pre-inference duplicate-output detection,
successful-input repair, claim renewal, fail-closed running cancel, and graph
state-machine claim/fencing validation are implemented.

Goal: prevent duplicate graph side effects when an invocation is retried or a
worker races leadership changes.

Tasks:

1. Add automation output idempotency key metadata to graph commits:
   - invocation ID;
   - run/attempt ID;
   - binding ID;
   - target node ID;
   - claim/fencing token.
2. [x] Reject graph output commits from stale claims at graph Raft apply time.
3. [x] Record successful output idempotency state through Raft-owned execution
   state after successful graph commit, with pre-commit duplicate-output
   detection and repair for already-applied output.
4. [x] Ensure retried invocations can detect already-applied output.

Acceptance criteria:

- Retrying a completed invocation does not duplicate output mutations.
- A stale worker cannot commit after claim/leadership loss.
- Successful-output records survive restart/snapshot.

Tests:

- Duplicate invocation retry test.
- Stale claim/fencing rejection test.
- Graph output idempotency test.
- Graph Raft apply rejects stale or incomplete automation output fences before
  mutating graph state.
- Automation output fence validation accepts only the current running invocation
  claim and rejects stale/terminal claims.

Validation:

```sh
go test ./internal/automation/service ./internal/graph/service
make test-phase-d
make test-phase-f
```

## Phase CAE-6: Failover reconciliation

Status: complete for graph-triggered automations. Graph Raft apply records
retained graph notification history on each node, follower delivery is
suppressed by the leader gate, and automation maintains a Raft-owned graph
replay cursor per space/domain. The local partition leader periodically replays
retained events after that cursor, recreates missing deterministic
invocations, and continues expired running invocations through the existing
Raft-owned claim/reclaim path. Retention gaps fail closed and surface as
recovery errors.

Goal: allow a new partition leader to safely continue automation work.

Tasks:

1. [x] Track a durable automation cursor over committed graph events per
   space/domain scope.
2. [x] Periodically reconcile committed retained graph history since the last
   cursor on the local partition leader.
3. [x] Recreate missing deterministic invocations.
4. [x] Reclaim expired running invocations through the existing claim path.
5. [x] Continue pending/retryable work through the existing leader-gated worker.
6. [x] Add diagnostics for skipped, replayed, reclaimed, and abandoned work.

Acceptance criteria:

- If the leader dies after graph commit but before enqueue, the partition leader
  can create the missing invocation from retained replay history.
- If the leader dies during execution, the partition leader can reclaim expired
  running claims according to retry policy.
- Completed invocations are not rerun during reconciliation.
- Retained-history gaps fail closed instead of silently advancing cursors.

Tests:

- [x] Reconciliation replay from graph event history.
- [x] Replay cursor snapshot/restore coverage.
- [x] Retained-history gap fails closed.
- [x] Simulated live leader failover before enqueue.
- [x] Simulated live leader failover during running invocation.

Validation:

```sh
go test ./internal/automation/service ./internal/daemon/app
make test-phase-d
```

## Phase CAE-7: Admin/runtime API routing and consistency

Status: complete. Runtime reads are served through partition-leader forwarding
when backend addresses are configured: list APIs aggregate each partition
leader's response, and get-run probes partition leaders until the authoritative
run is found. Local partition leaders use Raft ReadIndex before reading their
projection. Runtime mutations use the Raft-owned invocation path and fail closed
on non-leader nodes rather than mutating divergent node-local state.

Goal: make user-visible runtime APIs consistent in clustered mode.

Tasks:

1. Route retry/cancel/get/list runtime APIs to the authoritative partition owner,
   or serve them through replicated Raft state.
2. Ensure list APIs use committed/read-index semantics where required.
3. Return explicit fail-closed errors for unsupported clustered operations.
4. Add CLI/API error text that distinguishes:
   - configuration not found;
   - not current partition leader;
   - execution state unavailable;
   - stale claim/fencing rejection.

Acceptance criteria:

- Retry/cancel do not mutate node-local divergent state.
- Listing invocations/runs returns consistent results from multiple nodes.
- Unsupported operations fail with actionable errors.

Tests:

- [x] Runtime list/get consistency across nodes after Raft-owned writes.
- [x] Non-leader runtime list/get APIs forward to partition leaders instead of
  serving stale node-local files.
- [x] Retry/cancel fail closed on followers.
- [x] Running invocation cancel fails closed in Raft mode.

Validation:

```sh
go test ./internal/daemon/api/admin ./internal/cli/cmd ./internal/automation/service
```

## Phase CAE-8: Scheduled automation clustering

Status: complete for space-scoped scheduled workflow bindings. Domain-wide
scheduled bindings without a space scope remain fail-closed/no-op in Raft mode
because they do not identify an owning space partition.

Goal: enable scheduled automations only after they have single-owner checkpoint
semantics.

Tasks:

1. Partition scheduled bindings by space scope.
2. Evaluate schedules only on the current partition leader.
3. Make schedule checkpoints Raft-owned.
4. Use deterministic schedule invocation IDs based on binding ID and scheduled
   fire time/window.
5. Reconcile missed windows after failover according to durable checkpoint
   state, one due window per scheduler pass.

Acceptance criteria:

- Only one pod enqueues a scheduled invocation for a due window.
- Missed windows are handled deterministically after failover.
- Duplicate scheduled inference/graph writes are prevented.

Tests:

- [x] Multi-node due-schedule test with one invocation.
- [x] Duplicate schedule pass is idempotent after checkpoint update.
- [x] Failover before checkpoint update does not duplicate the deterministic
  invocation and lets the new leader advance the Raft-owned checkpoint.
- [x] Workflow instance/step runtime state is Raft-owned and snapshot/restored.

Validation:

```sh
go test ./internal/automation/service ./internal/daemon/app
make test-phase-d
```

## Phase CAE-9: End-to-end dev stack validation

Status: partially complete. Automated local Compose, K3s cluster, and K3s
Raft disruption smoke/edges validation passed; manual Knot PKM onboarding and
page-summary evidence remains to be captured.

Goal: prove the original Knot PKM scenario works end-to-end in a three-node Raft
stack.

Manual validation steps:

1. Start the three-node local Mycel cluster.
2. Create or identify a user content space/domain.
3. Run Knot PKM onboarding.
4. Confirm Intelligence Access remains provisioned:

   ```sh
   mycel inference profile list --space-id <space-id>
   mycel inference grant list --space-id <space-id>
   mycel inference policy list --space-id <space-id>
   ```

5. Confirm graph automation configuration exists from more than one node:

   ```sh
   mycel --daemon-addr 127.0.0.1:9091 procedure list --domain-id <content-domain-id>
   mycel --daemon-addr 127.0.0.1:9092 procedure list --domain-id <content-domain-id>
   mycel --daemon-addr 127.0.0.1:9093 procedure list --domain-id <content-domain-id>

   mycel --daemon-addr 127.0.0.1:9091 automation-binding list --domain-id <content-domain-id>
   mycel --daemon-addr 127.0.0.1:9092 automation-binding list --domain-id <content-domain-id>
   mycel --daemon-addr 127.0.0.1:9093 automation-binding list --domain-id <content-domain-id>
   ```

6. Update a Page/Entry graph object that should trigger page-summary.
7. Confirm exactly one invocation/run is created.
8. Confirm exactly one inference request and one output graph update occur.
9. Restart the leader pod during pending/running work and verify reconciliation.

Automated validation:

```sh
make test
make test-phase-d
make test-phase-f
make test-compose-cluster
```

Optional destructive K3s validation:

```sh
make test-k3s-cluster
make test-k3s-raft-disruption-smoke
make test-k3s-raft-disruption-edges
```

Latest automated evidence captured in this tranche:

- `make test`
- `make test-compose-cluster`
- `make test-k3s-cluster`
- `make test-k3s-raft-disruption-smoke`
- `make test-k3s-raft-disruption-edges`

## Rollout order

Recommended order:

1. CAE-1 state audit.
2. CAE-2 deterministic invocation IDs.
3. CAE-3 Raft-owned invocation/claim/run/idempotency state.
4. CAE-4 leader-only worker ownership.
5. CAE-5 fenced graph output commits.
6. CAE-6 failover reconciliation.
7. CAE-7 admin/runtime API consistency.
8. CAE-8 scheduled automation clustering.
9. CAE-9 full dev-stack validation.

The system should not enable clustered automation execution until at least
CAE-2 through CAE-5 are complete. Space-scoped scheduled workflow execution is
enabled in clustered mode through leader-only scheduling and Raft-owned
checkpoints/workflow runtime state. Domain-wide scheduled bindings without a
space scope remain fail-closed/no-op until they have an explicit partition
ownership model.

## Evidence checklist

Before marking this plan implemented, capture:

- changed files summary;
- new Raft record types and ownership classification;
- tests added for deterministic IDs, Raft replay, snapshots, leader ownership,
  claims, fencing, and failover;
- `make test` output;
- `make test-phase-d` output;
- `make test-phase-f` output;
- Compose three-node validation output;
- manual Knot PKM onboarding evidence showing page-summary procedure/binding and
  exactly-one invocation behavior.
