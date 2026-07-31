# Phase D — Raft Command Coverage Implementation Plan

## Status

In progress on `improved_clustering`. D0 is complete; D1/D2/D3/D4 have initial raft command coverage and focused tests; implementation tranches D5-D8 remain.

## Goal

Every durable, user-visible subsystem must have an explicit raft-mode consistency model. In multi-node raft mode, no durable user-visible state may be silently written only to one pod unless it is explicitly documented as derived/rebuildable or intentionally unsupported and fail-closed.

## Non-goals

- Do not implement arbitrary-pod client routing here; that is Phase E.
- Do not implement strong read-index semantics here except where needed to validate raft command coverage; that is Phase F.
- Do not build divergence repair tooling here; that is Phase G.
- Do not commit generated API/SDK code unless explicitly approved.

## Design rules

1. **Prefer raft ownership over local WAL in clustered mode.** If `MYCELD_CLUSTER_RAFT_NODE_ADDRS` configures a multi-node raft cluster, durable subsystem writes must use system raft or the deterministic partition raft group.
2. **Fail closed for gaps.** If a subsystem is not safe in raft mode yet, writes must return an explicit retryable/failed-precondition error rather than using local WAL/file state.
3. **Classify derived state explicitly.** Rebuildable caches may remain local, but their authoritative inputs/checkpoints/config must be raft-owned or disabled.
4. **Keep subsystem ownership.** Service behavior remains under each subsystem package; daemon remains the composition root.
5. **Make command coverage auditable.** Each WAL/durable record type must appear in a coverage inventory with owner, scope, apply path, tests, and status.

## Current known record inventory

This table is the starting audit map. D0 must confirm it against code before implementation begins.

| Subsystem | Record / state | Current known state | Phase D target |
| --- | --- | --- | --- |
| Cluster metadata | system metadata/bootstrap | System raft authoritative for static V1 | Keep; add coverage inventory/tests. |
| Admin identity | `identity.admin.put.v1`, `identity.admin.session.put.v1` | System raft apply/propose paths exist | Verify all admin durable writes use system raft in raft mode. |
| User identity | `identity.user.put.v1`, `identity.user.session.put.v1` | System raft apply/propose paths exist | Verify all user durable writes use system raft in raft mode. |
| Spaces/domains/ACL | `space.create_with_default_domain.v1`, `space.domain.create.v1`, `space.domain.update.v1`, `space.domain.delete.v1`, `space.acl.grant.v1` | Partition raft paths exist | Verify all writes use partition raft; close gaps. |
| Spaces | `space.delete.v1` | Initial partition raft coverage added in D1 | Continue multi-node validation and idempotency hardening. |
| Graph | `graph.commit.v1` | Partition raft commit path exists; Phase A fail-closed added | Verify every graph mutation reaches commit path and no local durable bypass remains. |
| Blob metadata | `blob.meta.put.v1`, `blob.meta.delete.v1` | Partition raft metadata paths exist | Verify metadata routes/proposals; document payload model and add negative tests. |
| Blob payloads | payload files/content | Payload availability is separate from metadata | Define V1 policy: shared/object-backed, replicated, or fail closed when payload missing. |
| Schema | `schema.put.v1`, `schema.delete.v1` | Initial partition raft coverage added in D2 | Continue graph-validation consistency validation. |
| Semantic global config | `semantic.global.mutation.v1` | Initial system raft coverage added in D4 | Continue multi-node/system replay validation. |
| Semantic space config/checkpoints/accounting | `semantic.space.mutation.v1`, `semantic.maintenance.mutation.v1`, `semantic.accounting.mutation.v1` | Space/maintenance use partition raft; accounting uses system raft | Continue authoritative-vs-derived validation and operational docs. |
| Embedding provider keys | `embedding.provider_key.put.v1`, `embedding.provider_key.delete.v1` | Legacy WAL-backed store superseded by semantic global credentials | Keep out of raft-mode daemon runtime; remove or migrate legacy API surface later. |
| Backup | `daemon.backup.policy.update.v1`, `daemon.backup.delete.v1` | WAL-backed | Policy/config should be system raft; execution should be single-runner/leader-owned or fail closed. |
| Change streams | checkpoints/events | Existing subsystem has durable/local behavior | Events should derive from committed raft changes; checkpoints need explicit model. |
| Automations | definitions/runs/audit | Persistent user-visible automation state exists outside this inventory | Audit stores; raft definitions/audit or leader-owned scheduler. |

## Phase D0 — Coverage inventory and guardrails

### Status

Initial implementation complete. See `phase-d-raft-record-coverage-inventory.md` and `internal/clustering/consensus/raft_record_coverage_test.go`. Remaining D0 work is to expand the inventory beyond WAL record types to durable stores that do not declare `wal.RecordType` constants.

### Tasks

- [x] Add a documented inventory under this plan or a generated/maintained table listing:
  - record type / store path;
  - subsystem owner;
  - raft scope: system, space partition, derived local, unsupported;
  - propose path;
  - apply path;
  - read consistency assumptions;
  - tests.
- [x] Add a lightweight static or unit test that fails when a WAL record type is registered but not classified. This can start as an explicit allowlist in a test, not generated code.
- [ ] Add a common cluster-mode helper/interface where useful so subsystems can fail closed when their raft executor is absent.
- [x] Update `docs/design/clustering-replication-reliability.md` to reference this detailed Phase D plan.

### Acceptance

- Every known WAL record type has a classification.
- New record types require an explicit consistency classification before tests pass.

## Phase D1 — Space/domain/ACL closure

### Why first

Spaces/domains/ACL decide ownership and validation context for graph/schema/semantic operations. Gaps here undermine every later subsystem.

### Status

Initial D1 implementation is in place: space/domain/ACL raft paths now include `space.delete.v1`, and metadata proposals fail closed when a partition group or leader is unavailable.

### Tasks

- Confirm all space/domain/ACL write APIs use raft paths in raft mode.
- Add partition raft command support for `space.delete.v1`, or fail closed in raft mode if delete is not supported yet.
- Verify domain create/update/delete commands are idempotent and replay-safe.
- Verify ACL/grant behavior is partition-owned and consistent across replicas.
- Add tests for no-leader/missing-group failures returning clear unavailable/failed-precondition errors.

### Acceptance

- No space/domain/ACL write commits to local WAL only in multi-node raft mode.
- Space/domain/ACL raft apply survives restart/replay in component tests.

## Phase D2 — Schema subsystem raft ownership

### Why high priority

Schema affects graph validation. If schemas diverge per pod, graph writes can pass validation on one pod and fail on another.

### Status

Initial D2 implementation is in place: schema put/delete use partition raft in raft mode, keyed by domain ID for the V1 domain-owned schema boundary, and fail closed when the partition group or leader is unavailable.

### Tasks

- Decide ownership: likely partition raft by domain/space ownership; system raft only if schemas are global.
- Add raft command builders/apply paths for:
  - `schema.put.v1`;
  - `schema.delete.v1`.
- Route schema writes through raft in raft mode.
- Make schema writes fail closed if partition group/leader is unavailable.
- Ensure graph schema validation reads from a schema view consistent with the transaction/space/domain route, or document the V1 boundary until Phase F.
- Add tests:
  - schema create/update/delete replicates in an in-process multi-node harness;
  - graph validation sees the same schema after raft apply;
  - no-leader schema write fails closed;
  - local WAL path remains valid in standalone mode.

### Acceptance

- Schema state cannot silently diverge across raft pods.
- Graph validation cannot depend on pod-local schema state in raft mode.

## Phase D3 — Blob metadata and payload safety

### Status

Initial D3 implementation is in place: blob metadata put/delete use partition raft in raft mode, metadata apply verifies/materializes payloads before exposing metadata, raft blob proposals fail closed without a leader, raft deletes no longer remove local payloads before consensus commits, raft delete apply rechecks graph references, and graph commits validate blob references through the blob subsystem before committing blob nodes.

V1 payload policy is **payload replicated/catch-up verified**: the proposer must have a local payload and, for configured multi-node raft deployments, authoritative cluster ID plus remote backend addresses must be available so followers can fetch and checksum-verify payloads while applying `blob.meta.put.v1`.

### Tasks

- Verify `blob.meta.put.v1` and `blob.meta.delete.v1` always use partition raft in raft mode.
- Confirm metadata apply is idempotent and replay-safe.
- Define V1 payload policy:
  1. shared/object-backed payload store, or
  2. payload replicated/catch-up verified, or
  3. blob operations fail closed in multi-node raft unless payload safety is configured.
- Ensure a graph node cannot commit a blob reference that only exists on one pod unless payload access is safe cluster-wide.
- Add tests for:
  - blob metadata replication;
  - missing payload failure mode;
  - backend payload forwarding/auth behavior;
  - restart/replay metadata consistency.

### Acceptance

- Blob metadata is raft-owned.
- Blob payload availability has an explicit enforced policy; no silent metadata/payload split-brain.

## Phase D4 — Semantic and embedding configuration classification

### Status

Initial D4 implementation is in place: semantic global configuration/credentials/policies route through system raft in raft mode, semantic accounting is classified as authoritative append-only audit and routes through system raft, semantic space and maintenance records remain partition-raft-owned, and legacy embedding provider-key WAL records are classified as unsupported/superseded by semantic global credentials in the raft-mode daemon runtime.

Derived vector/index data remains local/rebuildable from graph plus semantic configuration and is not treated as authoritative raft state in this tranche.

### Tasks

- Split semantic state into:
  - authoritative configuration/credentials/policies/checkpoints;
  - derived/rebuildable vector/index data;
  - accounting/audit records.
- Decide raft scope for each authoritative record:
  - global records likely system raft;
  - space records likely partition raft.
- Add or complete raft command coverage for:
  - `semantic.global.mutation.v1`;
  - `semantic.space.mutation.v1`;
  - `semantic.maintenance.mutation.v1`;
  - `semantic.accounting.mutation.v1`, if authoritative;
  - `embedding.provider_key.put.v1` / `delete.v1`, if still separate from semantic global config.
- For derived vector data, document freshness and rebuild behavior; ensure search responses do not imply stronger guarantees than provided.
- Add tests for rafted semantic config replication and derived-local rebuildability assumptions.

### Acceptance

- Authoritative semantic/embedding config cannot diverge silently.
- Derived semantic/index data is explicitly documented and operationally rebuildable.

## Phase D5 — Backup, automation, and change-stream ownership

### Backup

- Raft backup policy/config through system raft.
- Ensure backup execution is leader-elected or single-runner; non-leaders should not independently execute the same scheduled backup.
- Define retention/delete behavior under raft.

### Automations

- Audit automation definitions, invocations, run state, outputs, and audit records.
- Raft definitions/audit records or mark automation worker execution as leader-owned.
- Ensure workers do not execute the same durable invocation on multiple pods unless designed to be idempotent.

### Change streams

- Define committed raft change source for graph events.
- Decide checkpoint ownership: system raft, partition raft, or client-owned external checkpoint.
- Ensure checkpoint writes do not diverge per pod in raft mode.

### Acceptance

- Backup, automation, and change-stream durable state have explicit raft-mode behavior or fail closed.

## Phase D6 — Composite state-machine coverage and unknown command hardening

### Tasks

- Add tests around `compositeSystemStateMachine` and `compositePartitionStateMachine` to prove every classified record type is accepted by exactly one intended subsystem apply path.
- Unknown record types should fail loudly with group/scope/record context.
- Add metrics/logs for unsupported raft record types so operational failures are diagnosable.
- Consider a startup self-check that compares the registered/classified raft record set against subsystem capabilities.

### Acceptance

- A missing raft command handler is caught by tests before release.
- Runtime unsupported-record errors are actionable.

## Phase D7 — Multi-subsystem integration tests

### Tests

- Create a space/domain/schema, then graph records that require that schema; verify replicas converge.
- Create blob metadata/payload and graph blob node; verify reads from all active replicas or documented safe failure.
- Configure semantic policy/index, write graph data, run/search derived index; verify authoritative config convergence.
- Restart all nodes after multi-subsystem writes; verify state reload/replay convergence.
- Delete/update records for each covered subsystem and verify replay/idempotency.

### Acceptance

- Multi-subsystem workflows do not diverge after restart or follower catch-up.

## Phase D8 — Documentation and release gate updates

### Tasks

- Update `docs/design/clustering-replication-reliability.md` Phase D status.
- Update operations docs with unsupported/fail-closed subsystem behavior that remains after D.
- Add `make test-phase-d` once focused tests exist.
- Extend `make test-cluster-release-gate` only after the tests are stable enough for pre-release use.

### Acceptance

- Operators know which subsystem features are supported in multi-node raft mode.
- Release gates prove Phase D coverage without requiring every PR to run destructive clusters.

## Suggested implementation order

1. D0 inventory and guardrail test.
2. D1 space/domain/ACL closure, especially `space.delete.v1` decision.
3. D2 schema raft ownership.
4. D3 blob metadata/payload policy.
5. D4 semantic/embedding classification and raft gaps.
6. D5 backup/automation/change-stream ownership.
7. D6 command-handler hardening.
8. D7/D8 integration tests and docs/release gates.

## Definition of done

Phase D is complete when:

- every durable record type is classified;
- every user-visible durable write in raft mode uses system/partition raft or fails closed;
- derived/local-only state is documented and rebuildable;
- subsystem raft apply paths are replay-safe and idempotent;
- multi-subsystem tests prove convergence after restart;
- operator docs clearly identify remaining unsupported cluster-mode features, if any.
