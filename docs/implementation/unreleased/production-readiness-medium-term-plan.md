# Medium-term production readiness plan

Status: planning  
Last updated: 2026-11-18

This document captures the medium-term work needed to make MycelDB safe to run
as a production service on AWS/EKS for CommonFolio-style multi-tenant workloads.
It is intentionally broader than a single release plan: each area below should be
split into focused issues and implementation plans before execution.

## Goals

- Preserve customer data through upgrades, operator mistakes, and infrastructure
  failures.
- Allow clusters to grow from the initial three-pod deployment without a risky
  manual migration.
- Provide authoritative workspace usage values that CommonFolio can use for
  billing and quota decisions.
- Expose enough infrastructure capacity and pod telemetry to understand whether
  the cluster has room to serve current and future workloads, including
  heterogeneous node types.
- Characterize and improve performance for writes, reads, lexical search,
  semantic search, and mixed workloads.
- Make production state observable, auditable, and repairable.

## Non-goals and ownership boundaries

- Mycel must not own pricing, plans, invoices, subscription state, quota policy,
  grace periods, or customer messaging. CommonFolio owns those product and
  billing decisions.
- Mycel should expose current authoritative usage values and operational
  capacity observations. CommonFolio decides how to interpret those values for
  warnings, throttling, or billing.
- High-frequency operational telemetry such as CPU usage, memory usage, disk
  pressure, and network throughput should not be replicated through Raft. Raft is
  reserved for durable cluster membership, topology, placement, committed graph
  state, and deterministic usage counters.
- Operational physical-size observations must not be treated as billing-safe
  usage unless they are explicitly designed, labeled, and validated for that
  purpose.

## Cross-cutting design principles

1. **Separate authoritative accounting from observations.** Billing/quota-safe
   usage should be logical, deterministic, and grounded in committed state.
   Physical disk, index, WAL, object-store, CPU, memory, and network values are
   operational observations unless explicitly promoted to an authoritative
   accounting field.
2. **Expose freshness and authority.** Usage and telemetry APIs should state
   when values were measured, what source produced them, whether they are exact
   or approximate, and the committed revision/log position they represent when
   applicable.
3. **Prefer restore drills over theoretical backup confidence.** A backup is not
   trusted until it has been restored and verified in an isolated environment.
4. **Design for small initial clusters and future heterogeneous clusters.** The
   initial AWS deployment may run three similar pods, but APIs and placement
   metadata should not assume homogeneous equipment forever.
5. **Keep operational controls reversible where possible.** Scaling, upgrade,
   drain, backup, restore, and repair flows need preflight checks, dry-run
   support, and explicit failure states.
6. **Qualify with Mycel Lab.** New production-critical workflows should have
   Mycel Lab coverage, including dry-run preflights and destructive/recovery
   suites where appropriate.

## Workstream 1: Backup, restore, and rollback safety

### Problem

Operators need an easy, trustworthy way to back up the database before upgrades,
restore if an upgrade fails, and prove that customer data can survive disaster
or operator error.

### Desired capabilities

- Trigger an on-demand backup from CLI/API with clear progress and terminal
  status.
- Schedule recurring backups to durable object storage such as S3.
- Include all durable system state required to restore a functional cluster:
  Raft state/snapshots, graph state, identity/session metadata, space/domain
  metadata, attachment metadata, projection metadata, and any required object
  payloads or object-store references.
- Clearly distinguish required restore data from rebuildable state such as
  lexical/semantic indexes that can be regenerated from authoritative graph and
  attachment metadata.
- Restore into a new cluster for validation or rollback.
- Support pre-upgrade backup and post-upgrade restore drills.
- Verify backup integrity before considering it usable.
- Document operator runbooks for backup, restore, rollback, and failed-restore
  escalation.

### Medium-term deliverables

- `mycel backup create`, `mycel backup list`, `mycel backup verify`, and
  `mycel restore` operator flows, or equivalent admin APIs plus CLI wrappers.
- Backup manifests with version, source cluster identity, committed revision,
  included subsystems, object references, checksums, and encryption metadata.
- Restore verification that checks graph/domain identity, attachment manifests,
  Raft/log consistency, projection fences, and index rebuild requirements.
- Mycel Lab backup/restore suites that run against release-candidate images and
  validate cleanup/artifact safety.

### Open questions

- Which restore modes are required first: restore into a new cluster only, or
  also in-place restore over an existing cluster?
- How much index state should be backed up versus rebuilt after restore?
- What backup retention, encryption, and key-management defaults should the AWS
  deployment use?

## Workstream 2: Dynamic cluster scaling and pod membership

### Problem

The initial deployment can start with three pods, but production must support
adding capacity without rebuilding the database or performing a risky manual
migration.

### Desired capabilities

- Add pods/nodes to an existing cluster safely.
- Discover and admit new pods through an authenticated, operator-controlled
  membership flow.
- Rebalance partition groups, Raft roles, and background work across available
  capacity.
- Drain and remove pods without data loss.
- Preserve quorum and availability during membership changes.
- Surface clear readiness, placement, and rebalance progress.

### Medium-term deliverables

- Cluster membership APIs/runbooks for add, drain, remove, and replace.
- Placement metadata that can represent pod capacity class, zone, storage class,
  and role constraints.
- Rebalance planning with dry-run output explaining data movement, leadership
  movement, and expected risk.
- Mycel Lab scenarios for add-pod, drain-pod, remove-pod, and interrupted
  rebalance recovery.

### Open questions

- What is the first supported scaling unit: whole daemon pod, partition group,
  domain, or a more explicit shard abstraction?
- Should early rebalancing be manual/operator-approved only, with autoscaling
  recommendations added later?
- What minimum AWS/EKS primitives must be available before dynamic scaling is
  supported?

## Workstream 3: Workspace usage accounting for billing and quota

### Problem

CommonFolio needs to know current workspace size and usage to bill users and
throttle database use. These values must be more stable and auditable than
ordinary operational metrics.

### Ownership boundary

Mycel owns measurement. CommonFolio owns policy.

Mycel should expose current usage values such as:

- workspace/space/domain identifiers;
- graph logical bytes;
- node and edge counts;
- property/payload logical bytes;
- attachment count and logical bytes;
- pending-delete attachment logical bytes;
- vector/semantic object counts when they are quota-relevant;
- measurement time, committed revision, freshness, and authority.

CommonFolio should decide:

- plan limits;
- billable units and pricing;
- warning thresholds;
- hard throttling behavior;
- grace periods;
- invoice and subscription state.

### Authority classes

Usage fields should be explicitly classified, for example:

- `RAFT_COMMITTED_EXACT`: deterministic logical counters derived from committed
  state and safe for quota decisions.
- `STATE_MACHINE_EXACT`: exact local materialized values that are persisted in
  snapshots/checkpoints and recoverable from the committed log.
- `OBSERVED_APPROXIMATE`: operational observations such as physical disk usage,
  object-store physical bytes, or index file bytes.
- `REBUILDABLE_OPERATIONAL`: derived state such as lexical/semantic index size
  and freshness, useful for diagnostics but not inherently billable.

### Medium-term deliverables

- Current usage API by workspace/space and optionally domain.
- Stable units and field semantics for billable logical bytes and object counts.
- Raft-backed or replay-safe counters for quota-relevant logical usage.
- Snapshot/checkpoint support for counters so restore does not require an
  expensive full rescan before enforcing quotas.
- Tests for concurrent writes/deletes, restore/replay, pending-delete handling,
  and counter reconciliation.
- Documentation that explicitly says which fields CommonFolio may use for hard
  throttling and which are diagnostics only.

### Open questions

- Is the external product unit `workspace_id` mapped one-to-one to Mycel
  `space_id`, or is a separate workspace mapping required?
- Which attachment states count toward quota: active only, active plus
  pending-delete, or policy-defined by CommonFolio?
- Should semantic vectors count as graph-derived logical usage, operational
  index usage, or a separate quota dimension?

## Workstream 4: Infrastructure capacity inventory and pod telemetry

### Problem

Operators need to know the underlying capacity of each pod and node: disk size,
CPU type, memory, current usage, storage pressure, and hosted workload. Future
heterogeneous clusters require this information for placement and capacity
planning.

### Desired capabilities

- Cluster-level capacity summary:
  - total/healthy pod count;
  - total and allocatable CPU;
  - current CPU usage;
  - total and current memory usage;
  - persistent storage capacity, used, and free bytes;
  - object-store usage when observable;
  - leader and partition distribution;
  - capacity pressure status.
- Per-pod/node inventory:
  - pod ID, node ID, Kubernetes node name;
  - region/zone;
  - instance type if available;
  - CPU architecture and model if available;
  - CPU cores/allocatable CPU/current usage;
  - memory capacity/current usage;
  - PVC capacity/used/free bytes;
  - storage class;
  - Mycel version and uptime;
  - hosted partition groups and Raft roles;
  - indexing/semantic workload state.
- Freshness and source metadata for every sampled value.

### Medium-term deliverables

- Capacity summary API and per-node/pod telemetry API.
- Kubernetes/EKS collector for pod, PVC, cgroup, and node metadata.
- Optional cloud metadata enrichment for instance type and zone.
- Capacity-pressure diagnostics that separate storage pressure, CPU pressure,
  memory pressure, and under-replicated placement.
- Placement metadata that can support future heterogeneous equipment decisions
  without committing high-frequency telemetry through Raft.

### Open questions

- Which telemetry should be collected by Mycel daemons directly versus a
  sidecar/operator integration?
- Which values are available in all supported deployment modes, not just EKS?
- What is the first supported resource-class model for heterogeneous clusters?

## Workstream 5: Performance characterization and improvement

### Problem

We need to understand the cost and scaling behavior of insertion, graph reads,
lexical search, semantic search, hybrid search, backups, restore, and background
projection/indexing before we can set SLOs or capacity guidance.

### Desired capabilities

- Repeatable benchmarks for:
  - single-row and bulk graph insertion;
  - large atomic transactions;
  - indexed graph reads;
  - GQL/structured query shapes;
  - lexical search;
  - semantic search;
  - hybrid search;
  - attachment upload/download metadata paths;
  - backup and restore throughput;
  - concurrent mixed read/write/search workloads.
- Load profiles that match expected CommonFolio use cases.
- Baseline latency/throughput numbers for three-pod AWS clusters.
- Profiling hooks for CPU, memory, lock contention, Raft proposal latency,
  storage latency, and index/projection lag.
- Regression gates for critical performance paths.

### Medium-term deliverables

- Benchmark harnesses and documented workload fixtures.
- Performance dashboards and historical result storage.
- Initial SLO proposals for common API paths.
- Issue backlog for measured bottlenecks, prioritized by production impact.
- Mycel Lab performance qualification mode for release candidates.

### Open questions

- Which user-facing operations need hard SLOs first?
- What dataset shapes best represent early production workspaces?
- What minimum benchmark must pass before a daemon release can be promoted?

## Workstream 6: Observability and alerting

### Problem

Production operators need to know whether the cluster is healthy, why it is
unhealthy, and whether users are at risk before failures become data-loss or
availability incidents.

### Desired capabilities

- Metrics, structured logs, traces, and dashboards for:
  - Raft leadership, quorum, proposal latency, and replication lag;
  - graph write/read latency and error rates;
  - lexical and semantic index readiness;
  - semantic projection lag and freshness policy outcomes;
  - backup/restore status and most recent successful restore drill;
  - auth/session failures and delegated-session stale-client reasons;
  - capacity pressure;
  - per-workspace usage measurement freshness;
  - background job queues and provider failures.
- Alert rules for quorum risk, storage pressure, backup failure, stale usage
  counters, index/projection outage, elevated auth failures, and release-gate
  failure.

### Medium-term deliverables

- Metrics contract and dashboard bundle for the AWS deployment.
- Alert runbooks tied to actionable remediation steps.
- Redaction policy for logs and Mycel Lab artifacts.
- Console/operator UI surfaces for top-level cluster health.

## Workstream 7: Upgrade and rollback lifecycle

### Problem

Even with backups, operators need a predictable upgrade path and clear rollback
rules, especially when releases change storage formats, Raft records, or derived
projection behavior.

### Desired capabilities

- Pre-upgrade checks that validate backup freshness, cluster health, API/SDK
  compatibility, and release-bundle availability.
- Rolling upgrade procedure with readiness gates.
- Canary or blue/green option for high-risk releases.
- Post-upgrade checks that validate data access, search readiness, usage
  counters, and backup viability.
- Clear downgrade boundaries when newer state cannot be read by older daemons.
- Rollback runbook that chooses between binary rollback and restore-from-backup.

### Medium-term deliverables

- Release-candidate checklist and hermetic release execution bundle policy.
- Compatibility metadata consumed by deployment gates.
- Upgrade/rollback runbooks for AWS/EKS.
- Mycel Lab release-gate coverage that exercises backup, restore, and smoke
  traffic around upgrades.

## Workstream 8: Integrity, verification, and repair tooling

### Problem

Operators need tools to answer whether a cluster is internally consistent and
what to do when it is not.

### Desired capabilities

- `mycel doctor` or equivalent diagnostics for:
  - Raft membership and committed/applied state;
  - graph storage and indexes;
  - space/domain metadata;
  - identity/session stores;
  - attachment metadata and object-store references;
  - lexical/semantic indexes and projections;
  - usage counters versus recomputed samples.
- Backup verification independent of a full production restore.
- Repair or rebuild flows for derived state such as indexes and projections.
- Orphan attachment detection and safe cleanup.

### Medium-term deliverables

- Read-only cluster consistency report.
- Backup verifier with machine-readable output.
- Index/projection rebuild commands with dry-run and progress.
- Counter reconciliation tooling that reports drift without silently changing
  billing-relevant values.

## Workstream 9: Security and auditability

### Problem

Production operations require strong controls around data access, backups,
admin actions, delegated sessions, and diagnostic artifacts.

### Desired capabilities

- Audit log for admin actions such as backup, restore, add pod, drain pod,
  remove pod, quota-affecting usage reads, and security-sensitive auth actions.
- Backup encryption, key rotation, and least-privilege object-store access.
- Clear token/session rotation and delegated-session revocation behavior.
- Tenant isolation checks in backup, restore, search, and usage APIs.
- Redaction of credentials, tokens, private payloads, and tenant data in logs and
  reliability artifacts.

### Medium-term deliverables

- Admin audit event model and retention policy.
- Backup/restore security model and AWS IAM guidance.
- Sensitive-field redaction tests for logs/artifacts.
- Security review checklist for production-critical operator flows.

## Suggested sequencing

### Phase 0: Inventory and issue decomposition

- File focused issues for each workstream.
- Define ownership boundaries between Mycel, CommonFolio, deployment tooling, and
  Mycel Lab.
- Inventory existing backup/restore, cluster membership, telemetry, and usage
  code paths.

### Phase 1: Safety and accounting foundation

- Deliver easy backup/restore with restore validation.
- Deliver current workspace usage accounting for billing/quota integration.
- Add minimum observability for backup freshness, restore status, usage freshness,
  and cluster health.

### Phase 2: Cluster operations and capacity visibility

- Deliver add-pod/drain/remove-pod workflows.
- Deliver cluster capacity inventory and per-pod telemetry.
- Add placement metadata and rebalance dry-run plans.

### Phase 3: Performance and operational hardening

- Establish benchmark baselines and release-regression checks.
- Improve bottlenecks discovered under representative workloads.
- Expand integrity/repair tooling and production alert runbooks.

### Phase 4: Automation and heterogeneous optimization

- Add autoscaling recommendations or controlled automation.
- Use resource classes and telemetry to inform placement on heterogeneous
  equipment.
- Mature Console/operator UI for self-service operations.

## Immediate next steps

1. Create issues for the four highest-priority tracks:
   - backup/restore/rollback safety;
   - workspace usage accounting;
   - add-pod/dynamic membership;
   - infrastructure capacity inventory and pod telemetry.
2. Draft detailed design documents for workspace usage accounting and capacity
   telemetry because those APIs establish long-lived product and operations
   boundaries.
3. Extend Mycel Lab planning for backup/restore, add-pod, and release-gate
   upgrade/rollback scenarios.
4. Define the first production benchmark suite and dataset shape for CommonFolio
   workloads.
