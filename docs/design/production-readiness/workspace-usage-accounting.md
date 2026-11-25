# Workspace usage accounting for billing and quota

Status: draft  
Last updated: 2026-11-23

## Production requirement

Applications that use Mycel for paid or quota-limited workspaces need
authoritative current workspace usage values to bill users and throttle database
use. These values must be more stable, auditable, and precise than ordinary
operational metrics.

This work is mandatory for paid or quota-limited production deployments.
Approximate disk metrics are not sufficient for billing or hard throttling.

## Ownership boundary

Mycel owns measurement. The integrating application owns policy.

Mycel should expose current authoritative usage values. The integrating
application should own:

- plan limits;
- pricing;
- subscription state;
- invoices;
- warning thresholds;
- grace periods;
- hard or soft throttling decisions;
- customer-facing billing behavior.

Mycel should not encode product-plan policy such as "Pro plan has 10 GB".
Instead, Mycel should report values such as "workspace X uses N logical bytes as
of committed revision R".

## Mandatory first-cut capabilities

Before paid/quota-limited production, Mycel should provide:

- a current usage API by workspace/Mycel space, with domain-level filters or
  breakdowns when an application needs sub-workspace accounting;
- stable logical units for:
  - graph logical bytes;
  - node count;
  - edge count;
  - attachment count;
  - attachment logical bytes;
  - pending-delete attachment logical bytes;
- freshness and authority metadata:
  - measured time;
  - committed graph revision or equivalent state marker;
  - exact versus approximate;
  - billing/quota-safe versus diagnostic-only;
- replay/restore-safe counters for quota-relevant usage;
- tests for concurrent writes, deletes, restore/replay, pending-delete handling,
  and counter reconciliation.

## Proposed gRPC API for usage statistics

The initial API should be a read-only admin/control-plane API that lets
integrating applications and operators obtain authoritative usage snapshots. The
primary billing/quota scope is expected to be an application workspace or tenant
mapped to a Mycel space, but the API should also support domain-scoped
accounting because Mycel operations, blob descriptors, import/export, and
object-store paths are already space/domain scoped.

Mycel should report measurements and their authority; the integrating
application should still decide plan limits, warnings, grace periods, billing
effects, and whether to throttle a workspace or domain.

Working service name:

```proto
service AdminUsageAccountingService {
  rpc GetUsage(GetUsageRequest) returns (GetUsageResponse);
  rpc BatchGetUsage(BatchGetUsageRequest) returns (BatchGetUsageResponse);
  rpc ListUsage(ListUsageRequest) returns (ListUsageResponse);
  rpc GetUsageAccountingHealth(GetUsageAccountingHealthRequest) returns (GetUsageAccountingHealthResponse);
}
```

Use an explicit scope message rather than separate space-only and domain-only
APIs:

```proto
message UsageScope {
  oneof scope {
    SpaceUsageScope space = 1;
    DomainUsageScope domain = 2;
  }
}

message SpaceUsageScope {
  string space_id = 1;
}

message DomainUsageScope {
  string space_id = 1;
  string domain_id = 2;
}

enum DomainInclusionMode {
  DOMAIN_INCLUSION_MODE_UNSPECIFIED = 0;
  DOMAIN_INCLUSION_MODE_RETAINED_NON_SYSTEM = 1;
  DOMAIN_INCLUSION_MODE_RETAINED_ALL = 2;
  DOMAIN_INCLUSION_MODE_SYSTEM_ONLY = 3;
}
```

Domains are flat within a space, so a domain usage request always includes both
`space_id` and `domain_id`. There is no global domain-only scope. Application
workspace IDs are intentionally outside this API; applications own any
workspace-to-space mapping.

The service belongs on the admin API rather than the ordinary client API for the
first cut. Applications can expose selected values to end users through their own
product API/UI after applying product policy and tenant authorization.

### `GetUsage`

`GetUsage` returns the current usage snapshot for one scope.

Important request fields:

- `scope`: Mycel space or Mycel domain to measure.
- `domain_inclusion`: for space-scoped requests, choose whether retained
  non-system domains, retained all domains, or only system domains are included.
  The default is retained non-system domains for product-facing quota usage.
- `include_diagnostic_physical`: include non-quota-safe operational observations
  such as physical object-store or index bytes when available.
- `require_quota_safe`: fail closed if a complete quota-safe snapshot cannot be
  produced.
- `max_staleness_seconds`: optional freshness bound for quota decisions.
- `minimum_revision`: optional lower bound when the caller needs usage at or
  after a write it just committed.

The response should include a `UsageSnapshot` with:

- `scope`;
- `observed_at`;
- `as_of_revision` or equivalent state marker;
- `quota_safe_complete`;
- `metrics`;
- `blockers` or warnings when some requested values are unavailable, stale,
  approximate, or diagnostic-only.

### Metric row shape

A metric row keeps the API extensible while still making each value explicit
about units and authority:

```proto
message UsageMetric {
  string name = 1;
  uint32 metric_version = 2;
  uint64 value = 3;
  UsageUnit unit = 4;
  UsageAuthority authority = 5;
  bool quota_safe = 6;
  google.protobuf.Timestamp observed_at = 7;
  UsageRevision as_of_revision = 8;
  map<string, string> labels = 9;
}
```

Metric names should be stable and documented. First-cut quota-safe MVP metrics
should include:

- `graph.node_count`;
- `graph.edge_count`;
- `graph.logical_bytes`;
- `blob.active_logical_bytes`;
- `blob.active_count`;
- `blob.local_active_logical_bytes`;
- `blob.local_active_count`;
- `blob.object_store_active_logical_bytes`;
- `blob.object_store_active_count`;
- `blob.pending_delete_logical_bytes`;
- `blob.pending_delete_count`.

The S3/MinIO/object-store blob-area values above are logical usage derived from
committed Mycel blob metadata and payload descriptors, not a bucket inventory
scan. They are the right first-cut values for billing and quota because they are
associated with a Mycel space/domain and can be replayed/restored with Mycel
state. Because object-store keys are domain-scoped, Mycel can attribute
object-store-backed blob logical bytes to a domain as well as aggregate them to
the containing space.

### Scope and inclusion semantics

Mycel usage accounting is scoped to Mycel spaces and domains. Application
workspace IDs are not part of the Mycel API; applications own any external
workspace-to-space mapping.

For space-scoped snapshots, the default quota/product aggregate is retained
non-system domains in the space. System/internal domains are excluded by default
so application-facing quotas are not affected by Mycel implementation details.
Operator diagnostics may explicitly request retained all domains or system-only
usage with `domain_inclusion`.

For domain-scoped snapshots, graph, blob, and pending-delete metrics are limited
to the requested `(space_id, domain_id)`.

Archived or read-only domains still count while their data is retained. Deleted
or deletion-pending data should not be silently hidden: it should either remain in
active metrics until the corresponding logical delete commits, or move to the
appropriate `pending_delete_*` metrics until purge/reconciliation clears it.
Permanently purged data no longer counts toward current usage.

Cross-domain/shared ownership is out of scope for the first cut. Usage is
attributed to the domain that owns the committed graph/blob metadata. If future
cross-domain references are introduced, their accounting semantics should be
specified in a separate design update.

If the same payload bytes appear in multiple domains, domain-level logical usage
should count each domain's committed logical ownership; physical deduplication or
duplicate object keys are diagnostic storage concerns, not first-cut quota
semantics.

Optional diagnostic metrics may include values such as:

- `object_store.observed_physical_bytes`;
- `object_store.orphan_candidate_bytes`;
- `lexical_index.physical_bytes`;
- `semantic_index.physical_bytes`;
- `vector_store.physical_bytes`.

These diagnostic metrics are not quota-safe by default because they may include
storage-class overhead, object versions, multipart remnants, orphaned objects,
compression effects, or rebuildable derived indexes.

### Batch and list APIs

`BatchGetUsage` should accept multiple scopes and return one snapshot or status
per scope. This is the preferred API for billing jobs that already know the
workspace/space set or a set of domains to reconcile.

`ListUsage` should be paginated and intended for operator views, reconciliation
jobs, and small-to-medium tenant/workspace scans. It should support the same
freshness/diagnostic options as `GetUsage` but must not require callers to infer
quota safety from absent fields.

Useful list modes include:

- list all space snapshots visible to the caller;
- list all domain snapshots under one `space_id`;
- list a space snapshot with optional domain breakdown rows for operator/debug
  views.

A space snapshot with domain breakdown should make clear whether the domain rows
sum exactly to the space-level quota-safe totals. If system/internal domains are
excluded from a product-facing breakdown, the response should report that choice
explicitly rather than silently hiding bytes.

### Usage accounting health

`GetUsageAccountingHealth` reports whether the accounting system itself is safe
to use for quota decisions. It should surface:

- stale counter materialization;
- reconciliation failures;
- spaces with missing quota-safe values;
- restore/replay recovery state;
- diagnostic-only collectors that are unavailable.

This health response is for dashboards and launch gates; it is not a substitute
for scoped snapshots when making workspace, space, or domain billing/throttling
decisions.

### Throttling and usage-limit API

Usage measurement and usage enforcement should be related but not the same API.
`GetUsage`, `BatchGetUsage`, and `ListUsage` are read-only measurement APIs.
They should not accept plan names, prices, subscription state, warning thresholds,
or product-specific quota rules.

Applications can always enforce limits at the application layer by reading
usage, applying product policy, and refusing new writes/uploads before calling
Mycel. That is sufficient for warnings, billing, onboarding gates, and many soft
limits. It is not sufficient as the only hard-limit mechanism when multiple
writers, large imports, direct service-to-Mycel clients, or concurrent uploads
can race between measurement and commit.

If an application needs Mycel to reject writes after application policy has made
a decision, use a separate admission-control/write-gate API. It may live in the
same proto package, but should have separate RPCs and stricter capabilities than
the read-only usage-statistics calls.

Working service shape:

```proto
service AdminUsageAdmissionService {
  rpc UpsertUsageLimit(UpsertUsageLimitRequest) returns (UpsertUsageLimitResponse);
  rpc DeleteUsageLimit(DeleteUsageLimitRequest) returns (DeleteUsageLimitResponse);
  rpc GetUsageLimit(GetUsageLimitRequest) returns (GetUsageLimitResponse);
  rpc ListUsageLimits(ListUsageLimitsRequest) returns (ListUsageLimitsResponse);
}
```

The limit object should be scope-based and metric-based, not plan-based:

```proto
message UsageLimit {
  UsageScope scope = 1;
  string metric_name = 2;              // e.g. blob.active_logical_bytes
  uint64 hard_limit = 3;
  UsageUnit unit = 4;
  UsageLimitMode mode = 5;
  string policy_ref = 6;               // Application-owned non-secret reference.
  string reason = 7;                   // Operator/product reason, non-secret.
  google.protobuf.Timestamp expires_at = 8;
  string idempotency_key = 9;
}

enum UsageLimitMode {
  USAGE_LIMIT_MODE_UNSPECIFIED = 0;
  USAGE_LIMIT_MODE_OBSERVE_ONLY = 1;   // Report over-limit state, do not block.
  USAGE_LIMIT_MODE_SOFT_BLOCK = 2;     // Reject ordinary writes, allow explicit override capability.
  USAGE_LIMIT_MODE_HARD_BLOCK = 3;     // Reject quota-increasing writes without override.
}
```

Mycel still does not know that a workspace is on a "Pro" or "Enterprise" plan.
The application translates product policy into mechanical limits such as
"domain X may not exceed `blob.active_logical_bytes = 10 GiB`" and writes those
limits to Mycel when DB-side enforcement is required.

First-cut enforcement should apply only to quota-safe metrics with deterministic
write deltas. Good candidates are:

- blob upload logical bytes/counts;
- graph node/edge counts;
- graph logical bytes if the committed representation has a stable accounting
  function.

Diagnostic metrics such as physical S3 bucket bytes, index bytes, vector-store
files, and orphan candidates must not be used for hard enforcement.

Enforcement must be checked at commit/admission time using the same counters that
back quota-safe usage snapshots. A preflight read of `GetUsage` is not enough for
hard limits because concurrent writes can race. Large imports and streaming blob
uploads may need an internal reservation or commit-time delta check so Mycel does
not accept quota-increasing state beyond the configured hard limit.

Limit failures should return stable application-visible errors, for example a
gRPC `ResourceExhausted` status with `google.rpc.ErrorInfo` domain
`mycel.usage` and reasons such as `usage_limit_exceeded` or
`usage_write_gate_blocked`. Error details should identify the scope, metric name,
limit, current usage when safe, and attempted delta when known, without exposing
application plan details or secrets.

## Implementation alternatives

Database systems usually expose a mix of physical storage statistics,
approximate operational counters, and exact user-maintained counters. Mycel needs
to distinguish those classes because billing and hard throttling require stronger
semantics than ordinary observability.

### Alternative A: on-demand scans of durable state

The simplest implementation is to compute usage when `GetUsage` is called by
scanning graph state and blob metadata for the requested space or domain.

Pros:

- easy to reason about because values are derived directly from current durable
  state;
- useful as a reconciliation tool even if a faster counter path exists;
- avoids write-path complexity for a first prototype.

Cons:

- expensive for large spaces/domains;
- hard to serve frequently for dashboards, billing jobs, and admission checks;
- requires a read barrier or snapshot semantics to avoid mixing revisions across
  partitions;
- unsuitable as the only mechanism for hard limits because the value can change
  between scan and commit.

This is a good fallback and reconciliation strategy, not the preferred serving
path for production usage APIs.

### Alternative B: exact materialized counters updated on commit

Each quota-safe metric is maintained as a durable counter keyed by usage scope,
for example `(space_id, domain_id, metric_name)`. Graph and blob mutations
compute deterministic deltas and apply them with the same committed operation
that changes user-visible state.

Pros:

- fast `GetUsage` responses;
- replay/restore safe when counters are part of the committed state or rebuilt
  deterministically from it;
- suitable for hard limits when admission checks and counter updates are in the
  same commit path;
- clear distinction between logical quota-safe values and diagnostic physical
  observations.

Cons:

- every mutation path must produce correct idempotent deltas;
- deletes, pending-delete blobs, failed blob uploads, imports, restores, and
  retries need explicit tests;
- cross-partition or space-level limits may require an aggregate counter or
  reservation mechanism to avoid races;
- changing a metric definition may require counter versioning and backfill.

This is the preferred production serving model for quota-safe metrics.

### Alternative C: append-only usage delta ledger

Mutations append usage-delta records to a durable ledger, and a materializer
builds current counters from the ledger.

Pros:

- strong audit/reconciliation story;
- easy to replay and inspect how usage changed over time;
- useful for billing history if the application wants time-series usage exports.

Cons:

- a purely asynchronous materializer can be stale;
- hard limits still need synchronous admission checks or reservations;
- long ledgers need compaction/snapshots;
- more moving parts than simple committed counters.

A ledger can complement materialized counters, but should not be the only
first-cut implementation for current usage or hard limits.

### Alternative D: physical storage observations

Mycel can collect filesystem sizes, index sizes, object-store prefix sizes, S3
Inventory/Storage Lens data, or bucket/list results.

Pros:

- valuable for operations and capacity planning;
- can reveal orphaned objects, object-store lifecycle drift, or index bloat;
- comparable to many database-native storage-statistics APIs.

Cons:

- not reliably attributable to billable logical ownership;
- may include object versions, multipart remnants, compression, storage-class
  overhead, deleted-but-not-compacted data, or rebuildable derived indexes;
- object-store prefix scans can be expensive or eventually consistent depending
  on the source;
- not safe for hard quota enforcement.

These values should remain diagnostic unless a later design explicitly defines a
quota-safe interpretation.

### Alternative E: hybrid counters plus reconciliation

The chosen architecture is a hybrid:

1. serve quota-safe usage from exact committed counters;
2. periodically or on demand run durable-state scans to reconcile counters;
3. expose reconciliation failures through `GetUsageAccountingHealth`;
4. expose physical filesystem/object-store/index observations as diagnostic
   metrics only;
5. use the exact counters, not physical observations, for admission checks.

This gives fast billing/quota reads while preserving a correctness path to catch
counter bugs or restore/replay drift.

### Counter placement choices

There are two main placement choices for exact counters:

- **Partition/local counters**: counters are updated in the same Raft group or
  durable state machine that owns the graph/blob mutation. This scales better and
  keeps deltas close to the data, but space-level totals may need aggregation
  across partitions.
- **Global/system counters**: every mutation also updates a cluster-global
  counter store. This simplifies reads and space-level hard limits, but can
  become a write bottleneck and introduces cross-state-machine coupling.

A practical first cut is to keep authoritative deltas with the owning durable
state and materialize aggregate space/domain counters for the admin API. If hard
space-level limits must span multiple partitions, add a reservation or global
admission component rather than relying on a preflight usage read.

### Write-path enforcement

Hard limits require commit/admission-time enforcement. The implementation should
not rely on a caller reading `GetUsage` and then issuing a write, because
concurrent writers can race. For quota-increasing operations, Mycel should know
or compute an attempted delta and check:

```text
current_quota_safe_counter + attempted_delta <= configured_limit
```

inside the write/admission path.

Blob uploads need special care:

- if content length is known, Mycel can preflight the delta before accepting the
  upload and recheck at metadata commit;
- if content length is unknown, Mycel may need to reject the upload for scopes
  with hard byte limits, enforce a maximum stream size, or stage bytes and check
  before committing metadata;
- failed uploads must not increase active logical bytes;
- successful object-store upload followed by metadata commit failure must not
  increase active logical bytes, though it may create diagnostic orphan bytes.

Large imports may need chunk-level reservations or a final commit-time delta
check before imported graph/blob metadata becomes visible.

### How PostgreSQL approaches similar questions

PostgreSQL exposes many storage and activity statistics, but it generally does
not provide built-in per-tenant logical billing counters or quota enforcement for
application tenants.

Examples:

- `pg_database_size`, `pg_relation_size`, `pg_total_relation_size`, and
  `pg_indexes_size` report physical storage used by databases, tables, indexes,
  and TOAST data.
- `pg_stat_*` views expose operational counters such as tuple activity, blocks,
  temporary bytes, and I/O statistics. These are excellent for observability but
  are not a transactional billing/quota model.
- row counts in catalogs are often estimates; exact `count(*)` requires scanning
  or maintaining separate counters.
- per-tenant quotas are usually implemented by the application, by separating
  tenants into databases/schemas/tablespaces, by external storage/volume quotas,
  or by custom triggers/counter tables.

The PostgreSQL lesson is that physical size APIs are useful, but applications
that need tenant billing or hard limits usually maintain explicit logical
accounting rather than relying only on table/database file sizes.

### How MongoDB approaches similar questions

MongoDB exposes database and collection statistics through commands such as
`dbStats`, `collStats`, and `$collStats`. These include values such as document
count, average object size, logical data size, storage size, index size, and
WiredTiger/storage-engine details. In sharded deployments, these statistics are
aggregated across shards.

MongoDB and MongoDB Atlas also provide operational metrics and alerts for storage
capacity, collection growth, and cluster health. However, MongoDB does not
normally enforce arbitrary per-tenant application quotas inside a shared
collection. If tenants map to separate databases or collections, stats are easier
to attribute. If tenants share collections, applications typically maintain
logical counters or run aggregations/change-stream-driven materializers.

The MongoDB lesson is similar: database-native stats are valuable for operations
and collection/database-level accounting, but shared-tenant billing and hard
usage limits need explicit logical ownership and write-path enforcement.

## Chosen implementation model

Mycel should implement usage accounting with the hybrid model: exact committed
logical counters for quota-safe metrics, reconciliation scans for correctness,
and diagnostic physical collectors for operational visibility.

The implementation should optimize for these invariants:

- quota-safe metrics are deterministic functions of committed Mycel state;
- every quota-increasing or quota-decreasing mutation has an idempotent usage
  delta;
- `GetUsage` serves from durable/materialized counters rather than expensive
  object-store or graph scans;
- reconciliation can recompute usage from durable graph/blob metadata and detect
  or repair drift;
- physical storage observations are never promoted to hard-limit metrics unless a
  later design defines stronger semantics;
- hard usage limits are enforced in the write/admission path with the same
  counters that back quota-safe snapshots.

### Counter model

Usage counters should be keyed by metric definition and scope:

```text
(metric_name, metric_version, scope_type, space_id, domain_id)
```

For a space-scoped counter, `scope_type=space` and `domain_id` is empty. For a
domain-scoped counter, `scope_type=domain` and both `space_id` and `domain_id`
are set.

The first implementation should materialize both domain counters and their
containing space aggregate counters. Updating both with the same idempotent delta
makes `GetUsage(space)` fast and avoids requiring the admin API to fan out across
all domains. Reconciliation should verify that space aggregates equal the sum of
included domain counters plus any explicitly included system/internal scope.

Each metric definition should document:

- unit;
- authority class;
- whether it is quota-safe;
- whether it is valid for hard admission checks;
- how write deltas are computed;
- which domain inclusion mode is used for space aggregates;
- how deletes, pending deletes, imports, restore/replay, and rebuilds affect the
  counter.

Metric definitions should be versioned. If the accounting formula changes,
Mycel should either backfill/reconcile counters to the new version or expose both
versions until callers can migrate.

### Write-delta producers

The first quota-safe delta producers should be graph and blob mutations.

Graph mutations should emit deterministic deltas for:

- `graph.node_count`;
- `graph.edge_count`;
- `graph.logical_bytes`, defined as the UTF-8 byte sum of scalar values inside
  committed node `payload` maps. Map keys and structural JSON syntax do not
  count; labels, properties, metadata, edge payloads, and physical graph storage
  overhead are excluded.

Blob mutations should emit deterministic deltas for:

- `blob.active_logical_bytes`;
- `blob.active_count`;
- `blob.local_active_logical_bytes`;
- `blob.local_active_count`;
- `blob.object_store_active_logical_bytes`;
- `blob.object_store_active_count`;
- `blob.pending_delete_logical_bytes`;
- `blob.pending_delete_count`.

Blob upload accounting should occur when blob metadata becomes committed Mycel
state, not when bytes are merely staged locally or uploaded to object storage. A
successful object-store upload followed by metadata commit failure creates an
orphan diagnostic condition, not active logical usage.

Blob delete semantics should be explicit:

- deleting active metadata decrements active logical bytes/counts;
- if deletion enters a pending-delete state, pending-delete bytes/counts increase
  according to the documented policy;
- final payload cleanup changes diagnostic physical observations only, unless a
  future logical state transition says otherwise.

Imports and bulk writes should produce the same deltas as equivalent ordinary
mutations. They may aggregate deltas internally for efficiency, but the final
commit must remain idempotent and auditable.

### Durability and replay

Usage counters should be durable and replay/restore safe. Acceptable
implementation choices include:

- embedding usage deltas in the same Raft/WAL command that commits the graph/blob
  mutation; or
- writing a usage-delta record with the same operation identity and applying it
  transactionally with the owning mutation.

In either case, replay must be idempotent. Applying the same committed operation
more than once must not double-count usage. Delta records should therefore carry
or derive a stable mutation/operation identity when the underlying state machine
cannot otherwise guarantee exactly-once application.

Backups should include usage-counter state when it is part of durable Mycel
state. Restore should either restore counters directly and then reconcile, or
mark usage accounting as rebuilding until counters are recomputed from durable
metadata. `GetUsage(require_quota_safe=true)` should fail closed while required
quota-safe counters are unavailable or under rebuild.

### API serving path

`GetUsage` should read the materialized counter snapshot for the requested scope,
attach authority/freshness metadata, and return blockers when required metrics
are unavailable or stale. It should not scan S3/MinIO buckets or graph records on
the normal serving path.

`BatchGetUsage` should read multiple materialized snapshots with per-scope status
so one unavailable scope does not hide the status of other scopes.

`ListUsage` should page over materialized scope summaries. Domain breakdowns
should come from domain counters, not from re-scanning user data during the list
call.

`GetUsageAccountingHealth` should report:

- counter materialization lag;
- reconciliation drift;
- rebuild-in-progress state after restore/replay;
- metrics that are unavailable for hard-limit admission;
- diagnostic collector failures.

### Reconciliation path

Mycel should provide an internal reconciliation job that recomputes quota-safe
usage from durable graph/blob metadata and compares the result with materialized
counters.

The first reconciliation mode can be dry-run only and report differences through
`GetUsageAccountingHealth`. A later admin repair mode may update counters after
operator review. Reconciliation evidence should include non-secret scope IDs,
metric names, expected values, observed counter values, and the committed state
marker used for the scan.

Reconciliation is also the safety net for:

- restore/replay recovery;
- metric-version backfills;
- suspected counter bugs;
- import/export validation;
- test fixtures that intentionally corrupt or omit counter state.

### Diagnostic physical collectors

Physical storage collectors should run separately from quota-safe logical
accounting. They may report filesystem, object-store, index, vector-store, and
orphan-candidate observations using `OBSERVED_APPROXIMATE` or
`REBUILDABLE_OPERATIONAL` authority classes.

Object-store physical collection should prefer provider-native inventory or
bounded prefix scans and should remain optional. A missing or failed diagnostic
collector should not make quota-safe logical usage unavailable, but should be
visible in `GetUsageAccountingHealth` and optional diagnostic metrics.

### Admission and hard-limit enforcement

When usage limits are configured, the admission path should use exact quota-safe
counters and the attempted write delta:

```text
current_counter + attempted_delta <= hard_limit
```

This check must happen inside the write/admission path, not as a separate client
preflight. For partition-local counters, domain-level hard limits can usually be
checked where the mutation is committed. Space-level hard limits that span
partitions may require a reservation or aggregate admission component so
concurrent writers cannot exceed the limit by racing.

Admission should reject only quota-increasing operations for the affected metric
and scope. Quota-neutral reads and quota-decreasing deletes should remain allowed
unless a separate administrative block is configured.

## Implementation plan

The implementation should land in staged slices so read-only measurement becomes
usable before hard-limit admission is enabled. Each slice should preserve the
rule that physical observations are diagnostic and quota decisions use only
quota-safe committed counters.

### MVP boundary

The first implementation should support:

- scopes: Mycel space and Mycel domain;
- default space aggregate: retained non-system domains;
- quota-safe blob metrics:
  - `blob.active_logical_bytes`;
  - `blob.active_count`;
  - `blob.local_active_logical_bytes`;
  - `blob.local_active_count`;
  - `blob.object_store_active_logical_bytes`;
  - `blob.object_store_active_count`;
  - `blob.pending_delete_logical_bytes`;
  - `blob.pending_delete_count`;
- quota-safe graph metrics:
  - `graph.node_count`;
  - `graph.edge_count`;
  - `graph.logical_bytes`;
- read-only admin APIs: `GetUsage`, `BatchGetUsage`, `ListUsage`, and
  `GetUsageAccountingHealth`;
- dry-run reconciliation that reports drift but does not repair counters
  automatically.

The first implementation should not use these for hard quota enforcement:

- physical S3/MinIO/object-store bytes;
- PVC/filesystem bytes;
- index/vector/projection bytes;
- orphan-candidate bytes;
- `graph.logical_bytes` hard-limit enforcement until the formula has production history;
- cross-domain/shared-reference accounting.

### Phase 0: finalize contract details

Phase 0 freezes the contract details that affect storage compatibility, SDKs,
and later hard-limit enforcement. The decisions below are the baseline for the
first implementation.

#### Phase 0.1 API contract

The first API should live in `mycel.admin.v1`. The read-only MVP service is:

```proto
service AdminUsageAccountingService {
  rpc GetUsage(GetUsageRequest) returns (GetUsageResponse);
  rpc BatchGetUsage(BatchGetUsageRequest) returns (BatchGetUsageResponse);
  rpc ListUsage(ListUsageRequest) returns (ListUsageResponse);
  rpc GetUsageAccountingHealth(GetUsageAccountingHealthRequest) returns (GetUsageAccountingHealthResponse);
}
```

The hard-limit slice adds a separate admission service rather than mixing
measurement reads with write-gate configuration:

```proto
service AdminUsageAdmissionService {
  rpc UpsertUsageLimit(UpsertUsageLimitRequest) returns (UpsertUsageLimitResponse);
  rpc DeleteUsageLimit(DeleteUsageLimitRequest) returns (DeleteUsageLimitResponse);
  rpc GetUsageLimit(GetUsageLimitRequest) returns (GetUsageLimitResponse);
  rpc ListUsageLimits(ListUsageLimitsRequest) returns (ListUsageLimitsResponse);
}
```

`AdminUsageAdmissionService` is implemented as the DB-side hard write gate for
explicit quota-safe usage limits. Product policy, pricing, warnings, and plan
mapping remain outside Mycel.

Core accounting messages:

```proto
message GetUsageRequest {
  UsageScope scope = 1;
  DomainInclusionMode domain_inclusion = 2;
  bool include_diagnostic_physical = 3;
  bool require_quota_safe = 4;
  uint64 max_staleness_seconds = 5;
  UsageRevision minimum_revision = 6;
  repeated string metric_names = 7;
}

message GetUsageResponse {
  UsageSnapshot snapshot = 1;
}

message BatchGetUsageRequest {
  repeated GetUsageRequest requests = 1;
}

message BatchGetUsageResponse {
  repeated BatchUsageResult results = 1;
}

message BatchUsageResult {
  UsageScope scope = 1;
  oneof result {
    UsageSnapshot snapshot = 2;
    UsageStatus status = 3;
  }
}

message ListUsageRequest {
  UsageScope parent = 1; // empty for spaces; space scope for domains in a space.
  UsageListMode mode = 2;
  DomainInclusionMode domain_inclusion = 3;
  bool include_diagnostic_physical = 4;
  bool require_quota_safe = 5;
  uint64 max_staleness_seconds = 6;
  int32 page_size = 7;
  string page_token = 8;
}

message ListUsageResponse {
  repeated UsageSnapshot snapshots = 1;
  string next_page_token = 2;
}

message UsageSnapshot {
  UsageScope scope = 1;
  DomainInclusionMode domain_inclusion = 2;
  google.protobuf.Timestamp observed_at = 3;
  UsageRevision as_of_revision = 4;
  UsageAccountingState accounting_state = 5;
  bool quota_safe_complete = 6;
  repeated UsageMetric metrics = 7;
  repeated UsageStatus blockers = 8;
  repeated UsageStatus warnings = 9;
}

message UsageMetric {
  string name = 1;
  uint32 metric_version = 2;
  uint64 value = 3;
  UsageUnit unit = 4;
  UsageAuthority authority = 5;
  bool quota_safe = 6;
  bool hard_limit_eligible = 7;
  google.protobuf.Timestamp observed_at = 8;
  UsageRevision as_of_revision = 9;
  map<string, string> labels = 10;
}
```

`UsageStatus` is the common blocker/warning shape. It should include a stable
reason enum/string, severity, message, scope, metric name, and non-secret
evidence. Batch and list calls must report per-scope status instead of failing an
entire response when one scope is stale, unavailable, or unauthorized.

Initial enums:

- `UsageUnit`: `BYTES`, `COUNT`;
- `UsageAuthority`: `RAFT_COMMITTED_EXACT`, `STATE_MACHINE_EXACT`,
  `OBSERVED_APPROXIMATE`, `REBUILDABLE_OPERATIONAL`;
- `UsageAccountingState`: `READY`, `REBUILDING`, `RECONCILING`, `STALE`,
  `DRIFT_DETECTED`, `UNAVAILABLE`;
- `UsageListMode`: `SPACES`, `DOMAINS_IN_SPACE`, `SPACE_WITH_DOMAIN_BREAKDOWN`;
- `UsageStatusReason`: `COUNTERS_REBUILDING`, `COUNTER_STALE`,
  `RECONCILIATION_DRIFT`, `METRIC_UNAVAILABLE`, `METRIC_NOT_QUOTA_SAFE`,
  `UNSUPPORTED_SCOPE`, `UNSUPPORTED_REVISION`,
  `DIAGNOSTIC_COLLECTOR_UNAVAILABLE`.

#### Phase 0.2 MVP metric formulas

MVP quota-safe metrics use metric version `1`.

| Metric | Unit | Formula | Authority | Hard-limit eligible |
| --- | --- | --- | --- | --- |
| `graph.node_count` | count | Count committed, non-deleted graph nodes owned by the scope. | `RAFT_COMMITTED_EXACT` | yes |
| `graph.edge_count` | count | Count committed, non-deleted graph edges owned by the scope. | `RAFT_COMMITTED_EXACT` | yes |
| `graph.logical_bytes` | bytes | Sum UTF-8 bytes of scalar values inside committed node `payload` maps owned by the scope. Map keys and JSON structure do not count; labels, properties, metadata, edge payloads, blob payload bytes, and physical storage overhead are excluded. | `RAFT_COMMITTED_EXACT` | no for first enforcement |
| `blob.active_count` | count | Count committed active blob metadata descriptors owned by the scope. A descriptor is identified by its committed `(space_id, domain_id, blob_id)` ownership, not by graph reference count. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.active_logical_bytes` | bytes | Sum committed payload byte sizes from active blob metadata descriptors owned by the scope. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.local_active_count` | count | Count active blob descriptors whose payload backend is local filesystem. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.local_active_logical_bytes` | bytes | Sum active payload byte sizes for local-filesystem blob descriptors. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.object_store_active_count` | count | Count active blob descriptors whose payload backend is object-store/S3-compatible. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.object_store_active_logical_bytes` | bytes | Sum active payload byte sizes for object-store-backed blob descriptors. | `RAFT_COMMITTED_EXACT` | yes |
| `blob.pending_delete_count` | count | Count committed blob descriptors or tombstones in an explicit Mycel pending-delete state. If no pending-delete state exists for a backend, this metric is `0` only when Mycel can prove there is no pending-delete queue; otherwise it is unavailable. | `RAFT_COMMITTED_EXACT` | no for first enforcement |
| `blob.pending_delete_logical_bytes` | bytes | Sum payload byte sizes for committed pending-delete descriptors/tombstones. | `RAFT_COMMITTED_EXACT` | no for first enforcement |

For `graph.logical_bytes`, string scalar values count their UTF-8 byte length;
numeric and boolean scalar values count the byte length of their deterministic
text representation; `null` values count as zero; arrays and nested maps recurse
into their values. Payload map keys are intentionally excluded so the metric
tracks user payload content rather than schema/property overhead.

Duplicate or deduplicated payloads are counted by committed logical ownership,
not by physical object identity. If the same digest appears in multiple domains,
each domain's committed descriptor counts toward that domain. If the same digest
is uploaded again in the same domain and the existing descriptor is reused, usage
is not incremented a second time.

Object-store upload success does not count as active logical usage until Mycel
blob metadata commits. Upload-success/metadata-failure cases are diagnostic
orphan candidates only.

#### Phase 0.3 Revision and freshness

Use an explicit revision message rather than one opaque string:

```proto
message UsageRevision {
  string revision_token = 1;
  repeated UsageSourceRevision source_revisions = 2;
}

message UsageSourceRevision {
  string source = 1;       // subsystem or Raft group identifier, non-secret.
  string revision = 2;     // opaque committed revision/high-water mark.
}
```

For MVP, `revision_token` may be a daemon-generated opaque materialization token
that changes whenever the materialized counter snapshot changes. When a scope is
aggregated across multiple durable sources, `source_revisions` records the
per-source high-water marks used to compute the snapshot.

`max_staleness_seconds` compares the request to the counter snapshot's
`observed_at`/materialized time. `minimum_revision` is optional. If supplied and
Mycel cannot prove that all relevant source revisions are at or beyond it,
`require_quota_safe=true` must fail closed with `UNSUPPORTED_REVISION` or
`COUNTER_STALE` rather than returning a misleading snapshot.

#### Phase 0.4 Authorization capabilities

Define usage-specific capabilities instead of overloading backup or cluster
capabilities:

| Capability | Scope | Meaning |
| --- | --- | --- |
| `usage.read` | system, space, or domain | Read quota-safe usage snapshots and accounting health for the granted scope. |
| `usage.diagnostic.read` | system, space, or domain | Include diagnostic physical/operational metrics. |
| `usage.reconcile` | system or space | Trigger reconciliation dry-runs and read reconciliation evidence. |
| `usage.limit.manage` | system or space | Create, update, or delete usage limits. |
| `usage.limit.override` | system or space | Override soft-block admission when an operation explicitly supports overrides. |

`system.admin` implies these capabilities. First implementation may require
system-scoped grants for all admin usage RPCs, but the API and docs should keep
space/domain-scoped authorization semantics so least-privilege service accounts
can be added without changing the contract.

#### Phase 0.5 Counter placement and idempotency

Quota-safe counters must be updated from committed mutations, not from
best-effort background observation.

MVP placement decision:

- authoritative deltas are produced in the graph/blob durable commit path;
- deltas carry a stable mutation identity, scope, metric version, and metric
  deltas;
- a usage-accounting materializer applies those deltas idempotently to domain
  counters and retained-non-system space aggregate counters;
- materialized counters are durable Mycel state and are included in snapshots or
  reconstructable from committed mutation/delta history;
- reconciliation can recompute expected counters from graph/blob metadata and
  compare with materialized counters.

The materializer must record applied mutation identities or rely on an owning
state machine's exactly-once apply semantics. Replaying a committed mutation,
retrying a client request, or restoring a snapshot must not double-count usage.

#### Phase 0.6 Backup, restore, and rebuild behavior

Backups should include usage-counter state when counters are stored as durable
Mycel state. Backup manifests should identify usage-counter metric versions and
whether counters were included as authoritative state or marked rebuildable.

Restore behavior:

- if counters restore cleanly, mark usage accounting `RECONCILING` until a dry-run
  reconciliation verifies the restored counters;
- if counters are missing, stale, incompatible, or corrupted, mark usage
  accounting `REBUILDING` and recompute from durable graph/blob metadata;
- while state is `REBUILDING`, `GetUsage(require_quota_safe=true)` fails closed;
- hard-limit admission for quota-increasing writes is disabled/fail-closed until
  counters for the affected scope and metric are `READY`;
- quota-neutral reads and quota-decreasing deletes remain allowed unless a
  separate administrative block exists.

#### Phase 0.7 Phase 0 deliverables

Deliverables:

- this finalized design update;
- `mycel-api` proto PR plan;
- implementation issues split by API, daemon counters, blob deltas, graph deltas,
  reconciliation, backup/restore integration, diagnostics, and optional admission
  enforcement.

### Phase 1: API and SDK contract

Add the admin usage accounting service to `mycel-api` and regenerate clients.
The first daemon implementation may return `Unimplemented` or empty health until
counter storage lands, but the public contract should compile end to end.

Deliverables:

- protobuf service/messages/enums for usage scopes, snapshots, metrics, blockers,
  health, list pagination, and batch statuses;
- generated Go/Rust SDK types;
- thin SDK helpers for read-only usage calls;
- CLI command shape documented, even if daemon support lands in a later slice.

Acceptance:

- proto lint/generation passes;
- SDK compile tests cover the new request/response types;
- public docs describe metric authority and quota-safe semantics.

### Phase 2: usage counter core

Status: implemented in the daemon internal core.

Add an internal usage-accounting module that owns metric definitions,
materialized counters, scope aggregation, freshness metadata, and health state.

Deliverables:

- metric registry with name, version, unit, authority, quota-safe flag, and
  hard-limit eligibility;
- durable counter records or state-machine fields for domain counters and space
  aggregate counters;
- idempotent delta application helper;
- internal snapshot API used by admin handlers;
- rebuild/materialization state flags for restore or upgrade.

Acceptance:

- applying the same committed delta twice does not double count;
- space aggregates update with matching domain deltas;
- system/internal domain inclusion mode is explicit;
- startup/upgrade with missing counters reports a rebuild-required health state
  rather than returning misleading quota-safe usage.

### Phase 3: blob usage deltas

Status: implemented for committed blob metadata put/delete paths.

Wire blob metadata commit/delete paths to produce usage deltas. Count logical
usage when blob metadata is committed, not when payload bytes are staged or
uploaded.

Deliverables:

- active local blob byte/count deltas;
- active object-store blob byte/count deltas;
- pending-delete byte/count deltas according to the finalized delete policy;
- object-store orphan diagnostic signal for upload-success/metadata-failure
  cases without increasing active logical usage.

Acceptance:

- successful local and object-store blob commits increment the expected domain
  and space counters;
- failed uploads and failed metadata commits do not increment active logical
  usage;
- deletes decrement active usage and update pending-delete metrics as specified;
- replay/restore tests prove blob deltas are idempotent.

### Phase 4: graph usage deltas

Status: implemented for committed graph node/edge create/delete paths, node
payload logical-byte create/update/delete paths, and reconciliation-scan
comparison in Phase 6 health checks.

Wire graph mutation paths to produce node count, edge count, and node payload
logical-byte deltas.

Deliverables:

- node create/delete count deltas;
- edge create/delete count deltas;
- node payload logical-byte create/update/delete deltas;
- import/bulk-write aggregation that produces the same final deltas as ordinary
  mutations;
- tests for transaction abort, retry, replay, update, delete, and reconciliation
  drift behavior.

Acceptance:

- committed graph mutations update domain and space counters exactly once;
- aborted transactions and failed imports do not change usage;
- reconciliation scan agrees with materialized counters for representative graph
  fixtures.

### Phase 5: admin API, CLI, and authorization

Status: implemented for read-only usage snapshots, batch/list/health RPCs, CLI output, and usage read/diagnostic-read authorization gates.

Implement read-only usage RPC handlers on top of materialized counters and add
operator-facing CLI output.

Deliverables:

- `GetUsage`, `BatchGetUsage`, `ListUsage`, and `GetUsageAccountingHealth` daemon
  handlers;
- admin capability checks for usage read and diagnostic inclusion;
- JSON and human-readable CLI output;
- blockers/warnings for stale, rebuilding, unavailable, diagnostic-only, or
  non-quota-safe values.

Acceptance:

- unauthenticated and underprivileged callers are rejected;
- `require_quota_safe=true` fails closed when required counters are unavailable;
- list pagination is stable;
- batch responses return per-scope statuses.

### Phase 6: reconciliation and health

Status: implemented for dry-run graph/blob quota-safe reconciliation through usage health.

Add dry-run reconciliation that recomputes MVP quota-safe metrics from durable
metadata and compares them with materialized counters.

Deliverables:

- domain and space reconciliation scanner;
- bounded scan controls and progress/evidence;
- drift reporting in `GetUsageAccountingHealth`;
- optional admin/CLI command to trigger reconciliation dry-runs.

Acceptance:

- injected counter drift is detected and reported without exposing secrets;
- healthy counters produce no drift blockers;
- `require_quota_safe=true` behavior is documented when drift is present;
- reconciliation runs are bounded and cancellable or safely retryable.

Implemented behavior:

- `GetUsageAccountingHealth(include_reconciliation=true)` runs bounded dry-run
  scanners for durable graph and blob metadata and compares the recomputed
  quota-safe counters with materialized state;
- drift is reported as `reconciliation_drift` blockers with sanitized metric,
  scope, expected value, materialized value, and metric-version evidence;
- truncated or failed reconciliation scans report blocker health and
  `quota_safe_ready=false` instead of claiming fresh quota-safe readiness;
- reconciliation dry-runs are report-only and do not repair counters or mutate
  materialized values;
- callers that request reconciliation evidence need `usage.reconcile` in addition
  to `usage.read`;
- ordinary `GetUsage(require_quota_safe=true)` still uses materialized accounting
  state and does not run reconciliation on the serving path; operators should use
  health with reconciliation before relying on counters after suspected drift or
  restore.

### Phase 7: backup, restore, and rebuild integration

Status: implemented for repository-backed cluster backup manifests, restored data-dir usage readiness markers, fail-closed restored startup states, and reconciliation-cleared readiness.

Integrate usage counters with cluster backup/restore. Counters should be included
when they are durable state, but restore should still reconcile or rebuild before
claiming quota-safe readiness.

Deliverables:

- backup inclusion or explicit derived-state marker for usage counters;
- restore path that marks usage accounting rebuilding/reconciling when needed;
- readiness/health blockers while quota-safe counters are unavailable;
- tests for restore with present, missing, stale, or corrupted counter state.

Acceptance:

- restored clusters do not return misleading quota-safe usage;
- `GetUsage(require_quota_safe=true)` fails closed during rebuild;
- reconciliation can clear rebuild/drift state after restore.

Implemented behavior:

- usage counter state lives under `meta/usage/counters.json`, is captured by the
  existing data-dir archive path, and is identified in production backup-set
  manifests by a `usage_accounting` checkpoint barrier with metric-version and
  checksum evidence;
- restore execution, `restore-local`, and `restore-repository-local` write
  `meta/usage/restored-usage-accounting.json` in each restored data dir;
- restored daemons consume that marker at startup and move clean restored counter
  state to `RECONCILING`; missing, checksum-mismatched, incompatible, or corrupt
  restored counter state is moved to `REBUILDING` instead of aborting startup;
- while state is `RECONCILING` or `REBUILDING`, quota-safe snapshots fail closed
  (`quota_safe_complete=false`, and the admin RPC returns `FailedPrecondition`
  when `require_quota_safe=true`);
- a matching reconciliation dry-run can clear `RECONCILING`, `REBUILDING`, or
  previous `DRIFT_DETECTED` readiness state to `READY` without repairing counter
  values; drift or truncated/failed scans keep quota-safe readiness blocked;
- because health with reconciliation can persist a readiness transition, usage
  accounting health is not treated as a backup-quiesce read-only RPC.

### Phase 8: diagnostic physical collectors

Status: implemented for bounded local filesystem graph/blob/blob-metadata/lexical/semantic/projection diagnostic byte observations and collector health.

Add optional diagnostic collectors after quota-safe logical usage is available.
These collectors must not block quota-safe usage unless the caller explicitly
requires diagnostic values.

Deliverables:

- local filesystem/blob-directory observations;
- optional object-store prefix or inventory observations;
- index/vector/projection size observations where practical;
- diagnostic collector health in `GetUsageAccountingHealth`.

Acceptance:

- collector failures are reported as diagnostic health issues;
- physical observations are marked non-quota-safe;
- no S3/MinIO bucket scan runs on the normal `GetUsage` serving path unless
  explicitly requested and bounded.

Implemented behavior:

- `include_diagnostic_physical=true` runs bounded local filesystem collectors and
  appends non-quota-safe `diagnostic.*` metrics for local graph, blob payload,
  blob metadata, lexical, semantic/vector, projection, and aggregate local scope
  physical bytes;
- local collectors are scope-aware where paths are naturally scoped: domain
  diagnostics cover graph and lexical domain directories, while space diagnostics
  cover graph domains, blob payloads, blob metadata, lexical, semantic/vector,
  and projection paths for the space;
- diagnostic observations use `OBSERVED_APPROXIMATE`, `quota_safe=false`, and
  `hard_limit_eligible=false`;
- collector failures/truncation are reported as
  `diagnostic_collector_unavailable` warnings and health counters, without
  changing quota-safe readiness for logical counters;
- object-store/S3 inventory collection remains intentionally absent from the
  normal serving path; no S3/MinIO bucket scan is introduced in Phase 8.

### Phase 9: usage limits and admission enforcement

Implemented. DB-side hard usage limits are configured through
`AdminUsageAdmissionService` and enforced before quota-increasing graph/blob
commits. Limit records are stored durably with the materialized usage counter
state at `meta/usage/counters.json`; records include Mycel scope, metric name,
metric version, hard-limit value, enabled state, and non-secret `policy_ref` /
operator description fields.

Implemented scope/metric set:

- domain and space hard limits for quota-safe hard-limit-eligible metrics;
- graph metrics: `graph.node_count`, `graph.edge_count`; `graph.logical_bytes`
  is quota-safe but remains hard-limit ineligible for first enforcement;
- blob active logical metrics: `blob.active_logical_bytes`,
  `blob.active_count`, `blob.local_active_logical_bytes`,
  `blob.local_active_count`, `blob.object_store_active_logical_bytes`, and
  `blob.object_store_active_count`.

Implemented enforcement behavior:

- graph and blob commit paths reserve attempted positive deltas before the
  durable commit and release reservations when the commit applies or fails;
- configured limits fail closed while usage accounting is not `READY`;
- quota-neutral reads and quota-decreasing deletes do not require admission;
- diagnostic physical metrics and pending-delete metrics are rejected as hard
  limit targets because they are not hard-limit eligible;
- client-facing write rejections use gRPC `ResourceExhausted` with
  `google.rpc.ErrorInfo` domain `mycel.usage` and reasons
  `usage_limit_exceeded` or `usage_write_gate_blocked`;
- limit CRUD requires `usage.limit.manage` and emits durable admin activity
  events (`usage.limit.upserted`, `usage.limit.deleted`) when the activity
  service is configured.

Validation coverage includes limit CRUD, reservation races against outstanding
writes, fail-closed write gates while counters rebuild, diagnostic/non-eligible
metric rejection, blob admission rejection, graph admission rejection, admin auth,
CLI request construction, and SDK contract compile tests.

## Remaining design questions after Phase 0

Phase 0 resolves the MVP API shape, scope semantics, initial quota-safe metric
formulas, freshness representation, capabilities, counter placement, and
backup/restore behavior. The remaining questions are not blockers for the
read-only MVP, but they should be resolved before expanding the production
contract.

### Deferred metric definitions

- Decide whether semantic vectors are graph-derived logical usage, diagnostic
  derived usage, or a separate quota dimension.
- Decide whether any lexical/semantic/projection/index byte metric can ever be
  quota-safe. Default remains diagnostic-only.

### Pending-delete policy details

Phase 0 defines pending-delete metrics only for explicit Mycel pending-delete
state. The implementation still needs to verify each backend's current delete
state machine and decide whether to add a durable pending-delete queue or return
pending-delete metrics as unavailable until such a queue exists.

### Reconciliation repair

The MVP reconciliation mode is dry-run/report-only. A later design should define
operator-approved repair semantics, audit evidence, and whether repaired counters
require a fresh reconciliation before `quota_safe_complete=true` is restored.

### Hard-limit expansion

Phase 9 implements synchronous admission for the first quota-safe graph/blob
metric set at both domain and space scopes. Remaining design work includes:

- whether per-application policy wants domain-only, space-only, or combined
  limits for each deployment;
- whether limit records should move from daemon-local durable state into a
  system-Raft-backed cluster-wide catalog for single-write multi-node operation;
- how very large imports reserve usage across multiple graph/blob commits;
- whether future override semantics exist for `HARD_BLOCK` or only for
  `SOFT_BLOCK`-style application warnings;
- whether additional quota-safe metrics beyond hard-limit-eligible graph counts
  and committed blob logical usage should become hard-limit eligible.

### Diagnostic physical collectors

Phase 8 implements bounded local filesystem collectors for graph/blob/blob
metadata/search/vector/projection paths. Remaining backend-specific design work
covers S3/MinIO prefix observations, provider inventory, PVC/provider-native
stats, richer orphan-candidate detection, and any future collector scheduling
outside the normal usage serving path. Diagnostic observations remain optional
and non-quota-safe.

### SDK and CLI ergonomics

Go and Rust SDK helpers cover usage reads, health, and limit CRUD. The daemon CLI
covers usage snapshots, list/health diagnostics, and hard-limit management under
`mycel admin usage limits`. Future work can add richer tabular output and
operator playbooks for application-specific policy choices.

## Authority classes

Usage fields should be explicitly classified so integrating applications know
which fields can drive billing or throttling.

Suggested classes:

- `RAFT_COMMITTED_EXACT`: deterministic logical counters derived from committed
  state and safe for quota decisions.
- `STATE_MACHINE_EXACT`: exact local materialized values that are persisted in
  snapshots/checkpoints and recoverable from the committed log.
- `OBSERVED_APPROXIMATE`: operational observations such as physical disk usage,
  object-store physical bytes, or index file bytes.
- `REBUILDABLE_OPERATIONAL`: derived state such as lexical/semantic index size
  and freshness, useful for diagnostics but not inherently billable.

Hard throttling should use only fields explicitly documented as quota-safe.

## Launch gate

Production deployments with billing or throttling should require:

1. authoritative logical usage API available for each billable workspace;
2. documented units and field semantics;
3. clear application/Mycel ownership boundary;
4. restore/replay-safe counters or a proven reconciliation path;
5. freshness/authority metadata in API responses;
6. integration test proving the application can consume usage without relying on
   operational disk metrics.

## Follow-up design work

- Finalize pending-delete backend behavior and whether a durable pending-delete
  queue is required for blob cleanup/accounting.
- Gather production history for `graph.logical_bytes` before making it hard-limit eligible.
- Decide whether semantic vectors count as graph-derived logical usage,
  operational index usage, or a separate quota dimension.
- Define reconciliation repair semantics after the dry-run MVP.
- Narrow the first hard-limit enforcement slice and solve any required
  cross-partition reservation semantics.
- Design diagnostic physical collectors per backend.
- Define SDK helpers, CLI output, and operator examples for the finalized proto.
