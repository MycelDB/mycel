# Backup, restore, and rollback safety

Status: draft  
Last updated: 2026-11-20

This document defines the first production architecture for Mycel cluster
backups, restores, and rollback decisions for CommonFolio. It is intentionally
implementation-oriented: API, daemon, infrastructure, Lab, and operator runbooks
should use the same concepts, state transitions, safety gates, and manifest
contracts.

## 1. Overview and production scope

### Production requirement

Backup, restore, and rollback safety is the top hard requirement before
CommonFolio stores real customer production data in Mycel.

A bad upgrade, operator error, data corruption event, or infrastructure failure
must not become unrecoverable customer data loss. Operators need an easy,
trustworthy way to create backups, verify them, restore them into a new clean
cluster or safely replace an existing cluster's state, and decide whether
rollback means a binary rollback or a restore-from-backup operation.

### Mandatory first-cut capabilities

Before CommonFolio production, Mycel should provide:

- on-demand backup before upgrades;
- scheduled backup to durable S3-compatible storage;
- a backup manifest with checksums, format version, source cluster identity,
  included subsystems, and committed state markers;
- restore into a new clean cluster and controlled full-state restore into an
  existing cluster;
- backup verification;
- at least one successful restore drill from a release-candidate backup;
- a clear rollback runbook:
  - binary rollback when the older daemon can safely read the current state;
  - restore-from-backup when state has crossed a downgrade boundary.

### Scope and non-goals

The first production scope is full-cluster backup and full-state restore for the
CommonFolio production shape. It covers scheduled and on-demand cluster backups,
validated S3-compatible backup repositories, restore into a clean new cluster,
and destructive replacement of an existing cluster's Mycel state.

The first production scope does not include online point-in-time recovery,
partial workspace restore, merge/overlay restore, cross-cluster identity
transforms, or logical import/export downgrade paths. Mycel also does not own
CommonFolio billing, subscription policy, object-store payload backup policy, or
raw secret storage; it exposes and validates the Mycel state and references that
CommonFolio and deployment tooling need.

### Architecture summary

The production model is a cluster-level, manifest-driven backup system:

- backup operations quiesce writes, establish committed checkpoint barriers,
  capture immutable node/PVC sources, upload deterministic artifacts to an
  S3-compatible repository, validate the result, and publish `COMPLETE` last;
- restore operations plan and validate before mutation, then bootstrap a new
  cluster or destructively replace an existing cluster under a restore lock;
- `backup-set.json` and its matching `COMPLETE` marker are the authority for
  restore discovery; operation state alone is not enough;
- authoritative Mycel state, durable progress, external object-store dependencies,
  and derived index artifacts have separate backup and restore rules;
- raw secrets and customer payload bytes from external blob stores are not stored
  in Mycel backup manifests.

### Architectural invariants

- A production backup is not restorable until both `backup-set.json` and a
  matching `COMPLETE` marker validate.
- Backup correctness is based on committed checkpoint barriers, not timestamps or
  provider snapshot handles alone.
- Raw credentials and customer payload bytes from external blob stores are never
  embedded in Mycel backup manifests or operation records.
- First production raw physical/PVC restores preserve the backup cluster identity
  and require compatible topology, node/ordinal mapping, and source-fencing
  evidence when appropriate.
- Required authoritative state fails closed when missing or corrupt; optional
  derived artifacts may be omitted only when the manifest records rebuild or
  discard behavior.

### First-production decision summary

| Area | Decision |
| --- | --- |
| Repository | Use an external S3-compatible backup repository. Pod-local files are scratch only. |
| Capture | Use quiesced local/PVC snapshots with asynchronous upload, through a capture-provider abstraction. |
| Quiescence | Default to `CONTROL_ONLY`: ordinary client reads and writes are blocked during the short backup cut. |
| Restore targets | Support `NEW_CLUSTER_BOOTSTRAP` and destructive `EXISTING_CLUSTER_REPLACE`. |
| Cluster identity | Raw physical/PVC restore supports `PRESERVE_BACKUP_ID`; identity-transform modes are deferred. |
| Manifest authority | `backup-set.json` plus matching `COMPLETE` marker is the restore-discovery authority. |
| External blobs | External S3/MinIO payload bytes are dependencies backed up by object-store policy, not copied into Mycel backup sets by default. |
| Semantic records | Durable semantic/vector records are authoritative Mycel state; physical vector-search indexes are derived artifacts. |
| Derived defaults | `SEMANTIC_VECTOR_SEARCH = INCLUDE_REQUIRED`; lexical, graph checkpoint, and persistent query indexes are optional; memory adjacency indexes are discarded. |
| Credentials | Store references and fingerprints only; raw credentials and key material remain external. |
| Compatibility | Prefer restore with the same version or a newer patch in the same minor line; fail closed across unsupported format or feature boundaries. |
| Qualification | Require repository/manifest restore drills, negative fail-closed tests, crash-resume tests, capacity checks, and AWS `staging2` environment evidence before production launch. |

## 2. Data durability boundaries

### Data to protect

The backup boundary must include every durable subsystem needed to restore a
functional cluster:

- system/cluster metadata;
- Raft state and snapshots required for committed state recovery;
- graph state;
- identity and auth/session metadata needed for controlled recovery;
- space/domain metadata;
- attachment/blob metadata and required object payload descriptors;
- local blob payload files when Mycel uses a local blob backend;
- object-store references and validation metadata when Mycel uses an external
  S3-compatible blob backend;
- usage accounting counters or enough committed state to recompute them safely;
- projection metadata and readiness fences.

Derived state such as lexical and semantic indexes has a different restore
policy than authoritative graph state. The backup policy should decide, per
derived subsystem, whether indexes are required backup artifacts, optional backup
artifacts, or intentionally rebuilt after restore. CommonFolio production should
use a policy that backs up semantic index artifacts by default because rebuilding
them can be expensive and slow. The restore manifest must say which derived
artifacts are included, validated, discarded, or require rebuild.

#### Object/blob payload boundary

When Mycel uses a local blob backend, blob payload files are part of the Mycel
PVC state and must be included in the backup set.

When Mycel uses an external S3-compatible blob backend, including AWS S3 in
CommonFolio production or MinIO in self-hosted/non-AWS deployments, Mycel should
not copy object payload bytes into the Mycel backup set by default. The object
store must have its own backup, versioning, replication, lifecycle, or disaster
recovery policy. Mycel's backup responsibility is to preserve and validate the
references needed to make the restored graph/blob metadata point at the correct
external objects.

For object-store-backed blobs, the Mycel backup should include:

- authoritative blob metadata and payload descriptors from Mycel state;
- an object-reference manifest generated at the backup cut;
- bucket, prefix, key, region/endpoint fingerprint, and credential reference;
- blob ID, space ID, domain ID, size, checksum algorithm, checksum/digest, and
  available object-store evidence such as ETag or version ID;
- the external object-store backup policy or recovery-point reference when one
  is available from deployment tooling;
- validation evidence that each referenced object exists and matches the
  metadata Mycel can verify at the backup cut.

The Mycel backup manifest should record that object payload bytes are external
backup dependencies, not included artifacts. Restore planning should fail closed
for production if the referenced object-store backup/recovery point is missing or
if required object references cannot be verified, unless an operator explicitly
chooses a degraded restore mode that accepts unavailable blob payloads. The same
rule applies to MinIO when MinIO is used as the S3-compatible payload store: the
MinIO bucket backup is separate, while Mycel protects metadata, references, and
validation evidence.

### Derived index backup policy

The backup system should distinguish authoritative state from derived index
artifacts, but it should not assume all derived artifacts are cheap to rebuild.
The backup policy should decide how each derived subsystem is handled.

#### Policy modes

The policy should support per-derived-subsystem modes such as:

```text
INCLUDE_REQUIRED      # backup must include and validate this derived artifact
INCLUDE_OPTIONAL      # include when available, but restore may rebuild it
REBUILD_ALLOWED       # do not require backup artifact; rebuild after restore
DISCARD               # disposable cache; do not back up and do not rebuild as part of restore
```

The policy should also define restore behavior when an artifact selected by
policy is missing, corrupt, incomplete, or incompatible. `INCLUDE_REQUIRED`
should fail backup validation or restore planning unless the operator explicitly
chooses a degraded restore mode. `INCLUDE_OPTIONAL` should never publish an
invalid artifact: if optional artifact validation fails during backup, the backup
should omit the artifact, record a warning, and mark the subsystem
`REBUILD_REQUIRED` for restore.

#### First production policy

- Graph-authoritative state, Raft state, identity state, space/domain metadata,
  attachment metadata, object payload references or external object-store
  dependency evidence, usage counters, semantic rules, durable semantic records,
  embedding metadata, and projection fences are required backup inputs regardless
  of derived-index policy.
- CommonFolio production should configure semantic/vector physical index
  artifacts as `INCLUDE_REQUIRED` by default.
- Lexical physical index artifacts should be `INCLUDE_OPTIONAL` by default. The
  backup should include them when present and valid to reduce restore time, but
  restore may rebuild them from graph state when missing or invalid.
- Graph checkpoint artifacts should be `INCLUDE_OPTIONAL` by default. They speed
  graph open/replay, but authoritative graph segments remain the source of truth.
- Persistent graph/query index artifacts should be `INCLUDE_OPTIONAL` by default
  and are valid only when they match a valid graph checkpoint/revision. If the
  matching checkpoint is absent or incompatible, the persistent indexes are
  treated as `REBUILD_REQUIRED`.
- Metadata/adjacency memory indexes should use `DISCARD`; they are rebuilt in
  memory from restored graph state.
- Optional caches should use `DISCARD` and should not block backup or restore.

Default production derived-subsystem policy:

| Subsystem | Backup mode | Restore when missing/corrupt/incompatible | Frontier requirement | Notes |
| --- | --- | --- | --- | --- |
| `SEMANTIC_VECTOR_SEARCH` | `INCLUDE_REQUIRED` | `FAIL_CLOSED` by default; elevated degraded restore may choose `REBUILD_FROM_DURABLE_RECORDS` / `UNAVAILABLE_UNTIL_REBUILT`. | Semantic source frontier must satisfy the quiesced graph/semantic barrier, generation, rebuild epoch, model metadata, vector dimensions, and index format. | Expensive to rebuild; strict semantic search remains unavailable unless the restored or rebuilt index satisfies normal readiness checks. |
| `LEXICAL_SEARCH` | `INCLUDE_OPTIONAL` | `REBUILD_AFTER_RESTORE`. | Indexed graph revision must be at or behind the restored graph frontier for that domain. | Include valid artifacts to reduce restore time; rebuild from graph state if absent or invalid. |
| `GRAPH_CHECKPOINT` | `INCLUDE_OPTIONAL` | `REBUILD_AFTER_RESTORE` / replay from authoritative graph segments. | Checkpoint revision and segment offsets must be consistent with the graph barrier. | Speeds graph open/replay but is not the source of truth. |
| `PERSISTENT_GRAPH_QUERY_INDEX` | `INCLUDE_OPTIONAL` | `REBUILD_AFTER_RESTORE`. | Must match a valid checkpoint/revision, schema/index definitions, index format, and checksums. | Speeds query/index readiness; invalid or checkpoint-mismatched artifacts are ignored and rebuilt. |
| `METADATA_ADJACENCY_MEMORY_INDEX` | `DISCARD` | `DISCARD`. | Not applicable. | Rebuilt in memory from restored graph state. |

#### Semantic index requirements

When semantic index artifacts are included, the backup manifest should record:

- whether semantic index artifacts are required for the backup set;
- artifact URI, size, and checksum;
- source graph/domain/space frontier;
- projection generation or checkpoint identity;
- embedding model/provider identity needed to interpret stored vectors;
- vector dimensions and index format version;
- materializer/indexer version;
- whether durable embedding records are included;
- validation result and restore policy.

Restore should prefer using backed-up semantic index artifacts when they validate
against the restored authoritative state and supported index format. If semantic
index artifacts are missing, corrupt, incomplete, or incompatible, the restore
plan should fail closed by default for CommonFolio production unless the operator
explicitly chooses a degraded restore mode that rebuilds semantic indexes after
restore.

#### Semantic authoritative-vs-derived durability boundary

The semantic backup boundary should treat generated semantic records as durable
Mycel state, while treating physical vector-search indexes as derived artifacts.
The fact that a physical artifact is `INCLUDE_REQUIRED` for CommonFolio
production does not make it authoritative; it means production backup/restore
requires a validated copy unless an elevated degraded restore explicitly accepts
rebuild or temporary unavailability.

First production should use this semantic durability classification:

| Semantic state | Classification | Backup behavior | Restore behavior |
| --- | --- | --- | --- |
| Global inference/catalog metadata: packages, model endpoints, models, endpoint capabilities, vector-store catalog, credential metadata, credential lifecycle state, and secret references. | `AUTHORITATIVE_REQUIRED` | Include from the system/global semantic or inference metadata boundary. Manifests must record only safe credential references, not raw provider secrets. | Restore before semantic workers or search are enabled. Validate referenced credentials/secrets through the deployment secret mechanism. |
| Space semantic configuration: semantic rules, transitional semantic indexes, intelligence profiles, credential grants, inference policies, policy decisions, rule enablement, storage policy, provider-budget transition fields, and vector-store/profile references. | `AUTHORITATIVE_REQUIRED` | Include for every restored space. | Restore before semantic maintenance resumes. This configuration defines which records and indexes are valid. |
| Semantic generation, rebuild epoch, resolved configuration hash, semantic rule state, reliability state, logical semantic index state, latest-observed/analyzed/fresh-through frontiers, unresolved ranges, and capacity/rebuild status. This excludes physical search-index `state.json` files stored beside physical `latest.json` artifacts. | `DURABLE_PROGRESS_REQUIRED` / semantic fence | Include. These values are the durable fencing identity that prevents older work or old artifacts from being accepted as current. | Restore as progress/fence evidence, then revalidate. States may be downgraded to `REBUILD_REQUIRED`, `missing`, or `UNAVAILABLE_UNTIL_REBUILT` if matching records/artifacts are absent or target leadership changes. |
| Graph dirty-event history, semantic dirty-work items, work logs/state, and semantic maintenance checkpoints. | `DURABLE_PROGRESS_REQUIRED` | Include retained history and queue/checkpoint state at the quiesced barrier. Quiescence should pause semantic workers before capture. | Restore so maintenance can resume without losing graph changes. Work items with runtime claims such as `running` must not resume as already-owned work; they should be normalized back to pending/retryable work. |
| Durable embedding/vector records: semantic index manifest, append-only record segments, latest-record views, tombstone records, record provenance, source hash, source graph revision, semantic generation, configuration hash, rebuild epoch, model/endpoint/capability/vector-store identity, dimensions, and vector bytes. | `AUTHORITATIVE_REQUIRED` for semantic generated state | Include and validate. These records are the source for validating or rebuilding physical semantic search indexes without provider re-embedding. Unresolved pending publications must be drained, recovered, or treated as validation failures. | Restore before physical vector-search indexes are accepted. If these records are missing or corrupt, the backup set is not a complete production semantic backup; provider re-embedding is a separate degraded recovery path, not silent normal restore. |
| Physical semantic/vector search artifacts, such as per-rule/per-binding live-search files and physical index readiness state. | `DERIVED_POLICY_CONTROLLED` for `SEMANTIC_VECTOR_SEARCH`; CommonFolio default `INCLUDE_REQUIRED` | Include by default and validate against durable semantic records, semantic generation, rebuild epoch, resolved configuration hash, source graph frontier, vector dimensions, vector-store identity, materializer/index format, and checksums. | Accept only if validation succeeds. If missing/corrupt/incompatible, default production restore fails closed; degraded restore may rebuild from durable semantic records and keep strict semantic search unavailable until readiness checks pass. |
| Projection leases, common projection definitions/desired frontiers/checkpoints/replica status, artifact generation, leader node/term, materializer version, and readiness evidence. | `DURABLE_PROGRESS_REQUIRED` / readiness evidence | Include as part of projection metadata and semantic readiness evidence. | Do not blindly trust node-specific leases after restore. Revalidate or reissue them under the restored cluster identity, node mapping, leadership term, semantic generation, rebuild epoch, and fresh-through frontier before serving strict semantic reads. |
| Pending publication files, in-memory indexes, worker leases, provider requests in flight, temporary retry state, scratch files, and local process locks. | `EPHEMERAL_EXCLUDED` | Exclude. Quiescence should drain/recover pending publications and stop new provider work before capture; unresolved pending state after quiesce fails validation. | Never restore as ready state. Runtime workers recreate these from restored configuration, progress, records, and policy. |
| External vector-store payloads, if a future deployment uses a vector backend not fully stored inside Mycel data directories. | `EXTERNAL_DEPENDENCY` unless explicitly captured by a vector-store backup provider | Record references, provider identity, backup-policy/recovery-point evidence, and validation evidence. Do not embed raw provider credentials in Mycel manifests. | Production restore fails closed unless the external vector-store recovery point can be verified or a degraded rebuild-from-durable-records mode is explicitly authorized. |

This boundary means source graph state plus semantic rules is not sufficient by
itself for a normal CommonFolio production backup. Durable embedding/vector
records are part of the backup contract because they preserve generated semantic
outputs and avoid non-deterministic or expensive provider re-embedding during
restore. Physical vector-search indexes are still derived because they can be
rebuilt from those durable records, but CommonFolio should require them by
default to keep restore-time and launch-readiness risk bounded.

Backup validation should enforce these consistency rules for each
`space_id/domain_id/semantic_rule_id/embedding_binding_key` scope:

- semantic configuration, reliability state, durable vector records, projection
  readiness evidence, and included physical index artifacts agree on semantic
  generation, rebuild epoch, configuration hash, vector-store identity, vector
  dimensions, and materializer/index format;
- every restored latest-record view is backed by a valid segment record or
  tombstone record;
- durable vector record source graph revisions are not ahead of the restored
  graph frontier for their domain;
- included physical search artifacts contain only records from the matching
  durable record set and publish a fresh-through frontier accepted by the
  backup's semantic barrier;
- projection leases/checkpoints are treated as certification evidence, not as an
  authorization to serve reads until they are revalidated under the restored
  target leadership and identity mode;
- pending publication files, partial provider calls, or half-applied semantic
  maintenance transitions are not accepted as restorable ready state.

Restore ordering for semantic state should be:

1. restore graph-authoritative state and semantic/inference configuration;
2. restore durable semantic progress/fence state and durable embedding/vector
   records;
3. normalize runtime-only maintenance claims and reject unresolved pending
   publications;
4. validate included physical vector-search artifacts, or mark/rebuild them
   according to the selected degraded policy;
5. revalidate or reissue projection/readiness fences under the target cluster;
6. enable strict semantic reads only after normal semantic readiness checks pass.

#### Restore policies

The manifest should classify each derived subsystem with a restore policy such
as:

```text
INCLUDED_AND_VALIDATED
REBUILD_REQUIRED
REBUILD_FROM_DURABLE_RECORDS
OPTIONAL_CACHE_DISCARDED
UNAVAILABLE_UNTIL_REBUILT
```

For semantic search, `REBUILD_REQUIRED` or `UNAVAILABLE_UNTIL_REBUILT` is an
operator-visible degraded state, not the preferred production restore path.
Strict semantic search should remain unavailable until restored or rebuilt
indexes satisfy normal projection readiness checks.

## 3. Core concepts and policy model

The backup/restore design should use precise terms so API, CLI, manifests,
operator documentation, and implementation code do not overload the word
"backup".

### Backup repository

A backup repository is the shared, externally accessible location where Mycel
stores authoritative backup sets. For the first production design, a repository
is S3-compatible object storage:

- Amazon S3 for AWS/CommonFolio production;
- MinIO or another S3-compatible store for non-AWS, self-hosted, local, and
  Mycel Lab deployments.

A repository contains configuration such as bucket, prefix, endpoint, region,
path-style behavior, TLS settings, encryption policy reference, and credential
reference. It should not contain raw access keys in ordinary API requests.

### Backup repository credential reference

A credential reference identifies how the daemon resolves repository access. It
may point to IRSA or another cloud identity, a Kubernetes secret, Vault, an
external secret manager, or a deployment-specific credential source. The backup
API should pass references, not secrets.

### Capture provider

A capture provider is the environment-specific implementation used to create an
immutable source for node/PVC backup artifacts. It is selected by backup policy
or by a constrained `AUTO` policy. Capture providers must be hidden behind the
same backup operation, manifest, validation, and restore contract so operators do
not need a different backup API for AWS, non-AWS CSI, local K3s, or Mycel Lab.

The capture provider may use Kubernetes `VolumeSnapshot`, direct AWS EBS
snapshots, or a portable filesystem immutable copy. Regardless of provider, the
authoritative restorable output is the completed S3-compatible backup set, not
the transient provider snapshot or local staging copy.

### Backup policy

A backup policy describes scheduled backup behavior and retention. It answers:

- whether scheduled backups are enabled;
- which repository is the default destination;
- schedule kind and timing;
- archive format;
- retention policy;
- timeout and retry behavior;
- whether conservative read-only RPCs are allowed during quiescence;
- derived index handling per subsystem, such as required backup, optional backup,
  or rebuild-after-restore.

A backup policy is not itself a backup. It is configuration that may create
backup operations over time. A backup operation should capture the resolved
policy version or policy snapshot it used, so later policy edits do not change
the meaning of an in-flight or completed backup.

#### Backup policy structure

The initial production policy can be one cluster-scoped policy stored in system
metadata. Later work may add multiple named policies, but the first API should be
clear about the effective policy used for scheduled backups and as the default
baseline for ad-hoc backups.

Suggested conceptual structure:

```text
BackupPolicy
  policy_id = "cluster-default"
  version
  scheduled_enabled
  default_repository_id

  schedule
    mode = DISABLED | CRON | INTERVAL
    cron_expression              # when mode = CRON
    interval                     # when mode = INTERVAL
    timezone
    jitter
    missed_run_policy = SKIP | RUN_ONCE_WHEN_AVAILABLE

  retention
    keep_last_successful
    keep_for
    protect_manual_backups_default
    manual_backup_default_class = PINNED | DEFAULT
    pre_restore_safety_keep_for
    protect_pre_restore_safety_until_released
    delete_incomplete_after
    deletion_tombstone_keep_for
    external_dependency_min_retention_check = true

  consistency
    quiesce_required = true
    allow_read_only_during_quiesce
    read_allowlist_profile = CONTROL_ONLY | STRICT_READ_ONLY
    require_all_expected_nodes = true
    wait_for_convergence_timeout
    quiesce_timeout
    checkpoint_timeout

  capture
    provider = CSI_VOLUME_SNAPSHOT | FILESYSTEM_IMMUTABLE_COPY | AWS_EBS_SNAPSHOT | AUTO
    fallback_allowed
    snapshot_class_name          # for CSI_VOLUME_SNAPSHOT when required
    staging_storage_class_name   # for temporary clone/upload PVCs when required
    uploader_service_account_ref
    archive_format = TAR_ZSTD
    compression_level
    scratch_space_min_free_bytes
    temporary_snapshot_ttl

  upload
    multipart_part_size
    timeout
    retry_policy

  validation
    validate_before_complete = true
    require_complete_marker = true
    verify_checksums = true
    verify_expected_node_coverage = true
    verify_checkpoint_barriers = true
    verify_derived_state = true

  derived_subsystems[]
    subsystem
    backup_mode
    restore_on_missing
    min_source_frontier

  operation_limits
    max_runtime
    single_active_cluster_backup = true
    scheduler_conflict_policy = SKIP_WITH_REASON

  labels
  annotations
```

Derived subsystem policy entries should use stable subsystem identifiers. The
first set of policy-addressable derived subsystem identifiers should be:

| Subsystem | Meaning | First CommonFolio production default |
| --- | --- | --- |
| `LEXICAL_SEARCH` | Per-domain lexical search segments, manifests, and cursors. | `INCLUDE_OPTIONAL` |
| `SEMANTIC_VECTOR_SEARCH` | Semantic/vector physical index artifacts and related projection readiness evidence. | `INCLUDE_REQUIRED` |
| `GRAPH_CHECKPOINT` | Graph checkpoint artifacts that can reduce replay/restore time. | `INCLUDE_OPTIONAL` |
| `PERSISTENT_GRAPH_QUERY_INDEX` | Checkpoint-bound graph/query index sets. | `INCLUDE_OPTIONAL` |
| `METADATA_ADJACENCY_MEMORY_INDEX` | In-memory graph accelerators. | `DISCARD` |

The policy entry for a derived subsystem should describe both backup-time and
restore-planning behavior:

```text
DerivedSubsystemPolicy
  subsystem = SEMANTIC_VECTOR_SEARCH
  backup_mode = INCLUDE_REQUIRED | INCLUDE_OPTIONAL | REBUILD_ALLOWED | DISCARD
  restore_on_missing = FAIL_CLOSED | REBUILD_AFTER_RESTORE | MARK_UNAVAILABLE | DISCARD
  min_source_frontier = QUIESCED_BARRIER
  required_for_strict_reads = true
```

For the first production policy, semantic/vector physical index artifacts should
be `INCLUDE_REQUIRED` with `restore_on_missing = FAIL_CLOSED` unless an operator
explicitly chooses a degraded restore mode. Lexical, graph checkpoint, and
persistent graph/query index artifacts should be `INCLUDE_OPTIONAL`: include and
validate them when available, but do not let them block backup or restore if they
are missing, corrupt, incomplete, or incompatible. Metadata/adjacency memory
indexes should be `DISCARD`.

### Backup operation

A backup operation is one asynchronous attempt to create a backup set. It has an
operation identity, state, phase, requester/reason, timestamps, retry hints,
errors, blockers, and per-node progress.

For CommonFolio production, the normative backup operation is a cluster backup
operation. It runs preflight checks, quiesces the cluster, establishes checkpoint
barriers, captures per-node artifacts, uploads them to the repository, validates
them, writes the final backup-set manifest, and publishes the completion marker.

### Backup set

A backup set is the durable, restorable result of a successful backup operation.
It is identified by `backup_set_id` and lives under one repository prefix. A
backup set is complete only when all required artifacts, the final
`backup-set.json`, and the `COMPLETE` marker exist and validate.

Incomplete or failed operation output may exist in the repository, but it is not
a backup set that restore should accept.

### Backup-set manifest

The backup-set manifest, commonly `backup-set.json`, is the authoritative catalog
for a backup set. It records source cluster identity, created/completed times,
Mycel version/image, repository URI, expected node count, checkpoint/raft
barriers, per-node artifacts, sizes, checksums, and restore-relevant notes.

The manifest should not embed sensitive secrets. It may record non-sensitive
secret names, fingerprints, or references needed to verify that the restore
environment is configured correctly.

### Node artifact

A node artifact is the archive and node-level manifest produced by one
participating pod/node. A cluster backup set contains one required node artifact
per expected pod/PVC unless the design later introduces a different shard or
partition artifact model.

Each node artifact should record pod name, node ID, ordinal, raft node ID,
archive URI, node manifest URI, size, checksum, and applied/checkpoint indexes.

### Derived index artifact

A derived index artifact is a backup artifact for a rebuildable but potentially
expensive local structure such as a lexical index, semantic vector index, or
projection materialization. Derived artifacts are not the source of truth for
application data, but including them can materially reduce restore time.

Semantic index artifacts are special for CommonFolio production: they should be
backed up by default because recreating them may require expensive vector-index
rebuild work and, depending on the durable semantic state model, may otherwise
risk provider cost or non-deterministic re-embedding. Durable semantic records,
embedding metadata, model/version metadata, projection fences, and physical
semantic index artifacts should therefore be represented explicitly in backup
manifests.

### Completion marker

The completion marker is the final object written after validation succeeds. It
prevents restore from treating partially uploaded repository prefixes as valid
backup sets. Restore must fail closed when the marker is missing, mismatched, or
not supported by the manifest state.

### Validation report

A validation report records whether a repository or backup set is usable.
Repository validation checks reachability and permissions from the cluster.
Backup-set validation checks manifest structure, expected-node coverage,
artifact existence, sizes, checksums, checkpoint barriers, and completion marker.

### Restore plan

A restore plan is a dry-run result for applying a backup set to a target cluster.
It validates backup-set integrity, target-cluster compatibility, node/ordinal or
PVC mapping, required secrets, expected storage capacity, and whether the
selected restore target mode is safe.

### Restore operation

A restore operation is one asynchronous attempt to apply a backup set according
to a restore plan. The first production design should support both restore into a
new clean cluster and controlled full-state replacement of an existing cluster.

### Restore target modes

The first production restore design should support two full-cluster target modes:

```text
NEW_CLUSTER_BOOTSTRAP
EXISTING_CLUSTER_REPLACE
```

`NEW_CLUSTER_BOOTSTRAP` restores into a newly provisioned or otherwise empty
cluster. This is the safest disaster-recovery path because the target does not
contain live Mycel state that can be accidentally mixed with the backup.

`EXISTING_CLUSTER_REPLACE` restores into an existing Mycel deployment or cluster.
It is destructive: the selected backup set becomes the target cluster's durable
state, and any target state that differs from or is newer than the backup is
removed, replaced, or made inaccessible. It is not a merge, partial workspace
restore, online rollback, or best-effort overlay.

Existing-cluster replacement should require restore-plan preflight checks and
explicit operator intent:

- elevated admin authorization and an explicit destructive-restore confirmation;
- a target-wide restore lock that blocks writes and background mutators;
- no active non-terminal backup or restore operation for the target cluster;
- target membership, ordinal/PVC mapping, storage capacity, and daemon version
  compatibility checks;
- repository credential and object accessibility checks from the target cluster;
- a pre-restore safety backup of the target when possible, or an explicit
  operator waiver when the target is known bad, unreachable, or disposable;
- a wipe/reinitialize step for each target node's Mycel durable state, or a
  restore into replacement PVCs followed by a controlled cutover;
- post-restore validation before the target resumes normal traffic.

The restore plan should state how cluster identity is handled. Same-cluster
rollback normally preserves the backup set's cluster identity. Cross-cluster
restore or migration should require an explicit identity mode so operators do not
accidentally attach clients or automation to the wrong restored environment.

### Cluster identity modes

Cluster identity is the logical database-cluster identity, not just a Kubernetes
namespace, StatefulSet name, DNS name, or human display label. In raft mode the
system Raft metadata is authoritative for `cluster_id`, membership, partition
count, replica factor, and placement. Local files such as
`meta/clustering/node.json`, `meta/clustering/membership.json`, topology caches,
readiness state, and backend request validation are caches or consumers of that
authoritative identity.

The first production restore model should separate **logical identity** from
**deployment attachment**:

- `cluster_id` identifies the logical Mycel cluster that owns the restored data;
- `cluster_name` is a mutable human/operator label and must not be used as the
  correctness boundary;
- pod names, ordinals, PVC names, node DNS names, and backend advertise addresses
  are deployment-specific and may be supplied by the target mapping;
- graph IDs, space IDs, domain IDs, blob IDs, principal IDs, semantic rule IDs,
  and application data identifiers are data identity and are preserved by all
  full-cluster restore modes;
- the backup-set manifest is immutable lineage evidence and must continue to
  record the source cluster identity even when a restore adopts or generates a
  different target identity.

The conceptual identity modes are:

```text
PRESERVE_BACKUP_ID
ADOPT_TARGET_ID
GENERATE_NEW_ID
```

First production support matrix:

| Mode | Final `cluster_id` | First production full-state physical restore support | Intended use | Required safety checks |
| --- | --- | --- | --- | --- |
| `PRESERVE_BACKUP_ID` | The backup set's source `cluster_id`. | Required/default. | Same-cluster rollback, disaster-recovery replacement of the source cluster, or isolated restore drill. | Target is empty or destructively locked; node/ordinal/PVC mapping is compatible; if the restored cluster will be reachable by production clients or shared object stores, the old source cluster with the same `cluster_id` is fenced, stopped, destroyed, or otherwise proven unable to accept writes. |
| `ADOPT_TARGET_ID` | The target cluster's pre-existing/externally assigned `cluster_id`. | Not required for the first physical Raft-artifact implementation unless an explicit identity-transform path exists. `PlanClusterRestore` must return a blocker if the backup format/provider cannot safely transform identity. | Migration or destructive replacement where CommonFolio or deployment tooling needs to keep the target cluster registration while replacing its data with a backup. | Target identity exists and is locked; restore is not a merge; all source-vs-target lineage is recorded; restored data is transformed consistently before target startup; restored sessions are revoked. |
| `GENERATE_NEW_ID` | A new `cluster_<uuid>` generated by Mycel during restore planning/start. | Not required for the first physical Raft-artifact implementation unless an explicit identity-transform path exists. `PlanClusterRestore` must return a blocker if the backup format/provider cannot safely transform identity. | Staging/test clone, restore drill that must not share the production cluster identity, or migration that creates a new logical cluster. | Generated identity is recorded before apply; target has no conflicting state; CommonFolio or other clients are not automatically attached; restored sessions are revoked. |

For the first CommonFolio production implementation, the safe baseline for
manifest-driven physical/PVC restores is therefore `PRESERVE_BACKUP_ID`. The
other modes should remain in the design/API vocabulary, but they require either a
logical export/import restore path or an implementation-grade identity transform
that can rewrite every cluster-identity-bearing authoritative record and cache
without leaving old Raft metadata to replay the source identity. Until that exists,
planning must reject `ADOPT_TARGET_ID` and `GENERATE_NEW_ID` for physical backup
sets before any target state is mutated.

Identity transformation, when supported, must update or regenerate all of the
following consistently:

- authoritative system Raft metadata `cluster_id` and any selected `cluster_name`;
- local node identity caches, including `meta/clustering/node.json`;
- membership and topology caches, including `meta/clustering/membership.json` and
  peer records;
- readiness/diagnostic cluster identity evidence;
- backup/restore operation records stored in system metadata;
- backend-forwarding and cluster-internal validation inputs that carry
  `cluster_id`;
- any repository catalog pointer for future backups from the restored target.

It must not rewrite the immutable source backup-set manifest. It also must not
silently rewrite external object-store payload references, CommonFolio workspace
mappings, billing/quota policy, or application-level identifiers. Those remain
explicit deployment/CommonFolio responsibilities.

First production should keep node topology changes conservative:

- `node_count`, `partition_count`, and `replica_factor` must match the backup set;
- backup ordinals/PVCs map one-to-one to target ordinals/PVCs;
- Raft node IDs and partition placement are preserved unless a later design
  implements a tested topology-transform restore;
- target deployment fields such as pod names and backend advertise addresses may
  be validated or rendered from the restore plan, but any rewrite must be
  deterministic and recorded in the plan.

#### Auth/session effects

Full-cluster restore preserves principal records, role bindings, capability
bindings, service principals, and identity audit history as of the backup cut,
subject to the selected identity mode. It does not preserve target-cluster state
newer than the backup in `EXISTING_CLUSTER_REPLACE`.

Restore planning should expose an explicit auth-session policy:

```text
PRESERVE_RESTORED_SESSIONS
REVOKE_ALL_SESSIONS
```

Rules:

- target pre-restore sessions are always invalidated by
  `EXISTING_CLUSTER_REPLACE`, because the target identity store is replaced;
- `PRESERVE_RESTORED_SESSIONS` is allowed only with `PRESERVE_BACKUP_ID` for a
  same-logical-cluster rollback or disaster-recovery replacement;
- restored refresh/auth sessions preserved from the backup are only those that
  existed at the backup cut; tokens or sessions created after the backup should
  fail as stale/invalid after rollback;
- `REVOKE_ALL_SESSIONS` is required for `ADOPT_TARGET_ID` and `GENERATE_NEW_ID`
  and should be the default for restore drills, staging clones, and migrations;
- when sessions are revoked or absent, long-running clients and delegated service
  clients must re-authenticate/re-mint sessions and should observe the stable
  stale-session `Unauthenticated` error reasons documented by the auth/session
  API;
- token-signing secrets and external identity/provider credentials are supplied
  by the target deployment secret mechanism, not embedded in the backup manifest.

#### CommonFolio attachment semantics

Mycel should not decide CommonFolio subscription, billing, quota, customer
routing, or product-environment policy. Mycel should make restore identity and
lineage explicit so CommonFolio can attach the right environment deliberately.

- With `PRESERVE_BACKUP_ID`, CommonFolio may treat the restored target as the
  same logical Mycel cluster only if the previous source cluster is fenced or the
  restore is an isolated drill that is not attached to production traffic.
- With `ADOPT_TARGET_ID`, CommonFolio may keep the target cluster registration,
  but the restore operation must record that the target data was replaced from a
  source backup set. This is a destructive data replacement, not a merge.
- With `GENERATE_NEW_ID`, CommonFolio must register or attach the restored cluster
  as a new environment/cluster. Production client traffic and automation must not
  be attached automatically.
- In all modes, Mycel restores authoritative current usage values from the backup
  cut. CommonFolio remains responsible for interpreting those values for
  subscription, billing, quota, grace-period, and customer-notification policy.

#### Identity audit and lineage

Every restore plan and restore operation should record:

- `source_cluster_id`, `source_cluster_name`, `backup_set_id`, and repository ID;
- target pre-restore cluster identity, if any;
- final cluster identity and selected identity mode;
- target mode (`NEW_CLUSTER_BOOTSTRAP` or `EXISTING_CLUSTER_REPLACE`);
- node/ordinal/PVC mapping and any deployment-address rewrite;
- source-fencing evidence or explicit isolation statement when preserving the
  backup cluster ID outside the original deployment;
- auth-session policy and whether sessions were revoked;
- CommonFolio/deployment attachment reference or an explicit statement that no
  production attachment was performed;
- requester, reason, destructive confirmation, pre-restore safety backup or
  waiver, timestamps, and validation result.

Historical activity/audit records restored from the backup remain historical
records from the source state at the backup cut. New post-restore audit records
should use the final cluster identity and include restore lineage fields rather
than rewriting old history.

## 4. Consistency, quiescence, and checkpoint barriers

### Quiescence assumption

The first production-ready backup design assumes the cluster is quiesced before
capturing backup artifacts. This means Mycel stops admitting new writes, drains
active writes, pauses background mutators, establishes the necessary committed
state/checkpoint barriers, and only then captures the backup set.

Reads may be allowed during the quiesced window only through a conservative
read-only allowlist. Backup and quiesce status/control APIs must remain
available. The initial design may keep the cluster quiesced for the full capture
and upload window; later work can shorten the write outage by creating an
immutable local/PVC snapshot under quiescence, releasing quiescence, and then
uploading that snapshot asynchronously.

### Quiesce lease and checkpoint barrier

A quiesce lease is the coordinator-owned proof that new writes are blocked,
active writes are drained, and background mutators are paused for the current
operation. A checkpoint barrier is the committed state boundary used to decide
what the backup captures. Both concepts should be visible in operation status and
manifests because they explain what state the backup represents.

### Quiesce API and worker policy

First production should treat quiescence as an explicit cluster-wide operating
mode with a generated `quiesce_lease_id`, owner operation ID, mode, reason,
deadline, and per-participant drain status. It is not a process shutdown: Raft,
health probes, backup control, and capture-provider work continue, while ordinary
state-changing user/admin work is rejected or drained.

The first CommonFolio production default should be:

```text
allow_read_only_during_quiesce = false
read_allowlist_profile = CONTROL_ONLY
```

`CONTROL_ONLY` keeps the backup window simple: no ordinary client reads or writes
are admitted while the coordinator establishes barriers and creates immutable
local/PVC snapshots. Later policies may opt in to `STRICT_READ_ONLY`, but the
allowlist must be explicit and generated/reviewed from service descriptors; it
must not rely on method-name prefixes such as `Get*` or `List*` as the
correctness boundary.

Quiesce ingress classes:

| Class | `CONTROL_ONLY` behavior | `STRICT_READ_ONLY` behavior | Notes |
| --- | --- | --- | --- |
| Backup/restore owner control APIs | Allowed for the owning operation. Status/list/cancel/validate calls are allowed. A second start returns an active-operation conflict. | Same. | Includes cluster-backup coordinator APIs, restore status/cancel, repository validation, and local backup backend RPCs carrying the active operation/lease identity. |
| Cluster and process health/status APIs | Allowed if read-only. | Allowed if read-only. | Includes cluster status/health/runtime status, Raft group status, readiness, metrics, and bounded diagnostics that do not repair or write. |
| Internal Raft/backend traffic | Allowed. | Allowed. | Raft heartbeats/messages, read-index/barrier traffic, backend cluster view/status, and backup freeze/archive RPCs must keep working. Forwarded client requests are allowed only when the inner request is allowed by this policy. |
| Auth identity reads | `WhoAmI` / access-inspection style reads are allowed. | Same. | Token verification may continue. Login, refresh, logout, session revoke, delegated-session creation, and principal/session mutations are blocked because they write identity/session state. |
| Ordinary client data reads | Blocked. | Allowed only for the explicit bounded read allowlist below. | Existing read RPCs either drain before quiesce acquisition or, in `STRICT_READ_ONLY`, may continue without blocking the backup barrier. |
| Ordinary client/admin mutations | Blocked. | Blocked. | This includes graph, blob, schema, space/domain, semantic rule, automation, inference catalog, identity, activity, import/export, maintenance, and query mutation APIs. |
| Long-lived streams and artifact-producing reads | Blocked by default. | Blocked unless a later profile explicitly allows them with bounds. | New change streams, export creation/download streams, long blob downloads, and forensic exports can make the quiesced window harder to reason about. Existing streams may be canceled or allowed to finish according to the operation timeout. |

The first `STRICT_READ_ONLY` allowlist, if enabled by policy, should be limited
to bounded, non-mutating reads:

- space/domain/schema metadata reads: list/get spaces, domains, and domain
  schemas; schema validation only when it does not persist diagnostics;
- graph reads: get/list nodes and edges, list children, get parent;
- read-only query execution and explain, only after the query planner proves the
  request cannot mutate graph, schema, metadata, blobs, automations, or external
  providers;
- lexical and semantic search reads when the selected consistency policy is
  already satisfiable; search must not trigger rebuild, backfill, reindex, or
  provider work;
- semantic rule list/get and semantic reliability/status reads;
- metadata catalog reads;
- blob metadata reads, and blob payload reads only when the implementation can
  bound the stream and prove it will not create local staging files;
- automation, inference, principal, activity, backup, and cluster list/get/status
  reads that do not write audit/session/usage/lease state.

Everything else is blocked during quiescence unless it is executed by the backup
or restore coordinator as part of the active operation. In particular, these APIs
must be blocked in both profiles:

- `Login`, `Refresh`, `Logout`, auth-session revocation, delegated-session
  creation, principal creation/update/delete, role/capability grant changes, and
  password/session mutations;
- session opening, session heartbeat, read-write transaction begin/commit,
  transaction rollback/close as user traffic, and graph mutation operations;
- blob upload/delete and graph blob-node creation;
- schema, space, domain, semantic-rule, inference-catalog, credential, grant,
  policy, vector-store, automation, and procedure mutations;
- import operations, export creation/deletion, activity append, manual lexical or
  semantic rebuild/backfill/process/retry/cancel maintenance operations, graph
  checkpoint creation, and persistent graph/query index rebuilds;
- any API that can initiate model/provider calls, write usage events, write
  provider-budget attempts, or advance automation/semantic output state.

Background worker policy:

| Worker/subsystem | Quiesce behavior | Resume behavior |
| --- | --- | --- |
| Raft transport, Raft apply, membership/readiness, and barrier/freeze logic | Continue. They are needed to reach and prove the backup barrier. Ordinary user-data proposals are blocked, but backup control/barrier records may be proposed by the coordinator. | Continue normally after release. |
| Backup coordinator, capture provider, uploader/data mover, repository/object validation | Continue. These are the owner of the quiesce lease. | Continue upload/validation after quiescence is released when an immutable snapshot/copy has been captured. |
| Graph writes and graph-change publishing | Drain active commits, then block. No new graph mutations or graph-change events should be produced after the barrier. | Resume after release; consumers start from restored/advanced checkpoints. |
| Session/transaction runtime | Block new sessions and transactions. Mark open read-write transactions rollback-only or abort them during drain; allow already-entered commit/finalization work to finish before the drain deadline. | Clients may open new sessions/transactions after release; aborted transactions must be retried by clients. |
| Blob/object-store writes | Drain active uploads/deletes and object-store staging work. No new uploads, deletes, or staging cleanup that mutates backup inputs. | Resume and clean remaining non-restorable staging after release. |
| Semantic maintenance, analyzer, worker, backfill/rebuild, physical vector-index publication, projection lease advancement, provider invocation | Pause and drain active work. Do not start provider calls or publish new vector records/indexes after the barrier. | Resume from restored dirty queues, reliability state, and checkpoints; rebuilds continue only after readiness validation. |
| Lexical async replay/rebuild/compaction | Pause and drain active publication. Do not publish new lexical segments/cursors after the barrier. | Resume or rebuild according to derived-state policy. |
| Graph checkpoint and persistent graph/query index builders | Pause unless the backup coordinator explicitly requests a checkpoint/barrier artifact. | Resume scheduled/index work after release. |
| Automation scheduler, reconciliation, pending invocation processing, output commits, procedure inference | Pause and drain active output commits/provider calls. Do not schedule new runs or commit automation output during quiescence. | Resume from durable automation state and graph-change checkpoints. |
| Inference catalog/budget/usage writers and provider calls | Block catalog/policy mutations. Active provider calls either finish and durably record usage before the deadline or are canceled/failed without publishing partial semantic/automation output. | Resume normal invocation after release. |
| Graph-change notification/outbox consumers and retention/compaction | Source-feed delivery to mutating consumers pauses. Retention/compaction that mutates durable feed state pauses. Read-only diagnostics may continue. | Resume from durable consumer checkpoints. |
| Import/export and diagnostic artifact jobs | Jobs that write under the Mycel data directory pause or are rejected. Pure repository validation may continue. | Resume or retry after release. |
| Activity/audit appenders | Ordinary activity append is blocked. Backup/restore-owned audit records may be written through the coordinator control lane and must be labeled with the operation ID. | Resume ordinary append after release. |

Drain and timeout rules:

1. The coordinator first persists quiesce intent and operation ownership, then
   closes the ingress gate so no new disallowed RPCs or background tasks start.
2. Participants drain already-entered write work to zero and report structured
   blockers: subsystem, operation kind, count, oldest age, and safe diagnostic
   IDs. Secrets, payloads, query text, and customer data are not included.
3. Read behavior depends on the profile: `CONTROL_ONLY` drains active ordinary
   reads before quiesce is acquired; `STRICT_READ_ONLY` allows explicit bounded
   reads to continue and does not let them block the backup barrier.
4. Active writes that cannot finish by `quiesce_timeout` fail the backup before
   capture begins. First production should not force-kill pods or take a backup
   while a participant still reports active writes.
5. Open read-write transactions that have not entered commit are aborted or
   marked rollback-only by the session participant. A later commit attempt fails
   and must not publish graph state.
6. Partial uploads, pending semantic publications, half-applied maintenance
   transitions, and other unresolved ephemeral state after drain fail validation
   unless the subsystem has a documented recovery step that completes or discards
   them before the barrier.
7. Any failure after a participant lease is acquired must release all acquired
   quiesce leases before the backup operation reaches a terminal state.

Error behavior should be stable for clients and operators:

- disallowed work during backup quiescence returns gRPC `codes.Unavailable` with
  a stable `google.rpc.ErrorInfo` detail using domain `mycel.backup`, reason
  `cluster_quiesced`, and safe metadata such as `backup_operation_id`,
  `quiesce_lease_id`, `mode`, and `retryable_after_release = true`;
- a second backup/restore start while an operation is active returns a conflict
  or failed-precondition response, not an implicit queue;
- transactions aborted by quiescence return `codes.Aborted` or
  `codes.FailedPrecondition` with reason `transaction_aborted_by_quiesce`;
- authorization, visibility, and policy errors keep their normal
  `PermissionDenied`/`NotFound` behavior and are not reclassified as quiesce
  errors;
- `GetClusterBackupStatus` and related status APIs expose current blockers and
  timeout deadlines so operators can see why quiescence has not completed.

For destructive `EXISTING_CLUSTER_REPLACE`, the restore lock should be stricter
than backup quiescence: no ordinary data-plane reads are admitted, even if the
backup policy would allow `STRICT_READ_ONLY`, because target state is about to be
wiped or replaced.

### Checkpoint/barrier fields

A checkpoint barrier is the exact committed frontier that defines the backup cut.
It is not just a timestamp. It is a set of per-Raft-group, per-subsystem, and
per-derived-artifact facts that let restore prove that every included artifact is
consistent with the same quiesced state.

The logical barrier schema should use stable field names even if the final
protobuf/JSON nesting changes during API implementation:

```text
CheckpointBarrierSet
  schema_version = 1
  barrier_set_id
  backup_operation_id
  backup_set_id
  cluster_id
  quiesce_lease_id
  established_at
  coordinator_node_id
  coordinator_raft_node_id
  read_allowlist_profile = CONTROL_ONLY | STRICT_READ_ONLY
  write_gate_closed_at
  participants[]
  raft_groups[]
  system_metadata
  graph_domains[]
  identity
  usage_accounting[]
  object_references[]
  graph_change_feeds[]
  automation[]
  semantic[]
  lexical[]
  projections[]
  derived_artifacts[]
```

Frontier semantics:

- all numeric frontiers are inclusive: the backup contains committed state up to
  and including that frontier;
- authoritative subsystem barriers must be at the quiesced cut and must not lag
  behind the Raft group barrier that owns their writes;
- derived artifacts may have their own source frontier. Required artifacts must
  satisfy the policy's minimum source frontier; optional artifacts may lag only
  if the manifest marks them for rebuild/catch-up during restore;
- no included authoritative artifact may contain state newer than its recorded
  parent barrier;
- timestamps are diagnostic only and must not be used as the correctness proof.

Common fields on subsystem barriers:

```text
subsystem                 # stable subsystem name
scope                     # cluster | node | space | domain | rule_binding | object_store
space_id                  # when scoped to a space
domain_id                 # when scoped to a domain
owner_raft_group_id       # raft group that commits authoritative changes
owner_raft_barrier_index  # raft index used as the parent committed frontier
source_frontier_kind      # RAFT_INDEX | GRAPH_REVISION | USAGE_REVISION | OBJECT_REFERENCE_SET | PROJECTION_CHECKPOINT
source_frontier_value     # value for the kind above
checksum_sha256           # checksum of the persisted metadata/artifact evidence when available
validation_state          # INCLUDED_AND_VALIDATED | REBUILD_REQUIRED | EXTERNAL_DEPENDENCY_VERIFIED | FAILED
validation_error          # sanitized, optional
```

Required barrier records:

| Barrier record | Required fields | Validation rules |
| --- | --- | --- |
| `raft_groups[]` | `group_id`, `group_kind = SYSTEM | PARTITION`, `partition_id` when applicable, `barrier_index`, `commit_index`, `term`, `leader_raft_node_id`, `members[]`, `per_node[]` with `node_id`, `pod_name`, `raft_node_id`, `applied_index`, `last_index`, and `snapshot_index`. | Every expected Raft group is present. `commit_index >= barrier_index`; every captured node that hosts the group has `applied_index >= barrier_index`; membership matches system metadata; node artifacts record the same barrier indexes. |
| `system_metadata` | `cluster_id`, `cluster_name`, `bootstrap_epoch`, `node_count`, `partition_count`, `replica_factor`, `system_raft_group_id`, `system_raft_barrier_index`, `membership_hash_sha256`, `placement_hash_sha256`, and `expected_nodes[]` with `node_id`, `raft_node_id`, `ordinal`, and `pod_name`. | Source cluster identity in the manifest matches the barrier. Expected nodes are exactly the nodes captured. Membership/placement hashes match the restored system metadata before Raft groups are started. |
| `graph_domains[]` | `space_id`, `domain_id`, `partition_group_id`, `raft_barrier_index`, `graph_revision`, `domain_manifest_checksum_sha256`, `segment_set_checksum_sha256`, optional `latest_state_checksum_sha256`, `graph_change_journal_revision`, and `notification_current_revision`. | Domain graph revision is the committed domain frontier at the backup cut. Graph segments/manifests are not ahead of `graph_revision`. Notification/outbox current revision is not behind the graph frontier needed by retained consumers. Derived/search artifacts for the domain must reference a source revision `<= graph_revision` unless the policy requires equality. |
| `space_domain_schema` | `space_id`, `domain_id`, `space_catalog_revision` or parent Raft barrier, `domain_catalog_revision` or parent Raft barrier, `schema_revision` when available, `schema_hash_sha256`, and `visibility_policy_hash_sha256` when available. | Restored schemas and space/domain metadata are the definitions used to interpret graph records, query indexes, automation bindings, semantic rules, and usage scopes at the backup cut. |
| `identity` | `identity_raft_group_id`, `identity_raft_barrier_index`, `authorization_revision`, `principal_store_checksum_sha256`, `role_binding_checksum_sha256`, `capability_grant_checksum_sha256`, `refresh_session_store_checksum_sha256`, and selected `auth_session_policy`. | Principal/role/capability state is restored at the backup cut. Session handling follows the restore auth-session policy. Raw password hashes/secrets are stored only in their normal encrypted stores, never in manifests. |
| `usage_accounting[]` | `scope_kind`, `scope_id`, `owner_raft_group_id`, `owner_raft_barrier_index`, `logical_usage_revision`, `usage_event_high_water`, `budget_scope_high_water` when applicable, `current_values_checksum_sha256`, and `source_authority = RAFT_COMMITTED_LOGICAL | RECOMPUTABLE_FROM_AUTHORITATIVE_STATE`. | Values used for CommonFolio hard throttling/billing are logical and replay-safe. Physical/index/object-store observations are recorded only as non-authoritative diagnostics. Restore can either restore current values or recompute them from included authoritative state. |
| `object_references[]` | `usage = BLOB_PAYLOADS`, `provider`, `payload_backend`, `metadata_raft_group_id`, `metadata_raft_barrier_index`, `object_reference_manifest_uri`, `object_reference_manifest_checksum_sha256`, `object_count`, `logical_bytes`, `external_store_ref`, `external_recovery_point_ref`, and `validated_at`. | Blob metadata/descriptors match graph/blob state at the backup cut. For S3-compatible payload backends, referenced external objects and recovery points are verified or restore fails closed unless degraded restore is explicitly selected. |
| `graph_change_feeds[]` | `feed_kind = OUTBOX | NOTIFICATION`, `space_id`, optional `domain_id`, `current_revision`, `oldest_retained_revision`, `retained_event_count`, `retention_policy_hash_sha256`, and `consumer_cursors[]` with `consumer`, `last_graph_revision`, and optional `last_event_id`. | Feed state is consistent with graph revisions. No restored consumer cursor is ahead of retained feed/source graph state. Consumers with gaps are marked rebuild/replay-required. |
| `automation[]` | `space_id`, optional `domain_id`, `automation_raft_group_id`, `automation_raft_barrier_index`, `invocation_ledger_high_water`, `reconciliation_operation_high_water`, `graph_replay_cursors[]`, `procedure_control_revision`, `space_authorization_revision`, and `output_fence_checksum_sha256`. | Automation will not replay outputs past the restored graph/authorization frontiers. Running/leased work is normalized on restore and does not resume as already-owned work. |
| `semantic[]` | `space_id`, `domain_id`, `semantic_rule_id`, `embedding_binding_key`, `partition_group_id`, `raft_barrier_index`, `source_graph_revision`, `semantic_generation`, `rebuild_epoch`, `rule_configuration_hash`, `resolved_configuration_hash`, `vector_store_id`, `vector_space_key`, `dimensions`, durable record evidence (`record_manifest_checksum_sha256`, `active_segment`, `segment_count`, `record_count`, `last_record_id`, `last_record_offset`, `latest_view_checksum_sha256`, `tombstone_count`), maintenance evidence (`dirty_event_last_graph_revision`, `dirty_event_last_id`, `analyzed_through`, `fresh_through`, `unresolved_ranges_checksum_sha256`, `maintenance_checkpoints[]`), and physical artifact evidence when included (`artifact_generation`, `index_format_version`, `live_record_count`, `artifact_checksum_sha256`). | Durable semantic records are not ahead of `source_graph_revision`; latest views are backed by segment/tombstone records; semantic generation, rebuild epoch, configuration hash, vector dimensions, and vector-store identity match reliability/projection state. For CommonFolio's default required semantic artifacts, physical index `fresh_through` must satisfy the policy's required graph frontier. |
| `lexical[]` | `space_id`, `domain_id`, `source_graph_revision`, `cursor_indexed_graph_revision`, `manifest_checksum_sha256`, `segment_set_checksum_sha256`, `analyzer_version`, `index_format_version`, and policy action. | Included lexical artifacts are not ahead of the graph domain frontier. Missing, stale, or corrupt optional artifacts are marked `REBUILD_REQUIRED` and do not block restore. |
| `projections[]` | `projection_id`, `kind`, `space_id`, optional `domain_id`, `configuration_hash`, `configuration_epoch`, `materializer_version`, `source_frontier_revision`, optional `source_frontier_cursor`, `fencing_epoch`, `artifact_generation`, `state`, `replica_status[]`, and readiness tuple fields such as `leader_node`, `leader_term`, `semantic_generation`, `rebuild_epoch`, and `fresh_through` for semantic projections. | Projection checkpoints are readiness evidence, not a substitute for authoritative state. Restore must revalidate or reissue node/term-specific leases under the target cluster before strict reads are served. |
| `derived_artifacts[]` | `subsystem`, `artifact_id`, `policy_mode`, `included`, `restore_policy`, `source_frontier_kind`, `source_frontier_value`, `artifact_uri`, `checksum_sha256`, `format_version`, and `validation_state`. | Required derived artifacts must be present and validated. Optional invalid artifacts are omitted or marked rebuild-required. Discarded artifacts are never restored as ready state. |

Validation should fail closed when any required barrier is missing, any included
artifact is ahead of its authoritative parent frontier, any required checksum does
not match, any expected node lacks the necessary Raft applied indexes, or any
external dependency cannot be verified according to policy. Validation may record
warnings only for optional artifacts that are omitted or intentionally marked for
rebuild/discard.

Restore should use the barrier set to decide startup sequencing:

1. restore system metadata, node identity, and Raft state to the recorded Raft
   barriers;
2. validate graph/domain/schema state against graph-domain barriers;
3. restore identity, usage, object-reference, automation, and semantic durable
   progress barriers;
4. validate included derived artifacts against their source frontiers;
5. mark any rebuild-required optional projection/index state before admitting
   reads;
6. revalidate readiness/lease evidence under target cluster leadership.

## 5. Repository layout, capture providers, and restore algorithms

### Backup directory structure

A successful production backup should not live on the pods it protects. Pod-local
files are allowed only as temporary scratch space. The authoritative backup is a
complete backup set in a common, externally accessible backup repository.

The initial repository abstraction should be S3-compatible object storage:

- AWS/CommonFolio production uses Amazon S3.
- Non-AWS, self-hosted, local, and Mycel Lab deployments may use MinIO or
  another S3-compatible object store.

Backup correctness should rely on Mycel-authored manifests, explicit SHA256
checksums, and a final completion marker, not on backend-specific directory
semantics or object ETags. Restore should ignore incomplete backup sets and
accept only sets with a valid final manifest, all expected per-node artifacts,
matching checksums, and a completion marker.

The logical layout should group all per-pod artifacts under one backup-set prefix
such as:

```text
s3://<bucket>/<prefix>/clusters/<cluster_id>/backup-sets/<backup_set_id>/
  backup-set.json
  COMPLETE
  nodes/
    myceld-0/archive.tar.zst
    myceld-0/manifest.json
    myceld-1/archive.tar.zst
    myceld-1/manifest.json
```

For a 20-pod cluster, the backup set may contain 20 per-pod artifacts, but all
artifacts must be discoverable from one repository URI and one backup-set
manifest.

### Backup capture model and alternatives

A production backup is more than transferring a zipped directory tree. A zipped
or compressed archive may be the representation of a node artifact, but backup
correctness comes from the cluster-level protocol around that archive:

- resolve the effective backup policy;
- preflight repository access, credentials, version compatibility, cluster
  membership, and available scratch/storage capacity;
- quiesce writes and background mutators;
- establish committed checkpoint barriers for every required subsystem;
- capture node and derived-subsystem artifacts that correspond to those
  barriers;
- upload artifacts to the external repository;
- verify sizes, checksums, expected-node coverage, derived-state policy, and
  checkpoint/barrier evidence;
- write the final `backup-set.json` manifest and only then publish `COMPLETE`.

The archive format is therefore an implementation detail. Restore should trust
the manifest, checksums, policy evidence, and completion marker rather than the
fact that an object is a `.zip`, `.tar`, or directory copy.

Potential capture strategies:

1. **Quiesced per-node filesystem archive.** Under quiescence, each pod writes a
   deterministic archive of its durable Mycel directories plus a node manifest,
   uploads the archive, and the coordinator writes the backup-set manifest. This
   is simple and portable, but the write outage can last through archive and
   upload unless a local immutable snapshot is introduced.
2. **Quiesced local snapshot, asynchronous upload.** Under quiescence, each pod
   creates a local filesystem/PVC snapshot or immutable copy, records the
   checkpoint barrier, then releases quiescence while uploading the snapshot in
   the background. This shortens write outage but requires reliable local
   snapshot/copy semantics and scratch-space accounting.
3. **Storage-layer snapshots.** The coordinator quiesces Mycel, asks the
   infrastructure layer for PVC/EBS/CSI snapshots, records snapshot identifiers
   in the manifest, and optionally copies/export them to the backup repository.
   This can be fast, but is more cloud/provider-specific and restore must know
   how to materialize those snapshots in both new and existing clusters.
4. **Logical export plus artifact bundles.** Mycel exports authoritative system,
   graph, identity, usage, and semantic records through service-level code and
   attaches selected derived artifacts according to policy. This can be more
   portable and easier to validate across versions, but is more implementation
   work and may be slower for large clusters.
5. **Snapshot plus WAL/incremental chain.** A full backup is followed by
   incremental Raft/WAL/object changes. This reduces steady-state backup cost and
   enables point-in-time recovery, but it is significantly more complex and is
   not required for the first production cut.

The recommended first production path is a **manifest-driven full cluster backup**
using quiesced local/PVC snapshots with asynchronous upload to S3-compatible
storage. The design should keep the manifest and restore API independent of the
capture provider so later releases can add other snapshot mechanisms, logical
exports, or incrementals without changing the operator model.

#### Capture provider abstraction

Mycel should support multiple capture providers behind one backup contract. The
provider determines how each node/PVC creates an immutable source for upload; it
does not define what counts as a valid backup. Backup validity is always defined
by the Mycel backup-set manifest, checksums, checkpoint/barrier evidence,
derived-subsystem policy evidence, uploaded artifacts, and `COMPLETE` marker.

Conceptually:

```text
Backup operation
  quiesce + checkpoint barriers + policy resolution + validation + manifest

CaptureProvider
  creates an immutable source for each node/PVC under quiescence

BackupRepository
  stores deterministic uploaded artifacts, manifests, and completion marker
```

The first provider set should be:

```text
CSI_VOLUME_SNAPSHOT       # Kubernetes-native snapshots for EBS and other capable CSI drivers
FILESYSTEM_IMMUTABLE_COPY # portable fallback for local-path/dev/Lab/non-CSI installs
AWS_EBS_SNAPSHOT          # optional AWS-specific provider if direct EBS integration is chosen
```

Provider responsibilities:

1. report capabilities during repository/policy preflight;
2. verify required Kubernetes/cloud/storage primitives are available;
3. create an immutable node/PVC source during the quiesced window;
4. return provider evidence, such as snapshot class, snapshot name, clone/staging
   PVC, EBS snapshot ID, local immutable-copy path, generation ID, and source PVC
   identity;
5. expose a read path or data-mover plan for packaging deterministic artifacts;
6. clean up temporary snapshots, clones, staging PVCs, or local copies after
   success or terminal failure.

For `CSI_VOLUME_SNAPSHOT`, the snapshot may not be directly readable. The data
mover may need to create a temporary PVC from the `VolumeSnapshot`, mount that
PVC in an uploader pod, package deterministic artifacts, upload them to the
backup repository, and then delete the temporary PVC and snapshot according to
policy.

For `FILESYSTEM_IMMUTABLE_COPY`, the pod copies selected durable directories and
selected derived artifacts into a local immutable staging path while quiesced,
then uploads from that path after quiescence is released. This is the portable
fallback, but it can make the quiesced window longer and requires explicit local
capacity checks.

For `AWS_EBS_SNAPSHOT`, Mycel or an operator-owned controller can create EBS
snapshots directly. Because the authoritative backup remains the S3-compatible
backup set, direct EBS snapshots still need a data-mover path that materializes a
readable volume, packages deterministic artifacts, uploads them, and records only
non-secret provider evidence in the manifest.

Backup policy should select the provider explicitly or through a constrained
`AUTO` mode. Production policies should not silently fall back to a provider with
a materially worse outage or durability profile unless `fallback_allowed` is set
and the fallback is recorded in the operation and manifest.

#### First production capture strategy trade-offs

The two most practical first-production strategies have different operational
profiles:

| Strategy | Benefits | Drawbacks |
| --- | --- | --- |
| Quiesced per-node filesystem archive | Simplest correctness model; portable across Kubernetes, Docker Compose, bare metal, S3, and MinIO; easiest to implement and test in Mycel Lab; backup artifact is self-contained and easy to inspect; fewer dependencies on CSI/filesystem snapshot behavior; restore can unpack verified archives into new or replacement volumes. | Longest write outage because quiescence may need to cover archive creation and upload; outage grows with data size, node count, semantic index artifact size, and repository throughput; higher risk of timeout or operator pressure during large backups; requires enough local scratch space to build archives; failed upload may require repeating the quiesced capture. |
| Quiesced local/PVC snapshot with asynchronous upload | Much shorter write outage because quiescence only needs to cover checkpointing and creation of an immutable local snapshot/copy; upload can continue after writes resume; better fit for large semantic/vector artifacts; failed upload can often retry from the immutable snapshot without re-quiescing; closer to the later storage-snapshot model. | More implementation and operational complexity; requires reliable snapshot/copy semantics and proof that the snapshot is immutable and corresponds to the checkpoint barrier; needs scratch/PVC capacity planning and cleanup; local snapshot loss before upload completes can invalidate the operation; restore tooling must understand snapshot-produced artifact layout; harder to test uniformly across deployment environments. |

CommonFolio's first production implementation should use quiesced local/PVC
snapshots with asynchronous upload while keeping the manifest, policy,
validation, and restore contract independent of that capture strategy.

#### Backup algorithm for quiesced local snapshots

The backup operation should separate the short write-outage window from the
longer upload/validation window:

1. **Create or resume operation.** Resolve the idempotency key, enforce the
   single-active-cluster-backup rule, reserve a `backup_operation_id`, reserve a
   generated `backup_set_id`, and persist the operation in `PENDING`.
2. **Resolve policy.** Load the backup repository, backup policy, and any safe
   ad-hoc overrides. Persist the immutable effective policy snapshot on the
   operation.
3. **Preflight.** Validate repository reachability and permissions from the
   coordinator and expected pods; verify credential references; enumerate
   expected nodes, ordinals, PVCs, Raft groups, and derived subsystems; check
   daemon/version compatibility; select and validate the capture provider;
   verify snapshot/copy/data-mover capability; verify scratch, staging, and
   destination capacity; and reject the operation if a restore or another backup
   is active.
4. **Converge.** Wait for required pods and Raft groups to be healthy, caught up,
   and mapped to expected PVCs. If policy requires semantic/vector artifacts,
   wait for semantic projection/index readiness to reach the required freshness
   target before entering the short quiesced window, or fail/require explicit
   degraded-backup intent.
5. **Acquire quiesce lease.** Block new writes, drain active writes, and pause
   background mutators/materializers that can change durable backup inputs.
   Backup/quiesce status APIs and approved read-only APIs may remain available.
6. **Establish checkpoint barriers.** Record the committed state frontier for
   system metadata, Raft groups, graph partitions/domains, identity state, usage
   accounting, semantic state, object references, and projection/readiness
   fences. This barrier defines the backup cut.
7. **Freeze derived artifacts.** For each derived subsystem, apply the effective
   policy. Required included artifacts, especially semantic/vector indexes, must
   either be at the checkpoint barrier or carry enough source-frontier evidence
   for validation. Rebuildable/discarded subsystems record rebuild requirements
   instead of physical artifacts.
8. **Create immutable provider snapshots.** On each expected pod/PVC, invoke the
   selected capture provider to create an immutable source for the selected
   durable directories and selected derived artifacts. Depending on provider,
   this may be a Kubernetes `VolumeSnapshot`, direct EBS snapshot, temporary PVC
   clone, or filesystem immutable copy. Each node writes a snapshot manifest
   containing source barriers, included paths, excluded paths, sizes or
   preliminary digests when available, provider evidence, snapshot generation ID,
   and local or provider snapshot reference. The snapshot source must be
   immutable with respect to writes after this point.
9. **Release quiescence.** Once every required node has durably recorded its
   local snapshot manifest and the coordinator has recorded the cluster snapshot
   cut, release the quiesce lease and resume writes/background work. From this
   point, upload and validation may continue without extending the write outage.
10. **Package and upload asynchronously.** Each node or uploader packages the
    immutable provider source into one or more deterministic artifacts, computes
    checksums, and uploads them under a staging prefix in the S3-compatible
    repository. CSI/EBS providers may require a data-mover PVC or temporary
    restored volume before packaging. Uploads may retry without re-quiescing as
    long as the immutable provider source still exists and validates.
11. **Validate uploaded artifacts.** The coordinator verifies expected-node
    coverage, object existence, sizes, SHA256 checksums, node manifests,
    checkpoint/barrier consistency, derived-subsystem policy evidence, and
    repository readback. Missing or corrupt required artifacts fail the backup.
12. **Finalize backup set.** Write the final `backup-set.json` manifest with the
    effective policy snapshot, source barriers, node artifacts, derived-state
    evidence, validation report, supported restore modes, and repository URIs.
    Publish the `COMPLETE` marker only after the final manifest and all required
    artifacts validate.
13. **Clean up provider snapshots.** Delete temporary provider snapshots, clone
    PVCs, staging volumes, local immutable copies, and scratch files after
    successful finalization or after a failed operation is no longer retryable.
    Failed operations should leave enough operation history to explain whether
    repository staging objects and provider-side temporary resources can be
    safely expired.

A backup created by this algorithm is restorable only after the final manifest
and completion marker exist and validate. Provider snapshots and local copies are
temporary retry sources, not authoritative backups.

#### Restore algorithm for quiesced local snapshot backups

Restore consumes the external backup-set manifest and uploaded artifacts. It does
not depend on the original pods or their local snapshots still existing.

1. **Plan restore.** Read `backup-set.json` and `COMPLETE` from the repository;
   validate manifest version, checksums, expected-node coverage, source barriers,
   derived-state policy evidence, daemon/storage compatibility, required secrets,
   and target mode.
2. **Validate target mode.** For `NEW_CLUSTER_BOOTSTRAP`, prove the target is
   empty or newly provisioned. For `EXISTING_CLUSTER_REPLACE`, acquire or prove a
   target-wide restore lock, block writes/background mutators, verify destructive
   authorization and confirmation, verify no active backup/restore operation, and
   require a pre-restore safety backup or explicit waiver.
3. **Map artifacts to target storage.** Resolve each backup node/ordinal/PVC to a
   target node/ordinal/PVC or replacement volume. Verify expected node count,
   storage capacity, filesystem permissions, repository credentials, and object
   accessibility from the target environment.
4. **Stage artifacts.** Download required node artifacts and derived artifacts to
   target-local staging storage or stream them through a verified unpack path.
   Verify checksums before apply. Do not mutate target durable state until all
   required artifacts for the selected restore mode are available.
5. **Prepare target state.** For `NEW_CLUSTER_BOOTSTRAP`, initialize empty Mycel
   data directories or replacement PVCs. For `EXISTING_CLUSTER_REPLACE`, stop or
   isolate Mycel processes for the target nodes and wipe/reinitialize durable
   Mycel state, or restore into replacement PVCs and perform a controlled
   cutover.
6. **Apply node artifacts.** Unpack each verified node artifact into its mapped
   target data directory/PVC. Restore system metadata, Raft state/snapshots,
   graph state, identity state, usage state, object references, projection fences,
   and included derived artifacts exactly as described by the manifest.
7. **Apply cluster identity mode.** Preserve the backup cluster identity for
   same-cluster rollback unless the restore plan explicitly selected a supported
   cross-cluster identity mode such as adopting the target identity or generating
   a new one.
8. **Rebuild or mark derived state.** For every derived subsystem, follow the
   manifest and restore plan: validate included semantic/vector artifacts,
   schedule rebuilds for `REBUILD_REQUIRED` subsystems, discard cache-only state,
   and keep strict semantic search unavailable until restored or rebuilt indexes
   satisfy normal projection readiness checks.
9. **Run post-restore validation.** Verify Raft/log consistency, graph/domain
   checksums, identity/auth recovery, usage accounting, object-reference
   accessibility, derived-state readiness or rebuild status, and smoke tests for
   auth, graph read/write, and configured search modes.
10. **Resume service.** Mark the restore operation `SUCCEEDED`, release the
    restore lock, start or rejoin target nodes, resume writes/background workers,
    and expose any degraded derived-state rebuild status to operators. If
    validation fails after an irreversible apply phase, fail closed and require
    operator recovery from the pre-restore backup or replacement volumes.

#### CommonFolio infrastructure fit

The current CommonFolio AWS `staging2` infrastructure is a good foundation for
this capture strategy, but it is not yet the complete Mycel-native backup system.
It already has EKS, the AWS EBS CSI driver, a retained gp3 StorageClass for
Mycel PVCs, Mycel PVCs backed by EBS, AWS Backup protection for Mycel EBS
volumes, and an operator script that can create manual EBS snapshot sets before
risky Mycel operations.

Those pieces make local/PVC snapshot backup suitable for AWS `staging2`, but the
following gaps remain before it satisfies this design:

- Kubernetes `VolumeSnapshot` support is not currently declared as part of the
  checked-in AWS add-ons. The EBS CSI driver can support snapshots, but the
  cluster also needs snapshot CRDs, a snapshot controller, and a
  `VolumeSnapshotClass`, or Mycel must use an AWS/EBS snapshot integration
  directly.
- Current AWS Backup/EBS snapshots are provider-native recovery points, not the
  S3-compatible Mycel backup repository described here.
- The scheduled AWS Backup plan is not coordinated with Mycel quiesce/checkpoint
  barriers, so it should be treated as infrastructure-level crash/recovery
  protection rather than a validated Mycel backup set.
- The current Mycel S3 bucket and IRSA role are for blob payload storage, not a
  dedicated backup repository with backup-set manifests, completion markers, and
  backup retention policy.
- The manual EBS snapshot script is useful operational evidence, but it does not
  yet implement the full quiesce, derived-state policy, manifest validation, and
  restore-planning protocol.

Because AWS `staging2` is the intended production shape, it should be the
reference environment for first production qualification. The production path
should be:

```text
EKS + EBS CSI gp3 PVCs + snapshot/clone capture + S3 backup repository + IRSA
```

The missing AWS pieces should be added deliberately rather than treated as
implicit properties of EBS:

- either Kubernetes `VolumeSnapshot` CRDs/controller plus an EBS
  `VolumeSnapshotClass`, or a Mycel/operator-owned direct EBS snapshot provider;
- a dedicated S3-compatible backup repository bucket/prefix, separate from the
  blob payload bucket unless explicitly approved otherwise;
- an IRSA role and policy for backup repository writes/reads/listing and restore
  staging reads;
- an uploader path that can read the immutable snapshot or clone after quiescence
  is released and write deterministic artifacts plus checksums to S3.

#### Non-AWS portability requirement

The backup design must not become AWS-only. AWS `staging2` can be the production
reference implementation, but the Mycel backup contract should remain portable:

- the authoritative backup repository is S3-compatible object storage;
- backup-set validity is defined by Mycel manifests, checksums, policy evidence,
  and the `COMPLETE` marker;
- raw cloud snapshot handles are implementation details, not the only
  restorable backup artifact;
- restore should consume the uploaded backup-set artifacts and should not require
  the original source cluster, source PVCs, or cloud-native snapshot handles to
  still exist.

This suggests separating **capture providers** from the **backup repository**:

```text
CaptureProvider
  CSI_VOLUME_SNAPSHOT       # Kubernetes-native, works on EBS and other capable CSI drivers
  FILESYSTEM_IMMUTABLE_COPY # portable fallback for local-path/dev/Lab/non-CSI installs
  AWS_EBS_SNAPSHOT          # optional AWS-specific provider if VolumeSnapshot is not used

BackupRepository
  S3_COMPATIBLE             # AWS S3, MinIO, Ceph RGW, or another compatible object store
```

For non-AWS deployments:

- if the cluster has a snapshot-capable CSI driver, Mycel should use the same
  `CSI_VOLUME_SNAPSHOT` algorithm used by AWS/EBS;
- if the cluster lacks CSI snapshot support, Mycel should fall back to
  `FILESYSTEM_IMMUTABLE_COPY`, which still uses quiesce/checkpoint barriers and
  async upload but may hold quiescence longer while making the local immutable
  copy;
- MinIO or another S3-compatible service can be the backup repository;
- degraded or provider-specific features should be visible in the backup policy,
  operation status, and manifest.

The existing non-AWS `staging` environment runs on the shared K3s/local-path
cluster. It should not be assumed to support Kubernetes `VolumeSnapshot` or EBS
snapshot semantics. To support this design there, Mycel would need either a
snapshot-capable CSI/storage layer or the fallback immutable local-copy provider
with explicit capacity checks. First production qualification should therefore
prefer AWS `staging2`, while local K3s staging can remain a disposable or
fallback-copy test environment until it has equivalent snapshot support.

## 6. Backup-set manifest and completion contract

The backup-set manifest is the authoritative description of a completed backup
set. It turns a collection of repository objects into a restorable backup by
recording what was captured, where each artifact lives, how to verify it, and
what committed state the backup represents.

The manifest is used to:

- identify the backup set and source cluster;
- prove that every expected pod/PVC contributed its required artifact;
- verify artifact sizes and checksums;
- record the quiesced checkpoint/barrier boundary used for capture;
- guide restore planning, including target cluster compatibility and required
  node/ordinal mapping;
- support audit and troubleshooting for manual backups, scheduled backups, and
  upgrade rollback.

The manifest must not contain raw secrets, access keys, refresh tokens, bearer
tokens, TLS private keys, KMS key material, or customer payload data. It may
contain non-sensitive references, names, or fingerprints needed to verify that
the restore environment has the correct external configuration.

### Generation timing

The coordinator may assemble a draft manifest during the backup operation, but
the restorable backup-set manifest is written only during `FINALIZING`, after:

1. preflight and repository validation have passed;
2. the cluster has been quiesced;
3. checkpoint/barrier state has been established;
4. every expected node has captured and uploaded its artifact;
5. artifact size and checksum validation has passed;
6. expected-node coverage has been verified.

After writing the final manifest, the coordinator writes the `COMPLETE` marker as
the final publication step. Restore must require both a valid manifest and a
valid completion marker. A manifest without a completion marker is not enough to
consider the backup set restorable.

### Schema contract

The first production backup-set manifest is the JSON file
`backup-set.json` at the backup-set repository prefix. Its schema is
`mycel.backup_set` with `manifest_version = 1`. Earlier in-progress operation
records, per-node manifests, and uploader staging records are not restorable
backup-set manifests.

A `backup-set.json` is restorable only when all of the following are true:

- `schema == "mycel.backup_set"`;
- `manifest_version == 1`;
- `state == "SUCCEEDED"` and `complete == true`;
- every required top-level section is present;
- all required node artifacts and required derived artifacts have validated size
  and checksum evidence;
- consistency contains a complete `barrier_set` as described in
  [Checkpoint/barrier fields](#checkpointbarrier-fields);
- a valid `COMPLETE` marker exists and its manifest digest and identity fields
  match `backup-set.json`.

JSON encoding rules:

- The file is UTF-8 JSON with snake_case field names.
- Timestamps are RFC3339 UTC strings with a `Z` suffix.
- Fields that may hold 64-bit values are encoded as base-10 strings to avoid JSON
  number precision loss in clients and tooling. This includes Raft indexes,
  terms, graph revisions, semantic generations, rebuild epochs, byte sizes,
  offsets, logical byte counts, durable counters, and high-water marks. Small
  topology/cardinality fields such as `node_count`, `partition_count`,
  `replica_factor`, and `ordinal` may remain JSON numbers.
- SHA-256 checksum fields contain lowercase hexadecimal without a `sha256:`
  prefix. Hash fields that are not artifact checksums may use an explicit
  prefix, for example `idempotency_key_hash = "sha256:<hex>"`.
- Repository artifact URIs in production must be absolute `s3://` URIs under the
  backup-set prefix. `file://` or absolute filesystem URIs are acceptable only
  for local/Lab manifests.
- Arrays with natural identities must be unique and sorted deterministically:
  nodes by `ordinal`, Raft groups by `group_id`, graph/domain records by
  `space_id` then `domain_id`, and derived artifacts by `subsystem` then
  `artifact_id`.
- Raw secrets, bearer tokens, refresh tokens, access keys, private keys, KMS key
  material, and customer payload bytes are prohibited. The manifest stores
  references and fingerprints only.
- Unknown extension fields are ignored only when `compatibility.required_features`
  is empty or contains features supported by the restore binary. Unsupported
  required features fail restore planning.

Top-level schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `schema` | string constant | yes | Must be `mycel.backup_set`. |
| `manifest_version` | integer constant | yes | Must be `1` for the first production schema. |
| `backup_set_id` | string | yes | Coordinator-generated immutable backup-set identity. |
| `state` | enum | yes | Must be `SUCCEEDED` in a restorable manifest. Operation failure state belongs to operation metadata, not a published restorable backup set. |
| `complete` | boolean | yes | Must be `true` and must match the external `COMPLETE` marker. |
| `created_at` / `completed_at` | timestamp | yes | Backup operation creation and successful finalization times. |
| `operation` | object | yes | Request, schedule, policy, requester, and idempotency evidence. |
| `source` | object | yes | Source cluster identity, topology, daemon version, image, storage versions, and expected nodes. |
| `repository` | object | yes | Backup repository identity and logical prefix without raw credentials. |
| `policy` | object | yes | Immutable effective backup policy snapshot used by this backup. |
| `compatibility` | object | yes | Restore compatibility requirements and required feature flags. The exact version matrix is governed by the version-compatibility design, but the manifest must carry the fields. |
| `consistency` | object | yes | Quiesce evidence and the checkpoint barrier set. |
| `capture` | object | yes | Capture provider, archive/compression format, data mover, provider evidence, and fallback evidence. |
| `artifacts` | object | yes | Node archives, node manifests, inventories, and artifact-level size/checksum evidence. |
| `external_object_stores` | array | yes, may be empty | External S3-compatible payload stores that are dependencies of the backup set but whose payload bytes are not copied by default. |
| `derived_state` | array | yes | Derived subsystem policy decisions, included artifacts, source frontiers, and restore behavior. |
| `validation` | object | yes | Validation result, validator version, expected coverage, warnings, and check summaries. |
| `restore` | object | yes | Supported restore modes, identity/session options, topology requirements, and required secret/configuration references. |
| `audit` | object | yes | Labels, annotations, sanitized operator/request metadata, and redaction summary. |
| `extensions` | object | no | Future non-required extension fields. Required behavior must also be listed in `compatibility.required_features`. |

`operation` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `backup_operation_id` | string | yes | Durable operation ID that produced this backup set. |
| `trigger` | enum | yes | `MANUAL`, `SCHEDULED`, `PRE_RESTORE_SAFETY`, `TEST`, or another future value guarded by `required_features`. |
| `reason` | string | no | Sanitized operator reason. |
| `requested_by_ref` | string | no | Non-secret principal/operator reference. |
| `idempotency_key_hash` | string | no | Hash only; never the raw idempotency key. Required for ad-hoc start requests that supplied an idempotency key. |
| `started_at` / `completed_at` | timestamp | yes | Operation execution interval. |
| `schedule_id` | string | no | Present for scheduled backups. |
| `policy_id` / `policy_revision` | string | yes | Policy identity/revision used for the immutable effective policy snapshot. |
| `policy_hash_sha256` | hex string | yes | Hash of the canonical effective policy snapshot. |

`source` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `cluster_id` / `cluster_name` | string | yes | Source logical cluster identity. |
| `namespace` / `statefulset` | string | yes for Kubernetes deployments | Source workload identity. |
| `data_dir` | string | yes | Mycel data root captured on each node, normally `/data/mycel`. |
| `mycel_version` | string | yes | Source daemon version. |
| `mycel_api_version` | string | yes | API/protobuf release the daemon was built against. |
| `image` / `image_digest` | string | yes for container deployments | Source image and immutable digest. |
| `storage_format_versions` | object | yes | Format identifiers for system metadata/Raft, graph state, semantic records, vector-search indexes, blob metadata, automation state, and other stored subsystems. |
| `topology` | object | yes | `node_count`, `partition_count`, `replica_factor`, and `nodes[]`. |
| `topology.nodes[]` | array | yes | Each node has `node_id`, `node_name`, `pod_name`, `ordinal`, `raft_node_id`, and optional non-secret `pvc_ref`. |
| `membership_hash_sha256` / `placement_hash_sha256` | hex string | yes | Hashes of source membership and placement metadata used by restore planning. |

`repository` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `repository_id` | string | yes | Mycel backup repository ID. |
| `type` | enum | yes | First production value is `S3_COMPATIBLE`. |
| `uri` | string | yes | Logical backup-set prefix URI. |
| `bucket` / `prefix` | string | yes for S3-compatible repositories | Bucket and prefix for operator/debug readability. |
| `endpoint_fingerprint` | string | yes | Non-secret provider/region/endpoint fingerprint, such as `aws:s3:us-east-1` or a MinIO endpoint fingerprint. |
| `credential_ref` | string | yes | Reference to IRSA/Kubernetes Secret/Vault/External Secret, never raw credentials. |
| `encryption` | object | yes | `mode`, optional `kms_key_ref`, key fingerprint, and server-side/client-side encryption policy references. |
| `layout_version` | integer | yes | Repository object-layout version. First production value is `1`. |

`policy` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `policy_id` / `policy_revision` | string | yes | Backup policy identity and revision. |
| `policy_hash_sha256` | hex string | yes | Canonical effective policy hash; must match `operation.policy_hash_sha256`. |
| `retention_class` | string | yes | Retention class label used by retention/deletion policy. Exact retention behavior is defined by the retention design. |
| `consistency` | object | yes | Quiesce requirement, `allow_read_only_during_quiesce`, and `read_allowlist_profile`. |
| `capture` | object | yes | Selected provider or `AUTO`, fallback policy, snapshot class, data mover, archive format, compression, scratch thresholds, and temporary snapshot TTL. |
| `upload` | object | yes | Upload concurrency/part sizing/retry policy summary. |
| `validation` | object | yes | Required validation checks and checksum algorithm. |
| `derived_subsystems[]` | array | yes | One policy row per derived subsystem, including `subsystem`, `mode`, and degraded-restore allowance. |

`compatibility` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `required_features[]` | array | yes, may be empty | Feature flags that the restore binary must support. |
| `storage_format_versions` | object | yes | Copy of source storage/artifact format versions used by compatibility checks. |
| `min_restore_mycel_version` | string | yes | Minimum restore binary version known to understand this manifest/storage format. |
| `max_restore_mycel_version` | string | no | Optional upper bound when a future incompatible format is known. |
| `supported_restore_target_modes[]` | array | yes | First production includes `NEW_CLUSTER_BOOTSTRAP` and `EXISTING_CLUSTER_REPLACE` when topology checks pass. |
| `supported_identity_modes[]` | array | yes | For raw physical backup sets, first production lists only `PRESERVE_BACKUP_ID` as supported. |
| `requires_same_topology` | boolean | yes | First production raw physical backup sets use `true`. |
| `requires_source_fencing_for_preserved_identity` | boolean | yes | `true` for production restore that preserves `cluster_id` outside the original deployment. |

`consistency` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `quiescence` | object | yes | `mode = BACKUP`, lease ID, acquired/released/expiry times, allowed read profile, write-gate time, drain result, and participants. |
| `barrier_set` | object | yes | The `CheckpointBarrierSet` from the checkpoint/barrier section, including Raft, system metadata, graph/domain/schema, identity, usage, object-reference, automation, semantic, lexical, projection, and derived-artifact barriers. |

`capture` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `provider` | enum | yes | `CSI_VOLUME_SNAPSHOT`, `FILESYSTEM_IMMUTABLE_COPY`, `AWS_EBS_SNAPSHOT`, or a future required feature. |
| `provider_version` | string | yes | Driver/controller/provider version or fingerprint. |
| `fallback_used` | boolean | yes | Whether policy `AUTO`/fallback selected a non-primary provider. |
| `snapshot_class_name` | string | provider-dependent | CSI snapshot class when using CSI snapshots. |
| `data_mover` | string | yes | Data mover/uploader implementation. |
| `archive_format` | enum | yes | First production supported values are `TAR_ZSTD`, `TAR_GZIP`, or `TAR`. |
| `compression_level` | integer | no | Compression level when applicable. |
| `temporary_provider_refs_redacted` | boolean | yes | Must be `true` when temporary provider handles are not durable restore inputs. |
| `provider_evidence` | object | yes | Non-secret evidence such as source PVC UID, snapshot generation, clone/staging PVC refs, uploader service account ref, and cleanup status. |

`artifacts` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `archive_format` | enum | yes | Must match `capture.archive_format`. |
| `checksum_algorithm` | enum | yes | First production value is `SHA256`. |
| `nodes[]` | array | yes | Exactly one node artifact per expected source node/ordinal. |
| `nodes[].artifact_id` | string | yes | Stable artifact ID, usually `node:<ordinal>`. |
| `nodes[].pod_name`, `node_id`, `node_name`, `ordinal`, `raft_node_id` | mixed | yes | Source node identity and ordinal mapping. |
| `nodes[].source_pvc_ref` / `nodes[].source_data_dir` | string | yes | Captured PVC/data root reference. |
| `nodes[].archive_name` / `archive_uri` | string | yes | Deterministic archive name and repository URI. |
| `nodes[].manifest_name` / `manifest_uri` | string | yes | Per-node manifest name and URI. |
| `nodes[].inventory_uri` / `inventory_checksum_sha256` | string | yes | Per-node path/artifact inventory generated from the backup path inventory. |
| `nodes[].size_bytes` / `checksum_sha256` | uint64-string / hex string | yes | Archive size and digest. |
| `nodes[].capture_source` | object | yes | Provider-specific non-secret source evidence for this node. |
| `nodes[].applied_indexes` | object map | yes | Raft group ID to applied index at capture time, encoded as decimal strings. |
| `nodes[].raft_freeze` | object | yes | Per-node freeze evidence: lease ID, acquired/released/expiry times, and `groups[]` with barrier/applied/commit/last/snapshot indexes. |
| `additional_manifests[]` | array | yes, may be empty | Optional extra manifests, such as object-reference manifests, with URI, checksum, and purpose. |

`external_object_stores` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `usage` | enum | yes | First production value is `BLOB_PAYLOADS`. |
| `provider` | enum | yes | `S3_COMPATIBLE` for AWS S3, MinIO, or compatible stores. |
| `include_payload_bytes` | boolean | yes | `false` by default for external object stores. |
| `bucket` / `prefix` / `endpoint_fingerprint` | string | yes | Non-secret external payload-store identity. |
| `credential_ref` | string | yes | Restore-time credential reference. |
| `backup_policy_ref` / `recovery_point_ref` | string | yes for production | External object-store backup/recovery evidence. |
| `object_reference_manifest_uri` / `object_reference_manifest_checksum_sha256` | string | yes | Manifest of referenced external payload objects. |
| `object_count` / `logical_bytes` | uint64-string | yes | Count and logical bytes covered by the reference manifest. |
| `validation_state` | enum | yes | `EXTERNAL_DEPENDENCY_VERIFIED` or a terminal failure state; production manifests cannot publish failed required dependencies. |

`derived_state` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `subsystem` | enum | yes | `SEMANTIC_VECTOR_SEARCH`, `LEXICAL_SEARCH`, `GRAPH_CHECKPOINT`, `PERSISTENT_GRAPH_QUERY_INDEX`, or `METADATA_ADJACENCY_MEMORY_INDEX`. |
| `artifact_id` | string | yes | Stable artifact identity within the subsystem. |
| `policy_mode` | enum | yes | `INCLUDE_REQUIRED`, `INCLUDE_OPTIONAL`, `REBUILD_ALLOWED`, or `DISCARD`. |
| `included` | boolean | yes | Whether an artifact is included in the backup set. |
| `restore_policy` | enum | yes | `INCLUDED_AND_VALIDATED`, `REBUILD_REQUIRED`, `DISCARD`, `FAIL_CLOSED`, or explicit degraded mode. |
| `source_frontier_kind` / `source_frontier_value` | enum / string | yes when applicable | Frontier that the derived artifact indexes or checkpoints. |
| `artifact_uri`, `size_bytes`, `checksum_sha256`, `format_version` | mixed | required when `included == true` | Artifact location and validation evidence. |
| `validation_state` | enum | yes | Required artifacts must be `INCLUDED_AND_VALIDATED`; optional invalid artifacts are omitted and marked rebuild-required. |
| `metadata` | object | no | Subsystem-specific non-secret details such as vector dimensions, model/profile IDs, analyzer version, or checkpoint revision. |

`validation` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `result` | enum | yes | Must be `PASSED` for a published restorable manifest. |
| `validated_at` | timestamp | yes | Time the final manifest inputs were validated. |
| `validator_version` | string | yes | Validator implementation version/fingerprint. |
| `expected_node_count` / `node_artifact_count` | integer | yes | Node coverage counts; must match source topology. |
| `required_artifact_count` / `validated_artifact_count` | integer | yes | Required-artifact coverage counts. |
| `total_artifact_bytes` | uint64-string | yes | Total bytes of included Mycel backup artifacts, excluding external object-store payload bytes not copied into the backup set. |
| `checksum_algorithm` | enum | yes | First production value is `SHA256`. |
| `checks[]` | array | yes | Named checks with `name`, `result`, `checked_at`, and sanitized details. |
| `warnings[]` | array | yes, may be empty | Non-fatal warnings, primarily omitted optional derived artifacts. |
| `errors[]` | array | yes, empty for published restorable manifests | Published restorable manifests must not contain validation errors. |

`restore` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `supported_restore_target_modes[]` | array | yes | Modes allowed by the backup set, mirroring compatibility. |
| `supported_identity_modes[]` | array | yes | Raw physical first production manifests support `PRESERVE_BACKUP_ID` only. |
| `default_identity_mode` | enum | yes | First production value is `PRESERVE_BACKUP_ID`. |
| `supported_auth_session_policies[]` | array | yes | `PRESERVE_RESTORED_SESSIONS` and/or `REVOKE_ALL_SESSIONS` according to identity mode. |
| `requires_same_topology` | boolean | yes | First production raw physical backup sets require the same node/partition/replica topology. |
| `required_secret_refs[]` | array | yes, may be empty | Credential/key references required at restore time. |
| `required_external_dependencies[]` | array | yes, may be empty | External object-store recovery points or other deployment dependencies to verify before restore. |
| `operator_notes` | string | no | Sanitized notes for restore planning. |

`audit` schema:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `labels` / `annotations` | object | yes, may be empty | Non-secret operator metadata. |
| `redactions[]` | array | yes, may be empty | Names/categories of values intentionally redacted from the manifest. |
| `lineage` | object | no | Optional previous backup/restoration lineage. |

### COMPLETE marker schema

The repository prefix must contain a final `COMPLETE` marker written after
`backup-set.json`. The marker is a small JSON object; it is the publication fence
that prevents partially uploaded prefixes from being treated as backup sets.

```json
{
  "schema": "mycel.backup_complete",
  "marker_version": 1,
  "backup_set_id": "backup-set-20261120T010203Z-cluster-a",
  "state": "SUCCEEDED",
  "repository_id": "commonfolio-prod-s3",
  "manifest_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/backup-set.json",
  "manifest_size_bytes": "123456",
  "manifest_checksum_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "completed_at": "2026-11-20T01:05:41Z",
  "artifact_count": 8,
  "total_artifact_bytes": "987654321"
}
```

Restore discovery rules:

1. ignore repository prefixes without `COMPLETE`;
2. parse `COMPLETE` and fetch `manifest_uri`;
3. verify manifest size/checksum from the marker;
4. verify `schema`, `manifest_version`, `backup_set_id`, `state`, and `complete`;
5. verify manifest repository prefix and artifact URIs are within the backup-set
   prefix, except explicit external dependencies;
6. run full manifest validation before planning restore.

### Example

A shortened conforming v1 manifest has the following shape. Real manifests include
one node artifact per expected node and full barrier records for every subsystem
covered by the backup policy.

```json
{
  "schema": "mycel.backup_set",
  "manifest_version": 1,
  "backup_set_id": "backup-set-20261120T010203Z-cluster-a",
  "state": "SUCCEEDED",
  "complete": true,
  "created_at": "2026-11-20T01:02:03Z",
  "completed_at": "2026-11-20T01:05:41Z",
  "operation": {
    "backup_operation_id": "backup-op-20261120T010203Z-cluster-a",
    "trigger": "MANUAL",
    "reason": "before upgrade",
    "requested_by_ref": "operator:admin@example.com",
    "idempotency_key_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "started_at": "2026-11-20T01:02:03Z",
    "completed_at": "2026-11-20T01:05:41Z",
    "policy_id": "commonfolio-production-default",
    "policy_revision": "7",
    "policy_hash_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  },
  "source": {
    "cluster_id": "cluster-a",
    "cluster_name": "commonfolio-prod-mycel",
    "namespace": "commonfolio-prod",
    "statefulset": "myceld",
    "data_dir": "/data/mycel",
    "mycel_version": "v0.23.1",
    "mycel_api_version": "v0.23.0",
    "image": "docker.io/myceldb/mycel:v0.23.1",
    "image_digest": "sha256:327b98d979854fc15f17219f3e7727883ebc2d2542e0ef66779aa05f1c55b722",
    "storage_format_versions": {
      "system_raft": "1",
      "graph": "1",
      "semantic_records": "1",
      "semantic_vector_search": "1",
      "blob_metadata": "1"
    },
    "topology": {
      "node_count": 1,
      "partition_count": 64,
      "replica_factor": 1,
      "nodes": [
        {
          "node_id": "node-a",
          "node_name": "myceld-0",
          "pod_name": "myceld-0",
          "ordinal": 0,
          "raft_node_id": "1",
          "pvc_ref": "pvc:commonfolio-prod/myceld-data-myceld-0"
        }
      ]
    },
    "membership_hash_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    "placement_hash_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
  },
  "repository": {
    "repository_id": "commonfolio-prod-s3",
    "type": "S3_COMPATIBLE",
    "uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/",
    "bucket": "commonfolio-mycel-backups",
    "prefix": "clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/",
    "endpoint_fingerprint": "aws:s3:us-east-1",
    "credential_ref": "aws-irsa:mycel-backup-writer",
    "encryption": {
      "mode": "SSE_KMS",
      "kms_key_ref": "alias/commonfolio-mycel-backups",
      "key_fingerprint": "kms:us-east-1:alias/commonfolio-mycel-backups"
    },
    "layout_version": 1
  },
  "policy": {
    "policy_id": "commonfolio-production-default",
    "policy_revision": "7",
    "policy_hash_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "retention_class": "production-standard",
    "consistency": {
      "quiesce_required": true,
      "allow_read_only_during_quiesce": false,
      "read_allowlist_profile": "CONTROL_ONLY"
    },
    "capture": {
      "provider": "CSI_VOLUME_SNAPSHOT",
      "fallback_allowed": false,
      "snapshot_class_name": "commonfolio-mycel-ebs-snapshot",
      "data_mover": "temporary-pvc-uploader",
      "archive_format": "TAR_ZSTD",
      "scratch_space_min_free_bytes": "2147483648",
      "temporary_snapshot_ttl": "PT6H"
    },
    "upload": {
      "max_concurrency": 4,
      "retry_policy": "STANDARD_EXPONENTIAL_BACKOFF"
    },
    "validation": {
      "checksum_algorithm": "SHA256",
      "require_complete_marker": true
    },
    "derived_subsystems": [
      {
        "subsystem": "SEMANTIC_VECTOR_SEARCH",
        "mode": "INCLUDE_REQUIRED",
        "degraded_restore_allowed": false
      },
      {
        "subsystem": "LEXICAL_SEARCH",
        "mode": "INCLUDE_OPTIONAL",
        "degraded_restore_allowed": true
      }
    ]
  },
  "compatibility": {
    "required_features": [],
    "storage_format_versions": {
      "system_raft": "1",
      "graph": "1",
      "semantic_records": "1",
      "semantic_vector_search": "1",
      "blob_metadata": "1"
    },
    "min_restore_mycel_version": "v0.23.1",
    "supported_restore_target_modes": [
      "NEW_CLUSTER_BOOTSTRAP",
      "EXISTING_CLUSTER_REPLACE"
    ],
    "supported_identity_modes": [
      "PRESERVE_BACKUP_ID"
    ],
    "requires_same_topology": true,
    "requires_source_fencing_for_preserved_identity": true
  },
  "consistency": {
    "quiescence": {
      "mode": "BACKUP",
      "lease_id": "quiesce-backup-set-20261120T010203Z-cluster-a",
      "acquired_at": "2026-11-20T01:02:09Z",
      "released_at": "2026-11-20T01:02:19Z",
      "expires_at": "2026-11-20T01:07:09Z",
      "allow_read_only_during_quiesce": false,
      "read_allowlist_profile": "CONTROL_ONLY",
      "write_gate_closed_at": "2026-11-20T01:02:10Z",
      "drain_result": "DRAINED",
      "participants": [
        {
          "node_id": "node-a",
          "pod_name": "myceld-0",
          "status": "QUIESCED"
        }
      ]
    },
    "barrier_set": {
      "schema_version": 1,
      "barrier_set_id": "barrier-backup-set-20261120T010203Z-cluster-a",
      "backup_operation_id": "backup-op-20261120T010203Z-cluster-a",
      "backup_set_id": "backup-set-20261120T010203Z-cluster-a",
      "cluster_id": "cluster-a",
      "quiesce_lease_id": "quiesce-backup-set-20261120T010203Z-cluster-a",
      "established_at": "2026-11-20T01:02:14Z",
      "coordinator_node_id": "node-a",
      "coordinator_raft_node_id": "1",
      "read_allowlist_profile": "CONTROL_ONLY",
      "write_gate_closed_at": "2026-11-20T01:02:10Z",
      "participants": [
        {
          "node_id": "node-a",
          "pod_name": "myceld-0",
          "status": "CAPTURED"
        }
      ],
      "raft_groups": [
        {
          "group_id": "system",
          "group_kind": "SYSTEM",
          "barrier_index": "12345",
          "commit_index": "12345",
          "term": "4",
          "leader_raft_node_id": "1"
        },
        {
          "group_id": "partition:0",
          "group_kind": "PARTITION",
          "partition_id": 0,
          "barrier_index": "67890",
          "commit_index": "67890",
          "term": "9",
          "leader_raft_node_id": "1"
        }
      ],
      "system_metadata": {
        "cluster_id": "cluster-a",
        "cluster_name": "commonfolio-prod-mycel",
        "system_raft_group_id": "system",
        "system_raft_barrier_index": "12345",
        "node_count": 1,
        "partition_count": 64,
        "replica_factor": 1,
        "membership_hash_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "placement_hash_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      },
      "graph_domains": [
        {
          "space_id": "space-a",
          "domain_id": "domain-a",
          "partition_group_id": "partition:0",
          "raft_barrier_index": "67890",
          "graph_revision": "127",
          "domain_manifest_checksum_sha256": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
          "segment_set_checksum_sha256": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        }
      ],
      "identity": {
        "identity_raft_group_id": "system",
        "identity_raft_barrier_index": "12345",
        "authorization_revision": "8842",
        "principal_store_checksum_sha256": "1111111111111111111111111111111111111111111111111111111111111111"
      },
      "usage_accounting": [],
      "object_references": [],
      "graph_change_feeds": [],
      "automation": [],
      "semantic": [],
      "lexical": [],
      "projections": [],
      "derived_artifacts": []
    }
  },
  "capture": {
    "provider": "CSI_VOLUME_SNAPSHOT",
    "provider_version": "ebs.csi.aws.com/v1.x",
    "fallback_used": false,
    "snapshot_class_name": "commonfolio-mycel-ebs-snapshot",
    "data_mover": "temporary-pvc-uploader",
    "archive_format": "TAR_ZSTD",
    "compression_level": 6,
    "temporary_provider_refs_redacted": true,
    "provider_evidence": {
      "uploader_service_account_ref": "serviceaccount:commonfolio-prod/mycel-backup-uploader",
      "cleanup_status": "COMPLETED"
    }
  },
  "artifacts": {
    "archive_format": "TAR_ZSTD",
    "checksum_algorithm": "SHA256",
    "nodes": [
      {
        "artifact_id": "node:0",
        "pod_name": "myceld-0",
        "node_id": "node-a",
        "node_name": "myceld-0",
        "ordinal": 0,
        "raft_node_id": "1",
        "source_pvc_ref": "pvc:commonfolio-prod/myceld-data-myceld-0",
        "source_data_dir": "/data/mycel",
        "archive_name": "mycel-system-20261120T010203Z-myceld-0-backup-set-20261120T010203Z-cluster-a.tar.zst",
        "archive_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/nodes/myceld-0/archive.tar.zst",
        "manifest_name": "mycel-system-20261120T010203Z-myceld-0-backup-set-20261120T010203Z-cluster-a.manifest.json",
        "manifest_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/nodes/myceld-0/manifest.json",
        "inventory_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/nodes/myceld-0/inventory.json",
        "inventory_checksum_sha256": "2222222222222222222222222222222222222222222222222222222222222222",
        "size_bytes": "123456789",
        "checksum_sha256": "3333333333333333333333333333333333333333333333333333333333333333",
        "capture_source": {
          "provider": "CSI_VOLUME_SNAPSHOT",
          "source_pvc": "myceld-data-myceld-0",
          "snapshot_generation": "snapgen-20261120T010214Z"
        },
        "applied_indexes": {
          "system": "12345",
          "partition:0": "67890"
        },
        "raft_freeze": {
          "lease_id": "quiesce-backup-set-20261120T010203Z-cluster-a",
          "acquired_at": "2026-11-20T01:02:09Z",
          "released_at": "2026-11-20T01:02:19Z",
          "expires_at": "2026-11-20T01:07:09Z",
          "groups": [
            {
              "group_id": "system",
              "barrier_index": "12345",
              "applied_index": "12345",
              "commit_index": "12345",
              "term": "4",
              "last_index": "12345",
              "snapshot_index": "12000",
              "leader_raft_node_id": "1"
            }
          ]
        }
      }
    ],
    "additional_manifests": []
  },
  "external_object_stores": [
    {
      "usage": "BLOB_PAYLOADS",
      "provider": "S3_COMPATIBLE",
      "include_payload_bytes": false,
      "bucket": "commonfolio-prod-mycel-blobs",
      "prefix": "mycel/blobs/",
      "endpoint_fingerprint": "aws:s3:us-east-1",
      "credential_ref": "aws-irsa:mycel-blob-reader",
      "backup_policy_ref": "aws-backup:s3:commonfolio-prod-mycel-blobs",
      "recovery_point_ref": "aws-backup-recovery-point:rp-20261120T010203Z",
      "object_reference_manifest_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/external-objects/blob-payloads.json",
      "object_reference_manifest_checksum_sha256": "4444444444444444444444444444444444444444444444444444444444444444",
      "object_count": "413",
      "logical_bytes": "99887766",
      "validation_state": "EXTERNAL_DEPENDENCY_VERIFIED"
    }
  ],
  "derived_state": [
    {
      "subsystem": "LEXICAL_SEARCH",
      "artifact_id": "lexical:space-a:domain-a",
      "policy_mode": "INCLUDE_OPTIONAL",
      "included": false,
      "restore_policy": "REBUILD_REQUIRED",
      "source_frontier_kind": "GRAPH_REVISION",
      "source_frontier_value": "127",
      "validation_state": "REBUILD_REQUIRED"
    },
    {
      "subsystem": "SEMANTIC_VECTOR_SEARCH",
      "artifact_id": "semantic-vector:space-a:domain-a:rule-a:binding-a",
      "policy_mode": "INCLUDE_REQUIRED",
      "included": true,
      "restore_policy": "INCLUDED_AND_VALIDATED",
      "source_frontier_kind": "GRAPH_REVISION",
      "source_frontier_value": "127",
      "artifact_uri": "s3://commonfolio-mycel-backups/clusters/cluster-a/backup-sets/backup-set-20261120T010203Z-cluster-a/derived/semantic-vector/index.tar.zst",
      "size_bytes": "55555555",
      "checksum_sha256": "5555555555555555555555555555555555555555555555555555555555555555",
      "format_version": "1",
      "validation_state": "INCLUDED_AND_VALIDATED",
      "metadata": {
        "embedding_model": "text-embedding-model-id",
        "vector_dimensions": "1536",
        "semantic_generation": "12",
        "rebuild_epoch": "2"
      }
    }
  ],
  "validation": {
    "result": "PASSED",
    "validated_at": "2026-11-20T01:05:39Z",
    "validator_version": "mycel-backup-validator/v1",
    "expected_node_count": 1,
    "node_artifact_count": 1,
    "required_artifact_count": 2,
    "validated_artifact_count": 2,
    "total_artifact_bytes": "179012344",
    "checksum_algorithm": "SHA256",
    "checks": [
      {
        "name": "node_artifact_coverage",
        "result": "PASSED",
        "checked_at": "2026-11-20T01:05:39Z"
      }
    ],
    "warnings": [],
    "errors": []
  },
  "restore": {
    "supported_restore_target_modes": [
      "NEW_CLUSTER_BOOTSTRAP",
      "EXISTING_CLUSTER_REPLACE"
    ],
    "supported_identity_modes": [
      "PRESERVE_BACKUP_ID"
    ],
    "default_identity_mode": "PRESERVE_BACKUP_ID",
    "supported_auth_session_policies": [
      "PRESERVE_RESTORED_SESSIONS",
      "REVOKE_ALL_SESSIONS"
    ],
    "requires_same_topology": true,
    "required_secret_refs": [
      "aws-irsa:mycel-backup-reader",
      "k8s-secret:mycel-user-store-key"
    ],
    "required_external_dependencies": [
      "aws-backup-recovery-point:rp-20261120T010203Z"
    ]
  },
  "audit": {
    "labels": {
      "environment": "production"
    },
    "annotations": {},
    "redactions": [
      "raw_credentials",
      "bearer_tokens",
      "private_keys"
    ]
  }
}
```

### Required manifest validation

Restore must fail closed when the manifest is missing, cannot be parsed, has an
unsupported `schema` or `manifest_version`, is not `SUCCEEDED`, is not matched by
a valid `COMPLETE` marker, references missing artifacts, contains checksum or
size mismatches, has incomplete expected-node coverage, has required derived
artifacts missing or invalid, has authoritative artifacts ahead of their recorded
barriers, has unverifiable external object-store dependencies, contains raw
secret material, or fails target-cluster compatibility checks.

## 7. Operation lifecycle, reliability, and cleanup

Backup and restore operations should be durable asynchronous state machines stored
in system metadata/system Raft. The state describes the cluster-level operation;
per-node, per-artifact, per-subsystem, and per-dependency progress is reported as
structured detail inside the operation status.

Common rules for both operation types:

- State transitions are compare-and-swap updates against the current operation
  state and `phase_generation`, so a restarted coordinator cannot accidentally
  complete stale work.
- Each non-terminal state records `entered_at`, `updated_at`, `coordinator_node_id`,
  `coordinator_lease_id`, attempt number, blockers, and the cleanup actions that
  are safe to run if the coordinator crashes.
- Terminal states are immutable except for appending audit/cleanup observations.
- A retry of the start request with the same idempotency key returns the existing
  operation. It does not create a second operation or rewind state.
- Crash recovery rehydrates the operation from system metadata, validates any
  external artifacts already written, reacquires any required coordinator lease,
  and resumes from the last durable phase boundary. It never trusts local process
  memory.
- Cancellation is a state transition request, not a signal to abruptly kill pods.
  The coordinator must move through `CANCELING`, release any locks/leases it can
  safely release, and then enter a terminal state.
- A published backup set is defined only by a valid repository
  `backup-set.json` plus matching `COMPLETE` marker. Operation state alone is
  never sufficient for restore.

### Backup operation state machine

Backup state enum:

```text
PENDING
PREFLIGHTING
WAITING_FOR_CLUSTER_CONVERGENCE
QUIESCING
CHECKPOINTING
CAPTURING
RELEASING_QUIESCE
UPLOADING
VALIDATING
FINALIZING
SUCCEEDED
CANCELING
CANCELED
FAILED
```

Happy path for the first production quiesced local/PVC snapshot design:

```text
PENDING
  -> PREFLIGHTING
  -> WAITING_FOR_CLUSTER_CONVERGENCE  # optional/repeated until ready
  -> QUIESCING
  -> CHECKPOINTING
  -> CAPTURING
  -> RELEASING_QUIESCE
  -> UPLOADING
  -> VALIDATING
  -> FINALIZING
  -> SUCCEEDED
```

`RELEASING_QUIESCE` is explicit because first production should release the
write outage after immutable capture sources are created and before the longer
upload/validation window. A simpler implementation may move quickly through this
state or keep reads/writes blocked longer, but status should still expose when
ordinary traffic is safe to resume.

Backup states:

| State | Entry action / durable checkpoint | Allowed next states | Cancel/retry/crash behavior |
| --- | --- | --- | --- |
| `PENDING` | Create operation record, generated `backup_operation_id`, generated `backup_set_id`, repository ID, effective request, idempotency hash, and initial `phase_generation`. | `PREFLIGHTING`, `CANCELING`, `FAILED` | Cancel is always safe. Crash resumes by assigning a coordinator and entering `PREFLIGHTING`. |
| `PREFLIGHTING` | Resolve repository, credentials, effective policy, expected nodes/PVCs, capture provider, version/storage compatibility, active-operation conflicts, and basic capacity checks. Persist the immutable effective policy snapshot and policy hash. | `WAITING_FOR_CLUSTER_CONVERGENCE`, `QUIESCING`, `CANCELING`, `FAILED` | Safe to retry from the beginning. Non-retryable configuration errors fail here. Cancel leaves no repository backup set. |
| `WAITING_FOR_CLUSTER_CONVERGENCE` | Persist structured blockers while waiting for pods, Raft quorum, expected nodes, partition leadership, replica catch-up, and required semantic/index readiness before quiesce. | `PREFLIGHTING`, `QUIESCING`, `CANCELING`, `FAILED` | Safe to retry/re-evaluate. Timeout produces `FAILED` with blockers. Cancel is safe. A topology/policy change may return to `PREFLIGHTING`. |
| `QUIESCING` | Acquire the cluster backup quiesce lease, close write gates, drain active writes, pause mutating workers, and persist participant status. | `CHECKPOINTING`, `CANCELING`, `FAILED` | Crash recovery checks whether the lease is still valid. If valid, continue; if expired before capture, release/clean up and fail or reacquire according to policy. Cancel must release gates and workers before terminal state. |
| `CHECKPOINTING` | With quiesce held, establish and persist the `CheckpointBarrierSet`: Raft barriers, graph/domain frontiers, identity/usage/object/semantic/derived frontiers, and per-node observed indexes. | `CAPTURING`, `CANCELING`, `FAILED` | If the barrier set is incomplete and the quiesce lease was lost, the operation fails because the backup cut is no longer valid. If the barrier set is complete and lease remains valid, resume waiting for participants. |
| `CAPTURING` | Create immutable capture sources for every expected node/PVC and required derived artifact. Persist provider evidence, snapshot/copy generation IDs, and per-node capture completion. | `RELEASING_QUIESCE`, `CANCELING`, `FAILED` | Per-node capture is idempotent by artifact ID/generation. If any required immutable source is missing and the quiesce lease is no longer valid, fail rather than capture after writes resumed. Cancel deletes temporary capture sources and releases quiesce. |
| `RELEASING_QUIESCE` | Verify required immutable sources exist, then release quiesce gates and resume ordinary writes/background mutators. Persist `quiesce_released_at`. | `UPLOADING`, `CANCELING`, `FAILED` | Release is idempotent. Crash recovery first proves the cluster is no longer held by this backup lease, then resumes upload. Cancel is still safe because no `COMPLETE` marker exists. |
| `UPLOADING` | Upload or copy per-node archives, per-node manifests, inventories, object-reference manifests, and included derived artifacts to the repository prefix. Persist artifact URIs, sizes, checksums, and upload attempts. | `VALIDATING`, `CANCELING`, `FAILED` | Retry missing or checksum-mismatched uploads from immutable sources. If an immutable source was lost before upload completes, fail. Cancel stops uploads and records the prefix incomplete; no `COMPLETE` marker may be written. |
| `VALIDATING` | Validate artifact coverage, checksums, sizes, manifests, path inventory, barrier consistency, required derived artifacts, external object-store dependencies, and repository prefix safety. | `FINALIZING`, `CANCELING`, `FAILED` | Validation is repeatable. Cancel is safe until final publication begins. Optional artifact problems become warnings/rebuild markers; required problems fail. |
| `FINALIZING` | Write final `backup-set.json`, verify it by reading it back, then write `COMPLETE` as the last publication fence. Persist `manifest_uri`, manifest checksum, and completion marker evidence. | `SUCCEEDED`, `FAILED` | This is the backup publication boundary. If crash recovery finds a valid `COMPLETE` matching the manifest, mark `SUCCEEDED`. If only a manifest exists, verify and write `COMPLETE` or fail the prefix closed. Cancellation is rejected once final publication has begun. |
| `SUCCEEDED` | Backup set is complete, validated, and restorable from the repository. Active backup lock is released. Temporary-provider cleanup may continue as best-effort background cleanup with warnings. | none | Terminal. Idempotent start retries return this operation. |
| `CANCELING` | Stop safe work, release quiesce if held, release coordinator locks, delete temporary snapshots/copies/staging PVCs when safe, and mark repository prefix incomplete. | `CANCELED`, `FAILED` | Re-entrant cleanup state. If cleanup itself fails before publication, terminal state may be `FAILED`, but the prefix remains unrestorable because no valid `COMPLETE` exists. |
| `CANCELED` | Operation ended by operator/scheduler cancellation before publication. | none | Terminal. No restorable backup set was produced. |
| `FAILED` | Operation ended without publishing a valid completed backup set, or final publication could not be proven valid. | none | Terminal. Restore discovery must ignore the prefix unless a later explicit validation operation finds a valid manifest and `COMPLETE`. |

Backup failure and publication rules:

- `FAILED` and `CANCELED` operations must not write a valid `COMPLETE` marker.
- If `COMPLETE` exists and matches a valid `backup-set.json`, crash recovery must
  prefer repository truth and mark the operation `SUCCEEDED`, even if the process
  crashed before updating system metadata.
- If repository objects exist without `COMPLETE`, restore discovery ignores the
  prefix. Cleanup/retention may delete it according to incomplete-prefix rules.
- Quiesce must be released on every non-terminal exit path from `QUIESCING`,
  `CHECKPOINTING`, or `CAPTURING` unless the process crashed. Crash recovery must
  inspect and clear expired/stale quiesce leases before admitting ordinary work.

### Restore operation state machine

Restore operations use a separate state enum because they have an irreversible
apply boundary that backup operations do not have.

Restore state enum:

```text
PENDING
PLANNING
VALIDATING_BACKUP_SET
VALIDATING_TARGET
ACQUIRING_RESTORE_LOCK
STAGING_ARTIFACTS
READY_TO_APPLY
PREPARING_TARGET
APPLYING_ARTIFACTS
NORMALIZING_RESTORED_STATE
POST_RESTORE_VALIDATING
RESUMING_SERVICE
SUCCEEDED
CANCELING
CANCELED
FAILED
FAILED_REQUIRES_OPERATOR
```

Happy path:

```text
PENDING
  -> PLANNING
  -> VALIDATING_BACKUP_SET
  -> VALIDATING_TARGET
  -> ACQUIRING_RESTORE_LOCK
  -> STAGING_ARTIFACTS
  -> READY_TO_APPLY
  -> PREPARING_TARGET
  -> APPLYING_ARTIFACTS
  -> NORMALIZING_RESTORED_STATE
  -> POST_RESTORE_VALIDATING
  -> RESUMING_SERVICE
  -> SUCCEEDED
```

The irreversible restore boundary starts when the operation first mutates target
durable state that cannot be abandoned without an explicit recovery step. For
`EXISTING_CLUSTER_REPLACE`, this is the earlier of wiping/reinitializing target
Mycel data or cutting over replacement PVCs. For `NEW_CLUSTER_BOOTSTRAP`, this
boundary is normally lower risk because the target should be empty and not serving
production traffic, but the operation should still record when target durable
state first changes.

Restore states:

| State | Entry action / durable checkpoint | Allowed next states | Cancel/retry/crash behavior |
| --- | --- | --- | --- |
| `PENDING` | Create restore operation record with request, repository ID, backup set ID, target mode, requested identity mode, auth-session policy, idempotency hash, and destructive-confirmation evidence when applicable. | `PLANNING`, `CANCELING`, `FAILED` | Cancel is safe. Crash resumes by assigning a coordinator and entering `PLANNING`. |
| `PLANNING` | Resolve or revalidate the restore plan: manifest URI, target mode, identity plan, node/ordinal/PVC mapping, source-fencing evidence, CommonFolio attachment ref, selected degraded options, and required secrets/dependencies. | `VALIDATING_BACKUP_SET`, `CANCELING`, `FAILED` | Safe to retry. If `PlanClusterRestore` produced a persisted plan, this state checks that it is still current rather than trusting stale plan data. |
| `VALIDATING_BACKUP_SET` | Read `COMPLETE`, fetch `backup-set.json`, verify marker/manifest checksum, parse schema/version/features, validate artifacts, required derived state, object-store dependencies, and compatibility fields. | `VALIDATING_TARGET`, `CANCELING`, `FAILED` | Safe to retry from repository truth. Missing/corrupt required artifacts fail before target mutation. Cancel is safe. |
| `VALIDATING_TARGET` | Verify target cluster/environment: exclusive-operation constraints, topology, node/PVC/storage class/capacity, daemon version, repository read credentials, restore secret refs, source fencing for preserved identity, and pre-restore safety backup or explicit waiver for existing-cluster replacement. | `ACQUIRING_RESTORE_LOCK`, `CANCELING`, `FAILED` | Safe to retry. Target has not been mutated. First production should require pre-restore safety-backup evidence here rather than silently launching a nested backup. |
| `ACQUIRING_RESTORE_LOCK` | Acquire target-wide restore lock. For `EXISTING_CLUSTER_REPLACE`, block ordinary reads/writes, pause mutating workers, and prevent backup starts. For `NEW_CLUSTER_BOOTSTRAP`, lock the bootstrap target resources. | `STAGING_ARTIFACTS`, `CANCELING`, `FAILED` | Reacquire or release stale locks on crash recovery. Cancel releases any acquired lock. |
| `STAGING_ARTIFACTS` | Download or stream required node archives, node manifests, inventories, included derived artifacts, and object-reference manifests into verified staging. Verify size/checksum before any target durable mutation. | `READY_TO_APPLY`, `CANCELING`, `FAILED` | Resume partial downloads by checksum. Cancel deletes staging and releases the restore lock. Required missing/corrupt inputs fail before mutation. |
| `READY_TO_APPLY` | All required inputs are staged and verified; restore lock is held; target mutation has not begun. Persist an apply plan with node mappings and `irreversible_started = false`. | `PREPARING_TARGET`, `CANCELING`, `FAILED` | Last fully safe cancellation point. Crash recovery can remain in this state or proceed. Cancel cleans staging and releases lock. |
| `PREPARING_TARGET` | Stop/isolate target Mycel processes as needed, create or select target data directories/PVCs, wipe/reinitialize target state or prepare replacement-PVC cutover, and persist `irreversible_started_at` before first destructive action. | `APPLYING_ARTIFACTS`, `CANCELING`, `FAILED`, `FAILED_REQUIRES_OPERATOR` | Before `irreversible_started_at`, cancel/fail can release the lock with target unchanged. After it is set, cancellation is rejected and failures become `FAILED_REQUIRES_OPERATOR` unless the replacement-PVC strategy can prove no production cutover occurred. |
| `APPLYING_ARTIFACTS` | Unpack/apply each node artifact into its mapped target data directory/PVC, restore Raft/system metadata, graph, identity, usage, object references, durable semantic records, and included derived artifacts. Persist per-node `apply_started`/`apply_complete` markers. | `NORMALIZING_RESTORED_STATE`, `FAILED_REQUIRES_OPERATOR` | Idempotent by node mapping and apply marker. Crash recovery verifies completed nodes and reapplies incomplete nodes from staged artifacts. Cancellation is rejected. |
| `NORMALIZING_RESTORED_STATE` | Apply identity mode, auth-session policy, source-fencing audit, restore lineage, runtime-claim normalization, rebuild-required derived-state markers, and readiness/lease reissue requirements. | `POST_RESTORE_VALIDATING`, `FAILED_REQUIRES_OPERATOR` | Idempotent. Running/leased work is normalized rather than resumed as owned work. Cancellation is rejected. |
| `POST_RESTORE_VALIDATING` | Validate restored Raft/log state, system metadata, node identity, graph/domain checksums, identity/auth access, usage values or recomputation, object-reference accessibility, semantic durable records, derived artifact readiness/rebuild status, and smoke tests. | `RESUMING_SERVICE`, `FAILED_REQUIRES_OPERATOR` | Repeatable. Validation failure after apply fails closed and keeps ordinary traffic blocked. |
| `RESUMING_SERVICE` | Start or rejoin target nodes, release restore lock only after readiness gates pass, resume writes/background workers, and publish degraded derived-state rebuild status. | `SUCCEEDED`, `FAILED_REQUIRES_OPERATOR` | Resume is idempotent. If service admission cannot be proven safe, keep lock and fail requiring operator action. |
| `SUCCEEDED` | Restore completed and service admission policy has been satisfied. Restore lineage and audit are durable. | none | Terminal. |
| `CANCELING` | Stop safe work, delete staging, release restore lock if no irreversible mutation occurred, and record cancellation reason. | `CANCELED`, `FAILED` | Re-entrant cleanup state. If cleanup fails before mutation, terminal state may be `FAILED` but target service remains unchanged or locked according to cleanup result. |
| `CANCELED` | Restore ended before irreversible target mutation. | none | Terminal. Target production state was not replaced. |
| `FAILED` | Restore failed before irreversible target mutation, or failed during planning/validation/staging with target unchanged. | none | Terminal. Lock should be released unless cleanup evidence says operator attention is required. |
| `FAILED_REQUIRES_OPERATOR` | Restore failed after irreversible target mutation or with uncertain target state. Ordinary traffic remains blocked by restore lock or readiness gates until an operator performs a follow-up restore, rollback to pre-restore safety backup, or explicit manual recovery. | none | Terminal for this operation, but the cluster remains administratively unsafe. New restore/recovery actions require elevated authorization and explicit acknowledgement of this state. |

Restore cancellation legality:

| State range | Cancellation behavior |
| --- | --- |
| `PENDING` through `READY_TO_APPLY` | Allowed. Operation moves through `CANCELING` to `CANCELED` after staging/locks are cleaned up. |
| `PREPARING_TARGET` before `irreversible_started_at` | Allowed if target durable state is provably unchanged. |
| `PREPARING_TARGET` after `irreversible_started_at`, `APPLYING_ARTIFACTS`, `NORMALIZING_RESTORED_STATE`, `POST_RESTORE_VALIDATING`, `RESUMING_SERVICE` | Rejected. The safe outcome is to finish or fail closed as `FAILED_REQUIRES_OPERATOR`. |
| Any terminal state | Rejected/no-op; terminal operations are immutable except for audit/cleanup observations. |

Restore crash-resume rules:

- Before `READY_TO_APPLY`, resume may discard and restage local temporary files as
  long as repository artifacts validate.
- At `READY_TO_APPLY`, all mutation inputs must be verified and durable enough to
  survive coordinator restart, or the operation must restage before proceeding.
- After `irreversible_started_at`, recovery must continue the restore or fail as
  `FAILED_REQUIRES_OPERATOR`; it must not silently roll forward to serving
  traffic and must not silently roll back target state.
- Per-node apply markers must be written outside the archive being unpacked, or
  in a restore-control area that is not deleted by the next node apply attempt.
- Restore lock/readiness gates remain authoritative after coordinator crash. A
  restarted coordinator must prove post-restore validation before releasing them.

### Backup and restore concurrency and idempotency

Cluster data-protection operations are mutually exclusive when they affect the
same cluster durable state. At most one cluster backup operation may be
non-terminal at a time, and no backup may start while a restore operation is
non-terminal or while a previous restore is in `FAILED_REQUIRES_OPERATOR` with
ordinary traffic blocked. A restore operation may not start while any backup is
non-terminal; the active backup must first be canceled or allowed to finish, and
restore may consume only a backup set that is already published with a valid
`COMPLETE` marker.

Backup non-terminal states are:

```text
PENDING
PREFLIGHTING
WAITING_FOR_CLUSTER_CONVERGENCE
QUIESCING
CHECKPOINTING
CAPTURING
RELEASING_QUIESCE
UPLOADING
VALIDATING
FINALIZING
CANCELING
```

Backup terminal states are:

```text
SUCCEEDED
CANCELED
FAILED
```

Restore non-terminal states are:

```text
PENDING
PLANNING
VALIDATING_BACKUP_SET
VALIDATING_TARGET
ACQUIRING_RESTORE_LOCK
STAGING_ARTIFACTS
READY_TO_APPLY
PREPARING_TARGET
APPLYING_ARTIFACTS
NORMALIZING_RESTORED_STATE
POST_RESTORE_VALIDATING
RESUMING_SERVICE
CANCELING
```

Restore terminal states are:

```text
SUCCEEDED
CANCELED
FAILED
FAILED_REQUIRES_OPERATOR
```

If a scheduled backup is already running and an operator attempts to start a
manual backup, Mycel should not start a second backup and should not implicitly
queue the manual request. The start request should report a conflict with the
active operation and include enough information for the caller to inspect it,
such as active backup set ID, state, source, and retry hint.

The operator can then choose to:

- wait for the active backup to finish and use that backup set;
- cancel the active backup if cancellation is still safe;
- start a new manual backup after the active operation reaches a terminal state.

If the scheduler fires while a manual backup or restore is running, it should
apply the same single-active-operation rule. The scheduler should skip or record
the scheduled run as blocked by `backup_or_restore_already_active`, compute the
next scheduled time according to policy, and avoid canceling or preempting the
operator-initiated operation.

Idempotent retries are different from conflicting starts. If a caller retries the
same start request with the same idempotency key, the API should return the same
operation it already created. A new request with a different idempotency key while
another data-protection operation is active should be treated as an
active-operation conflict, not as an idempotent retry.

### Failure, retry, and cleanup rules

Failures should be classified by where they occur relative to the backup
publication boundary and restore irreversible-apply boundary. The default
production behavior is fail closed: do not publish an unvalidated backup set, do
not admit ordinary traffic after an uncertain restore, and do not silently switch
to degraded semantics without an explicit policy/authorization decision.

#### General retry rules

- Automatic retries are allowed only for operations that are idempotent at the
  current phase boundary: repository reads, `HEAD`/checksum verification,
  multipart upload parts, provider-status polling, staging downloads, cleanup of
  tagged temporary resources, and repeated validation reads.
- Automatic retries must be bounded by the effective policy: maximum attempts,
  maximum elapsed time, and exponential backoff with jitter. Operation status
  should expose `retry_count`, `next_retry_at`, and sanitized `last_error`.
- A backup operation must not create a new backup cut after quiescence has been
  released. If the barrier/capture source is invalid or lost after release, the
  current operation fails and a new backup operation is required.
- A restore operation must not roll back target durable state silently after
  `irreversible_started_at`. After that point it either continues safely or fails
  as `FAILED_REQUIRES_OPERATOR`.
- Terminal operation records are not reopened. A later operator retry creates a
  new operation, except that a start request with the original idempotency key
  returns the original terminal operation.
- Non-retryable failures include unsupported manifest features, incompatible
  storage formats, unsupported identity mode for the backup type, topology
  mismatch for first-production raw physical restores, missing source-fencing
  evidence when preserving a production identity, raw secret material detected in
  a manifest, invalid `COMPLETE` marker, required artifact corruption, and any
  required external dependency that cannot be verified unless degraded restore is
  explicitly selected.

#### Cleanup task model

Cleanup must be durable and idempotent. Whenever an operation creates a temporary
or partially published resource, it should append a cleanup task to the operation
record before or at the same durable transition that creates the resource.

Suggested cleanup task fields:

```text
CleanupTask
  cleanup_task_id
  operation_type = BACKUP | RESTORE
  operation_id
  resource_type = QUIESCE_LEASE | RESTORE_LOCK | PROVIDER_SNAPSHOT | CLONE_PVC |
                  UPLOADER_POD | LOCAL_STAGING_DIR | REPOSITORY_PREFIX |
                  REPOSITORY_OBJECT | RESTORE_STAGING_DIR | RESTORE_APPLY_MARKER
  resource_ref            # non-secret reference
  created_in_state
  safe_after_state        # state after which cleanup may run
  must_run_before_terminal
  retry_policy
  deadline
  status = PENDING | RUNNING | SUCCEEDED | FAILED | WAIVED
  attempts
  last_error              # sanitized
```

Cleanup priorities:

1. release quiesce gates and restore locks that block client traffic;
2. stop uploader/staging workers that may continue writing partial artifacts;
3. clean provider snapshots, clone PVCs, staging PVCs, local immutable copies,
   and local restore staging directories;
4. mark incomplete repository prefixes as incomplete or delete them according to
   the incomplete-prefix policy;
5. retain sanitized failure evidence needed for audit and debugging.

Cleanup must never delete a completed backup set unless the retention/deletion
policy explicitly selects it and validation confirms the set has a matching
`backup-set.json` and `COMPLETE` marker. Cleanup of incomplete prefixes
must first prove that no active operation owns the prefix and that no valid
`COMPLETE` marker is present.

#### Backup failure matrix

| Failure point | Retry behavior | Cleanup behavior | Terminal state / result |
| --- | --- | --- | --- |
| Start request duplicates an existing idempotency key | No new work; return existing operation. | None. | Existing operation state. |
| Different start request while backup/restore is active | Do not retry automatically; caller may retry after active operation is terminal. | None. | Request rejected with conflict; no new operation, unless an operation record had already been created, in which case it fails in `PREFLIGHTING`. |
| Repository, credential, policy, version, or capture-provider configuration invalid in `PREFLIGHTING` | Non-retryable until configuration changes. | Release coordinator lease; no repository backup set should exist. | `FAILED`. |
| Transient repository/provider/API error in `PREFLIGHTING` | Retry within policy budget. | None beyond coordinator lease renewal. | Continue or `FAILED` after retry budget. |
| Cluster convergence timeout before quiesce | Re-evaluate blockers until timeout. Operator may start a new backup later. | Release coordinator lease; no repository backup set should exist. | `FAILED` with structured blockers. |
| Quiesce acquisition/drain timeout | Retry acquisition only while no writes have been captured and policy timeout remains. | Release partial quiesce leases, reopen gates, resume paused workers. | `FAILED` or `CANCELED`. |
| Coordinator crash before quiesce | New coordinator resumes from durable state. | Clean abandoned coordinator lease if expired. | Operation continues or fails according to resumed phase. |
| Coordinator crash while quiesce is held | New coordinator checks quiesce lease and participant status. Continue only if lease and barrier/capture invariants remain valid. | If invalid/expired, release stale quiesce and fail closed. | Continue, or `FAILED` if the backup cut cannot be proven. |
| Participant/pod crash before immutable capture is complete | Wait for recovery while quiesce lease and timeouts remain valid. | If timed out, release quiesce and delete partial capture resources. | Continue or `FAILED`. |
| Checkpoint/barrier mismatch or missing required subsystem barrier | Retry barrier collection only while quiesce is still held and no immutable capture has been accepted. | Release quiesce; delete partial barrier/capture evidence. | `FAILED`. |
| Provider snapshot/copy fails for one node before quiesce release | Retry that node if provider guarantees no partial source is reused incorrectly. | Delete partial provider snapshot/copy for the node. | Continue or `FAILED` after retry budget. |
| Required immutable capture source is lost after quiesce release | Do not recapture under the same operation because the backup cut has passed. | Delete remaining temporary sources and mark repository prefix incomplete. | `FAILED`; operator may start a new backup operation. |
| Upload transient failure | Retry missing object/part using deterministic keys and checksum verification. | Keep immutable source until upload succeeds or operation fails/cancels. | Continue or `FAILED` after retry budget. |
| Upload checksum/size mismatch | Re-upload from immutable source if available; otherwise fail. | Delete or overwrite the mismatched staging object; never publish `COMPLETE`. | Continue or `FAILED`. |
| Optional derived artifact missing/corrupt/incompatible | Do not fail production backup unless policy requires it. | Omit invalid optional artifact, record warning, mark rebuild-required. | Continue. |
| Required node artifact, path inventory, derived artifact, object-reference manifest, or external dependency validation fails | Retry validation reads or upload from immutable source if the source is still valid. | Mark prefix incomplete; clean temporary provider resources. | `FAILED` if required evidence cannot be made valid. |
| Final manifest write fails before `COMPLETE` | Retry writing/reading `backup-set.json` within policy budget. | Prefix remains incomplete until `COMPLETE` is written; cleanup may later delete it. | Continue or `FAILED`. |
| Coordinator crashes after manifest write but before `COMPLETE` | Resume `FINALIZING`: verify manifest checksum and either write `COMPLETE` or fail the prefix closed. | Do not delete manifest until recovery has decided. | `SUCCEEDED` if `COMPLETE` is written; otherwise `FAILED`. |
| Coordinator crashes after `COMPLETE` write but before system metadata says `SUCCEEDED` | Recovery validates repository truth and updates operation state. | Temporary provider cleanup runs as background cleanup. | `SUCCEEDED`. |
| Cleanup of provider snapshots/staging resources fails after successful publication | Do not invalidate the completed backup set. | Record cleanup warning/task and retry asynchronously. | `SUCCEEDED` with cleanup warnings. |
| Cancellation before `FINALIZING` | Move through `CANCELING`. | Stop uploads, release quiesce if held, delete temporary sources where safe, mark prefix incomplete. | `CANCELED` or `FAILED` if cleanup cannot reach a safe terminal state. |
| Cancellation during/after final publication begins | Reject cancellation. | Finish publication or fail closed according to finalization evidence. | `SUCCEEDED` or `FAILED`. |

#### Restore failure matrix

| Failure point | Retry behavior | Cleanup behavior | Terminal state / result |
| --- | --- | --- | --- |
| Duplicate restore start with same idempotency key | No new work; return existing operation. | None. | Existing operation state. |
| Backup set lacks valid `COMPLETE`, manifest digest mismatch, unsupported schema/features, or missing required artifact during `VALIDATING_BACKUP_SET` | Retry repository reads only if the error is transient. Do not mutate target. | Delete local validation cache only. | `FAILED`. |
| Required external object-store dependency cannot be verified | Retry dependency validation while within policy budget. Degraded restore requires explicit selected degraded option and authorization. | No target mutation. | `FAILED` unless degraded restore was explicitly accepted. |
| Target topology/storage/version/secret/source-fencing/destructive-confirmation check fails | Non-retryable until target configuration or evidence changes. | Release coordinator lease; no target mutation. | `FAILED`. |
| Restore lock cannot be acquired | Retry while timeout remains. | Release partial lock if acquired by this attempt. | Continue or `FAILED`. |
| Coordinator crash before `READY_TO_APPLY` | Rehydrate from operation state, reacquire restore lock if needed, revalidate manifest and staged inputs. | Stale staging can be reused only after checksum validation; otherwise delete/restage. | Continue or `FAILED` before mutation. |
| Staging download or checksum failure | Retry download from repository. | Delete corrupt staging object; keep target unchanged. | Continue or `FAILED`. |
| Cancellation through `READY_TO_APPLY` | Allowed. | Delete staging, release restore lock, leave target durable state unchanged. | `CANCELED` or `FAILED` if cleanup cannot prove safety. |
| Failure in `PREPARING_TARGET` before `irreversible_started_at` | Retry safe preparation or cancel. | Release lock and delete staging if abandoning. | `FAILED` or `CANCELED`; target unchanged. |
| Failure in `PREPARING_TARGET` after wipe/reinitialize/cutover begins | Do not cancel or silently roll back. | Keep restore lock/readiness gates; preserve evidence and staged artifacts needed for recovery. | `FAILED_REQUIRES_OPERATOR`. |
| Coordinator/pod crash during `APPLYING_ARTIFACTS` | Resume from durable per-node apply markers and verified staged artifacts. | Do not delete staging or unlock service until post-restore validation passes. | Continue or `FAILED_REQUIRES_OPERATOR`. |
| Artifact corruption discovered after target mutation began | Non-retryable unless another verified copy of the same artifact exists. | Keep target locked; preserve evidence. | `FAILED_REQUIRES_OPERATOR`. |
| Identity/session normalization or derived-state marking fails after apply | Retry idempotent normalization. | Keep target locked. | Continue or `FAILED_REQUIRES_OPERATOR`. |
| Post-restore validation fails | Retry validation only for transient checks. Do not admit ordinary traffic. | Keep target locked/readiness-failed; preserve restored state and diagnostics. | `FAILED_REQUIRES_OPERATOR`. |
| Service resume/readiness fails | Retry start/readiness checks while safe. | Keep restore lock or readiness gate in blocking mode. | Continue or `FAILED_REQUIRES_OPERATOR`. |
| Cleanup of staging fails after successful restore | Do not invalidate the restored cluster. | Record cleanup warning/task and retry asynchronously. | `SUCCEEDED` with cleanup warnings. |

#### Stale-resource janitor

A background janitor should periodically reconcile durable operation records,
repository prefixes, provider snapshots, staging PVCs, local scratch directories,
and restore locks. It should be conservative:

- adopt or resume a non-terminal operation only after the previous coordinator
  lease has expired or been explicitly transferred;
- release expired backup quiesce leases only after checking the operation state
  and participant evidence;
- never release a restore lock for an operation at or after
  `irreversible_started_at` unless a successful restore or explicit elevated
  operator recovery action proves service admission is safe;
- delete provider snapshots, clone PVCs, uploader pods, local immutable copies,
  and restore staging directories only when they carry this operation's resource
  tags and no active operation references them;
- ignore completed backup prefixes during janitor cleanup; completed-set deletion
  belongs to retention/deletion policy;
- treat repository prefixes without `COMPLETE` as incomplete and eligible for
  incomplete-prefix cleanup only after no active operation owns them and after the
  retention policy's incomplete-prefix grace period;
- record janitor actions as audit/cleanup observations on the relevant operation
  when the operation record still exists.

Cleanup failures are operational alerts, not reasons to publish invalid backups
or unlock unsafe restores. Required cleanup that blocks traffic, such as quiesce
lease or restore-lock release, should page operators if automatic retries cannot
complete within policy.

### Retention and deletion rules

Retention applies to completed backup sets, incomplete repository prefixes,
temporary provider resources, restore staging resources, operation history, and
sanitized deletion/audit tombstones. These categories intentionally have
different rules: deleting a temporary snapshot is not the same as deleting a
completed backup set, and deleting a Mycel backup set is not the same as deleting
external blob payload recovery points.

#### Retention classes

Each completed backup set should have exactly one retention class recorded in the
operation record and `backup-set.json`:

| Retention class | Meaning | Automatic deletion |
| --- | --- | --- |
| `DEFAULT` | Normal scheduled or ad-hoc backup governed by the cluster backup policy. | Eligible only when outside both count-based and age-based retention windows. |
| `PINNED` | Operator-protected backup set. Manual backups default here when `protect_manual_backups_default = true`. | Not eligible for automatic deletion. Must be unpinned or explicitly force-deleted by an elevated operator. |
| `EXPIRE_AFTER` | Backup set with explicit `expires_at`. Useful for drills or temporary pre-upgrade backups. | Eligible after `expires_at`, unless it is still protected by active restore usage, legal/operator hold, or minimum-successful-backup rules. |
| `PRE_RESTORE_SAFETY` | Safety backup taken or referenced before destructive `EXISTING_CLUSTER_REPLACE`. | Protected while the restore is non-terminal, failed/unsafe, or still inside the configured safety window. First production should also require explicit operator release before automatic deletion. |

First CommonFolio production defaults should be conservative:

```text
keep_last_successful = 7
keep_for = 14d
protect_manual_backups_default = true
manual_backup_default_class = PINNED
pre_restore_safety_keep_for = 14d
protect_pre_restore_safety_until_released = true
delete_incomplete_after = 24h
deletion_tombstone_keep_for = 1y
external_dependency_min_retention_check = true
```

Operators may configure longer retention. Shorter retention should require an
explicit production policy review because it can invalidate disaster-recovery
expectations and object-store dependency windows.

#### Completed backup-set retention

Automatic retention should run after successful backup finalization and on a
periodic janitor schedule. It may compute candidates only from completed backup
sets with a valid `backup-set.json` and matching `COMPLETE` marker. Incomplete or
unknown prefixes are handled by incomplete-prefix cleanup, not completed-set
retention.

A completed backup set is eligible for automatic deletion only when all of the
following are true:

1. no backup or restore operation is non-terminal for the cluster;
2. no restore operation, restore plan, failed unsafe restore, or CommonFolio
   attachment/audit record references the backup set as an active input;
3. the backup set is not `PINNED` and has no legal/operator hold;
4. the backup set is not `PRE_RESTORE_SAFETY` awaiting explicit release and is
   not inside `pre_restore_safety_keep_for`;
5. the backup set is not among the newest `keep_last_successful` restorable
   backup sets for the repository/cluster;
6. the backup set is older than `keep_for`, or its `EXPIRE_AFTER.expires_at` has
   passed;
7. at least one newer completed backup set has passed validation since this
   backup set was created;
8. external object-store recovery points referenced by newer retained backups are
   still valid, so deleting this Mycel backup does not mask a dependency gap.

The retention predicate is intentionally an AND of count, age/expiry, protection,
and dependency-safety checks. In particular, age alone must not delete the last
known restorable backup, and count alone must not delete a recently created
backup still inside the configured recovery window.

#### Incomplete and failed backup output

Repository prefixes without a valid `COMPLETE` marker are not backup sets. They
must be ignored by restore discovery. They may be cleaned only by incomplete-prefix
cleanup, and only after all of the following are true:

- no active operation owns the prefix;
- any coordinator lease associated with the operation is expired or released;
- any crash recovery window for `FINALIZING` has passed;
- the prefix has no valid `COMPLETE` marker;
- the operation is terminal or the operation record is old enough to be treated
  as orphaned under the janitor policy;
- the prefix is older than `delete_incomplete_after`.

If a prefix contains `backup-set.json` but lacks `COMPLETE`, the janitor must not
delete it immediately. It should first check whether a `FINALIZING` operation can
be resumed safely. Only terminal failed/canceled or orphaned prefixes are eligible
for incomplete cleanup.

Unknown prefixes that do not match Mycel-generated backup-set naming and do not
have a corresponding operation/catalog record should not be deleted automatically.
They may be reported as unmanaged repository content. Deleting unknown prefixes
requires explicit operator intent outside normal `DeleteClusterBackupSet`.

#### Pre-restore safety backups

For destructive `EXISTING_CLUSTER_REPLACE`, a pre-restore safety backup is a
recovery input, not ordinary retention inventory. If Mycel creates the safety
backup, it should assign `retention_class = PRE_RESTORE_SAFETY`. If an operator
supplies `pre_restore_backup_set_id`, the restore operation should treat that set
as protected for the restore lifecycle.

Rules:

- a pre-restore safety backup cannot be automatically deleted while the restore
  operation is non-terminal;
- if the restore reaches `FAILED_REQUIRES_OPERATOR`, the safety backup remains
  protected until an elevated operator records recovery completion or explicitly
  waives the protection;
- after successful restore, first production should keep the safety backup for at
  least `pre_restore_safety_keep_for` and require explicit operator release before
  automatic retention may delete it;
- manual deletion before the safety window closes requires elevated
  authorization, destructive confirmation, and an audit reason.

#### External object-store dependencies

Deleting a Mycel backup set deletes Mycel backup repository objects only. It does
not delete external S3/MinIO blob payload objects and does not delete external
object-store backup/recovery points by default. Mycel may record that a deleted
backup no longer needs a particular external recovery point, but the external
store's retention policy remains owned by deployment/IaC/operator tooling.

Production retention should check for dependency alignment: an external object
store recovery point referenced by a retained Mycel backup should not expire
before that Mycel backup's retention window. If Mycel can observe that an
external dependency expires too early or is already missing, it should alert and
block deletion of older fallback backups when those older backups are still needed
for a valid recovery chain.

#### Manual deletion lifecycle

`DeleteClusterBackupSet(repository_id, backup_set_id)` should be treated as a
safe deletion operation, not a raw prefix delete. The normal deletion lifecycle is:

```text
ACTIVE
  -> DELETE_REQUESTED
  -> DELETING_PUBLICATION_MARKER
  -> DELETING_ARTIFACTS
  -> DELETE_VALIDATING
  -> DELETED
```

Failure leaves the catalog entry in `DELETE_FAILED` with retryable cleanup tasks.
A deletion retry resumes from durable deletion state; it must not create a new
backup-set identity or delete outside the original generated backup-set prefix.

Deletion steps:

1. authorize the caller with elevated backup-admin permission;
2. reject deletion if the backup set is referenced by any non-terminal restore,
   unsafe failed restore, active restore plan, or active backup/delete operation;
3. reject retention-driven deletion for `PINNED`, unreleased
   `PRE_RESTORE_SAFETY`, legal hold, or policy-protected backups; manual deletion
   of those sets requires an explicit force/unpin/release override permitted by
   policy, elevated authorization, destructive confirmation, and an audit reason;
4. load and validate `COMPLETE` and `backup-set.json` to obtain the exact
   generated repository prefix and expected artifact list;
5. write durable delete intent/tombstone metadata in Mycel system metadata before
   deleting objects;
6. delete or invalidate the `COMPLETE` marker first so restore discovery stops
   treating the prefix as restorable;
7. delete artifacts enumerated by the manifest and any additional objects under
   the generated backup-set prefix that carry this backup-set ID;
8. validate that required repository objects are gone or carry object-store delete
   markers, depending on S3/MinIO versioning configuration;
9. keep a deletion tombstone/audit record for at least `deletion_tombstone_keep_for`.

If deletion fails after the `COMPLETE` marker has been deleted, the backup set is
no longer restorable and should remain in `DELETE_FAILED` until cleanup
completes. The tombstone must preserve enough information for operators to
understand what was deleted and what residual objects remain.

#### S3/MinIO deletion safety

The deleter must use generated backup-set identity, not user-supplied arbitrary
paths. It may delete only objects under the manifest's backup-set prefix, except
for explicitly recorded repository-level tombstones. It must reject empty bucket
prefixes, `..`-like path traversal, mismatched cluster IDs, mismatched repository
IDs, artifacts outside the prefix, and manifests whose artifact URIs do not match
the expected generated layout.

For versioned S3 repositories, deletion may create delete markers rather than
permanently erasing bytes. The deletion record should capture available object
version/delete-marker IDs. For MinIO or non-versioned S3-compatible stores,
deletion may be permanent, so operator confirmation and tombstone audit are more
important.

Bulk object deletion should be chunked and retryable. Each chunk should verify
that the listed objects still belong to the intended backup set immediately
before deletion. A partially successful chunk leaves a retryable cleanup task and
must never broaden the prefix to compensate.

#### Repository deletion

`DeleteBackupRepository(repository_id)` deletes the Mycel repository
configuration, not the object-store bucket and not completed backup-set artifacts.
It should fail while any backup/restore/delete operation references the
repository. It should also fail while the repository has known completed backup
sets unless the operator explicitly requests a catalog-only detach/forget mode and
accepts that future restore requires external bootstrap configuration.

Repository deletion must not delete external blob payload buckets, external
object-store recovery points, KMS keys, IAM roles, IRSA bindings, Kubernetes
Secrets, or Terraform-managed infrastructure.

#### Retained audit trail

Mycel should retain an audit/tombstone record after deletion even when the backup
objects are gone. The record should include:

- `backup_set_id`, source cluster ID/name, repository ID, and generated prefix;
- creation/completion time, deletion request time, deletion completion time;
- requester/deleter references and sanitized reasons;
- retention class, pin/hold/release history, and policy hash;
- manifest checksum, artifact count, total bytes, and external dependency refs;
- deletion result, residual object count if any, and cleanup warnings.

Audit/tombstone records must not contain raw secrets or customer payload bytes.
They are retained according to `deletion_tombstone_keep_for` and may also be sent
to the deployment's audit/logging system.

## 8. System state, repository state, and bootstrap

Backup configuration and backup artifacts have different authority and storage
requirements. The design should keep them separate.

### Mycel system metadata

The following backup/restore records should be stored as system-scoped
administrative metadata replicated through the system Raft group:

- backup repository definitions, excluding raw secrets;
- backup policies and schedules;
- retention policy;
- active backup operation records;
- active restore operation records;
- recent backup/restore operation history;
- repository validation status and most recent validation evidence;
- pointers to known backup sets in the external repository.

This makes backup configuration cluster-authoritative, visible through admin
APIs, and resilient to coordinator pod restart or leadership movement.

### External backup repository

The durable backup output lives in the S3-compatible backup repository, not in
system Raft and not on pod-local storage. This includes:

- backup-set manifest;
- completion marker;
- per-node archives;
- per-node manifests;
- checksums;
- persisted validation reports, if retained.

The backup-set manifest in the repository is the authoritative restore artifact.
Mycel's internal catalog may cache or reference known backup sets, but restore
must validate repository artifacts directly before accepting a backup set.

### External bootstrap configuration

A clean restore environment must be able to find backups even if the original
Mycel cluster and its system metadata are gone. Therefore, deployment tooling must
also preserve bootstrap information outside Mycel, such as:

- repository type;
- bucket and prefix;
- endpoint and region;
- credential reference or identity binding;
- required TLS/CA references;
- encryption policy reference.

For AWS, this information may live in Terraform, Helm values, Kubernetes
ConfigMaps/Secrets, IAM role bindings, or operator runbooks. For MinIO, it may
live in deployment manifests and the secret manager used by that environment.

### Storage summary

| Information | Authoritative storage |
| --- | --- |
| Repository config, excluding raw secrets | Mycel system metadata / system Raft |
| Backup schedule and policy | Mycel system metadata / system Raft |
| Backup operation state | Mycel system metadata / system Raft |
| Restore operation state | Mycel system metadata / system Raft |
| Credential references | Mycel system metadata / system Raft |
| Raw credentials | External secret or identity system |
| Backup archives and manifests | S3-compatible backup repository |
| Disaster-recovery bootstrap repository info | External deployment/IaC/operator config |

## 9. Security, credentials, and encryption

Mycel should store **credential references**, not credential material. Raw S3 or
MinIO access keys, session tokens, bearer tokens, refresh tokens, TLS private
keys, client-side encryption keys, KMS plaintext data keys, and password material
must not appear in system metadata, backup policies, operation records,
`backup-set.json`, `COMPLETE`, logs, errors, diagnostics, or Mycel Lab artifacts.

Credential references are deployment bindings that tell the daemon/operator how to
obtain authority at runtime. They are not portable secrets and they are not, by
themselves, proof that restore will work in a new environment. Restore planning
must map the backup set's required secret capabilities to target-environment
references and validate those references before staging or applying artifacts.

### Supported reference schemes

The first production design should support these conceptual reference schemes:

| Reference scheme | Production use | Notes |
| --- | --- | --- |
| `aws-irsa:<namespace>/<service_account>` or an equivalent role binding reference | Preferred for AWS/CommonFolio production repository, uploader, and object-store access. | Mycel records the service account/role reference and role fingerprint/ARN where available, never temporary AWS credentials. |
| `k8s-secret:<namespace>/<name>#<key>` | Acceptable for MinIO/self-hosted/non-AWS when identity binding is unavailable. | Mycel may validate that the Secret/key exists and that mounted/resolved credentials work, but must not persist or log the value. Production should constrain namespaces and RBAC. |
| `external-secret:<namespace>/<name>` | Preferred indirection for Kubernetes External Secrets deployments. | Mycel records the ExternalSecret or projected Secret reference, not the upstream provider secret path unless policy allows it. |
| `vault:<mount>/<path>#<field>` or another external secret-manager URI | Optional deployment-specific resolver. | The resolver must return credentials only in process memory and must support redaction/audit. |
| `kms-key:<provider>/<key-ref>` | Backup repository encryption/decryption and key-rotation evidence. | Records key identity/fingerprint/version, never key material. |
| `tls-ca:<namespace>/<config_or_secret>#<key>` | MinIO/private S3 endpoint TLS validation. | Records CA/certificate bundle reference and fingerprint, never private keys. |
| `env:<name>` or `file-ref:<path>` | Local development and Mycel Lab only. | Not production-qualified unless explicitly accepted by policy. Values are redacted from artifacts and logs. |

A normalized secret reference used in policy, operations, and manifests should
carry purpose and validation metadata:

```text
SecretReference
  ref                         # one of the schemes above; no raw value
  purpose = BACKUP_REPOSITORY_WRITE | BACKUP_REPOSITORY_READ |
            BACKUP_REPOSITORY_DELETE | CAPTURE_PROVIDER |
            EXTERNAL_OBJECT_STORE_READ | REPOSITORY_ENCRYPTION |
            INTERNAL_STATE_KEY | TLS_TRUST
  required_capabilities[]     # e.g. s3:PutObject, s3:GetObject, kms:Decrypt
  scope                       # repository ID, bucket/prefix, namespace, or provider
  fingerprint                 # non-secret identity/hash when available
  rotation_generation         # optional deployment-controlled generation
  not_after                   # optional expiry/rotation deadline
  last_validated_at
  validation_status = UNKNOWN | VALIDATED | FAILED
```

The manifest should record the source references and fingerprints for audit, plus
`restore.required_secret_refs[]` / `restore.required_external_dependencies[]` as
capability requirements. A restore target may satisfy those requirements with
**different** target references, for example a different IRSA service account in a
new namespace, as long as validation proves the target reference can read/decrypt
the repository artifacts and access required external dependencies.

### Least-privilege capability separation

Backup credentials should be split by capability where the platform allows it:

| Capability | Required operations | Preferred production binding |
| --- | --- | --- |
| Repository writer/uploader | Create multipart uploads, upload objects under the generated backup-set prefix, abort failed multipart uploads, `HEAD`/read back uploaded objects for validation, and list only the relevant prefix. | IRSA for coordinator/uploader pods. |
| Repository reader/restorer | List/fetch `COMPLETE`, `backup-set.json`, node artifacts, inventories, object-reference manifests, and derived artifacts. | Separate IRSA or restore service account. |
| Repository deleter/retention janitor | Delete only generated backup-set prefixes selected by retention/deletion policy. | Separate elevated IRSA/service account; not needed for ordinary backup. |
| Capture provider | Create/delete `VolumeSnapshot`, clone/staging PVCs, uploader pods, or direct EBS snapshots depending on provider. | Kubernetes RBAC for CSI snapshots; AWS role only for direct EBS provider. |
| External blob/object validation | `HEAD`/read metadata for blob payload objects and verify object-store recovery points. | Existing blob-reader identity or a backup-specific read-only identity. |
| Repository encryption/decryption | `kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey`, or provider equivalents according to encryption mode. | KMS grants scoped to backup repository writer/reader/deleter roles. |

CommonFolio AWS production should prefer IRSA and AWS KMS. A MinIO/self-hosted
deployment may use Kubernetes Secrets or External Secrets for access keys plus a
TLS CA reference. Static environment/file credentials should remain a dev/Lab
escape hatch and should cause production repository validation to fail unless an
explicit non-production profile is selected.

### Encryption policy

All backup repository traffic should use TLS. For AWS S3 this means normal HTTPS
certificate validation. For MinIO or private S3-compatible endpoints, repository
configuration must include `tls_server_name` and/or `ca_ref` when the endpoint is
not trusted by the system roots. Disabling TLS verification is local-development
only and must fail production validation.

Backup artifacts should be encrypted at rest. First production should use
server-side encryption rather than introducing Mycel-managed client-side payload
keys:

| Encryption mode | Production status | Manifest evidence |
| --- | --- | --- |
| `SSE_KMS` | Preferred for AWS/CommonFolio production. | KMS key reference, key fingerprint/ARN, provider, algorithm, and validation that writes used the configured key. |
| `SSE_S3` / provider-managed encryption | Acceptable for some S3-compatible deployments when policy approves. | Provider encryption mode and validation evidence from object metadata where available. |
| `SSE_C` / customer-provided per-object key | Not supported for first production. | Would require raw key handling; reject unless a future design adds a safe resolver. |
| `CLIENT_ENVELOPE` | Future/required-feature design. | Would require encrypted data-key envelopes and key-rotation semantics. Not part of first production. |
| `NONE` | Dev/Lab only. | Production validation fails unless explicitly accepted by a non-production profile. |

The backup manifest and completion marker should record encryption references and
validation evidence, not keys. For example, `repository.encryption` may include
`mode`, `kms_key_ref`, `key_fingerprint`, `algorithm`,
`repository_default_encryption_verified`, and `validated_at`.

Key rotation rules:

- Rotating repository or KMS keys must not make retained backups unreadable.
- A KMS key version/grant required by a retained backup must remain decryptable
  until the backup set and all dependent audit/restore obligations expire.
- Repository validation should verify both write/encrypt and read/decrypt using a
  sentinel object under a validation prefix.
- Restore planning should verify decrypt permission for the target reference
  before staging artifacts.
- Disabling or deleting a KMS key referenced by retained backups should be treated
  as a production-risk event and surfaced as an alert.

### Repository and dependency validation

`ValidateBackupRepository` and backup/restore preflight should prove effective
credentials without exposing secret values. At minimum, validation should:

1. resolve the credential reference using the deployment resolver;
2. write a non-secret sentinel object under a generated validation prefix when
   write capability is required;
3. `HEAD`/read the sentinel and verify size/checksum and server-side encryption
   metadata;
4. list the validation prefix and the intended backup-set prefix with scoped
   permissions;
5. abort a test multipart upload or validate multipart capability when the upload
   policy requires it;
6. delete the sentinel only when the role being validated includes delete
   capability; otherwise leave a short-lived validation object governed by janitor
   cleanup;
7. validate restore-reader credentials separately from backup-writer credentials;
8. validate TLS CA/server-name configuration for MinIO/private endpoints;
9. validate KMS decrypt permission with a sentinel encrypted by the configured
   key;
10. validate external object-store read/recovery-point references for blob
    payload dependencies.

Validation reports should include reference IDs, fingerprints, capability names,
status, and sanitized provider error categories. They must not include access key
IDs when policy treats them as sensitive, secret values, authorization headers,
session tokens, signed URLs, presigned URL query strings, or raw provider error
messages that echo credentials.

### Restore-time secret requirements

A clean `NEW_CLUSTER_BOOTSTRAP` restore cannot depend on the original Mycel
system metadata being available. Deployment/IaC/operator configuration must
provide bootstrap repository information and target credential references. Restore
planning should require target bindings for:

- backup repository read/list access;
- repository decrypt/KMS access;
- external object-store read/recovery-point validation for blob payloads;
- internal Mycel state keys that are external to the backup set, such as user
  store encryption keys, JWT/signing keys, or TLS/private-key material when the
  restored state requires them;
- TLS CA references for private S3-compatible endpoints;
- capture/provider/staging credentials if the restore path needs cloud/Kubernetes
  resources.

For `EXISTING_CLUSTER_REPLACE`, the target may already have configured
references, but the restore plan must still validate that they satisfy the backup
set's requirements. Target pre-restore credentials and sessions are not
implicitly trusted after restore; auth-session handling follows the selected
restore `auth_session_policy`.

If a required secret reference is missing, resolves to insufficient capability,
uses the wrong KMS key, fails TLS validation, or cannot decrypt/read repository
objects, restore planning fails before target mutation.

### Redaction and audit

Every code path that handles backup/restore credentials should use structured
redaction:

- log/reference secret refs by stable ref ID and fingerprint only;
- strip `Authorization`, `X-Amz-Signature`, `X-Amz-Credential`, session tokens,
  cookies, and presigned URL query parameters from errors and traces;
- prefer logical `s3://bucket/prefix/object` URIs in manifests over provider URLs
  that carry signatures;
- redact environment variables, mounted Secret contents, generated AWS session
  credentials, MinIO access keys, and TLS private keys from Mycel Lab artifacts;
- record secret-resolution and KMS-use audit events by purpose, ref, principal,
  operation ID, and result, but not by raw credential value.

Credential updates, repository encryption policy changes, KMS key changes,
repository secret-reference changes, and force-delete/unpin/release operations
should require elevated admin authorization and produce audit events. Failed
validation should be observable and actionable, but sanitized.

## 10. Compatibility and capacity planning

### Version compatibility and downgrade boundaries

Backup/restore compatibility is governed by the backup manifest, storage format
versions, required feature flags, topology/identity constraints, and the restore
binary's declared capabilities. It is not governed by SDK/client compatibility.
For CommonFolio production, operators should plan to restore with the same Mycel
minor release line and a daemon version equal to or newer than the backup source
patch unless a compatibility matrix explicitly says otherwise.

#### Compatibility fields

Every backup set should record enough compatibility data for restore planning to
fail before target mutation:

```text
Compatibility
  source_mycel_version
  source_mycel_api_version
  source_image_digest
  manifest_version
  repository_layout_version
  archive_format
  required_features[]
  storage_format_versions
    system_raft
    system_metadata
    graph
    graph_checkpoint
    identity
    auth_sessions
    blob_metadata
    semantic_rules
    semantic_records
    semantic_vector_search
    lexical_search
    automation
    usage_accounting
  derived_artifact_format_versions
  min_restore_mycel_version
  max_restore_mycel_version
  supported_restore_target_modes[]
  supported_identity_modes[]
  requires_same_topology
  downgrade_safe_until_storage_migration = true | false
```

Restore planning must reject the backup set when:

- `manifest_version` or `repository_layout_version` is unsupported;
- any `required_features[]` value is unknown to the restore binary;
- a required authoritative `storage_format_versions` value is unsupported;
- `source_mycel_version` is newer than the restore binary and no explicit forward
  compatibility declaration exists;
- the backup set requires a restore target mode or identity mode unsupported by
  the target binary;
- the raw physical backup requires same topology and the target node count,
  partition count, replica factor, Raft node IDs, or ordinal/PVC mapping differs;
- the archive format or compression is unsupported;
- external dependency versions or object-store semantics required by the manifest
  cannot be validated.

#### First production restore matrix

The first production matrix should be conservative:

| Backup source | Restore binary | Default result | Notes |
| --- | --- | --- | --- |
| Same version/image digest | Same version/image digest | Allowed if manifest, topology, credentials, and artifacts validate. | Preferred for drills and emergency restore. |
| Same minor line, newer patch | Newer patch in same minor line | Allowed when storage format versions and required features are supported. | Normal patch-level restore path, e.g. a `v0.23.1` backup restored by `v0.23.2`. |
| Same minor line, older patch | Older patch in same minor line | Blocked unless the older binary declares support for all recorded storage/artifact formats and required features. | Do not assume patch downgrade safety after storage/manifest changes. |
| Older minor line backup | Newer minor line binary | Allowed only through an explicit migration/compatibility declaration. | The restore operation may upgrade storage formats after successful apply, but must record that it crossed a compatibility boundary. |
| Newer minor line backup | Older minor line binary | Blocked. | Requires restore with a newer/same-capability binary or a logical export path designed for downgrade. |
| Raw physical backup with `ADOPT_TARGET_ID` or `GENERATE_NEW_ID` | Any first-production binary | Blocked. | Those modes remain design/API vocabulary until logical import or identity-transform support exists. |
| Unsupported required derived artifact format | Same/newer binary | Blocked for `INCLUDE_REQUIRED`; rebuild/omit for optional artifacts according to policy. | Authoritative state compatibility is separate from derived artifact compatibility. |

The restore binary should publish a machine-readable capability table so
`PlanClusterRestore` can evaluate this matrix deterministically. The table should
include supported manifest versions, storage format versions, archive formats,
capture-provider evidence it understands, restore target modes, identity modes,
and derived artifact format versions.

#### Binary rollback versus restore-from-backup

Rollback after a bad upgrade has two distinct meanings:

1. **Binary rollback.** Restart the previous binary against the current durable
   state. This is allowed only if the previous binary declares that it supports
   every current storage format and no irreversible migration or feature flag has
   been activated.
2. **Restore-from-backup rollback.** Restore a backup taken before the upgrade or
   before the bad operation. This is required when storage formats changed,
   irreversible migrations ran, durable semantic/vector record formats changed,
   identity/session semantics changed incompatibly, or the previous binary cannot
   prove support for current storage.

Upgrade runbooks should therefore include a pre-upgrade backup and a rollback
branch:

```text
if old binary supports current storage formats and no irreversible migration ran:
  binary rollback may be used
else:
  restore from a pre-upgrade backup set
```

A daemon should refuse to start with an older binary against storage whose
`storage_format_versions` are newer than the binary supports. It should fail
closed with a clear operator error rather than attempting best-effort downgrade.

#### Derived artifacts and format changes

Derived artifact compatibility is evaluated per subsystem:

- `SEMANTIC_VECTOR_SEARCH = INCLUDE_REQUIRED` artifacts must be readable and must
  match semantic generation, rebuild epoch, vector dimensions, model/profile
  identity, and artifact format version. If not, first production restore fails
  closed unless an elevated degraded restore explicitly rebuilds from durable
  semantic records.
- `LEXICAL_SEARCH`, `GRAPH_CHECKPOINT`, and `PERSISTENT_GRAPH_QUERY_INDEX`
  artifacts are optional by default. Unsupported or stale formats are omitted and
  marked `REBUILD_REQUIRED`.
- `METADATA_ADJACENCY_MEMORY_INDEX` is discarded and does not create version
  compatibility blockers.

Authoritative semantic/vector records are not optional derived artifacts. If the
restore binary cannot read durable semantic records, the backup set is not
compatible for production restore.

#### Compatibility audit

Restore operations should persist compatibility decisions, including source
version, target version, storage format checks, required features, derived format
actions, and whether a binary rollback was allowed or rejected. This evidence is
part of the rollback audit trail and should be included in restore-plan output.

### Capacity checks and sizing formulas

Backup and restore preflight must prove that the selected policy has enough
source, staging, target, and repository capacity before quiescence or target
mutation begins. Sizing estimates should be conservative and should appear in
operation status so operators can see which resource is the bottleneck.

#### Inputs

Preflight should collect or estimate:

```text
N                                # expected node count
source_pvc_capacity_bytes[i]
source_pvc_used_bytes[i]
classified_authoritative_bytes[i]
classified_durable_progress_bytes[i]
classified_local_blob_payload_bytes[i]
classified_required_derived_bytes[i]
classified_optional_derived_bytes[i]
archive_compression_ratio[i]     # default to 1.0 until observed history exists
repository_effective_upload_bps
repository_effective_download_bps
local_copy_bps[i]
provider_snapshot_create_seconds[i]
expected_write_rate_bps[i]       # for snapshot delta/cost estimates during upload
multipart_part_size_bytes
multipart_concurrency
restore_parallelism
safety_margin                    # first production default 20%, minimum 1 GiB
```

External S3/MinIO blob payload bytes are not included in Mycel backup artifact
bytes by default. They are tracked as external dependency logical bytes and
validated through the object-reference manifest.

#### Artifact and repository sizing

For each node:

```text
included_uncompressed_bytes[i] =
  classified_authoritative_bytes[i] +
  classified_durable_progress_bytes[i] +
  classified_local_blob_payload_bytes[i] +
  classified_required_derived_bytes[i] +
  included_valid_optional_derived_bytes[i] +
  manifest_inventory_overhead_bytes[i]

estimated_archive_bytes[i] =
  max(observed_recent_archive_bytes[i],
      included_uncompressed_bytes[i] * archive_compression_ratio[i])
```

For the backup set:

```text
estimated_backup_set_bytes =
  sum(estimated_archive_bytes[i] for i in nodes) +
  included_shared_derived_artifact_bytes +
  object_reference_manifest_bytes +
  backup_set_manifest_bytes +
  complete_marker_bytes +
  validation_report_bytes +
  multipart_overhead_bytes

repository_required_free_or_quota_bytes =
  estimated_backup_set_bytes * 1.20 + 1GiB
```

S3 does not expose a universal free-space value, so repository preflight should
validate write/list/read capability and, where available, account quota, bucket
quota, lifecycle, object-lock, KMS, and cost-budget signals. MinIO/self-hosted
repositories should expose available capacity when possible; production preflight
should fail if the repository cannot accept the estimated backup-set bytes plus
headroom.

#### Source-side capture sizing

For `CSI_VOLUME_SNAPSHOT` and direct `AWS_EBS_SNAPSHOT`, source PVC free space is
not the main capacity for the snapshot object, but preflight must still verify:

- source PVCs are healthy and not above the operational full threshold;
- snapshot class/provider quotas allow one snapshot per expected PVC plus any
  temporary clone/staging PVCs;
- uploader/staging PVC capacity is large enough to materialize a readable clone
  or package artifacts;
- snapshot delta/cost risk during upload is acceptable.

For `FILESYSTEM_IMMUTABLE_COPY`, the source or scratch filesystem must hold the
immutable copy:

```text
filesystem_copy_scratch_required[i] =
  included_uncompressed_bytes[i] * 1.20 + 1GiB
```

If the implementation writes an archive to local scratch before uploading instead
of streaming from the immutable copy, add `estimated_archive_bytes[i]` to the
scratch requirement. Production preflight should fail when required scratch space
is unavailable; it should not enter quiescence hoping space is sufficient.

Uploader/data-mover staging capacity depends on implementation:

```text
streaming_uploader_scratch_required =
  multipart_part_size_bytes * multipart_concurrency * 2 + 512MiB

archive_staging_required[i] =
  estimated_archive_bytes[i] * 1.20 + 1GiB

clone_pvc_required[i] =
  source_pvc_capacity_bytes[i]
```

First production should use the larger of the implementation's measured staging
need and the conservative formula above.

#### Restore sizing

Restore preflight should verify target capacity before `READY_TO_APPLY`:

```text
restore_staging_required =
  if staging_all_nodes:
    sum(estimated_archive_bytes[i]) * 1.20 + 1GiB
  else:
    max(estimated_archive_bytes[i] for i in nodes) * restore_parallelism * 1.20 + 1GiB

minimum_target_pvc_capacity[i] =
  max(source_pvc_capacity_bytes[i], included_uncompressed_bytes[i] * 1.20 + 1GiB)
```

For first-production raw physical/PVC restores, target PVC capacity should be at
least the source PVC capacity recorded in the manifest, and filesystem type,
mount options, permissions, and ownership must be compatible. Replacement-PVC
cutover may require temporary capacity for both the old target PVCs and the new
restored PVCs until validation succeeds.

If external object-store payload bytes are not included in Mycel artifacts,
restore capacity for those payloads is governed by the external object-store
recovery process. Mycel should still validate that referenced external stores have
required objects/recovery points available.

#### Timeouts and outage estimates

Quiesce timeout should be sized from the expected drain, checkpoint, and capture
source creation time:

```text
backup_quiesce_window_estimate =
  active_write_drain_p95 +
  checkpoint_barrier_p95 +
  max(provider_snapshot_create_seconds[i]) +
  quiesce_release_overhead
```

For the filesystem immutable-copy provider, immutable copy time happens inside
the quiesced window:

```text
filesystem_copy_quiesce_window_estimate =
  active_write_drain_p95 +
  checkpoint_barrier_p95 +
  max(included_uncompressed_bytes[i] / local_copy_bps[i]) +
  quiesce_release_overhead
```

Upload and restore staging estimates:

```text
backup_upload_time_estimate =
  estimated_backup_set_bytes / repository_effective_upload_bps * 1.50 + fixed_overhead

restore_download_time_estimate =
  estimated_backup_set_bytes / repository_effective_download_bps * 1.50 + fixed_overhead
```

Policy timeouts should be greater than these estimates plus a safety margin. If
preflight cannot estimate throughput, it should either run a small repository
probe or require an operator-supplied throughput floor. First production should
fail preflight rather than starting a backup whose quiesce or upload timeout is
obviously too small.

#### Capacity failure behavior

Capacity preflight failures are non-retryable until the operator changes the
environment or policy. The operation status should include the resource, required
bytes, available bytes when known, formula inputs, and remediation hints. During a
running operation, capacity exhaustion follows the failure matrix: no `COMPLETE`
for backup, no target mutation before restore inputs are staged, and
`FAILED_REQUIRES_OPERATOR` if target mutation had already become irreversible.

## 11. Admin API surface

This section is design guidance only; it is not an immediate protobuf change.
The existing `mycel.admin.v1.AdminBackupService` is the right ownership boundary,
but it should be reviewed before implementation against this production backup
model.

The production-facing admin API should model:

- a configured backup repository, not a pod-local output directory;
- S3-compatible repository settings that cover both AWS S3 and MinIO;
- cluster backup as the primary production backup operation;
- cluster restore as a first-class operation, not an out-of-band procedure;
- restore into both new clean clusters and existing clusters through an explicit
  full-state replacement mode;
- validation of repositories and backup sets;
- capture-provider selection and capability reporting;
- quiesce/checkpoint/capture/upload/validation/finalization phases;
- a shared backup-set manifest with per-node artifacts;
- restore acceptance based on manifest, checksums, expected-node coverage, and a
  completion marker;
- credential references rather than raw object-store credentials in API
  messages.

Current path-oriented concepts such as `backup_dir`, `output_dir`, and
`backup_set_path` should be revisited. The future API should prefer repository
identifiers, logical `s3://` backup-set URIs, and repository configuration
resolved from deployment secrets or identity such as IRSA. Raw S3/MinIO access
keys should not be supplied in ordinary backup requests.

### Repository model sketch

The API should expose repository configuration as an admin-managed resource. The
exact protobuf shape is deferred, but the conceptual resource is:

```text
BackupRepository
  repository_id
  display_name
  type = S3_COMPATIBLE
  endpoint                 # optional for AWS S3, required for MinIO/custom S3
  region
  bucket
  prefix
  force_path_style         # commonly true for MinIO
  credential_ref           # IRSA, Kubernetes secret, Vault ref, etc.
  tls_server_name / ca_ref # when needed for MinIO or private endpoints
  server_side_encryption   # deployment-specific policy reference, not raw key
```

Repository validation must prove that the current cluster can use the repository
for the required backup workflow. At minimum, each expected pod should be able to
write, read, `HEAD`, and list the intended backup-set prefix, and the coordinator
should be able to write the final backup-set manifest and completion marker.

### Ad-hoc cluster backup request structure

An ad-hoc backup should be a request to create one cluster backup operation from
the current backup policy, optionally with safe per-operation overrides. It
should not be a separate code path from scheduled backup. The coordinator should
resolve the effective policy at operation creation time and store that resolved
policy snapshot with the operation and backup-set manifest.

Suggested conceptual request/response shape:

```text
StartClusterBackupRequest
  idempotency_key              # required for safe caller retries
  repository_id                # optional; defaults to policy default_repository_id
  reason                       # human-readable audit reason, e.g. "before upgrade"
  display_name                 # optional operator label, not identity
  retention_class = DEFAULT | PINNED | EXPIRE_AFTER | PRE_RESTORE_SAFETY
  expire_after                 # only when retention_class = EXPIRE_AFTER
  labels
  annotations

  policy_overrides
    capture
      provider
      fallback_allowed
      snapshot_class_name
      staging_storage_class_name
    archive_format
    consistency
      allow_read_only_during_quiesce
      read_allowlist_profile = CONTROL_ONLY | STRICT_READ_ONLY
      wait_for_convergence_timeout
      quiesce_timeout
      checkpoint_timeout
    validation
      verify_checksums
      verify_expected_node_coverage
      verify_checkpoint_barriers
      verify_derived_state
    derived_subsystems[]
      subsystem
      backup_mode
      restore_on_missing
    operation_timeout

StartClusterBackupResponse
  operation
    backup_operation_id
    backup_set_id              # reserved/generated before capture starts
    repository_id
    repository_uri_prefix
    state
    source = MANUAL
    requested_by               # derived from admin auth context
    requested_at
    idempotency_key_hash
    policy_version
    effective_policy_summary
```

Ad-hoc overrides should be constrained by production guardrails:

- raw repository credentials are never supplied in the start request;
- `backup_set_id` and repository prefix are generated by Mycel to avoid
  collisions and path-injection risks;
- quiescence, checkpoint barriers, completion marker validation, and checksum
  validation cannot be disabled for production backups;
- capture-provider fallback cannot silently change outage or durability
  expectations; any fallback must be policy-authorized and recorded;
- overrides may make derived-subsystem handling stricter, but relaxing a policy
  such as `SEMANTIC_VECTOR_SEARCH = INCLUDE_REQUIRED` should require explicit
  degraded-backup intent and elevated authorization;
- the resolved effective policy is immutable for the operation once the operation
  record is created;
- retrying with the same `idempotency_key` returns the same operation, while a
  different idempotency key conflicts with any active non-terminal cluster
  backup.

### Restore planning and start request structure

Restore should also be modeled as plan-then-start. Planning is safe and
repeatable; starting is the operation that changes target cluster state.

Suggested conceptual shape:

```text
PlanClusterRestoreRequest
  repository_id
  backup_set_id
  target
    mode = NEW_CLUSTER_BOOTSTRAP | EXISTING_CLUSTER_REPLACE
    target_cluster_ref           # deployment/namespace/cluster identifier
    expected_node_count
    node_mapping[]               # backup node/ordinal/PVC -> target node/ordinal/PVC
    cluster_identity_mode = PRESERVE_BACKUP_ID | ADOPT_TARGET_ID | GENERATE_NEW_ID
    auth_session_policy = PRESERVE_RESTORED_SESSIONS | REVOKE_ALL_SESSIONS
    source_cluster_fencing_evidence # required when preserving a live-production identity outside the original deployment
    commonfolio_attachment_ref      # optional external attachment/audit reference, not billing policy
    allow_destructive_replace    # required true for EXISTING_CLUSTER_REPLACE
    require_pre_restore_backup
    degraded_derived_state_policy = FAIL_CLOSED | ALLOW_POLICY_DEGRADED
  labels
  annotations

PlanClusterRestoreResponse
  restore_plan_id
  restorable
  target_mode
  backup_set_summary
  target_summary
  identity_plan
    source_cluster_id
    target_pre_restore_cluster_id
    final_cluster_id
    identity_mode
    auth_session_policy
    identity_transform_supported
    required_fencing_evidence[]
  required_secret_refs[]
  required_node_mapping[]
  derived_state_actions[]
  destructive_actions[]         # populated for EXISTING_CLUSTER_REPLACE
  blockers[]
  warnings[]

StartClusterRestoreRequest
  restore_plan_id
  idempotency_key
  reason
  destructive_confirmation       # required for EXISTING_CLUSTER_REPLACE
  pre_restore_backup_set_id      # when a safety backup was created/required
  source_cluster_fencing_evidence # repeated/confirmed if required by the plan
  labels
  annotations

StartClusterRestoreResponse
  restore_operation_id
  restore_plan_id
  state
  target_mode
  started_at
  requested_by
  idempotency_key_hash
```

For `EXISTING_CLUSTER_REPLACE`, `StartClusterRestore` should fail unless the
plan is still fresh, the target restore lock can be acquired, destructive intent
is explicit, and any required pre-restore safety backup or waiver is present.

### Basic service interface

The basic admin interface should offer these method groups. Names are design
placeholders; final protobuf naming can be adjusted during API implementation.

```text
service AdminBackupService {
  # Backup repository configuration.
  ListBackupRepositories()
  GetBackupRepository(repository_id)
  CreateBackupRepository(repository)
  UpdateBackupRepository(repository)
  ValidateBackupRepository(repository_id)
  DeleteBackupRepository(repository_id)

  # Backup policy and scheduling.
  GetBackupPolicy()
  UpdateBackupPolicy(policy)

  # Cluster backup operation lifecycle.
  StartClusterBackup(request)
  GetClusterBackupStatus(backup_operation_id)
  ListClusterBackupOperations(page_size, page_token)
  CancelClusterBackup(backup_operation_id, reason)

  # Backup-set catalog and validation.
  ListClusterBackupSets(repository_id, page_size, page_token)
  GetClusterBackupSet(repository_id, backup_set_id)
  ValidateClusterBackupSet(repository_id, backup_set_id)
  DeleteClusterBackupSet(repository_id, backup_set_id)

  # Cluster restore lifecycle.
  PlanClusterRestore(request)
  StartClusterRestore(request)
  GetClusterRestoreStatus(restore_operation_id)
  ListClusterRestores(page_size, page_token)
  CancelClusterRestore(restore_operation_id, reason)
}
```

Method semantics:

- `StartClusterBackup` is asynchronous. It owns preflight, quiesce, checkpoint,
  capture, upload, validation, backup-set manifest creation, and completion-marker
  publication. It succeeds only after the complete backup set exists in the
  configured repository.
- `GetClusterBackupStatus` reports operation phase and per-node artifact state,
  including quiesce/checkpoint/capture/upload/validation/finalization progress.
- `ListClusterBackupOperations` lists recent operation attempts, including
  failed, canceled, running, and successful attempts.
- `ListClusterBackupSets` lists completed backup sets known to the cluster and
  should not require inspecting pod-local files.
- `ValidateClusterBackupSet` reads the repository manifest and verifies expected
  nodes, artifact existence, sizes, checksums, committed barriers, and completion
  marker before declaring the set restorable.
- `DeleteClusterBackupSet` runs the completed backup-set deletion lifecycle from
  the retention/deletion rules: validate `COMPLETE` and `backup-set.json`, reject
  protected or in-use backup sets, delete the publication marker before artifacts,
  delete only objects under the generated backup-set prefix, and retain an audit
  tombstone. It must not delete incomplete or unknown prefixes without explicit
  operator intent.
- `PlanClusterRestore` is a dry-run operation. It validates the backup set,
  target cluster compatibility, expected node/ordinal mapping, required secrets,
  storage sizing, and whether the target satisfies the selected mode's
  clean-target or destructive-replacement preconditions.
- `StartClusterRestore` is asynchronous. The first supported production modes
  should be `NEW_CLUSTER_BOOTSTRAP` and `EXISTING_CLUSTER_REPLACE`. Existing
  cluster replacement is destructive and must follow a restore plan that proves
  the target is locked, compatible, safely mapped, and explicitly authorized.
- `CancelClusterRestore` is idempotent while cancellation is safe. Once restore
  reaches an irreversible apply phase, cancellation should fail closed or report
  that operator recovery is required.

The single-daemon backup API may remain useful for development, diagnostics, or
single-node deployments, but CommonFolio production should treat cluster backup
to a shared S3-compatible repository as the normative path.

## 12. Validation, qualification, and launch gates

### Validation expectations

A backup should not be considered production-safe until it can be restored and
verified in isolation. Minimum validation should include:

- manifest and `COMPLETE` checksum validation;
- version, storage-format, required-feature, topology, and identity-mode
  compatibility checks;
- repository credential, TLS, and encryption validation;
- graph/domain identity checks;
- attachment manifest/object-reference checks;
- Raft/log consistency checks;
- projection/index rebuild requirement checks;
- capacity checks for capture, upload, staging, restore, and target PVCs;
- restore-mode precondition checks for both new-cluster bootstrap and
  existing-cluster replacement;
- usage counter availability or reconciliation checks;
- negative/fail-closed checks for missing/corrupt required artifacts;
- smoke tests for auth, graph read/write, and search after restore.

### Mycel Lab and environment qualification tests

Production backup/restore should be release-gated by both portable Mycel Lab
suites and environment-specific qualification in the target AWS/CommonFolio
shape. A raw passing unit test is not enough; operators need evidence that the
actual deployment can quiesce, capture, upload, validate, restore, and resume.

#### Required environment qualification

AWS/CommonFolio `staging2` should be the first production reference environment.
A qualification record should prove:

- EKS nodes and Mycel pods are healthy with expected node count, ordinals, PVCs,
  storage class, filesystem type, and resource requests/limits;
- EBS CSI driver is installed and healthy;
- Kubernetes `VolumeSnapshot` CRDs, snapshot controller, and a compatible
  `VolumeSnapshotClass` exist, or the direct `AWS_EBS_SNAPSHOT` provider is
  explicitly selected and validated;
- Mycel PVCs use the expected encrypted gp3 StorageClass and have enough capacity
  for backup/restore sizing formulas;
- a dedicated S3-compatible backup repository bucket/prefix exists separately
  from the blob payload bucket unless policy explicitly allows sharing;
- backup writer, restore reader, retention/deleter, capture-provider, KMS, and
  external blob/object validation credential references validate through sentinel
  probes;
- repository TLS/encryption/KMS settings are validated;
- external blob/object-store backup or recovery-point evidence exists for payload
  stores that are not copied into the Mycel backup set;
- source-fencing and destructive existing-cluster replacement runbooks are
  documented and rehearsed;
- alerts exist for stale backup, failed backup, failed restore, cleanup failure,
  credential/encryption validation failure, and restore unsafe/locked state.

Non-AWS/self-hosted qualification should prove the same Mycel contract with the
available provider: `CSI_VOLUME_SNAPSHOT` when supported, otherwise
`FILESYSTEM_IMMUTABLE_COPY` with MinIO or another S3-compatible repository and
explicit scratch/capacity limits. Local-path K3s should be labeled as a fallback
copy/dev qualification unless it has snapshot semantics equivalent to production.

#### Required Mycel Lab suites

The first implementation should add or extend Mycel Lab suites that produce
artifacts safe for release review:

| Suite | Purpose | Minimum assertions |
| --- | --- | --- |
| `k3d-cluster-backup-restore-new` | Full cluster backup to S3-compatible repository and restore into a clean new cluster. | Valid manifest/`COMPLETE`, all node artifacts restored, auth/graph/search smoke tests pass. |
| `k3d-cluster-backup-existing-replace` | Destructive existing-cluster replacement flow in an isolated lab cluster. | Restore lock blocks ordinary traffic, pre-restore safety backup/waiver is enforced, replacement succeeds, lineage/audit recorded. |
| `k3d-backup-missing-complete-fail-closed` | Ensure incomplete prefixes are not restorable. | Prefix with artifacts and manifest but no `COMPLETE` is ignored/rejected. |
| `k3d-backup-corrupt-required-artifact-fail-closed` | Required artifact corruption handling. | Checksum mismatch or missing node artifact prevents restore planning. |
| `k3d-backup-optional-derived-rebuild` | Optional derived artifact handling. | Corrupt/missing lexical/checkpoint/query index artifacts are omitted and marked `REBUILD_REQUIRED`, not published as valid. |
| `k3d-semantic-vector-required-restore` | CommonFolio semantic/vector default. | `SEMANTIC_VECTOR_SEARCH = INCLUDE_REQUIRED` artifacts validate; strict semantic reads are unavailable until readiness passes and then return expected results. |
| `k3d-backup-crash-resume-upload-finalize` | Coordinator crash/retry behavior. | Crash during upload resumes from immutable source; crash after manifest before `COMPLETE` finishes or fails closed; crash after `COMPLETE` marks operation succeeded. |
| `k3d-restore-crash-resume-and-unsafe-fail` | Restore crash/retry behavior. | Crash before apply resumes/cancels safely; crash after irreversible apply resumes or ends `FAILED_REQUIRES_OPERATOR` without admitting traffic. |
| `k3d-retention-deletion-safety` | Retention/deletion safety. | Pinned/pre-restore backups are protected, incomplete prefixes expire only after grace, deletion removes `COMPLETE` first, unknown prefixes are not deleted. |
| `compose-minio-repository-credentials-encryption` | Portable S3-compatible repository validation. | MinIO credentials/TLS/encryption policy are validated; raw secrets are absent from manifests/log artifacts. |
| `k3d-capacity-preflight` | Capacity checks. | Insufficient scratch/staging/target/repository capacity fails before quiesce or target mutation with structured blockers. |
| `k3d-version-compatibility-gates` | Compatibility matrix enforcement. | Unsupported manifest/storage/required feature/version/topology/identity modes fail during planning. |

Existing suites such as `k3d-system-backup-restore` remain useful for lower-level
system backup coverage, but CommonFolio production launch should require the
cluster-level repository/manifest/restore suites above.

#### Negative and chaos test requirements

Release-gate coverage should include at least these negative tests:

- missing `COMPLETE` marker;
- manifest checksum mismatch with `COMPLETE`;
- missing node artifact;
- corrupt archive checksum;
- unsupported `manifest_version` or `required_features[]`;
- unsupported required semantic/vector artifact format;
- topology mismatch for first-production raw physical restore;
- source-fencing evidence missing for preserved production identity;
- wrong or insufficient repository/KMS credentials;
- external blob/object recovery point missing;
- capacity below required threshold;
- attempted ordinary writes during quiesce and attempted ordinary reads/writes
  during existing-cluster restore lock.

Chaos coverage should intentionally kill the coordinator/uploader/restore worker
at representative phase boundaries: before quiesce, while quiesce is held, after
immutable capture before upload, during upload, after manifest before `COMPLETE`,
after `COMPLETE` before system metadata update, during restore staging, and after
restore irreversible apply begins.

#### Launch qualification artifact

Each release-candidate qualification should produce a durable report containing:

- daemon image digest and API/SDK/Lab versions tested;
- environment provider details and credential/encryption reference fingerprints;
- backup policy hash and derived-subsystem policy;
- backup-set ID, manifest checksum, `COMPLETE` checksum, artifact counts/bytes,
  and external dependency evidence;
- restore target mode, identity mode, auth-session policy, and topology mapping;
- capacity estimates versus observed sizes/timings;
- pass/fail status for positive, negative, retention, credential, and chaos
  tests;
- proof that artifacts/logs were scanned for raw secrets and unsafe paths.

CommonFolio production launch should require a passing qualification report for
the current release-candidate image in the target production-like environment and
should keep that report with release artifacts.

### Launch gate

CommonFolio production launch should require:

1. scheduled and on-demand backups working in the target AWS deployment;
2. restore into both a clean new environment and an existing-cluster replacement
   path;
3. documented and rehearsed rollback procedure that distinguishes binary rollback
   from restore-from-backup rollback;
4. restore verification evidence from at least one current release candidate;
5. passing Mycel Lab cluster-level backup/restore suites, including negative and
   chaos cases;
6. environment qualification evidence for AWS `staging2`/production shape,
   including EBS CSI/snapshot, S3 repository, IRSA/KMS, capacity, and external
   object-store dependency validation;
7. alerts for backup failure, stale backup state, credential/encryption
   validation failure, cleanup failure, and restore unsafe/locked state.

### Follow-up implementation work

- Translate this design into focused API/protobuf changes after review.
- Implement repository/policy/operation/manifest storage in system metadata and
  the S3-compatible repository.
- Implement capture providers, data movers, path-inventory validation,
  checkpoint/barrier validation, derived-subsystem policies, and restore planning.
- Implement credential-reference resolvers, encryption validation, retention,
  deletion, janitor cleanup, and audit/tombstone records.
- Add the Mycel Lab and environment qualification suites listed above and wire
  them into release gates.

## Appendix A. Backup path inventory

This appendix records the initial per-node path inventory for backups rooted at
the Mycel data directory, normally `/data/mycel` in CommonFolio deployments.
Capture providers may snapshot or clone the whole PVC, but the uploader and
manifest should still classify what the snapshot contains so restore behavior is
explicit and auditable.

Production backup should run in strict inventory mode:

- every top-level path in the Mycel data directory must be classified here or by
  a newer manifest schema version;
- classified authoritative directories are included recursively unless a more
  specific exclusion is listed;
- derived directories are included, omitted, or rebuilt according to backup
  policy;
- symlinks, sockets, device files, partial upload files, and unclassified
  temporary files should fail validation unless they are under an explicitly
  excluded path;
- object-store payload bytes are included only for the local blob backend, not
  for S3-compatible blob backends.

Classifications:

```text
AUTHORITATIVE_REQUIRED       # required for data correctness
DURABLE_PROGRESS_REQUIRED    # required to preserve committed consumer/progress state
DERIVED_POLICY_CONTROLLED    # included/rebuilt/discarded according to backup policy
DIAGNOSTIC_OPTIONAL          # excluded by default; may be attached for debugging
EPHEMERAL_EXCLUDED           # never part of a restorable backup set
EXTERNAL_DEPENDENCY          # validated reference to state backed up outside Mycel
```

### Top-level data directory inventory

| Path | Classification | Default production behavior | Restore behavior |
| --- | --- | --- | --- |
| `meta/` | `AUTHORITATIVE_REQUIRED` | Include recursively, except explicitly excluded temp files. | Restore before daemon startup; validate schema/format versions and identity mode. |
| `meta/clustering/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore according to cluster identity mode; validate node/ordinal mapping before joining. |
| `meta/raft/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore Raft hard state, entries, snapshots, and group storage before starting Raft groups. |
| `meta/wal/` | `AUTHORITATIVE_REQUIRED` | Include when WAL is enabled. | Restore WAL progress/checkpoint evidence with `wal/` segments. |
| `meta/semantic/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore semantic Raft/deduplication metadata before semantic maintenance resumes. |
| `meta/inference_runtime/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore inference runtime/global metadata and usage accounting state. |
| `wal/` | `AUTHORITATIVE_REQUIRED` when WAL is enabled | Include when WAL is enabled. | Replay/validate with `meta/wal/` progress and checkpoint files. |
| `graphs/` | Mixed; see graph inventory below | Include authoritative graph/semantic/inference state; handle derived subpaths by policy. | Restore graph state before search, semantic, inference, and automation consumers resume. |
| `identity/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore principal, auth/session, and identity state; invalidate/revalidate runtime sessions as required. |
| `schema/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore schema/catalog state before graph query/index validation. |
| `automations/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore workflow definitions, invocations, ledgers, control barriers, audit/output state, and durable cursors. |
| `activity/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore activity/audit event stream if present. |
| `blob_meta/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore blob metadata and payload descriptors before blob reads are enabled. |
| `blobs/<space>/` | Backend-dependent | Include payload files only when Mycel uses the local blob backend. For object-store backends, exclude payload staging and rely on external object-store backup. | Local backend: restore payload files. S3-compatible backend: restore descriptors and validate external object references. |
| `blobs/_object_store_staging/` | `EPHEMERAL_EXCLUDED` | Exclude. Quiescence should drain active object-store uploads. | Never restored. |
| `graph-change-outbox/` | `DURABLE_PROGRESS_REQUIRED` | Include. | Restore source-feed/outbox state; consumers still detect gaps. |
| `graph-change-notification/` | `DURABLE_PROGRESS_REQUIRED` | Include. | Restore bounded notification history and delivery state; consumers still detect gaps. |
| `search/lexical/` | `DERIVED_POLICY_CONTROLLED` for `LEXICAL_SEARCH` | Default is `INCLUDE_OPTIONAL`. | If included, validate manifest/cursor/segments against graph frontier; otherwise rebuild from graph state. |
| `exports/` | `EPHEMERAL_EXCLUDED` | Exclude. | User export scratch/output is not restored as cluster state. |
| `log/`, `logs/` | `DIAGNOSTIC_OPTIONAL` | Exclude from restorable backup by default. | May be attached to operation artifacts but should not affect restore. |

### Graph and per-space inventory

| Path | Classification | Default production behavior | Restore behavior |
| --- | --- | --- | --- |
| `graphs/<space>/domains/<domain>/manifest.mycel` | `AUTHORITATIVE_REQUIRED` | Include. | Validate domain store format before opening the graph. |
| `graphs/<space>/domains/<domain>/segments/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore graph transaction/node/edge history and replay/validate to the checkpoint barrier. |
| `graphs/<space>/domains/<domain>/raft-applied-commands.json` | `AUTHORITATIVE_REQUIRED` | Include when present. | Restore Raft command dedupe state for the domain/partition. |
| `graphs/<space>/domains/<domain>/checkpoints/` | `DERIVED_POLICY_CONTROLLED` for `GRAPH_CHECKPOINT` | Default should be `INCLUDE_OPTIONAL`. | If included, validate checkpoint manifest and checksums; otherwise rebuild/latest-state replay from authoritative graph segments. |
| `graphs/<space>/domains/<domain>/indexes/` | `DERIVED_POLICY_CONTROLLED` for `PERSISTENT_GRAPH_QUERY_INDEX` | Default should be `INCLUDE_OPTIONAL`. | If included, validate persistent index manifests/checksums against graph checkpoint; otherwise rebuild. |
| `graphs/<space>/domains/<domain>/projection-artifacts/` | `DURABLE_PROGRESS_REQUIRED` / evidence | Include when present. | Validate local projection artifact evidence; corrupt evidence invalidates the related artifact and may require rebuild/fail closed by policy. |
| `graphs/<space>/semantic/maintenance/` | `DURABLE_PROGRESS_REQUIRED` | Include. | Restore semantic dirty events, work state, and maintenance checkpoints. |
| `graphs/<space>/semantic/indexes/<semantic_index_id>/manifest.ksem` | `AUTHORITATIVE_REQUIRED` for semantic records | Include. | Validate semantic vector record manifest before semantic search readiness. |
| `graphs/<space>/semantic/indexes/<semantic_index_id>/records/` | `AUTHORITATIVE_REQUIRED` for semantic records | Include. | Restore durable embedding/vector records so semantic indexes can be validated or rebuilt without provider re-embedding. |
| `graphs/<space>/semantic/indexes/<semantic_index_id>/records/latest/` | `AUTHORITATIVE_REQUIRED` for semantic records | Include, except unresolved pending publications. | Restore latest-record views/provenance and validate against segment records. |
| `graphs/<space>/semantic/indexes/<semantic_index_id>/records/latest/pending/` | `EPHEMERAL_EXCLUDED` unless drained | Quiescence should drain/recover pending semantic publications; unresolved pending records should fail validation. | Never restore unresolved pending publication state as ready data. |
| `graphs/<space>/semantic/search_indexes/<rule>/<binding>/latest.json` | `DERIVED_POLICY_CONTROLLED` for `SEMANTIC_VECTOR_SEARCH` | CommonFolio production default: `INCLUDE_REQUIRED`. | Validate against semantic record frontier, generation, rebuild epoch, model metadata, dimensions, and index format. |
| `graphs/<space>/semantic/search_indexes/<rule>/<binding>/state.json` | `DERIVED_POLICY_CONTROLLED` for `SEMANTIC_VECTOR_SEARCH` | Include with required semantic physical index artifacts. | Restore readiness state only after validating the corresponding physical index. |
| `graphs/<space>/semantic_projection_leases.json` | `DURABLE_PROGRESS_REQUIRED` / readiness evidence | Include. | Revalidate leases/fences against restored leadership, generation, rebuild epoch, and freshness. |
| `graphs/<space>/inference/` | `AUTHORITATIVE_REQUIRED` | Include. | Restore space-scoped inference/catalog runtime state used by semantic/inference paths. |

### Lexical search inventory

| Path | Classification | Default production behavior | Restore behavior |
| --- | --- | --- | --- |
| `search/lexical/<space>/<domain>/manifest.json` | `DERIVED_POLICY_CONTROLLED` for `LEXICAL_SEARCH` | Default `INCLUDE_OPTIONAL`. | Validate analyzer/index format and segment list if included. |
| `search/lexical/<space>/<domain>/cursor.json` | `DERIVED_POLICY_CONTROLLED` for `LEXICAL_SEARCH` | Default `INCLUDE_OPTIONAL` with lexical manifest. | Validate indexed graph revision is not ahead of restored graph frontier. |
| `search/lexical/<space>/<domain>/segments/` | `DERIVED_POLICY_CONTROLLED` for `LEXICAL_SEARCH` | Default `INCLUDE_OPTIONAL` with lexical manifest. | Validate segment metadata/checksums; otherwise rebuild from graph state. |

### Blob/object-store inventory

| State | Classification | Default production behavior | Restore behavior |
| --- | --- | --- | --- |
| Local blob payload files under `blobs/<space>/objects/` | `AUTHORITATIVE_REQUIRED` when local backend is used | Include. | Restore payload files and validate size/checksum against `blob_meta/`. |
| External S3/MinIO payload bytes | `EXTERNAL_DEPENDENCY` | Do not include in Mycel backup set by default. Back up through S3/MinIO backup policy. | Restore planner verifies object-store recovery point and object references before enabling blob reads. |
| External object-reference manifest in backup repository, for example `external-objects/blob-payloads.json` | `EXTERNAL_DEPENDENCY` evidence | Include in the Mycel backup repository. | Validate bucket/prefix/key, size, checksum/digest, ETag/version ID when available, endpoint fingerprint, and backup policy reference. |

### Node artifact inventory evidence

Each node artifact manifest should summarize the classified inventory it captured.
Suggested fields per inventory entry:

```text
NodeArtifactInventoryEntry
  path
  subsystem
  classification
  backup_policy
  included
  restore_behavior
  barrier_ref
  checksum_sha256
  size_bytes
  file_count
  notes
```

For directories, `checksum_sha256` should be a deterministic tree checksum over
included regular files and their relative paths. For excluded paths, the entry
should record `included = false` and the reason. For external object-store
payloads, the inventory should point to the object-reference manifest rather than
including payload bytes.
