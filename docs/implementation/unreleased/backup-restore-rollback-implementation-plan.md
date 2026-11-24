# Backup, restore, and rollback implementation plan

Status: planning  
Last updated: 2026-11-20

This plan breaks the production backup/restore architecture in
`docs/design/production-readiness/backup-restore-rollback.md` into reviewable
phases across `mycel-api`, `mycel`, `mycel-go-sdk`, `mycel-rust-sdk`,
`mycel-console`, and `mycel-lab`.

Each phase should leave the system working. A phase may add inactive APIs,
feature-flagged daemon paths, or hidden Console views, but it must not make the
current supported backup/cluster behavior worse and it must pass the normal CI for
each touched repository.

## Delivery principles

- **Reviewable phases.** Each phase should be small enough to review as one
  architecture/contract slice, even if it lands as several PRs per repository.
- **Working mainline.** Every merged PR must leave the repo buildable and tests
  passing. Cross-repo phases should be ordered so generated SDKs and daemon
  consumers never depend on unpublished API definitions.
- **Feature-gated rollout.** New production backup/restore execution paths remain
  disabled or explicitly marked experimental until their Lab suites and operator
  preflights pass.
- **Additive API evolution until cutover.** Existing path-oriented/single-daemon
  backup APIs may remain while the repository-backed cluster APIs are introduced.
  Removal/deprecation should be a later coordinated release decision.
- **Repository truth over operation state.** The daemon may cache operation state,
  but restore discovery must be based on `backup-set.json` plus matching
  `COMPLETE`.
- **SDKs follow API changes promptly.** Every phase with `mycel-api` protobuf
  changes should include generated Go/Rust SDK updates and basic client-side
  examples/tests.
- **Console follows stable daemon behavior.** Console UI should avoid exposing a
  workflow until the corresponding daemon path is implemented and safe to use.
- **Lab gates production behavior.** Destructive or recovery flows are not
  production-ready until Mycel Lab proves positive, negative, and crash/retry
  cases.

## Cross-repository release order for API-changing phases

For any phase that changes public protobufs or generated types:

1. `mycel-api`: add or update protobufs, generated code, changelog notes, and
   API docs.
2. `mycel-go-sdk` and `mycel-rust-sdk`: regenerate bindings, add ergonomic
   helpers where appropriate, and update compatibility metadata.
3. `mycel`: consume the new generated API and implement daemon behavior behind
   flags or safe defaults.
4. `mycel-console`: expose only implemented daemon workflows; hide incomplete
   flows behind development flags if useful.
5. `mycel-lab`: add or extend suites that exercise the delivered behavior.

A coordinated release should happen only after a phase reaches a coherent,
tested state across the repositories it touches.

## Phase 0: Work breakdown and guardrails

### Goal

Create the issue structure, feature flags, and CI expectations for the work
without changing runtime behavior.

### `mycel-api`

- File issues for the new admin backup API surface:
  - repository resources;
  - policy resources;
  - backup operation state;
  - backup-set catalog/manifest summary;
  - restore planning and restore operations;
  - retention/deletion operations.
- Decide whether the future API evolves the existing
  `mycel.admin.v1.AdminBackupService` or introduces new messages with deprecated
  legacy fields retained for a transition.

### `mycel`

- Add build/feature flags or configuration knobs for experimental production
  backup/restore paths.
- Add an internal package skeleton only if it is useful for follow-up PRs; no
  behavior change.
- Create issues for subsystem owners: quiesce, repository, manifest, restore,
  semantic/derived state, retention, and Lab qualification.

### SDKs

- No generated-code changes yet.
- File SDK tracking issues for generated clients and backup/restore helper APIs.

### `mycel-console`

- File UX/design tracking issues for repository configuration, backup operations,
  backup-set catalog, restore planning, destructive confirmation, and validation
  status.

### `mycel-lab`

- File suite tracking issues and identify which suites are dry-run, destructive,
  chaos, or environment-specific.

### Review gate

- Architecture document remains the source of truth.
- All tracking issues exist with repository ownership and phase labels.
- No runtime behavior changes.

## Phase 1: API contract foundation and SDK generation

### Goal

Introduce the public contract types needed by later phases while keeping daemon
runtime behavior unchanged or returning explicit `Unimplemented`/`FailedPrecondition`
for unimplemented flows.

### `mycel-api`

- Add stable enums/messages for:
  - `BackupRepository` and S3-compatible repository config;
  - `BackupPolicy`, retention classes, capture provider, derived subsystem
    policy, validation options, and quiesce profile;
  - `BackupOperation`, backup states, blockers, cleanup tasks, and node
    artifact status;
  - `BackupSetSummary`, manifest summary, compatibility summary,
    validation summary, and deletion state;
  - `PlanRestore`, restore plan summary, restore target modes, identity
    modes, auth-session policy, degraded derived-state policy, and restore
    operation states;
  - common `SecretReference`, `EncryptionPolicy`, `CapacityEstimate`, and
    compatibility/version records.
- Add service methods as additive API where possible:
  - repository CRUD/validation;
  - policy get/update;
  - deployment backup start/status/list/cancel;
  - backup-set list/get/validate/delete;
  - restore plan/start/status/list/cancel.
- Preserve legacy fields/methods long enough for existing users unless a
  coordinated breaking release explicitly removes them.

### SDKs

- Regenerate Go and Rust bindings.
- Add lightweight helper constants for operation states and restore target modes.
- Add unit tests that compile sample client calls but do not require a daemon
  implementation.

### `mycel`

- Update generated API dependency only if needed for compile coverage.
- Add no-op or explicit unavailable handlers only after the daemon consumes the
  new proto in a later phase.

### `mycel-console`

- No visible UI changes. Optionally update generated/admin client types if the
  Console build depends directly on API artifacts.

### `mycel-lab`

- No behavior changes.

### Review gate

- `mycel-api` compatibility/generation checks pass.
- SDK CI passes with regenerated types.
- No daemon or Console user-visible behavior changes.

## Phase 2: Daemon repository, policy, and operation catalog foundation

### Goal

Persist backup/restore configuration and operation metadata in Mycel system
metadata/system Raft without executing real backups or restores.

### `mycel-api`

- Finalize repository/policy/catalog request/response fields needed for daemon
  implementation.
- Add pagination and validation-result shapes if not covered in phase 1.

### `mycel`

- Implement system metadata storage for:
  - backup repository definitions, excluding raw secrets;
  - backup policy and schedule;
  - active/recent backup operation records;
  - active/recent restore operation records;
  - backup-set catalog pointers and validation evidence;
  - retention/deletion tombstones.
- Implement repository CRUD and policy get/update admin handlers.
- Implement operation idempotency-key hashing and active-operation conflict
  detection, but do not execute capture/restore yet.
- Implement audit records for repository/policy mutations.

### SDKs

- Add typed helpers for repository and policy CRUD calls.
- Add error helpers for active-operation conflicts when the API exposes stable
  `ErrorInfo` reasons.

### `mycel-console`

- Add a hidden or read-only Backup settings foundation:
  - list configured repositories;
  - show effective policy summary;
  - show operation history if available.
- Do not expose start/restore buttons yet.

### `mycel-lab`

- Add lightweight tests for repository/policy admin APIs against a test daemon.
- Add artifact-safety checks ensuring credential fields are redacted.

### Review gate

- Existing daemon behavior is unchanged unless backup admin APIs are called.
- Repository/policy data survives daemon restart and system-Raft replay.
- No raw credentials are accepted or persisted.

## Phase 3: S3-compatible repository validation, credentials, and encryption

### Goal

Implement repository reachability and credential/encryption validation for AWS S3
and MinIO without producing production backup sets.

### `mycel-api`

- Add/complete validation request options and structured validation evidence:
  capability, credential reference, TLS result, KMS/encryption result, sentinel
  object result, and sanitized error category.

### `mycel`

- Implement credential-reference resolvers for first supported schemes:
  - IRSA/service account references for AWS/EKS;
  - Kubernetes Secret or ExternalSecret-backed references for MinIO/self-hosted;
  - dev/Lab environment/file references gated as non-production only;
  - KMS key references and TLS CA references.
- Implement S3-compatible validation probes:
  - write/read/HEAD/list sentinel object;
  - multipart capability when required;
  - server-side encryption metadata validation;
  - KMS decrypt validation;
  - TLS CA/server-name validation for MinIO/private endpoints.
- Add structured redaction for provider errors, URLs, and credential material.
- Add repository validation status caching in system metadata.

### SDKs

- Add repository validation helper methods and examples.
- Add tests that validate redaction-friendly error surfaces.

### `mycel-console`

- Add repository configuration and validation UI:
  - configured refs/fingerprints only;
  - validation status and last sanitized error;
  - no raw credential display or entry unless explicitly dev-only.

### `mycel-lab`

- Add MinIO fixture support for repository validation.
- Add tests proving manifests/log artifacts do not contain raw access keys,
  session tokens, or presigned URL query strings.

### Review gate

- Repository validation works against MinIO in CI/Lab and can be configured for
  AWS/EKS.
- Raw secrets are not written to manifests, operation records, logs, or Lab
  artifacts.
- Existing backup behavior remains available.

## Phase 4: Manifest, inventory, compatibility, and capacity validators

### Goal

Implement validation libraries and planners before wiring them to destructive or
long-running operations.

### `mycel-api`

- Stabilize manifest summary, compatibility summary, capacity estimate, and
  validation-result fields.
- Add explicit required-feature and storage-format reporting where needed.

### `mycel`

- Implement Go structs and validators for:
  - `backup-set.json` schema version 1;
  - `COMPLETE` marker schema;
  - `CheckpointBarrierSet` evidence;
  - node artifact inventory and deterministic tree checksums;
  - derived subsystem policy rows;
  - external object-reference manifest evidence;
  - compatibility matrix and required feature checks;
  - capacity sizing formulas and structured blockers.
- Implement offline/local validation commands or internal test hooks for fixture
  manifests.
- Ensure all validators fail closed for missing required artifacts, unsupported
  features, checksum mismatches, and topology incompatibility.

### SDKs

- Expose manifest/validation summaries through generated types.
- Add optional SDK-side convenience formatting for validation blockers; do not
  duplicate daemon validation logic.

### `mycel-console`

- Add read-only validation result presentation components that can be reused by
  repository, backup-set, and restore screens.

### `mycel-lab`

- Add fixture-based negative tests:
  - missing `COMPLETE`;
  - manifest checksum mismatch;
  - corrupt required artifact;
  - unsupported manifest/required feature;
  - topology mismatch;
  - raw-secret artifact scan.

### Review gate

- Validator test coverage lands before capture/restore execution.
- Validators can be run deterministically from fixture data.
- The daemon still does not mutate cluster state through new backup/restore flows.

## Phase 5: Quiescence hardening and participant model

### Goal

Make quiescence production-grade and explicit before capture code depends on it.

### `mycel-api`

- Add stable quiesce/backup error reasons if needed by clients and Console.
- Add structured participant/blocker fields not already present.

### `mycel`

- Replace method-prefix read allowance with explicit `CONTROL_ONLY` and future
  `STRICT_READ_ONLY` allowlist machinery.
- Implement or harden quiesce participants for graph, session/transaction, blob,
  semantic, lexical, automation, inference, notification/outbox, import/export,
  and activity/audit subsystems.
- Emit stable gRPC errors:
  - `mycel.backup` / `cluster_quiesced`;
  - `transaction_aborted_by_quiesce`.
- Add drain/blocker status, deadlines, and safe diagnostic metadata.
- Ensure quiesce cleanup and stale-lease recovery work after coordinator crash.

### SDKs

- Add helpers for classifying quiesce-related retryable errors where useful.
- Ensure no SDK treats `PermissionDenied`/`NotFound` as quiesce or retry signals.

### `mycel-console`

- Add operation-blocker display components and quiesce status presentation.
- Do not expose manual quiesce controls unless an operator/admin design requires
  them.

### `mycel-lab`

- Add tests for writes rejected during quiescence and control APIs remaining
  available.
- Add crash/stale lease tests where practical.

### Review gate

- Existing tests plus new quiesce tests pass.
- Quiescence can be entered and exited without running a backup.
- Ordinary operations resume after release.

## Phase 6: Cluster backup execution MVP with portable filesystem capture

### Goal

Create completed backup sets in an S3-compatible repository using the portable
`FILESYSTEM_IMMUTABLE_COPY` provider. This is the first end-to-end backup path,
primarily for Lab and non-AWS portability.

### `mycel-api`

- Complete `StartBackup`, `GetBackupOperation`, list/cancel, and backup-set
  list/get/validate fields needed by the MVP.
- Add repository-backed request fields and deprecate/avoid path-only
  `output_dir` semantics for production paths.

### `mycel`

- Implement the backup operation state machine through:
  `PENDING -> PREFLIGHTING -> WAITING_FOR_CLUSTER_CONVERGENCE -> QUIESCING ->
  CHECKPOINTING -> CAPTURING -> RELEASING_QUIESCE -> UPLOADING -> VALIDATING ->
  FINALIZING -> SUCCEEDED`.
- Implement single-active backup/restore conflict handling and idempotent start.
- Implement `FILESYSTEM_IMMUTABLE_COPY` capture with capacity preflight.
- Upload node artifacts, node manifests, inventories, validation reports,
  `backup-set.json`, and `COMPLETE` to S3/MinIO.
- Implement cancel before finalization and fail-closed incomplete prefixes.
- Implement `ValidateBackupSet` against repository truth.

### SDKs

- Add backup start/status/list/cancel/validate helper functions.
- Add polling helpers with cancellation contexts and terminal-state handling.

### `mycel-console`

- Add backup operation UI:
  - start on-demand backup;
  - list/status/progress;
  - blockers/retry hints;
  - validation warnings;
  - copy backup-set ID/URI.
- Gate behind a feature flag until Lab coverage passes.

### `mycel-lab`

- Add MinIO-backed cluster backup suite using the filesystem provider.
- Add missing/corrupt required artifact and missing `COMPLETE` fail-closed tests.
- Add cancellation-before-finalization test.

### Review gate

- A Lab cluster can create and validate a repository-backed backup set.
- No restore path is required yet, but backup-set validation must be strict.
- Existing daemon functionality remains available after backup operations finish.

## Phase 7: Restore planning and new-cluster bootstrap restore

### Goal

Restore a completed backup set into a clean new cluster without supporting
existing-cluster replacement yet.

### `mycel-api`

- Add/complete restore planning and restore operation methods:
  `PlanRestore`, `StartRestore`, status/list/cancel.
- Include identity plan, required secret refs, node mapping, capacity blockers,
  compatibility blockers, and derived-state actions in plan responses.

### `mycel`

- Implement restore planning for `NEW_DEPLOYMENT_BOOTSTRAP` with
  `PRESERVE_BACKUP_ID` only.
- Validate manifest, `COMPLETE`, repository credentials, KMS/decrypt, topology,
  capacity, and required external dependencies before mutation.
- Implement artifact staging, checksum verification, node artifact apply,
  restored-state normalization, post-restore validation, and service admission.
- Persist restore operation state and lineage/audit evidence.
- Reject `ADOPT_TARGET_ID` and `GENERATE_NEW_ID` for raw physical backup sets.

### SDKs

- Add restore plan/start/status/cancel helpers and terminal-state polling.
- Add examples for planning a new-deployment restore.

### `mycel-console`

- Add restore planning UI for new-deployment restore:
  - select backup set;
  - show compatibility/capacity/secret blockers;
  - show identity plan and required node mapping;
  - start only when plan is restorable.

### `mycel-lab`

- Add `k3d-cluster-backup-restore-new` suite.
- Validate auth, graph read/write, search readiness behavior, and lineage after
  restore.

### Review gate

- Lab proves backup -> clean restore -> smoke tests.
- Existing-cluster replacement remains unavailable and explicitly blocked.

## Phase 8: Derived state, semantic/vector completeness, and external object dependencies

### Goal

Bring backup/restore completeness up to the CommonFolio production data boundary,
including semantic durability, required semantic-vector artifacts, optional
lexical/checkpoint/query artifacts, and external object-store references.

### `mycel-api`

- Add any missing derived-state, semantic/vector, and external object dependency
  summary fields to manifest/backup-set/restore-plan messages.
- Ensure degraded derived-state options are explicit and auditable.

### `mycel`

- Implement derived subsystem policy enforcement:
  - `SEMANTIC_VECTOR_SEARCH = INCLUDE_REQUIRED` by default;
  - `LEXICAL_SEARCH`, `GRAPH_CHECKPOINT`, and `PERSISTENT_GRAPH_QUERY_INDEX =
    INCLUDE_OPTIONAL`;
  - `METADATA_ADJACENCY_MEMORY_INDEX = DISCARD`.
- Validate durable semantic/vector records as authoritative required state.
- Include/validate semantic physical vector-search artifacts when required.
- Omit invalid optional artifacts and mark `REBUILD_REQUIRED`.
- Generate and validate external object-reference manifests for S3/MinIO blob
  payloads without copying payload bytes into the Mycel backup set.
- Restore semantic state in the documented order and keep strict semantic reads
  unavailable until readiness validates.

### SDKs

- Add typed helpers or formatting for derived-state actions and degraded restore
  blockers.

### `mycel-console`

- Show derived-state policy, warnings, and restore actions:
  included, rebuild-required, discarded, unavailable until rebuilt.
- Make degraded semantic restore choices explicit and confirmation-gated.

### `mycel-lab`

- Add suites for:
  - required semantic-vector artifact restore;
  - optional lexical/checkpoint/query rebuild;
  - external object-reference validation;
  - degraded derived-state fail-closed behavior.

### Review gate

- A backup set missing required semantic durable state or required semantic-vector
  artifacts fails validation/restore planning.
- Optional derived artifact corruption does not publish invalid artifacts and does
  not block restore when policy allows rebuild.

## Phase 9: Existing-cluster replacement, retention, and deletion

### Goal

Support destructive full-state replacement of an existing cluster and implement
safe retention/deletion lifecycle.

### `mycel-api`

- Complete destructive restore fields:
  destructive confirmation, pre-restore safety backup/waiver,
  source-fencing evidence, restore lock state, `FAILED_REQUIRES_OPERATOR`.
- Complete retention/delete API fields and deletion tombstone summaries.

### `mycel`

- Implement target-wide restore lock stricter than backup quiescence.
- Implement `EXISTING_DEPLOYMENT_REPLACE` planning and start gates:
  elevated authorization, explicit destructive confirmation, compatible topology,
  node/PVC mapping, no active operation, source fencing, required safety backup or
  waiver, and capacity checks.
- Implement wipe/reinitialize or replacement-PVC cutover path.
- Implement post-restore validation before service admission.
- Implement `FAILED_REQUIRES_OPERATOR` and operator recovery behavior.
- Implement retention classes, incomplete-prefix cleanup, manual delete lifecycle,
  deletion tombstones, and repository-config deletion semantics.

### SDKs

- Add destructive restore confirmation helpers that make the dangerous fields
  explicit.
- Add delete/retention helper methods and terminal-state handling for delete
  operations if surfaced as asynchronous operations.

### `mycel-console`

- Add existing-cluster replacement wizard with strong destructive UX:
  - plan review;
  - source-fencing evidence;
  - safety backup/waiver display;
  - typed confirmation or equivalent;
  - unsafe failed-restore state banner.
- Add backup-set retention/pin/delete UI with protected-state explanations.

### `mycel-lab`

- Add `k3d-cluster-backup-existing-replace` suite.
- Add retention/deletion safety suite:
  pinned protection, safety-backup protection, incomplete-prefix cleanup, deletion
  removes `COMPLETE` first, unknown prefixes are not deleted.

### Review gate

- Existing-cluster replacement works only in isolated/destructive Lab scenarios.
- Failed unsafe restore never admits ordinary traffic.
- Retention/deletion cannot delete protected or unknown data by accident.

## Phase 10: CSI/EBS snapshots and asynchronous upload for AWS/CommonFolio shape

### Goal

Implement the first production capture strategy for AWS/CommonFolio: quiesced
local/PVC snapshots with asynchronous upload to S3.

### `mycel-api`

- Add/complete capture-provider evidence fields for CSI VolumeSnapshot, direct
  EBS snapshots if selected, uploader pods, snapshot class, clone/staging PVCs,
  fallback use, and cleanup status.

### `mycel`

- Implement `CSI_VOLUME_SNAPSHOT` provider:
  - preflight snapshot CRDs/controller/classes;
  - create snapshots under quiescence;
  - create temporary clone/staging PVCs where required;
  - release quiescence after immutable sources are proven;
  - upload asynchronously;
  - clean provider resources through durable cleanup tasks.
- Optionally implement `AWS_EBS_SNAPSHOT` only if the Kubernetes CSI path is not
  sufficient for CommonFolio's environment.
- Add provider quota/capacity checks and provider evidence in manifests.
- Harden crash recovery around provider snapshots and uploader/data-mover pods.

### SDKs

- Generated updates only, unless provider evidence helpers are useful.

### `mycel-console`

- Show capture provider, snapshot class, fallback use, upload progress, and
  cleanup warnings in backup operation detail.

### `mycel-lab`

- Add k3d CSI-capable tests when the Lab environment can support snapshots.
- Keep filesystem-copy provider tests as portable fallback.
- Add CommonFolio/AWS environment qualification hooks or artifact collection for
  `staging2`.

### Review gate

- AWS `staging2` or an equivalent production-like environment can produce a
  validated backup set using the selected snapshot provider.
- Quiescence is released after immutable capture source creation, not after full
  upload, for the snapshot provider path.

## Phase 11: Compatibility, rollback, capacity, and chaos hardening

### Goal

Turn the implementation into a robust release-gated production workflow.

### `mycel-api`

- Finalize compatibility/capacity fields based on implementation experience.
- Avoid further breaking changes unless a coordinated release explicitly accepts
  them.

### `mycel`

- Implement daemon capability tables for manifest/storage/derived-artifact
  formats.
- Enforce downgrade refusal at daemon startup when storage formats are too new.
- Persist compatibility audit evidence on restore operations.
- Harden capacity estimates with observed backup/upload/restore timing history.
- Add alert surfaces/metrics for stale backups, failed backups/restores,
  cleanup failures, credential/encryption failures, and unsafe restore locks.

### SDKs

- Add compatibility/capacity formatting helpers if useful to operators.
- Add examples for common backup, validation, restore, and rollback workflows.

### `mycel-console`

- Add compatibility and capacity blocker presentation.
- Add rollback decision display: binary rollback allowed versus restore-from-backup
  required.
- Add alert/unsafe-state indicators.

### `mycel-lab`

- Add chaos and negative suites:
  - coordinator crash before/during quiesce;
  - crash after capture before upload;
  - crash during upload;
  - crash after manifest before `COMPLETE`;
  - crash after `COMPLETE` before operation state update;
  - crash before restore apply;
  - crash after irreversible apply;
  - unsupported version/feature/topology gates;
  - insufficient capacity gates.

### Review gate

- Crash/retry tests demonstrate fail-closed behavior.
- Compatibility and capacity blockers are clear in API, Console, and Lab
  artifacts.

## Phase 12: Production release readiness and coordinated rollout

### Goal

Ship a coordinated release train with proof that the production backup/restore
workflow works in the target environment.

### `mycel-api`

- Tag the final API release for the backup/restore production contract.
- Publish changelog and compatibility notes.

### SDKs

- Release Go and Rust SDK versions generated from the final API.
- Include backup/restore helper documentation and examples.

### `mycel`

- Release daemon image with production feature gates enabled only when policy and
  environment validation pass.
- Publish operator runbooks for backup, restore, rollback, failed restore, and
  retention/deletion.

### `mycel-console`

- Release Console support for repository configuration, backup operations,
  backup-set catalog/validation, restore planning/start, retention/delete, and
  unsafe-state reporting.

### `mycel-lab`

- Add a canonical release-gate suite or extend `cluster-release-gate` to include
  the production backup/restore suites that are safe for the target environment.
- Produce a qualification artifact containing image digests, API/SDK/Lab
  versions, backup-set manifest checksums, restore results, capacity estimates,
  credential/encryption validation, and artifact safety scan results.

### Review gate

- Passing Mycel Lab release-gate evidence exists for the release-candidate image.
- AWS/CommonFolio `staging2` or equivalent production-like qualification evidence
  exists.
- Runbooks and rollback decision tree are reviewed.
- Operators can create, validate, restore, and delete backup sets through API and
  Console without direct pod-local file access.

## Cross-phase acceptance checklist

Every phase should answer these before merge/release:

- Does the current supported system still work if the new feature is unused?
- Are all new public API fields documented and covered by generated SDKs?
- Are raw credentials and customer payload bytes absent from logs, manifests, and
  Lab artifacts?
- Does the phase add tests for its fail-closed behavior, not just happy paths?
- Does Console avoid exposing operations that the daemon cannot safely execute?
- Is operation state resumable after daemon restart where the phase introduces
  long-running work?
- Does Mycel Lab have enough coverage to prevent regression of the phase's safety
  contract?

## Suggested issue breakdown

The phases above should be converted into focused issues rather than one large
implementation ticket. A practical issue split is:

- API: repository/policy resources;
- API: backup operation and backup-set catalog;
- API: restore planning and restore operation;
- daemon: system metadata storage for backup/restore resources;
- daemon: S3/MinIO repository validation and credential references;
- daemon: manifest/checkpoint/path-inventory validators;
- daemon: quiesce hardening;
- daemon: filesystem-copy backup execution;
- daemon: new-deployment restore;
- daemon: semantic/derived/external-object completeness;
- daemon: existing-cluster replacement;
- daemon: retention/deletion/janitor;
- daemon: CSI/EBS snapshot provider and uploader;
- Console: repository/policy UI;
- Console: backup operations and catalog UI;
- Console: restore planning/replacement UI;
- Lab: repository validation and artifact safety;
- Lab: backup/restore positive suites;
- Lab: negative/fail-closed suites;
- Lab: crash/resume and release-gate suites;
- SDKs: generated clients and helper APIs for each API release slice.
