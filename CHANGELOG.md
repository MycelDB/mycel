# Changelog

All notable changes to mycel should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Go module semantic versioning. Before `v1.0.0`, daemon behavior, CLI flags, and internal implementation details may still evolve, but user/operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

### Added

- Added daemon-authoritative, WAL-replay-safe inference budget scopes for procedure and semantic embedding work, with route-pinned rule-owned scopes, atomic pre-provider attempt reservation, retry/concurrency/batch/token/deadline/duration/versioned-cost enforcement, bounded retention, conservative ambiguous-outcome accounting, authenticated capability discovery, fixed-snapshot immutable attempt pages, and sanitized reconciliation evidence (#239).
- Added daemon-owned procedure-inference attempt/run evidence, typed bounded polling, immutable execution attribution, procedure control barriers, authorization start/publication fences, crash-safe publication reconciliation, and sanitized lifecycle/capacity/retention diagnostics (#239).
- Added generation-scoped semantic record provenance, distinct analyzed/fresh revision frontiers, bounded gap/capacity diagnostics, pinned rebuild with delta catch-up, stale-completion rejection, and Raft-snapshotted reliability state (#238).
- Added integrity-protected direct semantic pagination and authorization-safe hard eligibility filters with bounded candidate post-filter diagnostics (#238).
- Added the opt-in, credential-free `token-basis-v1` fake embedding strategy with exact normalized vectors, finite scripted delays/errors/refusals/wrong-dimension outcomes, bounded concurrency-safe attempt accounting, and explicit process-reset semantics (#237).
- Added system-Raft-authoritative vector-store identity enforcement with immutable UUIDs, required normalized-unique machine keys and display names, UUID-preserving rename, structured conflict details, fail-closed legacy ambiguity, and validated snapshot/restore behavior (#258).

### Changed

- Search now requires channel-owned query text in `lexical.query` and `semantic.query`, rejects continuation tokens from the removed shared-query fallback contract, and binds new tokens only to explicit channel queries. The CLI lexical command populates `lexical.query`; hybrid search uses separate `--lexical-query` and `--semantic-query` flags (#242).
- Non-empty semantic-rule display names are now case-insensitively unique within each space/domain. Admin collisions return `ALREADY_EXISTS`, concurrent storage mutations revalidate the authoritative catalog, and persisted catalogs or restored snapshots containing duplicate normalized names fail closed without changing the protobuf or JSON field shape (#262).
- Admin semantic rules now require enum-only graph-change trigger events in `trigger.events`, reject `UNSPECIFIED` and unknown values, canonicalize trigger sets deterministically, and no longer accept or project legacy string events (#257). This intentionally requires regenerated clients.
- Vector-store package application and catalog updates now use UUID identity, preserve semantic-rule references across display-name changes, reject case/whitespace-normalized collisions atomically, and never select an arbitrary duplicate legacy key (#258).
- Procedure-inference RPCs are additive for existing clients; the new CLI audit/control commands require the matching unreleased API/SDK contract merged through `mycel-api` #43, Go SDK #32, and Rust SDK #31. The daemon release API ref pins the exact merged contract until an approved API tag exists. After procedure-inference/control Raft or snapshot state is written, downgrade requires a supported full backup/restore path and an older daemon must not consume the newer retained state directly.
- Admin inference credentials now enforce catalog endpoint/auth-mode compatibility, accept `auth_type=none` without creating, rotating, or resolving secret material, and reject unknown statuses fail-closed. Credential creation, status changes, API-key rotation, and credential/secret deletion are conditional single WAL/Raft lifecycle mutations with directory-synced pre-write intents that preserve concurrent revocation, reuse secret identity, retain failed local records behind one global/space/maintenance WAL fence, stop Raft application without advancing the applied index on fatal persistence failure, durably retain typed deterministic rejection outcomes across restart with torn-tail repair, fail-closed journal validation, and serialized marker compaction, prevent unsafe snapshot advancement while failed entries remain replayable, rebuild derived projections in reference-safe order, reject deletion of referenced secrets, and prevent endpoint changes from orphaning credentials (#238).
- Semantic search now rejects legacy or incompatible records without matching configuration/route provenance, fails closed when a bounded hard-filter scan cannot prove completeness, and never relabels configured route IDs as provider-observed route evidence (#238).
- Semantic rebuild and publication transitions now use rule-scoped fencing, immutable revision-pinned graph reads, monotonic same-epoch publication ordering, and guarded generation/epoch/state changes, preserving cross-rule concurrency while rejecting stale baseline, completion, and failure updates (#238).
- Cancelled semantic maintenance remains a bounded freshness barrier until explicitly covered by a pinned rebuild; revision zero is preserved as an explicit empty-domain rebuild target (#238).
- Bounded explicit semantic reconciliation and physical-index reconstruction now share the rule publication fence while ordinary refresh/delete paths use per-key latest-record lookup and offset/checksum-journaled, idempotent repair across segment sync, intent promotion, keyed-view, and physical-publication failures; prospectively bounded, length-checked segment recovery prevents corruption-driven allocation, torn-tail overtaking, and intent overflow, exact tombstone provenance prevents newer same-key deletes from being swallowed, ordering is generation/current-epoch aware, and binding paths are collision resistant (#238).
- Graph deletes resolve exact binding/store/source and target-epoch identities including key-only stores, while selector updates or nested-root deletes enqueue cleanup before freshness can advance (#238).
- Semantic analyzer admission now enforces prospective retained-item, gap-interval, revision-span, and serialized-range limits before allocating another reliability barrier; stable IDs/timestamps and claim/retry/completion transitions replicate deterministically, snapshot-derived rebuild state, legacy timestamp fallback, and lease reset preserve deterministic semantic timestamps and durable bytes, active-consumer checkpoints compact dirty events and completed work through durable WAL/Raft mutations, and hard mutation/byte thresholds checkpoint retry-churn logs independently of analyzer availability (#238).
- Inference now rejects empty, non-finite, and declared-dimension-mismatched embedding responses before publication; the historical unconfigured fake connector response remains unchanged (#237).

### Fixed

- Raft-cluster semantic backfill, dirty-work processing, and rebuild requests now execute on the semantic partition leader that owns the authoritative derived vector index used by forwarded search. Routing requires authenticated cluster transport, operation leases revalidate leader and term through publication/completion, rebuild ownership permits safe new-term takeover of abandoned work, and per-node projection leases prevent promoted empty or stale nodes from serving search until a reconciled local baseline with current provenance and authoritative freshness is established. Rebuild also holds the backup gate throughout orchestration; missing or changed leadership fails closed instead of acknowledging locally invisible records.
- Raft-forwarded graph reads now preserve deterministic graph error codes across the internal backend boundary, including `NotFound` for absent entities instead of relabeling absence as `Internal`.
- Semantic-rule enable/disable mutations are now ID-qualified and version-conditional, preserve one replicated update timestamp, reject concurrent identity replacement, and return the exact successfully committed state instead of a request-local cache entry. Conditional mutations use an explicitly versioned WAL/Raft record so unsupported mixed-version replicas fail closed, and deterministic WAL rejections remain recoverable without blocking later progress.

## [v0.20.0] - 2026-10-19

### Added

- Added durable graph transaction commit idempotency using principal/space/domain-scoped keys, canonical mutation binding, original-result replay, bounded retention/capacity, delivery repair, runtime diagnostics, and Raft snapshot preservation (#220).
- Added the transport-neutral logical query model, shared binder, rule-based physical planner, real physical-plan Explain output, and bounded pull-operator runtime used by structured queries and GQL (#213).
- Added native indexed, semantic, traversal, broad, mutation, and script execution with finite work/memory budgets, transaction-overlay visibility, rollback journals, lifecycle revalidation, and request/view-bound cursors (#213).

### Changed

- Replaced subsystem-to-subsystem service coupling with neutral model/runtime contracts and consumer-owned narrow ports for graph, blob, inference, semantic, and automation composition (#195, #196, #197, #198, #199).
- Moved query execution ownership out of the daemon transport adapter so each accepted plan has one native executor while generated protobuf and gRPC dependencies remain at the API boundary (#213).
- Made lexical and hybrid pagination candidate-aware and request-bound, with bounded filtering/hydration and compatible legacy lexical continuation where the source plan remains safe (#206).
- Reused each unique query term's live document frequency during BM25 scoring, removing broad-term quadratic posting-list rescans without changing ranking semantics (#214).

### Fixed

- Made lexical indexing and operator rebuilds domain-revision-aware, revision-pinned, monotonic, and restart-replayable without blocking graph/Raft writes (#205).
- Hardened query execution against unbounded scans, stale or cross-request cursors, partial mutation results, overlay accounting gaps, traversal cycles, and failed-statement write leakage (#213).
- Preserved cancellation-safe committed automation finalization and bounded transaction finalization state while preventing writes after rollback or close (#213).

### Compatibility

- Use `mycel-api`, Go SDK, and Rust SDK v0.20.1 for durable commit idempotency fields, diagnostics, and corrected repeatable-read contract documentation. The v0.20.1 API/SDK patch is wire-compatible with v0.20.0. Existing commits without idempotency keys remain supported.
- Graph Raft snapshots advance to version 2 to retain idempotency state; version 1 remains readable. Older daemons are not expected to read version 2 snapshots, so downgrade requires a compatible pre-upgrade snapshot or rebuild procedure.
- Query compatibility remains governed by the documented QRX0/QRX4 gates. Continuation cursors are accepted only when their source-safe plan family, request, schema, revision, and read view remain compatible.

## [v0.19.4] - 2026-10-13

### Fixed

- Fixed automation Raft replay so stale/superseded invocation updates are treated as no-ops when durable invocation state already dominates the replayed command, preventing restart readiness failures from `stale automation invocation update` (#188, #189).
- Fixed graph Raft replay so superseded stale graph commits repair applied-command markers instead of surfacing replay-time `graph conflict` errors, while preserving live conflict behavior (#188, #189).
- Serialized graph applied-command marker persistence to avoid concurrent marker write temp-file races during replay/idempotency repair (#189).

### Compatibility

- Patch release for the daemon only. Public protobuf/API bindings remain compatible with `mycel-api v0.19.0`; no SDK, Console, or Lab release is required for API compatibility.

## [v0.19.3] - 2026-10-13

### Added

- Added daemon configuration for the hybrid weighted-RRF rank constant via `MYCELD_SEARCH_HYBRID_RRF_K`, with the effective value reported in hybrid-search diagnostics and query plans (#183).

### Changed

- Productized weighted reciprocal-rank-fusion hybrid search as the documented app-facing hybrid retrieval path, including formula, candidate-count guidance, semantic min-score gating behavior, diagnostics, and downstream usage recommendations (#181, #182).

### Fixed

- Fixed semantic and hybrid search result hydration so vector hits are read from the current graph/domain revision rather than graph revision zero, preventing valid search hits from being dropped after indexing (#178, #179, #180).
- Fixed semantic rule backfill and maintenance so per-binding physical search-index state is mirrored into central semantic search-index metadata, keeping rule summaries and search availability aligned with rebuilt `latest.json`/`state.json` files (#184, #185).
- Refreshed semantic rule/index dirty queue counts after maintenance processing completes so rule summaries do not continue to report stale pending work (#184, #185).

### Compatibility

- Patch release for the daemon only. Public protobuf/API bindings remain compatible with `mycel-api v0.19.0`; no SDK, Console, or Lab release is required.

## [v0.19.2] - 2026-10-10

### Fixed

- Fixed identity principal Raft startup replay after restoring a snapshot when the committed tail contains a duplicate-principal application outcome, preventing daemon CrashLoop/startup failure during snapshot replay recovery (#172, #173).

### Changed

- Docker publish now reads the compatible `mycel-api` ref from `.release/mycel-api-ref`, with an optional workflow-dispatch override, so daemon-only patch releases no longer require semantically empty matching API compatibility tags (#174, #175).

### Compatibility

- Patch release for the daemon only. Public protobuf/API bindings remain compatible with `mycel-api v0.19.0`; Docker publish uses `.release/mycel-api-ref` to consume that API ref without requiring a matching API tag.

## [v0.19.1] - 2026-10-10

### Fixed

- Fixed stale persisted graph `meta.automation` from being treated as an active automation output fence during ordinary graph updates, preventing completed/expired automation metadata from blocking later user or client writes (#166, #167).
- Restricted automation output fence validation to explicitly marked automation-output mutations while preserving fail-closed validation for active automation writes (#167).

### Compatibility

- Patch release for the daemon only. No public protobuf/API, CLI, SDK, Console, or Lab changes are required beyond updating the daemon binary/container image.

## [v0.19.0] - 2026-10-09

### Changed

- Unified space access enforcement around `Space.OwnerID` plus identity scoped roles/capabilities, so space/domain visibility and data-plane access now use the same identity grant model exposed by admin principal APIs (#160).
- Canonicalized legacy `space.owner` role requests to `space.admin`; actual ownership remains represented only by the space owner field (#160).

### Removed

- Removed the legacy space-local ACL implementation, storage path, raft/WAL grant commands, snapshot ACL state, and daemon `GrantSpacePrincipal` adapter now that the API contract no longer exposes that RPC (#160).

### Compatibility

- Operators and clients should use matching `mycel-api`/SDK v0.19.0 bindings for the removed admin space grant RPC.
- Existing `Space.OwnerID` ownership continues to grant owner-derived access. Existing legacy `meta/access.json` space ACL grants are no longer read or enforced; migrate delegated access to identity scoped role/capability grants.

## [v0.18.0] - 2026-10-07

### Added

- Added asynchronous cluster backup operations matching `mycel-api` v0.18.0, including operation start/status/cancel flow, readiness blockers, waitable CLI support, and durable operation state (#146).
- Added offline cluster system-backup restore CLI primitives, including restore planning and local restore execution with fail-closed archive, manifest, identity, and raft metadata validation (#156).
- Added persistent daemon access-token signing secrets with file and inline environment configuration so access tokens survive daemon restarts and can be verified across clustered nodes (#150).
- Added read-only transaction graph snapshot reads pinned to `GraphTransaction.BaseRevision`, including historical node/edge reads, structured scans, hierarchy, adjacency, subtree traversal, and raft read barriers (#157).

### Changed

- Delegated cluster/system reliability validation make targets to canonical Mycel Lab suites and documented the native Compose/k3d validation, backup/restore, restart soak, and release-gate flows.
- Changed backup quiescence so daemon-owned cluster backup operations honor the configured read-admission policy while still blocking writes and unknown methods during backup (#155).
- Changed semantic dirty-event and graph checkpoint background writers to enter runtime mutation gates, reducing the risk of untracked durable writes during cluster backup (#154).

### Fixed

- Fixed raw tar user backup auto-compression behavior (#147).
- Fixed same-raft-ID PVC rejoin snapshot reporting when a node rejoins from empty storage (#149).
- Excluded transient `exports/` data from system backup snapshots (#154).

### Compatibility

- Operators and clients should use matching `mycel-api`/SDK v0.18.0 bindings for the async cluster backup API contract.
- Read-only transactions now provide repeatable graph reads at their begin-time base revision instead of current-read behavior. Long-running read-only transactions may retain additional in-memory historical graph versions until bounded snapshot-retention/compaction is introduced.
- Cluster restore remains an explicit offline operator workflow; restore CLI commands do not contact daemon login paths and require safe, empty target data directories by default.
- Multi-node access-token verification now requires shared daemon signing secret material via the configured signing-secret file or inline base64 secret.

## [v0.17.0] - 2026-09-30

### Added

- Added advanced-storage graph durability and observability surfaces, including domain graph checkpoint/index status reporting compatible with `mycel-api` v0.17.0.
- Added personal data export support and raft restart/write soak coverage used to validate clustered durability.

### Changed

- Hardened raft restart and disruption behavior so unadmitted local-node forwarding states return retryable `Unavailable` while true cluster-ID mismatches remain `PermissionDenied`.
- Pinned Docker publish workflow actions to full commit SHAs.

### Fixed

- Fixed clustered bootstrap admin identity divergence by deriving a deterministic bootstrap admin principal ID across fresh nodes (#126).
- Fixed graph Raft replay idempotency when graph effects exist but applied-command markers are missing.
- Fixed graph notification initialization to fail closed when its data directory is unset (#122).
- Updated Bash scripts to use Bash `[[ ... ]]` conditionals where Bash shebangs are used (#124).

### Compatibility

- Operators should use matching `mycel-api`/SDK v0.17.0 bindings for the new graph checkpoint/index status fields.
- Fresh clustered deployments now bootstrap the configured admin username to the same principal ID on every node. Existing clusters with already-diverged bootstrap admin principal IDs require reset or explicit repair; the change prevents recurrence for new data directories.

## [v0.16.1] - 2026-09-24

### Changed

- Moved lexical graph-change indexing off the synchronous graph commit path. Lexical indexing now consumes durable graph-change notifications asynchronously and replays retained history from the indexed graph revision after restart (#94).

### Fixed

- Made lexical graph-change replay idempotent for duplicate/replayed revisions.
- Closed graph-change notification registrations cleanly during daemon shutdown so background delivery cannot continue after runtime teardown.

### Compatibility

- No protobuf/API or SDK changes. This daemon patch release uses the same API contract as v0.16.0.

## [v0.16.0] - 2026-09-24

### Added

- Added the server-side graph `replace_references` operation for transaction-scoped reference edge reconciliation, matching `mycel-api`/SDK v0.16.0 (#93).
- Added graph write tracing and benchmarking support used to diagnose Commonfolio update latency (#91).

### Changed

- Reduced graph write latency by moving semantic dirty marker processing off the user-facing graph Raft commit path (#89).
- Reduced Raft write amplification by appending entries to the new `entries.log` raft storage format instead of rewriting the full legacy `entries.pb` file on each append (#95).

### Fixed

- Fixed multi-second AddNode/UpdateNode latency observed in production-like Commonfolio graph writes by combining async semantic maintenance, batched caller writes, server-side reference replacement, and faster raft entry appends (#84, #92).
- Populated stable transaction IDs for raft-applied graph dirty events so async semantic maintenance can process replicated graph changes safely.

### Compatibility

- New v0.16.0 daemons read legacy `entries.pb` raft logs and write the append-only `entries.log` format after startup. Older daemon versions are not expected to read `entries.log`; downgrade only from a pre-upgrade PVC snapshot or after following the raft storage downgrade guidance in the operator docs.
- Clients using server-side reference replacement should use matching `mycel-api` and SDK v0.16.0 bindings.

## [v0.15.0] - 2026-09-18

### Added

- Added graph procedure and automation-binding `put` commands for file-based authoring (#60).
- Consolidated procedure and binding management under the canonical `mycel automation` namespace (#61).
- Added server-backed validation for graph procedures and automation bindings (#62).
- Added `image_analysis` support for graph automation procedure inference operations (#63).
- Added runnable image attachment summary graph automation examples and operator documentation (#77).

### Changed

- Cleaned up legacy and ambiguous automation CLI command surfaces while preserving compatibility where needed (#65).
- Improved clustered readiness gating so client readiness waits for Raft write readiness in clustered deployments.

### Fixed

- Passed image blob payloads through to image-analysis graph automations (#74).
- Made the OpenAI-compatible chat connector honor `output_token_parameter=max_completion_tokens` metadata while preserving the default `max_tokens` request shape (#76).

## [v0.13.0] - 2026-09-12

### Added

- Added `object_store` blob backend configuration with first-class S3-compatible provider settings for AWS S3, MinIO, and LocalStack while retaining legacy `s3` aliases (#48).
- Made the local compose cluster start MinIO and use it as the default object-store blob backend for Mycel daemons (#48).

### Fixed

- Cleaned up production magic numbers by moving operational knobs into configuration and replacing storage/protocol literals with named constants (#46).

## [v0.12.0] - 2026-09-09

### Added

- Added hybrid search design/implementation planning and internal weighted reciprocal-rank fusion primitives for lexical + semantic search orchestration.

### Fixed

- Reduced K3s raft disruption gate flakes by retrying transient committed-read probes and treating restarted pod process availability, not Kubernetes readiness, as the active-disruption restart barrier (#42).

## [v0.11.1] - 2026-09-09

### Added

- Added third-party notices and license inventory for release compliance.

### Removed

- Removed deprecated root CLI commands: `mycel init`, `mycel acl`, and `mycel accounting`.

### Fixed

- Fixed raft backend transport churn by reusing pooled per-peer gRPC connections and bounding raft send timeouts.
- Made compose cluster release gates self-contained under `tests/compose/cluster`.
- Fixed K3s PVC replacement recovery so a same-node-ID raft member with empty persistent storage catches up instead of remaining stale after rejoin.

## [v0.11.0] - 2026-09-07

### Added

- Added lexical search v1: per-space/domain BM25 indexes, `SearchService` gRPC endpoints, freshness diagnostics, leader-owned indexing/forwarding seams, CLI search/status/rebuild commands, and operator documentation.

### Fixed

- Reduced raft-heavy release gate flakes by retrying transient lossless graph notification delivery failures and serializing raft-sensitive package phases by default.

## [v0.10.0] - 2026-09-04

### Added

- Added S3-backed blob payload storage for new blob uploads, with local storage remaining the default and existing local blobs remaining local (#6).
- Added REPL line editing and command history, with sensitive command filtering (#12).
- Added REPL paste-friendly command handling for semicolon-terminated commands, continuation lines, and pasted multi-command blocks (#5).

### Changed

- Moved product-facing query roadmap pages out of repository docs and replaced them with public-manual pointers (#14).
- Improved standalone daemon health semantics so a ready standalone node is not marked unhealthy solely because cluster admission does not apply (#17).

### Fixed

- Fixed `space` subcommands when run inside the REPL (#7).
- Fixed a backup scheduler retry-after state race after transient backup failures (#9).

## [v0.9.0] - 2026-08-31

### Added

- First public-release baseline for the MycelDB daemon and CLI.
- Open-source project documentation: README links, contributing guide, security policy, code of conduct, changelog, pull request template, issue templates, and gitleaks false-positive baseline.
- Operator-facing daemon/CLI coverage for spaces, domains, graph/query workflows, semantic/inference setup, automation, activity, backups, and raft/cluster reliability.

### Changed

- Documented daemon-only repository boundaries and SDK/API consumption guidance.
- Documented release validation expectations for daemon-only checks, public Go surface checks, docs checks, tests, builds, and cluster-sensitive gates.

## Release notes policy

For each release, add a dated section such as:

```md
## [v0.9.0] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
```

Include notes for daemon behavior, CLI flags, configuration, storage formats, raft/clustering behavior, backup/restore behavior, authentication/authorization, public protobuf/API changes, migration requirements, and matching `mycel-api`/SDK versions.
