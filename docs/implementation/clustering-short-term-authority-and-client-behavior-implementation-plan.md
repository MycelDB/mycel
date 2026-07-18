# Clustering Short-Term Authority and Client Behavior Implementation Plan

## Status

Implementation plan for `clustering-short-term-authority-and-client-behavior.md`.

## Objective

Make the short-term static-primary cluster model enforceable and observable across daemon write paths, CLI/SDK clients, and `mycel-admin`.

The target behavior is:

```text
standalone node        -> accepts local writes
clustered primary      -> accepts clustered writes
clustered follower     -> rejects client/operator clustered writes
clustered unadmitted   -> rejects client/operator clustered writes
```

Followers remain useful for reads, status, diagnostics, local auth sessions, cluster join/admission persistence, and future replication apply.

## Implementation principles

- Centralize authority decisions in daemon runtime helpers.
- Keep module code simple: mutation entry points call a common guard.
- Do not block internal recovery/WAL applier paths.
- Do not block local operational state required for join, topology, sessions, diagnostics, or future replication.
- Return stable gRPC codes for write rejection.
- Add primary hints where practical, but do not require full write forwarding.
- Keep sessions node-local until cluster-wide auth is designed and implemented.

## Existing implementation baseline

Already implemented:

- cluster authority persistence and local role derivation
- `Runtime.RequireWriteAuthority()`
- cluster membership `AddClusterNode` requires primary
- space module write path calls the write-authority guard
- `mycel-admin` shows primary/role information in cluster UI

Still needed:

- classify all durable mutation paths
- guard remaining cluster-authoritative writes
- add primary-hint error metadata
- improve client/UI error handling
- add tests for follower/unadmitted rejection across modules
- document node-local session behavior in user/admin docs

## Phase 1: Mutation classification inventory

Create a durable write classification table in the design docs or as a companion audit file.

For each mutation path, classify as one of:

| Classification | Meaning |
| --- | --- |
| `standalone-or-primary` | Client/operator durable cluster mutation. Allowed on standalone and clustered primary only. |
| `local-node-only` | Local runtime or operational state. Allowed on any node. Not cluster-authoritative. |
| `read-only-safe-anywhere` | Read/query/status operation. Allowed on any node, subject to authz. |
| `replication-apply-only-on-follower` | Future replicated apply path. May mutate follower state only from primary-originated WAL. |

Initial expected classification:

| Area | Mutations | Classification |
| --- | --- | --- |
| Cluster membership | add node / issue join token | `standalone-or-primary` in clustered mode; already guarded |
| Authority | future promotion/change authority | explicit authority protocol only |
| Spaces/domains/templates | create/update/delete/grant/import | `standalone-or-primary`; already guarded through space write path |
| Graph commits | commit graph/document/template data | `standalone-or-primary` |
| Blob metadata | put/delete metadata | `standalone-or-primary` |
| User/admin accounts | create/update/delete credentials, roles, bootstrap mutations after startup | `standalone-or-primary` |
| Sessions/login | create/refresh/revoke local sessions | `local-node-only` short term |
| Semantic config/provider keys | create/update/delete provider config/secrets | `standalone-or-primary` |
| Semantic indexing/accounting/job state | durable index/accounting/backfill state | `standalone-or-primary`; follower compute must be ephemeral only |
| Backup policy/delete | update/delete policy, destructive backup metadata mutations | `standalone-or-primary` |
| Backup trigger | decide explicitly: likely primary-only for cluster-authoritative backups, possibly local-only for node diagnostics |
| Health/status/topology reads | read-only | `read-only-safe-anywhere` |
| WAL recovery/appliers | local apply of committed records | internal only, not guarded by client write authority |

Deliverables:

- Add the classification table to the short-term design doc or a new audit doc.
- Link the classification table from this implementation plan.

## Phase 2: Central authority error helper and primary hints

Extend runtime authority helpers so all modules return consistent errors.

### Required behavior

Clustered unadmitted write attempt:

```text
gRPC code: PermissionDenied
message:   local node is not admitted to a cluster
```

Clustered follower write attempt:

```text
gRPC code: FailedPrecondition
message:   node is not cluster primary
```

### Primary hint metadata

Where authority is known, include primary hints as gRPC metadata/trailers or a structured error detail:

- `mycel-primary-node-id`
- `mycel-primary-node-name`
- `mycel-primary-admin-addr`, if known
- `mycel-primary-backend-addr`, if known
- `mycel-authority-epoch`

Implementation options:

1. gRPC trailers/headers for immediate compatibility.
2. `google.rpc.ErrorInfo` details for richer SDK support.
3. Both, if not too invasive.

Recommended first step: metadata keys, because CLI and Tauri clients can inspect them without changing protobuf response shapes.

Deliverables:

- Add helper in `internal/daemon/runtime` for authority rejection errors.
- Update `RequireWriteAuthority()` to use the helper.
- Add unit tests for codes/messages and metadata when primary is known.

## Phase 3: Guard remaining daemon mutation modules

Apply `Runtime.RequireWriteAuthority()` or a module-local wrapper to all `standalone-or-primary` mutation entry points.

### Important rule

Guard public/client/operator mutation entry points, not WAL applier methods.

WAL appliers, recovery paths, and future replication apply paths must remain able to mutate local state from committed records.

### Candidate modules

#### Admin/user modules

Paths to review:

- admin creation/update/delete
- operator/admin credential mutation
- user creation/update/delete
- password/role/permission mutation

Expected policy: `standalone-or-primary`.

Session creation remains `local-node-only` for now.

#### Blob module

Paths to review:

- blob metadata put
- blob metadata delete
- blob lifecycle metadata changes

Expected policy: `standalone-or-primary` for authoritative metadata.

Raw local cache/file cleanup can remain local-only if it does not change cluster-visible metadata.

#### Graph module

Paths to review:

- graph commits
- document/template graph writes
- schema/domain graph mutations if not already covered by space module

Expected policy: `standalone-or-primary`.

Reads and queries remain allowed on followers.

#### Semantic module

Paths to review:

- provider config/key mutations
- semantic global/space config mutations
- accounting mutations
- maintenance/backfill durable state
- index metadata writes

Expected policy: `standalone-or-primary` for durable cluster state.

Ephemeral compute/cache warming may remain local-only, but must not mark authoritative completion or update accounting.

#### Backup module

Paths to review:

- policy update/delete
- backup metadata deletion
- destructive backup operations
- backup trigger behavior

Expected policy:

- policy/delete: `standalone-or-primary`
- trigger: decide and document. Conservative first implementation should require primary for cluster-visible backups.
- status/list files: read-only anywhere.

Deliverables:

- Guard each selected mutation entry point.
- Add module tests for at least one guarded mutation per module.
- Ensure applier/recovery tests still pass.

## Phase 4: Tests and shared test helpers

Avoid duplicated setup by adding helper utilities for authority scenarios.

Suggested helper capabilities:

- create standalone runtime
- create clustered primary runtime
- create clustered unadmitted runtime
- create clustered admitted follower runtime with authority pointing elsewhere
- initialize target module with a runtime

Test expectations for each guarded module:

| Scenario | Expected result |
| --- | --- |
| standalone | write succeeds |
| clustered primary | write succeeds |
| clustered follower | `FailedPrecondition` |
| clustered unadmitted | `PermissionDenied` |

For complex modules, one representative mutation is enough initially, plus confidence that all mutations share the guarded entry path.

Deliverables:

- Shared test helper package or local helper functions.
- Tests for each guarded module.
- Full `go test ./internal/...` passing.

## Phase 5: Client and UI error handling

Improve user-facing behavior when writes are attempted against followers.

### CLI

For cluster-aware commands and other write commands:

- detect `FailedPrecondition` with `node is not cluster primary`
- print a clear message
- print primary hint if present
- suggest reconnecting to the primary

Example:

```text
write rejected: connected daemon is a follower
primary: node-a (127.0.0.1:9093), authority epoch 1
retry with --daemon-addr 127.0.0.1:9093
```

### SDKs

Expose enough structured error information for callers to detect follower rejection.

Short-term:

- helper predicate: `IsNotPrimary(err)`
- helper extraction: `PrimaryHintFromError(err)` if metadata exists

Do not implement automatic write retry yet unless scoped separately.

### mycel-admin

When a write action fails because the connected daemon is a follower:

- show a clear error banner/toast
- display current primary when known
- suggest reconnecting to the primary
- disable obvious write actions on known followers where practical

Examples:

- Add Node modal should be disabled or show primary-only notice on follower.
- Future create/update/delete actions should show primary-only notices.

Deliverables:

- CLI error formatting for not-primary errors.
- SDK helper methods or documented error metadata behavior.
- `mycel-admin` primary-only UX for cluster membership actions.

## Phase 6: Session policy documentation and tests

Document and test node-local session behavior.

Short-term rule:

- login/session creation is allowed on followers
- sessions are node-local
- follower sessions can be used for follower reads
- follower sessions do not allow follower writes
- tokens may not be valid on other daemons

Deliverables:

- Add docs note to admin/operator auth documentation.
- Add tests showing login/session creation on a follower is not blocked by write authority.
- Add tests showing a follower-created session still receives write rejection for guarded writes.

## Phase 7: Manual end-to-end validation

Validate the short-term cluster behavior manually or with an e2e script.

Scenario:

1. Start bootstrap `node-a`.
2. Confirm `node-a` is primary in CLI and `mycel-admin`.
3. Add `node-b` from `node-a` and start it with join token.
4. Confirm `node-b` is follower.
5. Login to `node-b`.
6. Run read/status commands against `node-b`; they should succeed.
7. Attempt guarded write against `node-b`; it should fail with not-primary.
8. Repeat the write against `node-a`; it should succeed.
9. Confirm UI shows primary role in General and Topology views.

Deliverables:

- Manual validation notes in the design/status doc.
- Optional script under `scripts/` for repeatable dev validation.

## Validation commands

Run after each phase as appropriate:

```bash
cd mycel
go test ./internal/...
```

For proto/API changes, also run:

```bash
cd mycel
./scripts/generate-proto.sh

cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
```

For SDK/UI changes:

```bash
cd mycel-go-sdk
go test ./...

cd ../mycel-rust-sdk
cargo check -p mycel-proto
cargo check -p mycel-sdk

cd ../mycel-admin/src-tauri
cargo check

cd ..
npm test -- --runInBand
npm run build
```

## Acceptance criteria

This plan is complete when:

- all client/operator durable cluster mutations are classified
- all `standalone-or-primary` mutations are guarded
- followers can still perform reads, status, login/session creation, and local operational tasks
- WAL appliers/recovery paths are not blocked by authority guards
- follower write rejection returns stable codes/messages
- primary hints are available where practical
- CLI/SDK/UI can present clear not-primary errors
- tests cover standalone, primary, follower, and unadmitted scenarios for representative guarded modules
- full validation passes

## Deferred work

The following belong to later architecture phases and should not be implemented as part of this plan:

- WAL streaming replication
- transparent follower write forwarding
- shared cluster token signing
- automatic leader election
- manual promotion/fencing protocol
- read consistency APIs such as `after_lsn` or linearizable reads
