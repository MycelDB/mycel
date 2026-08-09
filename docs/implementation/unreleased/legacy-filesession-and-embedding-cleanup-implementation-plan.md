# Legacy File-Session and Embedding Migration Cleanup Implementation Plan

## Status

Cleanup plan for `refactor_for_performance` and later release lines. LC1-LC8
are implemented using the non-breaking API compatibility-stub path: legacy
file-session/session API code and legacy embedding migration internals are
removed, while the public admin migration RPC remains as a closed-window
compatibility surface until a future breaking API branch removes it.

## Context

mycel is now daemon-owned and subsystem-oriented. Active graph writes should flow
through daemon services and raft/WAL-owned subsystem storage, and active semantic
configuration should use the semantic/inference resource model. Two legacy areas
remain as compatibility/migration scaffolding:

1. `internal/graph/filesession` plus `internal/session/api`, the old file-backed
   graph session abstraction.
2. Legacy embedding profile/provider-key migration support, centered on
   `internal/embedding/store` and `internal/semantic/migration/legacy_embeddings.go`.

Both areas are internal implementation code, but they still affect tests,
internal semantic readers, and public/admin migration surfaces. Removal must be
staged and validated carefully.

## Goals

- Remove legacy file-session implementation after replacing all active runtime
  dependencies with subsystem-native readers/services.
- Remove legacy embedding profile/provider-key migration support after the
  migration window is closed.
- Preserve raft/WAL authority and fail-closed behavior during removal.
- Avoid automatic repair, restore, merge, rebalance, PVC repair, or
  authoritative-node selection.
- Keep each phase functional and independently testable.
- Update docs and tests so removed components are not referenced as current
  behavior.

## Non-goals

- No generated API/SDK deletion until a breaking API cleanup is explicitly
  approved across `mycel-api`, SDKs, and downstream clients.
- No automatic data migration or background cleanup job.
- No replacement of raft storage in this cleanup.
- No removal of current semantic/inference catalog, connector, source assembly,
  or vector-store code unless it is proven to be legacy-only.

## Current audit

### File-session packages and files

Status: removed in LC5 after semantic readers and metadata-index tests moved off
legacy session APIs.

| Path | Former role | Removal posture |
| --- | --- | --- |
| `internal/graph/filesession/file_session.go` | File-backed graph session core: node/edge CRUD and graph apply behavior. | Removed. |
| `internal/graph/filesession/transaction.go` | File-session transaction staging/commit helpers. | Removed. |
| `internal/graph/filesession/file_hierarchy.go` | Legacy hierarchy helpers over file-session graph records. | Removed. |
| `internal/graph/filesession/file_blob.go` | Legacy blob node helpers tied to session API. | Removed as legacy-only behavior. |
| `internal/graph/filesession/metadata.go` | File-session metadata/tag/property index wiring. | Removed after `metadataindex` no longer imported `session/api` types. |
| `internal/graph/filesession/advanced_semantic.go` | File-session semantic source helpers. | Removed after semantic backfill/analyzer readers stopped depending on file sessions. |
| `internal/graph/filesession/*_test.go` | Coverage for legacy file-session behavior. | Deleted; metadata-index behavior was ported to `internal/graph/metadataindex/index_test.go`. |
| `internal/session/api/types.go` | Legacy internal session interface and DTOs consumed mostly by file-session and semantic backfill. | Removed. |

### File-session external dependencies

Pre-LC2/LC3 imports outside `internal/graph/filesession`:

| Consumer | Dependency | Current use | Replacement direction |
| --- | --- | --- | --- |
| `internal/semantic/service/module.go` | `internal/graph/filesession`, `internal/session/api` | `graphReader()` returns a file-session `sessionapi.Session` for semantic analyzer/backfill reads. | Replace with semantic-specific graph read adapter backed by graph subsystem storage/service. |
| `internal/semantic/backfill/types.go` | `internal/session/api` | `Runner.Session` provides `ListNodes` and `ListEdges`. | Replace with a narrow `GraphSourceReader` interface containing only needed read methods. |
| `internal/semantic/backfill/runner.go` | `Runner.Session` | Reads all nodes/edges for source selection and assembly. | Use `GraphSourceReader` supplied by semantic service; tests can use an in-memory fake. |
| `internal/semantic/backfill/runner_test.go` | `filesession`, `session/api` | Test fixture builds graph data through file-session writes. | Replace with in-memory/source-reader fixture or graph storage/service fixture. |
| `internal/graph/metadataindex/index.go` | `internal/session/api` | Uses session API DTOs for tag/property query inputs and summaries. | Move DTOs into `internal/graph/metadataindex` or a graph-owned internal query package. |

LC2/LC3 replace these dependencies with domain-scoped narrow graph readers:

```go
type GraphReader interface {
    GetNode(ctx context.Context, domainID graph.DomainID, id graph.NodeID) (graph.Node, error)
    Parent(ctx context.Context, domainID graph.DomainID, childID graph.NodeID) (*graph.Edge, error)
}

type GraphSourceReader interface {
    ListNodes(ctx context.Context, domainID graph.DomainID) ([]graph.Node, error)
    ListEdges(ctx context.Context, domainID graph.DomainID) ([]graph.Edge, error)
}
```

This narrower contract is the main removal seam.

### Legacy embedding migration packages and files

Status: internal legacy migration code removed in LC7; public API/CLI wrappers
remain only as closed-window compatibility surfaces.

| Path / surface | Former/current role | Removal posture |
| --- | --- | --- |
| `internal/embedding/store` | Minimal legacy embedding key/profile reader retained for migration. | Removed. |
| `internal/semantic/migration/legacy_embeddings.go` | Converted legacy embedding keys/profiles into semantic endpoints, credentials, grants, indexes, and policies. | Removed. |
| `internal/semantic/service.Module.MigrateLegacyEmbeddings` | Semantic service entry point for migration. | Removed. |
| `internal/daemon/api/admin/semantic_migration_service.go` | Admin gRPC server for legacy migration. | Kept as a closed-window compatibility RPC. |
| `internal/cli/cmd/semantic_migrate.go` | `mycel semantic migrate legacy-embeddings`. | Kept as a deprecated compatibility CLI that surfaces the daemon error. |
| `internal/cli/cmd/admin_inference_test.go` migration coverage | Tests legacy migration command/API. | Updated to expect the closed-window error. |
| `internal/clustering/consensus/raft_record_coverage_test.go` entries `embedding.provider_key.*` | Documented legacy provider-key records as unsupported/fail-closed in raft daemon mode. | Removed after legacy store/records were deleted. |
| `docs/design/admin/semantic-migration.md` | Current admin migration docs. | Updated to document closed-window compatibility behavior. |
| Generated admin migration protobuf in `internal/gen/...` | Generated from public API contract. | Do not edit manually; update only after `mycel-api` removal is approved and regenerated. |

### Legacy embedding dependencies to keep unless separately audited

The following are not automatically removed by this cleanup because they are used
by active semantic code:

- `internal/embedding/source.go` — source assembly used by semantic backfill.
- `internal/embedding/catalog` and `internal/embedding/domain` — catalog/domain
  types still used by semantic/inference provisioning.
- `internal/embedding/provider` — provider compatibility behavior may still be
  active; audit separately before removal.

## Phase LC1 — Lock the removal criteria

Status: completed for `refactor_for_performance` with a conservative
compatibility decision.

Decision:

1. The legacy embedding migration window is not treated as closed for this
   branch until release notes explicitly announce closure. Do not delete
   migration internals in LC1-LC5.
2. `AdminSemanticMigrationService.MigrateLegacyEmbeddings` is a public admin API
   surface. Removing it is a breaking API change and requires a coordinated
   `mycel-api` contract update plus SDK regeneration.
3. For the next cleanup tranche, keep the public API/service definitions and, if
   the migration window is closed before a breaking API release is prepared,
   return a clear closed-window/deprecated error for one release instead of
   deleting the service immediately. Actual API removal is deferred to LC6 on an
   explicit breaking-API branch.
4. File-session cleanup continued independently through LC5. The migration
   window was then closed using the non-breaking LC6 compatibility-stub path:
   public generated API stays in place, the daemon returns a closed-window
   error, and LC7 deletes the internal legacy reader/migration code.

Downstream audit as of LC1:

| Repo | Impact |
| --- | --- |
| `mycel` | CLI command and admin gRPC registration remain as a closed-window compatibility surface; semantic service migration entry point, legacy embedding store, migration internals, success-path migration test fixture, and `embedding.provider_key.*` raft coverage references were removed. |
| `mycel-api` | Defines `mycel.admin.v1.AdminSemanticMigrationService` and `MigrateLegacyEmbeddings`; removal is breaking. |
| `mycel-go-sdk` | Generated admin migration client and top-level `Client.SemanticMigration` field expose the API. |
| `mycel-rust-sdk` | Vendored proto and admin client expose the API. |
| `mycel-admin` | No direct migration command/UI invocation found in the LC1 audit. |
| `mycel-bench` | No migration usage found in the LC1 audit. |
| Knot PKM repos | No `semantic migrate legacy-embeddings` or `MigrateLegacyEmbeddings` usage found in the LC1 audit. |

Acceptance:

- Written decision exists for API removal timing.
- No embedding migration code removal starts until public compatibility decision
  is explicit for the target release.

## Phase LC2 — Replace semantic file-session reads with narrow graph readers

Status: implemented on `refactor_for_performance`.

1. Add a semantic graph read adapter that implements:
   - analyzer `GetNode`/`Parent`;
   - backfill `ListNodes`/`ListEdges`.
2. Back the adapter with graph subsystem storage/service rather than
   `internal/graph/filesession`.
3. Ensure reads respect current consistency expectations for the caller:
   - strong/read-index semantics where raft mode requires it;
   - local committed storage only for apply/internal contexts that are already
     on the authoritative path.
4. Preserve domain scoping and hierarchy semantics currently expected by
   analyzer/backfill.

Candidate adapter shape:

```go
type GraphSourceReader interface {
    GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
    Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error)
    ListNodes(ctx context.Context) ([]graph.Node, error)
    ListEdges(ctx context.Context) ([]graph.Edge, error)
}
```

Acceptance:

- `internal/semantic/service/module.go` no longer imports
  `internal/graph/filesession` or `internal/session/api`.
- Semantic analyzer and backfill tests pass with the new reader.
- Small and large synthetic Logseq tests still pass.

## Phase LC3 — Remove `session/api` from semantic backfill and metadata index

Status: implemented on `refactor_for_performance`.

1. Change `internal/semantic/backfill.Runner` from `sessionapi.Session` to a
   narrow semantic-owned graph source reader.
2. Replace `internal/semantic/backfill/runner_test.go` file-session fixtures with
   in-memory graph reader fixtures or graph subsystem fixtures.
3. Move metadata index DTOs out of `internal/session/api`:
   - `TagSummary`;
   - `PropertySummary`;
   - tag match mode;
   - property query operator/input.
4. Update `internal/graph/metadataindex` to use graph-owned types.

Acceptance:

- `rg "internal/session/api|sessionapi\." internal --glob '!internal/graph/filesession/**'`
  returns no non-file-session runtime uses.
- `go test ./internal/semantic/backfill ./internal/graph/metadataindex ./internal/semantic/...` passes.

## Phase LC4 — Port or delete file-session-only tests

Status: implemented on `refactor_for_performance`.

Review every test in `internal/graph/filesession` and classify it:

| Test area | Keep by porting? | Target replacement |
| --- | --- | --- |
| node/edge CRUD | Keep only if not already covered. | `internal/graph/service` or `internal/graph/storage`. |
| transaction staging/rollback | Keep if behavior exists in current session transaction service. | `internal/session/service` and daemon client transaction tests. |
| hierarchy move/cycle/order | Keep if not covered after adjacency work. | `internal/graph/service` hierarchy tests. |
| blob node helpers | Keep if current blob+graph behavior depends on it. | `internal/blob/service` plus graph service tests. |
| metadata/tag/property index | Keep metadata index behavior. | `internal/graph/metadataindex` tests with graph-owned DTOs. |
| legacy payload/props compatibility | Keep only current storage compatibility. | `internal/graph/storage/codec_test.go` and graph model helpers. |

Acceptance:

- No unique current behavior is lost without replacement coverage.
- File-session tests are either ported or deleted with rationale in commit notes.

## Phase LC5 — Delete file-session implementation

Status: implemented on `refactor_for_performance`.

After LC2-LC4 pass:

1. Delete `internal/graph/filesession`.
2. Delete `internal/session/api` if no imports remain.
3. Update daemon/runtime docs that mention file-session scaffolding.
4. Add a check to prevent reintroduction if appropriate:

```sh
rg "internal/graph/filesession|internal/session/api" internal
```

Acceptance:

- `go test ./internal/graph/... ./internal/semantic/... ./internal/session/...` passes.
- `MYCEL_API_ROOT=../mycel-api make test` passes.
- `make docs-check` and `git diff --check` pass.

## Phase LC6 — Deprecate or remove legacy embedding migration surface

Status: implemented on `refactor_for_performance` using the non-breaking
compatibility-stub path.

If API removal is not yet approved:

1. Keep public API but return a clear deprecation/closed-window error.
2. Update CLI help/docs to point users to semantic/inference configuration.
3. Leave generated code unchanged.

If API removal is approved:

1. Remove `AdminSemanticMigrationService` from `mycel-api` proto in a coordinated
   API branch.
2. Regenerate public generated SDK/API code as part of the API contract change.
3. Update mycel server registration to remove the service.
4. Remove CLI command `semantic migrate legacy-embeddings`.
5. Remove mycel tests that seed legacy embedding profiles/keys.
6. Remove docs from current admin semantic docs or move to historical release
   notes.

Acceptance:

- Public API compatibility decision is reflected in docs/release notes.
- `mycel-api`, Go SDK, Rust SDK, admin UI, and mycel are aligned if API removal
  occurs.

## Phase LC7 — Delete legacy embedding migration internals

Status: implemented on `refactor_for_performance` after LC6 switched the public
RPC to a closed-window compatibility error.

After LC6:

1. Delete `internal/semantic/migration/legacy_embeddings.go`.
2. Delete `internal/embedding/store` if no other imports remain.
3. Remove legacy `embedding.provider_key.*` entries from raft record coverage if
   no corresponding records exist.
4. Update `docs/design/semantic/embedding-package.md` and related implementation
   plans to stop describing migration-only stores as current behavior.

Acceptance:

- `rg "MigrateLegacyEmbeddings|legacy-embeddings|internal/embedding/store|storeembedding|embedding.provider_key" internal docs` shows only historical docs or no hits, according to release policy.
- `MYCEL_API_ROOT=../mycel-api make test` passes.
- `make docs-check` and `git diff --check` pass.

## Phase LC8 — Full validation and downstream alignment

Status: implemented for the non-breaking API path; no generated API/SDK changes
were required.

Run:

```sh
MYCEL_API_ROOT=../mycel-api make test
make docs-check
git diff --check
MYCEL_RUN_LARGE_LOGSEQ_IMPORT_TEST=1 \
MYCEL_LOGSEQ_IMPORT_TIMEOUT_SECONDS=900 \
go test ./internal/daemon/app \
  -run TestSyntheticLogseqDatastoreImportWithSemanticMaintenance \
  -count=1 \
  -timeout=20m
```

If API removal was performed, also validate coordinated repos:

```sh
# in mycel-api
make test

# in mycel-go-sdk
make test

# in mycel-rust-sdk
cargo test --workspace
```

Acceptance:

- Full mycel suite passes.
- Large synthetic Logseq import remains green.
- Downstream SDK/API/admin repos are aligned if public API changed.

## Rollback strategy

- File-session removal should be split from legacy embedding migration removal so
  either cleanup can be reverted independently.
- Public API removal must be coordinated in its own branch/PR and can be reverted
  separately from internal cleanup.
- No data repair or automatic migration should run as part of rollback.

## Resolved LC questions

1. `AdminSemanticMigrationService` remains in the public API until an explicit
   breaking API branch removes it. The migration window is closed in mycel using
   this compatibility path, and the RPC returns a clear closed-window error.
2. Metadata tag/property DTOs now live under `internal/graph/metadataindex`.
3. Semantic backfill and maintenance now use graph service read APIs through a
   semantic-owned adapter, preserving domain scoping and avoiding direct
   file-session reads.
4. The LC1 downstream audit found no Knot PKM invocation of `semantic migrate
   legacy-embeddings`.
