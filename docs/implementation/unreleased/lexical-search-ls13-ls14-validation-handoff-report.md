# Lexical search LS13-LS14 validation and handoff report

Date: 2026-09-06

Branches: `lexical_search` across `mycel`, `mycel-api`, `mycel-go-sdk`, `mycel-rust-sdk`, and `mycel-console`.

## Scope

This report closes the initial lexical search implementation tranche after LS9-LS12 integration work.

Completed release-readiness scope:

- Daemon Search API adapter and admin maintenance adapter.
- CLI lexical search/status/rebuild commands.
- Console lexical search page and Tauri command bindings.
- Operator/user documentation for query syntax, freshness, status, rebuild, troubleshooting, and limitations.
- Cross-repo changelog entries.
- SDK/API generation validation.

## Validation evidence

### `mycel-api`

```sh
make format
make test
git diff --check
```

Result: passed.

Coverage:

- protobuf formatting;
- `buf lint`;
- `buf format --diff --exit-code`;
- whitespace check.

### `mycel-go-sdk`

```sh
MYCEL_API_ROOT=../mycel-api make test
git diff --check
```

Result: passed.

Coverage:

- regenerated Go protobuf/gRPC bindings from the sibling lexical API checkout;
- `go test ./...`;
- whitespace check.

### `mycel-rust-sdk`

The Rust SDK generator uses `third_party/mycel-api` by default. The submodule pointer was updated to the lexical API commit before generation.

```sh
git -C third_party/mycel-api fetch ../../../mycel-api lexical_search
git -C third_party/mycel-api checkout FETCH_HEAD
make generate
cargo fmt --check
make ci
git diff --check
```

Result: passed.

Coverage:

- regenerated committed Rust `prost`/`tonic` bindings;
- `cargo fmt --check`;
- `cargo test`;
- `cargo build`;
- whitespace check.

### `mycel`

```sh
make docs-check
git diff --check
go test ./internal/search/lexical/... ./internal/daemon/api/client ./internal/daemon/api/admin ./internal/daemon/server ./internal/daemon/app ./internal/cli/cmd
make test
```

Result: passed.

Long-running gate follow-up:

```sh
make test-phase-d
go test ./internal/automation/service -run TestClusteredGraphAutomationOutputRetriesGraphConflict -count=50
make test-k3s-cluster
make test-k3s-system-backup-restore
make test-k3s-raft-disruption-smoke
make test-k3s-raft-disruption-edges
make test-cluster-raft-sensitive-gate
make test-cluster-release-gate
```

Results: passed.

Reliability fixes applied during follow-up:

- `scripts/testK3sCluster.sh` now applies `imagePullPolicy: IfNotPresent` by default for locally imported K3s images, avoiding `ImagePullBackOff` on tags that only exist in the local k3d image store.
- Lossless graph notification delivery now retries transient gRPC `Unavailable`, `DeadlineExceeded`, and `ResourceExhausted` consumer errors before recording delivery failure, which covers short raft no-leader/leader-forwarding windows for automation graph-change consumers.
- Raft-sensitive package phases D-G now use `go test -p $(RAFT_TEST_PACKAGE_PARALLELISM)` with a default of `1`, reducing host scheduler contention that caused false raft proposal timeout/no-leader flakes under release-gate load.

Coverage:

- lexical analyzer/parser/storage/index/service tests;
- daemon client/admin API package tests;
- daemon server/app package tests;
- CLI command tests;
- docs checks;
- daemon-only and public-surface checks through `make test`;
- full `go test ./...`.

### `mycel-console`

```sh
cargo check --manifest-path src-tauri/Cargo.toml
npm test -- --runInBand
npm run build
cargo test --manifest-path src-tauri/Cargo.toml
git diff --check
```

Result: passed.

Coverage:

- Tauri/Rust command compilation against Rust SDK lexical clients;
- frontend typecheck and production build;
- Jest suite;
- Tauri Rust tests;
- whitespace check.

Note: Vite reported the existing non-blocking large chunk warning during `npm run build`.

## Release notes summary

Add to coordinated release notes:

- Added lexical search v1 with a dedicated client `SearchService`, per-space/domain indexes, BM25 scoring, Lucene-style v1 query subset, freshness diagnostics, and follower-forwarding/fail-closed routing seams.
- Added admin lexical maintenance API for rebuild workflows.
- Added CLI commands for lexical search, status, and rebuild.
- Added Console lexical search UI with scope selection, freshness/status display, pagination, diagnostics toggle, and rebuild controls.
- Added Go and Rust SDK generated bindings plus thin service client fields.

## Compatibility and migration notes

- Existing daemon data directories start without lexical index files.
- Lexical indexes are derived local state and can be rebuilt from graph data.
- Logical/export-style backups do not need to include lexical index files.
- Search defaults fail closed for stale indexes unless callers opt in with `allow_stale` and optional `max_revision_lag`.
- Search responses return node IDs and diagnostics, not full node payloads.
- V1 does not support stemming, wildcard/prefix/fuzzy/range/fielded query syntax, snippets/highlighting, blob text extraction, or hybrid reranking.

## Handoff checklist

Before merging/releasing:

1. Ensure all five `lexical_search` branches include their changelog/release-note entries.
2. Ensure `mycel-rust-sdk/third_party/mycel-api` points at the lexical API commit used to generate Rust bindings.
3. Open PRs in dependency order:
   - `mycel-api`;
   - `mycel-go-sdk` and `mycel-rust-sdk` after API review;
   - `mycel` after API/SDK review;
   - `mycel-console` after Rust SDK review.
4. Keep issues open until the feature is released from `main`.
5. Re-run release gates on final PR heads if any generated protobuf or dependency pointers change.

## Residual risks / follow-ups

- Automatic lexical compaction remains deferred; rebuild is the v1 cleanup path for tombstone-heavy indexes.
- Hybrid lexical/semantic/metadata search orchestration remains future work.
- Fielded scoring, snippets/highlighting, and blob text extraction remain future work.
