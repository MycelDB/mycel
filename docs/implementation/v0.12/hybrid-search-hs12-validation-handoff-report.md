# Hybrid Search HS12 Validation and Handoff Report

## Status

Prepared on branch `feature/hybrid-lexical-semantic-search` after implementing HS0-HS12 groundwork across the coordinated MycelDB repos.

Tracking issue: [MycelDB/mycel#27](https://github.com/MycelDB/mycel/issues/27)

Design source: [Hybrid search design](../../design/search/hybrid-search.md)

Implementation plan: [Hybrid search implementation plan](hybrid-search-implementation-plan.md)

## Scope completed

### `mycel-api`

- Extended `mycel.client.v1.SearchService` API contracts for hybrid search:
  - `SEARCH_MODE_HYBRID`;
  - `SEARCH_SCORE_KIND_HYBRID_FUSED`;
  - `HybridSearchOptions`;
  - `HybridFusionStrategy`;
  - `LexicalSearchOptions`;
  - `SemanticSearchOptions`;
  - structured metadata `SearchFilters`;
  - `PropertyFilter` and `FilterOperator`;
  - `SearchResultSource` and `SearchResultSourceKind`.
- Updated API changelog.

### `mycel`

- Added design and implementation docs.
- Added internal hybrid search primitives:
  - weight normalization;
  - candidate-count defaulting/capping;
  - weighted reciprocal-rank fusion;
  - candidate deduplication;
  - deterministic tie-breaking;
  - hard metadata filter evaluation.
- Extended daemon `SearchService.Search` with `SEARCH_MODE_HYBRID`:
  - lexical candidate retrieval;
  - semantic candidate retrieval;
  - safe lexical-only degradation warnings when semantic search is not configured or no semantic rule is available;
  - `require_both` handling;
  - graph-node load/authorization before filter evaluation;
  - metadata hard filters;
  - fused score/source diagnostics;
  - explicit v1 rejection of hybrid `page_token`.
- Extended CLI:
  - `mycel search hybrid`;
  - lexical/semantic weights;
  - candidate counts;
  - semantic rule/binding selection;
  - semantic min score;
  - metadata label/node/property filters;
  - diagnostics rendering.
- Updated user-facing search operations docs and changelog.

### `mycel-go-sdk`

- Regenerated committed Go protobuf/gRPC bindings from the hybrid API branch.
- Added a compile-style test constructing a hybrid `SearchRequest`.
- Updated changelog.

### `mycel-rust-sdk`

- Regenerated committed Rust `prost`/`tonic` bindings from the hybrid API branch.
- Added a compile-style test constructing a hybrid `SearchRequest`.
- Updated changelog.

### `mycel-console`

- Updated Tauri lexical search bridge so the existing search command can issue lexical or hybrid requests.
- Added bridge support for:
  - hybrid mode;
  - weights;
  - candidate counts;
  - semantic rule/binding scope;
  - semantic min score;
  - structured filters;
  - result source diagnostics.
- Updated Search UI with:
  - lexical/hybrid mode selector;
  - weight controls;
  - candidate controls;
  - semantic rule/binding fields;
  - required-label and property-filter inputs;
  - source diagnostics display.
- Updated frontend types and changelog.

## Validation performed

### `mycel-api`

```sh
make test
```

Result: passed.

### `mycel`

Focused checks:

```sh
go test ./internal/search/hybrid ./internal/daemon/api/client ./internal/daemon/server ./internal/cli/cmd -count=1
python3 scripts/checkDocs.py
```

Full checks:

```sh
make test
make build
```

Result: passed.

### `mycel-go-sdk`

```sh
MYCEL_API_ROOT=../mycel-api make test
```

Result: passed.

### `mycel-rust-sdk`

```sh
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" make generate
cargo fmt
cargo test
```

Result: passed.

### `mycel-console`

```sh
npm test -- --watch=false
npm run build
cargo test --manifest-path src-tauri/Cargo.toml --no-run
```

Result: passed.

Note: full `cargo test --manifest-path src-tauri/Cargo.toml` was not used as the final validation because the Tauri test binary did not complete within the tool window after building. The compile/no-run validation completed successfully and verifies the Rust bridge builds against the regenerated SDK.

### Long-running Mycel gates

```sh
make test-cluster-release-gate
make test-compose-user-backup-restore
make test-cluster-soak
```

Result: passed.

```sh
make test-cluster-raft-sensitive-gate
make test-k3s-raft-disruption
```

Result: failed in the K3s raft disruption harness with transient disruption/readiness symptoms while final graph counts converged where the scenario reached final convergence.

Observed failures:

- `test-cluster-raft-sensitive-gate` passed the smoke profile, then failed `test-k3s-raft-disruption-edges` with `mixed committed read checks failed: 1`. The failed read event was transient: `rpc error: code = Unavailable desc = raft partition group 2 has no leader`. Final client and pod counts converged to `nodes=2704 edges=1352`.
- A rerun of `test-k3s-raft-disruption-edges` reproduced one transient read failure: `rpc error: code = Canceled desc = grpc: the client connection is closing`. Final client and pod counts converged to `nodes=2510 edges=1255`.
- Standalone `test-k3s-raft-disruption` failed with `timed out waiting for restarted pod myceld-0`; failure artifacts showed `myceld-0` running but not ready while `myceld-1` and `myceld-2` were ready.

Artifact locations:

- `artifacts/raft-disruption/20260909-132901-mycel-rdt-20260909-132901`
- `artifacts/raft-disruption/20260909-133312-mycel-rdt-20260909-133312`
- `artifacts/raft-disruption/20260909-133923-mycel-rdt-20260909-133923`

The failing disruption targets are not hybrid-search-specific, but they remain release-gate risks that need separate raft harness/recovery follow-up before treating the raft-sensitive long gate as green.

## Behavior notes

- Hybrid search remains unary/non-streaming.
- Existing lexical behavior is preserved for `SEARCH_MODE_UNSPECIFIED` and `SEARCH_MODE_LEXICAL`.
- Metadata filters are hard filters only and do not affect score.
- Fused scores use weighted reciprocal-rank fusion, not raw-score summation.
- Hybrid pagination is intentionally not supported in v1; `page_token` returns invalid argument.
- If semantic search is not configured or no semantic searchable rule exists, default hybrid mode returns lexical-only results with a warning unless `require_both=true`.
- Runtime semantic search failures still return errors unless explicitly classified as safe unavailability.

## Remaining risks / follow-up candidates

- Add deeper daemon service tests with fake semantic manager responses covering successful semantic-only and mixed-source results. Current coverage includes mapper/degradation helpers and pure fusion/filter unit tests.
- Add an end-to-end daemon fixture with an actual semantic rule/binding and lexical index once a lightweight semantic provider fixture is available.
- Consider a richer Console filter builder before exposing this as polished UX; the current UI uses compact text fields for labels and property filters.
- Decide whether `require_both` should remain in the first public API surface or be reserved before release.
- Decide whether no semantic rule should degrade to lexical-only or fail by default before release.

## Suggested merge order

1. Merge `mycel-api` API branch into `develop`.
2. Merge `mycel` implementation branch into `develop` after CI validates against the matching API branch or after `mycel-api` develop has the API changes.
3. Merge `mycel-go-sdk` regenerated bindings branch into `develop`.
4. Merge `mycel-rust-sdk` regenerated bindings branch into `develop`.
5. Merge `mycel-console` branch into `develop` after the Rust SDK develop branch is available or the path dependency points at a local matching checkout in CI.
6. Mark `mycel#27` as `status:fixed-in-develop` after all intended release repos have merged.

## Release handoff

Recommended coordinated release version: next minor release after `v0.11.1`, for example `v0.12.0`, because hybrid search adds public API and user-visible functionality.
